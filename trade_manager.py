#!/usr/bin/env python3
"""
trade_manager.py - Enhanced paper trading with improved signal tracking and profitability
"""

import logging
import asyncio
import aiohttp
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List

from telegram.ext import Application
from shared.file_io import safe_load, safe_save
from alerts.user_manager import UserManager
from config import OVERLAP_FILE, PORTFOLIOS_FILE, VALID_GRADES, USE_SUPABASE, BUCKET_NAME
import joblib

try:
    from supabase_utils import upload_file
except ImportError:
    upload_file = None

logger = logging.getLogger(__name__)

class PortfolioManager:
    """Manages virtual portfolios with enhanced position tracking."""

    def __init__(self, portfolio_file: Path):
        self.file = portfolio_file
        self.portfolios = safe_load(self.file, {})
        self._migrate_old_portfolios()
        logger.info(f"📈 PortfolioManager initialized with {len(self.portfolios)} portfolios.")
    
    def _migrate_old_portfolios(self):
        """Migrate old portfolio format to new format."""
        migrated = False
        
        for chat_id, portfolio in self.portfolios.items():
            # Add missing top-level keys
            if "reentry_candidates" not in portfolio:
                portfolio["reentry_candidates"] = {}
                migrated = True
            
            if "blacklist" not in portfolio:
                portfolio["blacklist"] = {}
                migrated = True
            
            if "pending_signals" not in portfolio:
                portfolio["pending_signals"] = {}
                migrated = True
            
            # Migrate pending signals to use epoch tracking
            for mint, signal in portfolio.get("pending_signals", {}).items():
                if "epochs" not in signal:
                    signal["epochs"] = []
                    signal["current_epoch_start"] = signal.get("signal_time")
                    signal["current_epoch_checks"] = signal.get("validation_checks", 0)
                    signal["current_epoch_passes"] = signal.get("validation_passes", 0)
                    migrated = True
            
            if "stats" not in portfolio:
                portfolio["stats"] = {
                    "total_trades": len(portfolio.get("trade_history", [])),
                    "wins": sum(1 for t in portfolio.get("trade_history", []) if t.get("pnl_usd", 0) > 0),
                    "losses": sum(1 for t in portfolio.get("trade_history", []) if t.get("pnl_usd", 0) <= 0),
                    "total_pnl": sum(t.get("pnl_usd", 0) for t in portfolio.get("trade_history", [])),
                    "best_trade": max([t.get("pnl_percent", 0) for t in portfolio.get("trade_history", [])] or [0]),
                    "worst_trade": min([t.get("pnl_percent", 0) for t in portfolio.get("trade_history", [])] or [0]),
                    "reentry_trades": 0,
                    "reentry_wins": 0
                }
                migrated = True
            
            if "last_pnl_update" not in portfolio:
                portfolio["last_pnl_update"] = None
                migrated = True
            
            # Migrate position objects
            for mint, pos in portfolio.get("positions", {}).items():
                if "name" not in pos:
                    pos["name"] = pos.get("symbol", "Unknown")
                    migrated = True
                
                if "signal_price" not in pos:
                    pos["signal_price"] = pos.get("entry_price", 0)
                    migrated = True
                
                if "entry_reason" not in pos:
                    pos["entry_reason"] = "Legacy Entry"
                    migrated = True
                
                if "partial_exits" not in pos:
                    pos["partial_exits"] = []
                    migrated = True
                
                if "remaining_percentage" not in pos:
                    pos["remaining_percentage"] = 100.0
                    migrated = True
                
                if "locked_profit_usd" not in pos:
                    pos["locked_profit_usd"] = 0.0
                    migrated = True
                
                if "last_pnl_milestone" not in pos:
                    pos["last_pnl_milestone"] = 0
                    migrated = True
            
            # Migrate watchlist objects
            for mint, item in portfolio.get("watchlist", {}).items():
                if "signal_liquidity" not in item:
                    item["signal_liquidity"] = 0
                    migrated = True
                
                if "highest_price" not in item:
                    item["highest_price"] = item.get("signal_price", 0)
                    migrated = True
                
                if "lowest_price" not in item:
                    item["lowest_price"] = item.get("signal_price", 0)
                    migrated = True
                
                if "entry_attempts" not in item:
                    item["entry_attempts"] = 0
                    migrated = True
                
                if "max_wait_minutes" not in item:
                    # UPDATED to 10 minutes as per new logic
                    item["max_wait_minutes"] = 10
                    migrated = True
                
                if "validation_passes" not in item:
                    item["validation_passes"] = 0
                    migrated = True
                
                if "validation_fails" not in item:
                    item["validation_fails"] = 0
                    migrated = True
                
                # Track price history for better momentum calculation
                if "price_history" not in item:
                    item["price_history"] = []
                    migrated = True
        
        if migrated:
            self.save()
            logger.info("✅ Successfully migrated old portfolio format to new format")

    def _sync_to_supabase(self):
        """Uploads the portfolios file to Supabase Storage."""
        if not USE_SUPABASE or not upload_file:
            return
        
        remote_path = f"paper_trade/{self.file.name}"
        try:
            if upload_file(str(self.file), bucket=BUCKET_NAME, remote_path=remote_path, debug=False):
                logger.info(f"☁️ Synced portfolios to Supabase: {remote_path}")
            else:
                logger.warning("☁️ Portfolio sync to Supabase failed.")
        except Exception as e:
            logger.exception(f"☁️ Exception during portfolio sync to Supabase: {e}")

    def save(self):
        """Save portfolios to disk and cloud."""
        safe_save(self.file, self.portfolios)
        self._sync_to_supabase()

    def get_portfolio(self, chat_id: str) -> Dict[str, Any]:
        """Get or create a portfolio for a user."""
        chat_id = str(chat_id)
        if chat_id not in self.portfolios:
            self.portfolios[chat_id] = {
                "capital_usd": 1000.0,
                "positions": {},
                "watchlist": {},
                "pending_signals": {},  # Tokens being evaluated
                "trade_history": [],
                "reentry_candidates": {},
                "blacklist": {},
                "stats": {
                    "total_trades": 0,
                    "wins": 0,
                    "losses": 0,
                    "total_pnl": 0.0,
                    "best_trade": 0.0,
                    "worst_trade": 0.0,
                    "reentry_trades": 0,
                    "reentry_wins": 0
                },
                "last_pnl_update": None
            }
        return self.portfolios[chat_id]
    
    def calculate_unrealized_pnl(self, chat_id: str, live_prices: Dict[str, float]) -> Dict[str, Any]:
        """Calculate total unrealized P/L for all open positions."""
        portfolio = self.get_portfolio(chat_id)
        positions = portfolio.get("positions", {})
        
        if not positions:
            return {
                "total_unrealized_usd": 0.0,
                "total_unrealized_pct": 0.0,
                "position_count": 0,
                "positions_detail": []
            }
        
        total_current_value = 0.0
        total_cost_basis = 0.0
        positions_detail = []
        
        for mint, pos in positions.items():
            if pos.get("status") != "active":
                continue
            
            current_price = live_prices.get(mint, pos.get("entry_price", 0))
            current_value = pos["token_amount"] * current_price
            cost_basis = pos["investment_usd"] * (pos["remaining_percentage"] / 100.0)
            
            unrealized_pnl = current_value - cost_basis
            unrealized_pct = (unrealized_pnl / cost_basis) * 100 if cost_basis > 0 else 0
            
            total_current_value += current_value
            total_cost_basis += cost_basis
            
            positions_detail.append({
                "symbol": pos["symbol"],
                "mint": mint,
                "current_price": current_price,
                "entry_price": pos["entry_price"],
                "peak_price": pos.get("peak_price", current_price),
                "current_value": current_value,
                "cost_basis": cost_basis,
                "unrealized_pnl_usd": unrealized_pnl,
                "unrealized_pnl_pct": unrealized_pct,
                "locked_profit_usd": pos.get("locked_profit_usd", 0),
                "remaining_pct": pos.get("remaining_percentage", 100)
            })
        
        total_unrealized = total_current_value - total_cost_basis
        total_unrealized_pct = (total_unrealized / total_cost_basis) * 100 if total_cost_basis > 0 else 0
        
        return {
            "total_unrealized_usd": total_unrealized,
            "total_unrealized_pct": total_unrealized_pct,
            "total_current_value": total_current_value,
            "total_cost_basis": total_cost_basis,
            "position_count": len(positions_detail),
            "positions_detail": sorted(positions_detail, key=lambda x: x["unrealized_pnl_pct"], reverse=True)
        }
    
    async def send_pnl_update(self, app: Application, chat_id: str, pnl_data: Dict[str, Any], 
                             trigger_reason: str = "periodic"):
        """Send unrealized P/L update to user."""
        portfolio = self.get_portfolio(chat_id)
        
        if pnl_data["position_count"] == 0:
            return
        
        total_pnl = pnl_data["total_unrealized_usd"]
        total_pct = pnl_data["total_unrealized_pct"]
        pnl_symbol = "🟢" if total_pnl >= 0 else "🔴"
        
        msg = f"{pnl_symbol} <b>UNREALIZED P/L UPDATE</b>\n\n"
        msg += f"<b>Open Positions:</b> {pnl_data['position_count']}\n"
        msg += f"<b>Total Value:</b> ${pnl_data['total_current_value']:,.2f}\n"
        msg += f"<b>Cost Basis:</b> ${pnl_data['total_cost_basis']:,.2f}\n"
        msg += f"<b>Unrealized P/L:</b> ${total_pnl:,.2f} ({total_pct:+.1f}%)\n\n"
        
        positions = pnl_data["positions_detail"][:5]
        msg += "<b>Positions:</b>\n"
        
        for pos in positions:
            pos_symbol = "🟢" if pos["unrealized_pnl_usd"] >= 0 else "🔴"
            locked_note = f" | 💰${pos['locked_profit_usd']:.0f}" if pos["locked_profit_usd"] > 0 else ""
            remaining_note = f" ({pos['remaining_pct']:.0f}%)" if pos["remaining_pct"] < 100 else ""
            
            msg += (f"\n{pos_symbol} <b>{pos['symbol']}</b>{remaining_note}\n"
                   f"   Entry: ${pos['entry_price']:.6f} → Now: ${pos['current_price']:.6f}\n"
                   f"   P/L: ${pos['unrealized_pnl_usd']:,.2f} ({pos['unrealized_pnl_pct']:+.1f}%){locked_note}\n")
        
        if len(pnl_data["positions_detail"]) > 5:
            msg += f"\n<i>...and {len(pnl_data['positions_detail']) - 5} more</i>\n"
        
        msg += f"\n<i>Available Capital: ${portfolio['capital_usd']:,.2f}</i>"
        
        try:
            await app.bot.send_message(chat_id=chat_id, text=msg, parse_mode="HTML")
            portfolio["last_pnl_update"] = datetime.utcnow().isoformat() + "Z"
            self.save()
            logger.info(f"📊 [{chat_id}] Sent P/L update: {total_pct:+.1f}% ({trigger_reason})")
        except Exception as e:
            logger.error(f"Failed to send P/L update to {chat_id}: {e}")

    def set_capital(self, chat_id: str, capital: float):
        """Set the starting capital for a user."""
        portfolio = self.get_portfolio(chat_id)
        portfolio["capital_usd"] = capital
        self.save()
        logger.info(f"💰 Set capital for {chat_id} to ${capital:,.2f}")

    async def add_to_pending_signals(self, chat_id: str, token_info: Dict[str, Any]):
        """Add a newly detected token to pending signals for epoch-based evaluation."""
        portfolio = self.get_portfolio(chat_id)
        mint = token_info['mint']

        # Skip if already in positions, watchlist, or blacklisted
        if mint in portfolio["positions"] or mint in portfolio["watchlist"] or mint in portfolio.get("blacklist", {}):
            return

        # Load signal price from overlap_results.pkl
        overlap_data = joblib.load(OVERLAP_FILE)
        history = overlap_data.get(mint, [])
        if not history or not isinstance(history[-1], dict):
            logger.warning(f"No valid history found for {mint} in overlap_results.pkl")
            return

        latest_entry = history[-1]
        signal_price = latest_entry.get("dexscreener", {}).get("current_price_usd")
        if signal_price is None:
            logger.warning(f"No signal price found for {mint} in overlap_results.pkl")
            return

        current_time = datetime.utcnow().isoformat() + "Z"

        portfolio["pending_signals"][mint] = {
            "signal_price": signal_price,
            "signal_time": current_time,
            "symbol": token_info['symbol'],
            "name": token_info['name'],
            "signal_liquidity": token_info.get('liquidity', 0),
            "last_check_time": current_time,
            "max_evaluation_minutes": 30,
            # Epoch tracking (6 epochs of 5 minutes each)
            "epochs": [],  # List of completed epochs with their stats
            "current_epoch_start": current_time,
            "current_epoch_checks": 0,
            "current_epoch_passes": 0,
            "current_epoch_number": 1
        }
        self.save()
        logger.info(f"🔍 [{chat_id}] Added {token_info['symbol']} to pending signals for epoch-based evaluation with signal price ${signal_price:.6f}")

    async def add_to_watchlist(self, chat_id: str, token_info: Dict[str, Any]):
        """Add a token to watchlist after passing epoch validation."""
        portfolio = self.get_portfolio(chat_id)
        mint = token_info['mint']

        if mint in portfolio["positions"] or mint in portfolio["watchlist"]:
            return

        portfolio["watchlist"][mint] = {
            "signal_price": token_info['price'],
            "signal_time": datetime.utcnow().isoformat() + "Z",
            "watchlist_added_time": datetime.utcnow().isoformat() + "Z",
            "symbol": token_info['symbol'],
            "name": token_info['name'],
            "signal_liquidity": token_info.get('liquidity', 0),
            "highest_price": token_info['price'],
            "lowest_price": token_info['price'],
            "entry_attempts": 0,
            "promoted_from_epoch": token_info.get('promoted_from_epoch', 0),
            "epoch_pass_rate": token_info.get('epoch_pass_rate', 0),
            # --- LOGIC CHANGE ---
            # Set max wait to 10 minutes as requested
            "max_wait_minutes": 10,
            "price_history": []  # Track recent prices for momentum analysis
        }
        self.save()
        logger.info(f"👀 [{chat_id}] Added {token_info['symbol']} to watchlist at ${token_info['price']:.6f} "
                   f"(from Epoch {token_info.get('promoted_from_epoch', '?')}) - 10 min window")

    def calculate_short_term_momentum(self, price_history: List[float], lookback: int = 3) -> float:
        """
        Calculate SHORT-TERM momentum from recent price movements.
        
        This is different from long-term momentum. We only look at the last few
        price points to see if the token is CURRENTLY gaining strength.
        
        Args:
            price_history: List of recent prices (oldest to newest)
            lookback: Number of recent prices to compare (default 3)
        
        Returns:
            Momentum percentage (positive = rising NOW, negative = falling NOW)
        """
        if len(price_history) < 2:
            return 0.0
        
        # Use only the most recent prices
        recent_prices = price_history[-min(lookback, len(price_history)):]
        
        if len(recent_prices) < 2:
            return 0.0
        
        # Compare most recent price to average of previous prices
        current = recent_prices[-1]
        previous_avg = sum(recent_prices[:-1]) / len(recent_prices[:-1])
        
        return ((current - previous_avg) / previous_avg) * 100

    async def execute_buy(self, app: Application, chat_id: str, mint: str, 
                         current_price: float, current_liquidity: float, entry_reason: str = "Entry"):
        """
        Execute buy with context-aware validation.
        
        CRITICAL CHANGE: Momentum check is now scenario-specific:
        - Dip Entry: NO momentum check (we WANT to buy dips)
        - Recovery Entry: NO momentum check (recovering from dips)
        - Momentum Entry: YES momentum check (must be rising NOW)
        """
        portfolio = self.get_portfolio(chat_id)
        watch_item = portfolio["watchlist"].get(mint)
        if not watch_item:
            return

        # Check capital FIRST (fail fast if insufficient funds)
        capital = portfolio["capital_usd"]
        
        # Dynamic position sizing
        if capital >= 5000:
            position_pct = 0.08
        elif capital >= 2000:
            position_pct = 0.10
        else:
            position_pct = 0.12
        
        investment_usd = min(capital * position_pct, 150)
        
        if capital < investment_usd:
            logger.warning(f"[{chat_id}] Insufficient capital to buy {watch_item['symbol']}")
            del portfolio["watchlist"][mint]
            self.save()
            return

        # CONTEXT-AWARE MOMENTUM CHECK
        # Only check momentum for "Strong Momentum" entry scenario
        if "Strong Momentum" in entry_reason or "sustained buying" in entry_reason:
            # For momentum entries, we want to see SHORT-TERM upward movement
            price_history = watch_item.get("price_history", [])
            
            if len(price_history) >= 2:
                short_term_momentum = self.calculate_short_term_momentum(price_history, lookback=3)
                
                # Require positive momentum for momentum entries
                if short_term_momentum < 0:
                    logger.info(f"[{chat_id}] Momentum entry blocked for {watch_item['symbol']} "
                               f"(short-term momentum: {short_term_momentum:.2f}%). "
                               f"Keeping in watchlist for reevaluation.")
                    
                    watch_item["last_check_time"] = datetime.utcnow().isoformat() + "Z"
                    watch_item["momentum_fail_count"] = watch_item.get("momentum_fail_count", 0) + 1
                    self.save()
                    return
                else:
                    logger.info(f"[{chat_id}] Momentum entry approved for {watch_item['symbol']} "
                               f"(short-term momentum: +{short_term_momentum:.2f}%)")
        
        # NO momentum check for Dip Entry or Recovery Entry
        # These scenarios are DESIGNED to buy at lower prices
        
        # Execute buy
        portfolio["capital_usd"] -= investment_usd
        token_amount = investment_usd / current_price

        portfolio["positions"][mint] = {
            "symbol": watch_item["symbol"],
            "name": watch_item["name"],
            "entry_price": current_price,
            "entry_time": datetime.utcnow().isoformat() + "Z",
            "entry_liquidity": current_liquidity,
            "signal_price": watch_item["signal_price"],
            "investment_usd": investment_usd,
            "token_amount": token_amount,
            "peak_price": current_price,
            "status": "active",
            "entry_reason": entry_reason,
            "partial_exits": [],
            "remaining_percentage": 100.0,
            "locked_profit_usd": 0.0,
            "last_pnl_milestone": 0
        }
        del portfolio["watchlist"][mint]
        self.save()

        # Calculate momentum for display purposes
        price_vs_signal = ((current_price - watch_item["signal_price"]) / watch_item["signal_price"]) * 100

        msg = (f"✅ <b>PAPER TRADE: BUY</b>\n\n"
               f"<b>Token:</b> {watch_item['name']} (${watch_item['symbol']})\n"
               f"<b>Entry Reason:</b> {entry_reason}\n"
               f"<b>Price:</b> ${current_price:.6f}\n"
               f"<b>vs Signal:</b> {price_vs_signal:+.1f}%\n"
               f"<b>Investment:</b> ${investment_usd:.2f}\n"
               f"<b>Liquidity:</b> ${current_liquidity:,.0f}\n")

        try:
            await app.bot.send_message(chat_id=chat_id, text=msg, parse_mode="HTML")
            logger.info(f"✅ [{chat_id}] BUY EXECUTED: {watch_item['symbol']} at ${current_price:.6f} | {entry_reason}")
        except Exception as e:
            logger.error(f"Failed to send buy confirmation for {chat_id}: {e}")

    async def execute_partial_sell(self, app: Application, chat_id: str, mint: str, 
                                   current_price: float, sell_percentage: float, reason: str):
        """Execute partial sell to lock in profits."""
        portfolio = self.get_portfolio(chat_id)
        position = portfolio["positions"].get(mint)
        if not position or position.get("status") != "active":
            return

        tokens_to_sell = position["token_amount"] * (sell_percentage / 100.0)
        sell_value_usd = tokens_to_sell * current_price
        cost_basis = position["investment_usd"] * (sell_percentage / 100.0)
        partial_pnl = sell_value_usd - cost_basis
        
        portfolio["capital_usd"] += sell_value_usd
        position["token_amount"] -= tokens_to_sell
        position["remaining_percentage"] -= sell_percentage
        position["locked_profit_usd"] += partial_pnl
        
        position["partial_exits"].append({
            "time": datetime.utcnow().isoformat() + "Z",
            "price": current_price,
            "percentage": sell_percentage,
            "value_usd": sell_value_usd,
            "pnl_usd": partial_pnl,
            "reason": reason
        })
        
        self.save()

        pnl_symbol = "🟢" if partial_pnl >= 0 else "🔴"
        pnl_pct = (partial_pnl / cost_basis) * 100
        
        msg = (f"{pnl_symbol} <b>PARTIAL SELL: {sell_percentage:.0f}%</b>\n\n"
               f"<b>Token:</b> {position['symbol']}\n"
               f"<b>Reason:</b> {reason}\n"
               f"<b>Sell Price:</b> ${current_price:,.6f}\n"
               f"<b>P/L:</b> ${partial_pnl:,.2f} ({pnl_pct:+.1f}%)\n"
               f"<b>Remaining:</b> {position['remaining_percentage']:.0f}%\n\n"
               f"<i>Capital: ${portfolio['capital_usd']:,.2f}</i>")
        
        try:
            await app.bot.send_message(chat_id=chat_id, text=msg, parse_mode="HTML")
            logger.info(f"📊 [{chat_id}] PARTIAL SELL {position['symbol']}: {sell_percentage}% at ${current_price:.6f}")
        except Exception as e:
            logger.error(f"Failed to send partial sell notification: {e}")

    async def execute_sell(self, app: Application, chat_id: str, mint: str, 
                          current_price: float, reason: str):
        """Backward compatible wrapper for execute_full_sell."""
        await self.execute_full_sell(app, chat_id, mint, current_price, reason)
    
    async def execute_full_sell(self, app: Application, chat_id: str, mint: str, 
                               current_price: float, reason: str):
        """Execute complete position exit."""
        portfolio = self.get_portfolio(chat_id)
        position = portfolio["positions"].get(mint)
        if not position or position.get("status") != "active":
            return

        remaining_value = position["token_amount"] * current_price
        remaining_cost = position["investment_usd"] * (position["remaining_percentage"] / 100.0)
        final_pnl = remaining_value - remaining_cost
        total_pnl = position["locked_profit_usd"] + final_pnl
        total_pnl_pct = (total_pnl / position["investment_usd"]) * 100

        portfolio["capital_usd"] += remaining_value
        position["status"] = "closed"

        stats = portfolio["stats"]
        stats["total_trades"] += 1
        stats["total_pnl"] += total_pnl
        if total_pnl > 0:
            stats["wins"] += 1
        else:
            stats["losses"] += 1
        stats["best_trade"] = max(stats["best_trade"], total_pnl_pct)
        stats["worst_trade"] = min(stats["worst_trade"], total_pnl_pct)

        entry_time = datetime.fromisoformat(position["entry_time"].rstrip("Z"))
        exit_time = datetime.utcnow()
        hold_duration = exit_time - entry_time
        
        trade_log = {
            **position,
            "exit_price": current_price,
            "exit_time": exit_time.isoformat() + "Z",
            "hold_duration_minutes": int(hold_duration.total_seconds() / 60),
            "total_pnl_usd": total_pnl,
            "total_pnl_percent": total_pnl_pct,
            "exit_reason": reason,
            "peak_profit_pct": ((position["peak_price"] - position["entry_price"]) / position["entry_price"]) * 100
        }
        portfolio["trade_history"].append(trade_log)
        
        should_blacklist = self._should_blacklist_token(trade_log, reason)
        should_watch_reentry = self._should_add_to_reentry(trade_log, reason)
        
        if should_blacklist:
            portfolio["blacklist"][mint] = {
                "reason": reason,
                "blacklisted_at": exit_time.isoformat() + "Z",
                "exit_price": current_price,
                "loss_pct": total_pnl_pct
            }
            logger.info(f"🚫 [{chat_id}] Blacklisted {position['symbol']} - {reason}")
        elif should_watch_reentry:
            portfolio["reentry_candidates"][mint] = {
                "symbol": position["symbol"],
                "name": position["name"],
                "first_exit_price": current_price,
                "first_exit_time": exit_time.isoformat() + "Z",
                "first_exit_reason": reason,
                "peak_price_seen": position["peak_price"],
                "best_pnl_pct": total_pnl_pct,
                "reentry_attempts": 0,
                "expires_at": (exit_time + timedelta(hours=6)).isoformat() + "Z"
            }
            logger.info(f"👁️ [{chat_id}] Watching {position['symbol']} for re-entry opportunity")
        
        del portfolio["positions"][mint]
        self.save()

        pnl_symbol = "🟢" if total_pnl >= 0 else "🔴"
        win_rate = (stats["wins"] / stats["total_trades"] * 100) if stats["total_trades"] > 0 else 0
        
        status_note = ""
        if should_blacklist:
            status_note = "\n❌ <i>Token blacklisted</i>"
        elif should_watch_reentry:
            status_note = "\n👁️ <i>Watching for re-entry</i>"
        
        msg = (f"{pnl_symbol} <b>FULL EXIT</b>\n\n"
               f"<b>Token:</b> {position['symbol']}\n"
               f"<b>Reason:</b> {reason}\n"
               f"<b>Hold Time:</b> {hold_duration.seconds // 60} mins\n"
               f"<b>Entry:</b> ${position['entry_price']:.6f}\n"
               f"<b>Exit:</b> ${current_price:,.6f}\n"
               f"<b>Peak:</b> ${position['peak_price']:.6f}\n\n"
               f"<b>Total P/L:</b> ${total_pnl:,.2f} ({total_pnl_pct:+.1f}%)\n\n"
               f"<i>Capital: ${portfolio['capital_usd']:,.2f}</i>\n"
               f"<i>Win Rate: {win_rate:.1f}% ({stats['wins']}/{stats['total_trades']})</i>"
               f"{status_note}")
        
        try:
            await app.bot.send_message(chat_id=chat_id, text=msg, parse_mode="HTML")
            logger.info(f"📉 [{chat_id}] FULL SELL {position['symbol']}. P/L: ${total_pnl:,.2f}")
        except Exception as e:
            logger.error(f"Failed to send full sell notification: {e}")
    
    def _should_blacklist_token(self, trade_log: Dict[str, Any], reason: str) -> bool:
        """Determine if a token should be blacklisted."""
        rug_keywords = ["Rug Pull", "Liquidity Drain", "Catastrophic"]
        if any(keyword in reason for keyword in rug_keywords):
            return True
        
        if trade_log["total_pnl_percent"] < -25:
            return True
        
        return False
    
    def _should_add_to_reentry(self, trade_log: Dict[str, Any], reason: str) -> bool:
        """Determine if we should watch this token for re-entry."""
        if "Take-Profit" in reason or "Partial" in reason:
            return True
        
        if "Time Exit" in reason and trade_log["total_pnl_percent"] > 10:
            return True
        
        if "Max Hold" in reason and trade_log["total_pnl_percent"] > 0:
            return True
        
        if "Trailing Stop" in reason and trade_log["peak_profit_pct"] >= 40:
            return True
        
        return False
    
    async def check_reentry_opportunity(self, app: Application, chat_id: str, 
                                       mint: str, current_data: Dict[str, Any]):
        """Check if a previously exited token is worth re-entering."""
        portfolio = self.get_portfolio(chat_id)
        candidate = portfolio["reentry_candidates"].get(mint)
        
        if not candidate:
            return
        
        expires_at = datetime.fromisoformat(candidate["expires_at"].rstrip("Z"))
        if datetime.utcnow() > expires_at:
            del portfolio["reentry_candidates"][mint]
            self.save()
            logger.info(f"⏰ [{chat_id}] Re-entry watch expired for {candidate['symbol']}")
            return
        
        if mint in portfolio["positions"]:
            return
        
        if candidate["reentry_attempts"] >= 2:
            del portfolio["reentry_candidates"][mint]
            self.save()
            logger.info(f"🚫 [{chat_id}] Max re-entry attempts reached for {candidate['symbol']}")
            return
        
        current_price = float(current_data.get("priceUsd", 0))
        current_liquidity = current_data.get("liquidity", {}).get("usd", 0)
        first_exit_price = candidate["first_exit_price"]
        
        reentry_triggered = False
        reentry_reason = ""
        
        if current_price >= first_exit_price * 1.15:
            buys_5m = current_data.get("txns", {}).get("m5", {}).get("buys", 0)
            if buys_5m >= 150 and current_liquidity >= 35000:
                reentry_triggered = True
                reentry_reason = f"Re-entry: Breakout (+{((current_price/first_exit_price - 1) * 100):.1f}% from exit)"
        
        if first_exit_price * 0.70 <= current_price <= first_exit_price * 0.85:
            buys_5m = current_data.get("txns", {}).get("m5", {}).get("buys", 0)
            buys_1h = current_data.get("txns", {}).get("h1", {}).get("buys", 0)
            sells_1h = current_data.get("txns", {}).get("h1", {}).get("sells", 0)
            ratio = buys_1h / sells_1h if sells_1h > 0 else buys_1h
            
            if buys_5m >= 180 and ratio >= 1.4 and current_liquidity >= 30000:
                reentry_triggered = True
                reentry_reason = f"Re-entry: Dip Buy (support at {((1 - current_price/first_exit_price) * 100):.0f}% below exit)"
        
        if current_price > candidate["peak_price_seen"] * 1.10:
            if current_liquidity >= 50000:
                reentry_triggered = True
                reentry_reason = "Re-entry: New ATH + Strong Liquidity"
        
        if reentry_triggered:
            candidate["reentry_attempts"] += 1
            self.save()
            
            await self.execute_buy(
                app, chat_id, mint, current_price, current_liquidity, 
                f"🔄 {reentry_reason}"
            )
            
            portfolio["stats"]["reentry_trades"] += 1
            del portfolio["reentry_candidates"][mint]
            self.save()


async def fetch_dexscreener_data(session: aiohttp.ClientSession, token_mint: str) -> Optional[Dict[str, Any]]:
    """Fetch best pair data from DexScreener."""
    url = f"https://api.dexscreener.com/latest/dex/tokens/{token_mint}"
    try:
        async with session.get(url, timeout=5) as response:
            if response.status != 200:
                return None
            data = await response.json()
            pairs = data.get("pairs")
            if not pairs:
                return None
            
            best_pair = max(pairs, key=lambda p: p.get("liquidity", {}).get("usd", 0))
            return best_pair
    except Exception:
        return None


def validate_token_criteria(data: Dict[str, Any], min_liquidity: float = 35000, 
                            min_buys_5m: int = 150, min_ratio: float = 1.2) -> bool:
    """Validate if a token meets trading criteria."""
    if not data or not data.get("priceUsd"):
        return False
    
    liquidity = data.get("liquidity", {}).get("usd", 0)
    buys_5m = data.get("txns", {}).get("m5", {}).get("buys", 0)
    buys_1h = data.get("txns", {}).get("h1", {}).get("buys", 0)
    sells_1h = data.get("txns", {}).get("h1", {}).get("sells", 0)
    ratio_1h = buys_1h / sells_1h if sells_1h > 0 else buys_1h
    
    market_cap = data.get("marketCap", 0)
    fdv = data.get("fdv", 0)
    volume_1h = data.get("volume", {}).get("h1", 0)
    
    passes_liquidity = liquidity >= min_liquidity
    passes_buys = buys_5m >= min_buys_5m
    passes_ratio = ratio_1h >= min_ratio
    passes_marketcap = market_cap >= 50000 or fdv >= 100000
    passes_volume = volume_1h >= 12000
    
    return all([passes_liquidity, passes_buys, passes_ratio, passes_marketcap, passes_volume])


async def trade_monitoring_loop(app: Application, user_manager: UserManager, 
                               portfolio_manager: PortfolioManager):
    """Enhanced monitoring with better entry/exit logic."""
    logger.info("🔄 TRADE LOOP: Enhanced monitoring starting.")
    await asyncio.sleep(10)
    
    pnl_update_counter = 0
    
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                trading_users = user_manager.get_trading_users()
                if not trading_users:
                    await asyncio.sleep(5)
                    continue

                mints_to_check = set()
                for chat_id in trading_users:
                    portfolio = portfolio_manager.get_portfolio(chat_id)
                    mints_to_check.update(portfolio.get("pending_signals", {}).keys())
                    mints_to_check.update(portfolio.get("watchlist", {}).keys())
                    mints_to_check.update(portfolio.get("positions", {}).keys())
                    mints_to_check.update(portfolio.get("reentry_candidates", {}).keys())
                
                if not mints_to_check:
                    await asyncio.sleep(1)
                    continue

                tasks = [fetch_dexscreener_data(session, mint) for mint in mints_to_check]
                results = await asyncio.gather(*tasks)
                live_data = {data['baseToken']['address']: data for data in results if data}
                
                live_prices = {mint: float(data["priceUsd"]) for mint, data in live_data.items() if data.get("priceUsd")}
                
                pnl_update_counter += 1
                should_send_periodic_pnl = (pnl_update_counter % 300 == 0)

                for chat_id in trading_users:
                    portfolio = portfolio_manager.get_portfolio(chat_id)
                    
                    if should_send_periodic_pnl and portfolio.get("positions"):
                        pnl_data = portfolio_manager.calculate_unrealized_pnl(chat_id, live_prices)
                        if pnl_data["position_count"] > 0:
                            await portfolio_manager.send_pnl_update(app, chat_id, pnl_data, "5-min update")
                    
                    # --- PENDING SIGNALS EVALUATION (EPOCH-BASED) ---
                    for mint in list(portfolio.get("pending_signals", {}).keys()):
                        signal = portfolio["pending_signals"][mint]
                        data = live_data.get(mint)
                        
                        if not data:
                            continue
                        
                        signal_time = datetime.fromisoformat(signal["signal_time"].rstrip("Z"))
                        total_elapsed_minutes = (datetime.utcnow() - signal_time).total_seconds() / 60
                        
                        last_check = datetime.fromisoformat(signal["last_check_time"].rstrip("Z"))
                        time_since_check = (datetime.utcnow() - last_check).total_seconds()
                        
                        # Check every 15 seconds
                        if time_since_check >= 15:
                            signal["last_check_time"] = datetime.utcnow().isoformat() + "Z"
                            
                            # Check if current epoch has completed (5 minutes)
                            epoch_start = datetime.fromisoformat(signal["current_epoch_start"].rstrip("Z"))
                            epoch_elapsed = (datetime.utcnow() - epoch_start).total_seconds() / 60
                            
                            # Perform validation check
                            passed = validate_token_criteria(data, min_liquidity=35000, min_buys_5m=150, min_ratio=1.2)
                            
                            if passed:
                                signal["current_epoch_passes"] += 1
                            
                            signal["current_epoch_checks"] += 1
                            
                            logger.debug(f"📊 [{chat_id}] {signal['symbol']} Epoch {signal['current_epoch_number']}: "
                                       f"{signal['current_epoch_passes']}/{signal['current_epoch_checks']} passed "
                                       f"({epoch_elapsed:.1f}/5.0 min)")
                            
                            # Check if epoch is complete (5 minutes)
                            if epoch_elapsed >= 5.0:
                                # Calculate pass rate for this epoch
                                epoch_pass_rate = signal["current_epoch_passes"] / signal["current_epoch_checks"] if signal["current_epoch_checks"] > 0 else 0
                                
                                # Store completed epoch
                                completed_epoch = {
                                    "epoch_number": signal["current_epoch_number"],
                                    "checks": signal["current_epoch_checks"],
                                    "passes": signal["current_epoch_passes"],
                                    "pass_rate": epoch_pass_rate,
                                    "completed_at": datetime.utcnow().isoformat() + "Z"
                                }
                                signal["epochs"].append(completed_epoch)
                                
                                logger.info(f"✅ [{chat_id}] {signal['symbol']} Epoch {signal['current_epoch_number']} complete: "
                                          f"{signal['current_epoch_passes']}/{signal['current_epoch_checks']} "
                                          f"({epoch_pass_rate*100:.1f}% pass rate)")
                                
                                # Check if this epoch passed the 2/3 threshold
                                if epoch_pass_rate >= 0.67:
                                    current_price = float(data["priceUsd"])
                                    current_liquidity = data.get("liquidity", {}).get("usd", 0)
                                    
                                    token_info = {
                                        "mint": mint,
                                        "price": current_price,
                                        "symbol": signal["symbol"],
                                        "name": signal["name"],
                                        "liquidity": current_liquidity,
                                        "promoted_from_epoch": signal["current_epoch_number"],
                                        "epoch_pass_rate": epoch_pass_rate
                                    }
                                    
                                    await portfolio_manager.add_to_watchlist(chat_id, token_info)
                                    del portfolio["pending_signals"][mint]
                                    portfolio_manager.save()
                                    
                                    logger.info(f"⭐ [{chat_id}] Promoted {signal['symbol']} to watchlist from Epoch {signal['current_epoch_number']} "
                                              f"({epoch_pass_rate*100:.0f}% pass rate)")
                                    continue
                                
                                # Start next epoch if we haven't reached 6 epochs yet
                                if signal["current_epoch_number"] < 6:
                                    signal["current_epoch_number"] += 1
                                    signal["current_epoch_start"] = datetime.utcnow().isoformat() + "Z"
                                    signal["current_epoch_checks"] = 0
                                    signal["current_epoch_passes"] = 0
                                    portfolio_manager.save()
                                    
                                    logger.info(f"🔄 [{chat_id}] {signal['symbol']} starting Epoch {signal['current_epoch_number']}")
                                else:
                                    # All 6 epochs completed, none passed - remove from pending
                                    del portfolio["pending_signals"][mint]
                                    portfolio_manager.save()
                                    
                                    epoch_summary = ", ".join([f"E{e['epoch_number']}:{e['pass_rate']*100:.0f}%" for e in signal["epochs"]])
                                    logger.info(f"❌ [{chat_id}] Dropped {signal['symbol']} after 6 epochs - no epoch passed 67% ({epoch_summary})")
                        
                        # Total timeout after 30 minutes regardless of epoch progress
                        if total_elapsed_minutes >= signal["max_evaluation_minutes"]:
                            epoch_summary = ", ".join([f"E{e['epoch_number']}:{e['pass_rate']*100:.0f}%" for e in signal["epochs"]])
                            del portfolio["pending_signals"][mint]
                            portfolio_manager.save()
                            logger.info(f"⏰ [{chat_id}] Evaluation timeout for {signal['symbol']} after 30 mins ({epoch_summary})")
                    
                    # --- RE-ENTRY CANDIDATES ---
                    for mint in list(portfolio.get("reentry_candidates", {}).keys()):
                        data = live_data.get(mint)
                        if data:
                            await portfolio_manager.check_reentry_opportunity(app, chat_id, mint, data)
                    
                    # --- WATCHLIST PROCESSING ---
                    for mint, item in list(portfolio.get("watchlist", {}).items()):
                        if mint in portfolio.get("blacklist", {}):
                            del portfolio["watchlist"][mint]
                            portfolio_manager.save()
                            continue
                        
                        data = live_data.get(mint)
                        if not data or not data.get("priceUsd"):
                            continue
                        
                        current_price = float(data["priceUsd"])
                        current_liquidity = data.get("liquidity", {}).get("usd", 0)
                        signal_price = item["signal_price"]
                        signal_time = datetime.fromisoformat(item["signal_time"].rstrip("Z"))
                        wait_time = (datetime.utcnow() - signal_time).total_seconds() / 60
                        
                        item["highest_price"] = max(item["highest_price"], current_price)
                        item["lowest_price"] = min(item["lowest_price"], current_price)
                        
                        # Track price history for momentum calculation (keep last 10 prices)
                        if "price_history" not in item:
                            item["price_history"] = []
                        item["price_history"].append(current_price)
                        if len(item["price_history"]) > 10:
                            item["price_history"].pop(0)
                        
                        # --- START: LOGIC CHANGE ---
                        # The harsh RE-VALIDATE block that was here has been REMOVED as requested.
                        # We now trust the epoch validation and look for an entry.

                        entry_triggered = False
                        entry_reason = ""
                        buys_5m = data.get("txns", {}).get("m5", {}).get("buys", 0)

                        # NEW CHECK: Only proceed if buy pressure is > 100 (user's requested floor)
                        if buys_5m > 100:
                            # ENTRY SCENARIO 1: Ideal dip (12-20% below signal)
                            # NO momentum check - we WANT to buy dips!
                            if signal_price * 0.80 <= current_price <= signal_price * 0.88:
                                entry_triggered = True
                                entry_reason = "Dip Entry (12-20% pullback)"
                            
                            # ENTRY SCENARIO 2: Continuing momentum
                            # HAS momentum check - must be rising NOW
                            elif wait_time >= 15 and current_price >= signal_price * 0.95:
                                # We already know buys_5m > 100.
                                # We just check for increasing liquidity.
                                if current_liquidity >= item["signal_liquidity"] * 1.15:
                                    entry_triggered = True
                                    entry_reason = "Strong Momentum (sustained buying)"
                            
                            # ENTRY SCENARIO 3: Recovery from dip
                            # NO momentum check - recovering from weakness
                            elif item["lowest_price"] < signal_price * 0.85 and current_price >= signal_price * 0.92:
                                # Note: max_wait_minutes is 10, so wait_time <= 30 is fine.
                                if wait_time <= 30: 
                                    entry_triggered = True
                                    entry_reason = "Recovery Entry (bounced from dip)"
                        
                        # else:
                        #   buys_5m <= 100, so we don't trigger an entry.
                        #   We just let the loop continue and wait.
                        
                        # Timeout check (max_wait_minutes is now 10)
                        if wait_time >= item["max_wait_minutes"]:
                            logger.info(f"⏰ [{chat_id}] Timeout for {item['symbol']} (10 min), demoting to pending")
                            del portfolio["watchlist"][mint]
                            
                            # Demote back to pending_signals as requested
                            current_time = datetime.utcnow().isoformat() + "Z"
                            portfolio["pending_signals"][mint] = {
                                "signal_price": signal_price,
                                "signal_time": signal_time.isoformat() + "Z",
                                "symbol": item["symbol"],
                                "name": item["name"],
                                "signal_liquidity": item["signal_liquidity"],
                                "last_check_time": current_time,
                                "max_evaluation_minutes": 30,
                                "epochs": [],
                                "current_epoch_start": current_time,
                                "current_epoch_checks": 0,
                                "current_epoch_passes": 0,
                                "current_epoch_number": 1
                            }
                            portfolio_manager.save()
                            continue
                        
                        # --- END: LOGIC CHANGE ---
                        
                        if entry_triggered:
                            await portfolio_manager.execute_buy(
                                app, chat_id, mint, current_price, current_liquidity, entry_reason
                            )
                    
                    # --- POSITION MANAGEMENT ---
                    for mint, pos in list(portfolio.get("positions", {}).items()):
                        data = live_data.get(mint)
                        if not data or not data.get("priceUsd"):
                            continue

                        current_price = float(data["priceUsd"])
                        current_liquidity = data.get("liquidity", {}).get("usd", 0)
                        entry_price = pos["entry_price"]
                        profit_pct = ((current_price - entry_price) / entry_price) * 100
                        
                        if current_price > pos["peak_price"]:
                            pos["peak_price"] = current_price
                            
                            last_milestone = pos.get("last_pnl_milestone", 0)
                            milestones = [25, 50, 100, 200, 500]
                            
                            for milestone in milestones:
                                if profit_pct >= milestone and last_milestone < milestone:
                                    pos["last_pnl_milestone"] = milestone
                                    
                                    unrealized_value = pos["token_amount"] * current_price
                                    cost_basis = pos["investment_usd"] * (pos["remaining_percentage"] / 100.0)
                                    unrealized_pnl = unrealized_value - cost_basis + pos.get("locked_profit_usd", 0)
                                    
                                    milestone_msg = (f"🚀 <b>MILESTONE: +{milestone}%</b>\n\n"
                                                   f"<b>Token:</b> {pos['symbol']}\n"
                                                   f"<b>Entry:</b> ${entry_price:.6f}\n"
                                                   f"<b>Current:</b> ${current_price:.6f}\n"
                                                   f"<b>Peak Gain:</b> +{profit_pct:.1f}%\n"
                                                   f"<b>Unrealized P/L:</b> ${unrealized_pnl:,.2f}\n\n"
                                                   f"<i>Keep riding or take profits! 🎯</i>")
                                    try:
                                        await app.bot.send_message(chat_id=chat_id, text=milestone_msg, parse_mode="HTML")
                                        logger.info(f"🎉 [{chat_id}] {pos['symbol']} hit +{milestone}% milestone")
                                    except Exception as e:
                                        logger.error(f"Failed to send milestone notification: {e}")
                                    break
                            
                            portfolio_manager.save()
                        
                        # RUG PULL PROTECTION
                        liq_drop_pct = ((pos["entry_liquidity"] - current_liquidity) / pos["entry_liquidity"]) * 100
                        
                        if liq_drop_pct >= 40:
                            await portfolio_manager.execute_full_sell(
                                app, chat_id, mint, current_price, 
                                f"🚨 Severe Rug Pull (Liquidity -{liq_drop_pct:.0f}%)"
                            )
                            continue
                        
                        if liq_drop_pct >= 35 and profit_pct < -5:
                            await portfolio_manager.execute_full_sell(
                                app, chat_id, mint, current_price, 
                                f"⚠️ Liquidity Drain + Price Drop (Liq -{liq_drop_pct:.0f}%, Price {profit_pct:.1f}%)"
                            )
                            continue
                        
                        # PARTIAL PROFIT TAKING (LOWERED THRESHOLDS)
                        remaining = pos["remaining_percentage"]
                        
                        # First partial: 40% at +30% (lowered from +40%)
                        if profit_pct >= 30 and remaining == 100:
                            await portfolio_manager.execute_partial_sell(
                                app, chat_id, mint, current_price, 40.0, 
                                "Take-Profit Level 1 (+30%)"
                            )
                            continue
                        
                        # Second partial: 30% at +50% (lowered from +80%)
                        if profit_pct >= 50 and remaining == 60:
                            await portfolio_manager.execute_partial_sell(
                                app, chat_id, mint, current_price, 30.0, 
                                "Take-Profit Level 2 (+50%)"
                            )
                            continue
                        
                        # Third partial: 20% at +100% (lowered from +150%)
                        if profit_pct >= 100 and remaining == 30:
                            await portfolio_manager.execute_partial_sell(
                                app, chat_id, mint, current_price, 20.0, 
                                "Take-Profit Level 3 (+100%)"
                            )
                            continue
                        
                        # TIME-BASED EXIT
                        entry_time = datetime.fromisoformat(pos["entry_time"].rstrip("Z"))
                        hold_minutes = (datetime.utcnow() - entry_time).total_seconds() / 60
                        
                        if hold_minutes >= 120:
                            buys_5m = data.get("txns", {}).get("m5", {}).get("buys", 0)
                            
                            if buys_5m < 100 or liq_drop_pct >= 20:
                                await portfolio_manager.execute_full_sell(
                                    app, chat_id, mint, current_price, 
                                    f"Time Exit (2hr+, low activity, +{profit_pct:.1f}%)"
                                )
                                continue
                        
                        if hold_minutes >= 240:
                            await portfolio_manager.execute_full_sell(
                                app, chat_id, mint, current_price, 
                                f"Max Hold Time (4hr, +{profit_pct:.1f}%)"
                            )
                            continue

            except Exception as e:
                logger.exception(f"❌ TRADE LOOP: Error in monitoring: {e}")
            
            await asyncio.sleep(1)


async def signal_detection_loop(app: Application, user_manager: UserManager, 
                               portfolio_manager: PortfolioManager):
    """Enhanced signal detection - adds to pending_signals for evaluation."""
    logger.info("🔍 SIGNAL LOOP: Enhanced detection starting.")
    await asyncio.sleep(5)
    
    processed_signals = set()
    
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                if not OVERLAP_FILE.exists():
                    await asyncio.sleep(15)
                    continue

                overlap_data = joblib.load(OVERLAP_FILE)
                trading_users = user_manager.get_trading_users()

                if not trading_users:
                    await asyncio.sleep(30)
                    continue
                    
                for token_id, history in overlap_data.items():
                    if not history or token_id in processed_signals:
                        continue
                    
                    blacklisted_by_any = False
                    for user_id in trading_users:
                        portfolio = portfolio_manager.get_portfolio(user_id)
                        if token_id in portfolio.get("blacklist", {}):
                            blacklisted_by_any = True
                            break
                    
                    if blacklisted_by_any:
                        continue
                    
                    latest = history[-1].get("result", {})
                    grade = latest.get("grade", "NONE")
                    
                    if grade in VALID_GRADES:
                        dex_data = await fetch_dexscreener_data(session, token_id)
                        if not dex_data:
                            continue

                        liquidity = dex_data.get("liquidity", {}).get("usd", 0)
                        market_cap = dex_data.get("marketCap", 0)
                        fdv = dex_data.get("fdv", 0)
                        
                        # Basic sanity checks
                        if liquidity < 25000 or (market_cap < 30000 and fdv < 50000):
                            continue
                        
                        logger.info(f"🆕 NEW SIGNAL: {token_id[:8]}... | Grade: {grade} | Liq: ${liquidity:,.0f}")
                        
                        token_info = {
                            "mint": token_id,
                            "price": float(dex_data["priceUsd"]),
                            "symbol": dex_data["baseToken"]["symbol"],
                            "name": dex_data["baseToken"]["name"],
                            "liquidity": liquidity
                        }
                        
                        for user_id in trading_users:
                            await portfolio_manager.add_to_pending_signals(user_id, token_info)
                        
                        processed_signals.add(token_id)

            except Exception as e:
                logger.exception(f"❌ SIGNAL LOOP: Error in detection: {e}")
            
            await asyncio.sleep(15)