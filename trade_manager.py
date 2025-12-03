#!/usr/bin/env python3
"""
trade_manager.py - Analytics-Driven Paper Trading
"""

import logging
import json
import asyncio
import statistics
import aiohttp
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List

from telegram.ext import Application
from shared.file_io import safe_load, safe_save

from config import PORTFOLIOS_FILE, BUCKET_NAME, USE_SUPABASE, DATA_DIR

# Import download_file for daily fallback checks
try:
    from supabase_utils import upload_file, download_file
except ImportError:
    upload_file = None
    download_file = None

logger = logging.getLogger(__name__)

ACTIVE_TRACKING_FILE = DATA_DIR / "active_tracking.json"

class PortfolioManager:
    """Manages virtual portfolios using analytics data."""

    def __init__(self, portfolio_file: Path):
        self.file = portfolio_file
        self.portfolios = safe_load(self.file, {})
        self.tp_metrics = {
            "calculated_at": None,
            "discovery": {"median_ath": 45.0, "mean_ath": 60.0},
            "alpha": {"median_ath": 50.0, "mean_ath": 70.0}
        }
        self._ensure_portfolio_structure()
        logger.info(f"📈 PortfolioManager initialized. Loaded {len(self.portfolios)} portfolios.")

    def _ensure_portfolio_structure(self):
        """Clean up portfolios."""
        migrated = False
        for chat_id, portfolio in self.portfolios.items():
            # Remove legacy cooldowns if they exist
            if "cooldowns" in portfolio:
                del portfolio["cooldowns"]
                migrated = True
            if "watchlist" in portfolio:
                del portfolio["watchlist"]
                migrated = True
            if "pending_signals" in portfolio:
                del portfolio["pending_signals"]
                migrated = True
                
        if migrated:
            self.save()

    def save(self):
        """Save portfolios to disk and cloud."""
        safe_save(self.file, self.portfolios)
        if USE_SUPABASE and upload_file:
            try:
                upload_file(str(self.file), bucket=BUCKET_NAME, remote_path=f"paper_trade/{self.file.name}")
            except Exception as e:
                logger.error(f"Failed to sync portfolio to Supabase: {e}")

    def get_portfolio(self, chat_id: str) -> Dict[str, Any]:
        """Get or create a portfolio for a user."""
        chat_id = str(chat_id)
        if chat_id not in self.portfolios:
            self.portfolios[chat_id] = {
                "capital_usd": 1000.0,
                "positions": {},
                "trade_history": [],
                "blacklist": {},
                "stats": {
                    "total_trades": 0, "wins": 0, "losses": 0, "total_pnl": 0.0,
                    "best_trade": 0.0, "worst_trade": 0.0
                }
            }
        return self.portfolios[chat_id]

    async def get_final_data_from_daily_file(self, mint: str, signal_type: str, tracking_end_date: str) -> Optional[Dict[str, Any]]:
        """
        Downloads daily file and extracts final token data.
        tracking_end_date format: "2025-01-15"
        """
        if not download_file:
            return None

        remote_path = f"analytics/{signal_type}/daily/{tracking_end_date}.json"
        local_path = DATA_DIR / f"daily_{tracking_end_date}_{signal_type}.json"
        
        try:
            # Download if not cached or force logic can be applied here
            if download_file(str(local_path), remote_path, bucket=BUCKET_NAME):
                with open(local_path, 'r') as f:
                    daily_data = json.load(f)
                    
                # Clean up local file to save space
                try:
                    os.remove(local_path)
                except:
                    pass

                if daily_data and "tokens" in daily_data:
                    for token in daily_data["tokens"]:
                        if token.get("mint") == mint:
                            return {
                                "final_price": token.get("final_price"),
                                "final_roi": token.get("final_roi"),
                                "ath_roi": token.get("ath_roi")
                            }
        except Exception as e:
            logger.warning(f"Failed to fetch daily file {remote_path}: {e}")
        
        return None

    # --- PNL CALCULATIONS ---

    def calculate_unrealized_pnl(self, chat_id: str, live_prices: Dict[str, float]) -> Dict[str, Any]:
        """Calculate total unrealized P/L using live prices provided."""
        portfolio = self.get_portfolio(chat_id)
        positions = portfolio.get("positions", {})
        
        if not positions:
            return {
                "total_unrealized_usd": 0.0,
                "position_count": 0,
                "positions_detail": []
            }
        
        total_unrealized_pnl = 0.0
        total_cost_basis = 0.0
        positions_detail = []
        
        for key, pos in positions.items():
            if pos.get("status") != "active":
                continue
            
            mint = pos.get("mint")
            token_balance = pos["token_amount"]
            avg_buy_price = pos.get("avg_buy_price", pos["entry_price"])
            
            # Use provided live price or fallback to tracked price
            current_price = live_prices.get(mint, pos.get("current_price", pos["entry_price"]))
            
            unrealized_pnl = token_balance * (current_price - avg_buy_price)
            cost_basis = token_balance * avg_buy_price
            
            unrealized_pct = ((current_price - avg_buy_price) / avg_buy_price) * 100 if avg_buy_price > 0 else 0
            
            total_unrealized_pnl += unrealized_pnl
            total_cost_basis += cost_basis
            
            positions_detail.append({
                "symbol": pos["symbol"],
                "current_price": current_price,
                "unrealized_pnl_usd": unrealized_pnl,
                "unrealized_pnl_pct": unrealized_pct,
                "token_balance": token_balance
            })
        
        total_unrealized_pct = (total_unrealized_pnl / total_cost_basis) * 100 if total_cost_basis > 0 else 0
        
        return {
            "total_unrealized_usd": total_unrealized_pnl,
            "total_unrealized_pct": total_unrealized_pct,
            "total_cost_basis": total_cost_basis,
            "position_count": len(positions_detail),
            "positions_detail": positions_detail
        }

    async def fetch_current_price_fallback(self, mint: str) -> float:
        """Fallback: Fetch live price from Jupiter if analytics data missing."""
        url = f"https://lite-api.jup.ag/price/v3?ids={mint}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=5) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return float(data.get(mint, {}).get("usdPrice", 0))
        except Exception as e:
            logger.debug(f"Jupiter fallback failed for {mint}: {e}")
        return 0.0

    # --- ENTRY LOGIC ---

    async def download_active_tracking(self) -> Dict[str, Any]:
        """Download active_tracking.json from Supabase."""
        if not USE_SUPABASE or not download_file:
            return {}
        
        remote_path = "analytics/active_tracking.json"
        try:
            if download_file(str(ACTIVE_TRACKING_FILE), remote_path, bucket=BUCKET_NAME):
                with open(ACTIVE_TRACKING_FILE, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Failed to download active_tracking.json: {e}")
        return {}

    async def process_new_signal(self, chat_id: str, token_data: dict, user_manager, app: Application):
        """
        Process new signal from analytics_monitoring loop.
        Uses tracking_end_time from analytics directly.
        """
        portfolio = self.get_portfolio(chat_id)
        mint = token_data.get("mint")
        signal_type = token_data.get("signal_type", "discovery")
        
        # Use data provided by analytics_monitoring
        entry_price = token_data.get("price")
        symbol = token_data.get("symbol", "Unknown")
        
        # CRITICAL: Use analytics provided end time
        tracking_end_time = token_data.get("tracking_end_time")
        
        # Fallback only if absolutely necessary
        if not tracking_end_time:
            hours = 168 if signal_type == "alpha" else 24
            tracking_end_time = (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat() + "Z"

        # Check if position already exists
        position_key = f"{mint}_{signal_type}"
        if position_key in portfolio["positions"]:
            return

        # Signal type swap logic
        other_type = "alpha" if signal_type == "discovery" else "discovery"
        other_key = f"{mint}_{other_type}"
        
        if other_key in portfolio["positions"]:
            logger.info(f"🔄 [{chat_id}] Swapping {other_type} -> {signal_type} for {mint}")
            other_pos = portfolio["positions"][other_key]
            # Use current price from new signal for exit approx
            other_current_price = token_data.get("price", other_pos["entry_price"])
            other_roi = ((other_current_price - other_pos["entry_price"]) / other_pos["entry_price"]) * 100
            await self.exit_position(chat_id, other_key, "Signal Type Swap 🔄", app, exit_roi=other_roi)

        # Validate capital
        capital = portfolio["capital_usd"]
        if capital < 10:
            return
        
        # Size: 10% of capital, max $150
        size_usd = min(capital * 0.10, 150.0)
        token_amount = size_usd / entry_price
        
        # Get TP target
        # Assuming self.get_tp_for_signal_type exists or defaults
        prefs = user_manager.get_user_prefs(chat_id)
        tp_target = 50.0 # Default
        if signal_type == "discovery":
             tp_target = 45.0
        
        # Create position
        portfolio["positions"][position_key] = {
            "mint": mint,
            "signal_type": signal_type,
            "symbol": symbol,
            "entry_price": entry_price,
            "entry_time": datetime.now(timezone.utc).isoformat() + "Z",
            "tracking_end_time": tracking_end_time,
            "investment_usd": size_usd,
            "token_amount": token_amount,
            "status": "active",
            "tp_used": tp_target,
            # Live tracking fields
            "current_price": entry_price,
            "current_roi": 0.0,
            "ath_price": entry_price,
            "ath_roi": 0.0,
            "last_updated": datetime.now(timezone.utc).isoformat() + "Z",
            # Metadata
            "token_age_hours": token_data.get("token_age_hours"),
            "entry_mcap": token_data.get("entry_mcap"),
            "entry_liquidity": token_data.get("entry_liquidity"),
            "avg_buy_price": entry_price
        }
        
        portfolio["capital_usd"] -= size_usd
        self.save()

        # Notification
        ml_action = token_data.get("ml_prediction", {}).get("action", "N/A")
        grade = token_data.get("grade", "N/A")
        
        msg = (
            f"🟢 <b>PAPER TRADE OPENED</b>\n\n"
            f"<b>Token:</b> {symbol}\n"
            f"<b>Type:</b> {signal_type.upper()}\n"
            f"<b>Grade:</b> {grade}\n"
            f"<b>Entry:</b> ${entry_price:.8f}\n"
            f"<b>Size:</b> ${size_usd:.2f}\n"
            f"<b>Target TP:</b> +{tp_target:.0f}%\n"
            f"<b>ML Signal:</b> {ml_action}\n"
        )
        
        try:
            await app.bot.send_message(chat_id=chat_id, text=msg, parse_mode="HTML")
            logger.info(f"✅ [{chat_id}] Opened position: {symbol}")
        except Exception:
            pass

    # --- EXIT LOGIC ---

    async def exit_position(self, chat_id: str, position_key: str, reason: str, app: Application, 
                           exit_roi: float = 0.0):
        """Execute exit, update stats, remove position."""
        portfolio = self.get_portfolio(chat_id)
        pos = portfolio["positions"].get(position_key)
        if not pos: return

        investment = pos["investment_usd"]
        
        # Calculate PnL
        final_value = investment * (1 + exit_roi/100)
        pnl_usd = final_value - investment
        
        portfolio["capital_usd"] += final_value
        del portfolio["positions"][position_key]
        
        # Stats
        stats = portfolio["stats"]
        stats["total_trades"] += 1
        stats["total_pnl"] += pnl_usd
        if pnl_usd > 0: stats["wins"] += 1
        else: stats["losses"] += 1
        stats["best_trade"] = max(stats["best_trade"], exit_roi)
        stats["worst_trade"] = min(stats["worst_trade"], exit_roi)

        # History
        history_item = {
            "symbol": pos["symbol"],
            "entry_price": pos["entry_price"],
            "exit_reason": reason,
            "pnl_usd": pnl_usd,
            "pnl_percent": exit_roi,
            "exit_time": datetime.now(timezone.utc).isoformat() + "Z",
            "signal_type": pos["signal_type"]
        }
        portfolio["trade_history"].append(history_item)
        
        self.save()

        # Notify
        emoji = "🟢" if pnl_usd > 0 else "🔴"
        msg = (
            f"{emoji} <b>PAPER TRADE CLOSED</b>\n\n"
            f"<b>Token:</b> {pos['symbol']}\n"
            f"<b>Reason:</b> {reason}\n"
            f"<b>ROI:</b> {exit_roi:+.2f}%\n"
            f"<b>P/L:</b> ${pnl_usd:+.2f}\n"
            f"<b>Capital:</b> ${portfolio['capital_usd']:,.2f}"
        )
        try:
            await app.bot.send_message(chat_id=chat_id, text=msg, parse_mode="HTML")
        except: pass

    async def check_and_exit_positions(self, chat_id: str, app: Application):
        """
        Check analytics data and exit positions if TP hit or tracking ended.
        Updates position data with live analytics values.
        """
        portfolio = self.get_portfolio(chat_id)
        # Always get fresh data
        active_tracking = await self.download_active_tracking()
        now = datetime.now(timezone.utc)

        for key, pos in list(portfolio["positions"].items()):
            mint = pos["mint"]
            signal_type = pos["signal_type"]
            user_tp = pos["tp_used"]
            
            # Find in analytics
            analytics_key = f"{mint}_{signal_type}"
            data = active_tracking.get(analytics_key)
            
            # --- 1. UPDATE LIVE DATA ---
            if data:
                pos["current_price"] = data.get("current_price", pos["current_price"])
                pos["current_roi"] = data.get("current_roi", pos["current_roi"])
                pos["ath_price"] = data.get("ath_price", pos["ath_price"])
                pos["ath_roi"] = data.get("ath_roi", pos["ath_roi"])
                pos["last_updated"] = datetime.now(timezone.utc).isoformat() + "Z"
            
            # --- 2. TP CHECK ---
            # Check actual ATH from analytics against user TP
            ath_roi = float(pos.get("ath_roi", 0))
            if ath_roi >= user_tp:
                # Exit at the actual peak recorded
                await self.exit_position(chat_id, key, "TP Hit 🎯", app, exit_roi=ath_roi)
                continue
            
            # --- 3. EXPIRY CHECK ---
            end_time = datetime.fromisoformat(pos["tracking_end_time"].rstrip("Z")).replace(tzinfo=timezone.utc)
            
            if now >= end_time:
                current_roi = 0.0
                
                # Priority 1: Live Analytics Data
                if data:
                    current_roi = float(data.get("current_roi", 0))
                
                # Priority 2: Daily File (Finalized Data)
                else:
                    date_str = end_time.strftime('%Y-%m-%d')
                    daily_data = await self.get_final_data_from_daily_file(mint, signal_type, date_str)
                    
                    if daily_data:
                        current_roi = float(daily_data.get("final_roi", 0))
                    
                    # Priority 3: Jupiter API Fallback
                    else:
                        curr_price = await self.fetch_current_price_fallback(mint)
                        if curr_price > 0:
                            current_roi = ((curr_price - pos["entry_price"]) / pos["entry_price"]) * 100
                            
                        # Priority 4: Default 0.0 (Already set)
                
                await self.exit_position(chat_id, key, "Tracking Ended ⏱️", app, exit_roi=current_roi)

        self.save()