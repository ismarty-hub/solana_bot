#!/usr/bin/env python3
"""
trade_manager.py - Manages paper trading portfolios, signals, and execution.
"""

import logging
import asyncio
import aiohttp
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional

from telegram.ext import Application
from shared.file_io import safe_load, safe_save
from alerts.user_manager import UserManager
from config import OVERLAP_FILE, PORTFOLIOS_FILE, VALID_GRADES, USE_SUPABASE, BUCKET_NAME
import joblib

# Supabase uploader
try:
    from supabase_utils import upload_file
except ImportError:
    upload_file = None

logger = logging.getLogger(__name__)

class PortfolioManager:
    """Manages virtual portfolios for paper trading."""

    def __init__(self, portfolio_file: Path):
        self.file = portfolio_file
        self.portfolios = safe_load(self.file, {})
        logger.info(f"📈 PortfolioManager initialized with {len(self.portfolios)} portfolios.")

    def _sync_to_supabase(self):
        """Uploads the portfolios file to Supabase Storage."""
        if not USE_SUPABASE or not upload_file:
            return
        
        # Define the remote path within the 'paper_trade' folder
        remote_path = f"paper_trade/{self.file.name}"
        try:
            if upload_file(str(self.file), bucket=BUCKET_NAME, remote_path=remote_path, debug=False):
                logger.info(f"☁️ Synced portfolios to Supabase: {remote_path}")
            else:
                logger.warning("☁️ Portfolio sync to Supabase failed.")
        except Exception as e:
            logger.exception(f"☁️ Exception during portfolio sync to Supabase: {e}")

    def save(self):
        """Save the current state of all portfolios to disk and sync to cloud."""
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
                "trade_history": []
            }
        return self.portfolios[chat_id]

    def set_capital(self, chat_id: str, capital: float):
        """Set the starting capital for a user."""
        portfolio = self.get_portfolio(chat_id)
        portfolio["capital_usd"] = capital
        self.save()
        logger.info(f"💰 Set capital for {chat_id} to ${capital:,.2f}")

    async def add_to_watchlist(self, chat_id: str, token_info: Dict[str, Any]):
        """Add a token to a user's watchlist if it's a valid signal."""
        portfolio = self.get_portfolio(chat_id)
        mint = token_info['mint']

        if mint in portfolio["positions"] or mint in portfolio["watchlist"]:
            return  # Avoid duplicate signals

        portfolio["watchlist"][mint] = {
            "signal_price": token_info['price'],
            "signal_time": datetime.utcnow().isoformat() + "Z",
            "symbol": token_info['symbol'],
            "name": token_info['name']
        }
        self.save()
        logger.info(f"👀 [{chat_id}] Added {token_info['symbol']} to watchlist at ${token_info['price']:.6f}")

    async def execute_buy(self, app: Application, chat_id: str, mint: str, current_price: float, current_liquidity: float):
        """Execute a virtual buy and move token from watchlist to positions."""
        portfolio = self.get_portfolio(chat_id)
        watch_item = portfolio["watchlist"].get(mint)
        if not watch_item:
            return

        capital = portfolio["capital_usd"]
        investment_usd = min(capital * 0.1, 100) # Invest 10% or $100, whichever is less
        if capital < investment_usd:
            logger.warning(f"[{chat_id}] Insufficient capital to buy {watch_item['symbol']}")
            del portfolio["watchlist"][mint]
            self.save()
            return

        portfolio["capital_usd"] -= investment_usd
        token_amount = investment_usd / current_price

        portfolio["positions"][mint] = {
            "symbol": watch_item["symbol"],
            "entry_price": current_price,
            "entry_time": datetime.utcnow().isoformat() + "Z",
            "entry_liquidity": current_liquidity,
            "investment_usd": investment_usd,
            "token_amount": token_amount,
            "peak_price": current_price,
            "status": "active"
        }
        del portfolio["watchlist"][mint]
        self.save()

        msg = (f"✅ <b>Paper Trade Executed: BUY</b>\n\n"
               f"<b>Token:</b> {watch_item['name']} (${watch_item['symbol']})\n"
               f"<b>Amount:</b> ${investment_usd:,.2f} USD\n"
               f"<b>Price:</b> ${current_price:,.6f}")
        try:
            await app.bot.send_message(chat_id=chat_id, text=msg, parse_mode="HTML")
            logger.info(f"📈 [{chat_id}] BOUGHT {watch_item['symbol']} for ${investment_usd:,.2f}")
        except Exception as e:
            logger.error(f"Failed to send buy notification to {chat_id}: {e}")

    async def execute_sell(self, app: Application, chat_id: str, mint: str, current_price: float, reason: str):
        """Execute a virtual sell and log the trade outcome."""
        portfolio = self.get_portfolio(chat_id)
        position = portfolio["positions"].get(mint)
        if not position or position.get("status") != "active":
            return

        sell_value_usd = position["token_amount"] * current_price
        pnl = sell_value_usd - position["investment_usd"]
        pnl_percent = (pnl / position["investment_usd"]) * 100

        portfolio["capital_usd"] += sell_value_usd
        position["status"] = "closed"

        trade_log = {
            **position,
            "exit_price": current_price,
            "exit_time": datetime.utcnow().isoformat() + "Z",
            "pnl_usd": pnl,
            "pnl_percent": pnl_percent,
            "reason": reason
        }
        portfolio["trade_history"].append(trade_log)
        del portfolio["positions"][mint]
        self.save()

        pnl_symbol = "🟢" if pnl >= 0 else "🔴"
        msg = (f"{pnl_symbol} <b>Paper Trade Executed: SELL</b>\n\n"
               f"<b>Token:</b> {position['symbol']}\n"
               f"<b>Reason:</b> {reason}\n"
               f"<b>P/L:</b> ${pnl:,.2f} USD ({pnl_percent:,.2f}%)\n\n"
               f"<i>New Capital: ${portfolio['capital_usd']:,.2f}</i>")
        try:
            await app.bot.send_message(chat_id=chat_id, text=msg, parse_mode="HTML")
            logger.info(f"📉 [{chat_id}] SOLD {position['symbol']}. Reason: {reason}. P/L: ${pnl:,.2f}")
        except Exception as e:
            logger.error(f"Failed to send sell notification to {chat_id}: {e}")

# --- Trading Logic ---
async def fetch_dexscreener_data(session: aiohttp.ClientSession, token_mint: str) -> Optional[Dict[str, Any]]:
    """Fetch and select the best pair data from DexScreener."""
    url = f"https://api.dexscreener.com/latest/dex/tokens/{token_mint}"
    try:
        async with session.get(url, timeout=5) as response:
            if response.status != 200:
                return None
            data = await response.json()
            pairs = data.get("pairs")
            if not pairs:
                return None
            
            # Prioritize pairs with highest liquidity
            best_pair = max(pairs, key=lambda p: p.get("liquidity", {}).get("usd", 0))
            return best_pair
    except Exception:
        return None

async def trade_monitoring_loop(app: Application, user_manager: UserManager, portfolio_manager: PortfolioManager):
    """High-frequency loop (1-sec) to monitor watchlists and active positions."""
    logger.info("TRADE LOOP:  Trader monitoring loop starting.")
    await asyncio.sleep(10) # Initial delay
    
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
                    mints_to_check.update(portfolio.get("watchlist", {}).keys())
                    mints_to_check.update(portfolio.get("positions", {}).keys())
                
                if not mints_to_check:
                    await asyncio.sleep(1)
                    continue

                tasks = [fetch_dexscreener_data(session, mint) for mint in mints_to_check]
                results = await asyncio.gather(*tasks)
                
                live_data = {data['baseToken']['address']: data for data in results if data}

                for chat_id in trading_users:
                    portfolio = portfolio_manager.get_portfolio(chat_id)
                    
                    # --- Process Watchlist (Check for Entry) ---
                    for mint, item in list(portfolio.get("watchlist", {}).items()):
                        data = live_data.get(mint)
                        if not data or not data.get("priceUsd"):
                            continue
                        
                        current_price = float(data["priceUsd"])
                        signal_price = item["signal_price"]

                        # ENTRY CONDITION: Price drops 16-20% from signal price, and is not positive
                        if signal_price * 0.80 <= current_price <= signal_price * 0.84:
                            await portfolio_manager.execute_buy(
                                app, chat_id, mint, current_price, data.get("liquidity", {}).get("usd", 0)
                            )
                    
                    # --- Process Positions (Check for Exit) ---
                    for mint, pos in list(portfolio.get("positions", {}).items()):
                        data = live_data.get(mint)
                        if not data or not data.get("priceUsd"):
                            continue

                        current_price = float(data["priceUsd"])
                        current_liquidity = data.get("liquidity", {}).get("usd", 0)

                        # Update peak price
                        pos["peak_price"] = max(pos.get("peak_price", current_price), current_price)

                        # EXIT 1: Rug Pull Protection
                        if current_liquidity < pos["entry_liquidity"] * 0.70:
                            await portfolio_manager.execute_sell(app, chat_id, mint, current_price, "Rug Pull Protection (Liquidity Drop >30%)")
                            continue
                        
                        # EXIT 2: Take Profit
                        if current_price >= pos["entry_price"] * 1.50:
                            await portfolio_manager.execute_sell(app, chat_id, mint, current_price, "Take-Profit (50%)")
                            continue

                        # EXIT 3: Trailing Stop-Loss
                        if current_price < pos["peak_price"] * 0.80:
                            await portfolio_manager.execute_sell(app, chat_id, mint, current_price, "Trailing Stop-Loss (20% from peak)")
                            continue
                        
                        # EXIT 4: Time-Based
                        entry_time = datetime.fromisoformat(pos["entry_time"].rstrip("Z"))
                        if datetime.utcnow() - entry_time > timedelta(hours=2):
                            buys_5m = data.get("txns", {}).get("m5", {}).get("buys", 0)
                            if buys_5m < 120 or current_liquidity < pos["entry_liquidity"] * 0.70:
                                await portfolio_manager.execute_sell(app, chat_id, mint, current_price, "Time-Based Exit (2hr+ and low volume/liq)")
                                continue

            except Exception as e:
                logger.exception(f"TRADE LOOP: Error in monitoring loop: {e}")
            
            await asyncio.sleep(1) # 1-second interval

async def signal_detection_loop(app: Application, user_manager: UserManager, portfolio_manager: PortfolioManager):
    """Slower loop (15-sec) to find new signals from the overlap file."""
    logger.info("SIGNAL LOOP: Signal detection loop starting.")
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
                    
                    latest = history[-1].get("result", {})
                    grade = latest.get("grade", "NONE")
                    
                    if grade in VALID_GRADES:
                        processed_signals.add(token_id)
                        
                        # Fetch dexscreener data to check signal conditions
                        dex_data = await fetch_dexscreener_data(session, token_id)
                        if not dex_data:
                            continue

                        # SIGNAL FILTERING
                        liquidity = dex_data.get("liquidity", {}).get("usd", 0)
                        buys_5m = dex_data.get("txns", {}).get("m5", {}).get("buys", 0)
                        buys_1h = dex_data.get("txns", {}).get("h1", {}).get("buys", 0)
                        sells_1h = dex_data.get("txns", {}).get("h1", {}).get("sells", 0)
                        ratio_1h = buys_1h / sells_1h if sells_1h > 0 else buys_1h
                        
                        if liquidity >= 30000 and buys_5m >= 150 and ratio_1h >= 1.2:
                            logger.info(f"SIGNAL LOOP: Found valid signal for {token_id}")
                            token_info = {
                                "mint": token_id,
                                "price": float(dex_data["priceUsd"]),
                                "symbol": dex_data["baseToken"]["symbol"],
                                "name": dex_data["baseToken"]["name"]
                            }
                            # Add to watchlist for all trading users
                            for user_id in trading_users:
                                await portfolio_manager.add_to_watchlist(user_id, token_info)

            except Exception as e:
                logger.exception(f"SIGNAL LOOP: Error in signal detection loop: {e}")
            
            await asyncio.sleep(15)