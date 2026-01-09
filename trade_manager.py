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

from config import PORTFOLIOS_FILE, BUCKET_NAME, USE_SUPABASE, DATA_DIR, SIGNAL_FRESHNESS_WINDOW

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
        # Safety flag: only allow uploads if we actually have data or if this is the first save AFTER initialization
        self._is_initialized = len(self.portfolios) > 0
        self.tp_metrics = {
            "calculated_at": None,
            "discovery": {"median_ath": 45.0, "mean_ath": 60.0, "mode_ath": 40.0, "smart_ath": 35.0},
            "alpha": {"median_ath": 50.0, "mean_ath": 70.0, "mode_ath": 45.0, "smart_ath": 40.0}
        }
        self._ensure_portfolio_structure()
        logger.info(f"📈 PortfolioManager initialized. Loaded {len(self.portfolios)} portfolios. (Initialized: {self._is_initialized})")

    async def calculate_tp_metrics_from_daily_files(self):
        """
        Calculate TP metrics (median, mean, mode) from past 3 days of daily files.
        - Only processes tokens matching the signal type (discovery calculates from discovery tokens, alpha from alpha tokens)
        - Includes ALL tokens (wins and losses), just excludes those with ATH ROI = 0
        - Rounds each ATH ROI to nearest multiple of 5 before calculating mode
        - If any metric is 0, defaults to 40%
        - Called at 2 AM UTC and on startup
        """
        if not download_file:
            logger.warning("Supabase not available, using default TP metrics")
            return
        
        try:
            now = datetime.now(timezone.utc)
            ath_roi_by_type = {"discovery": [], "alpha": []}
            files_downloaded = 0
            files_failed = 0
            tokens_processed = {"discovery": 0, "alpha": 0}
            
            logger.info("📥 Starting to download past 3 days of daily files...")
            
            # Collect ATH ROI from past 3 days
            for days_back in range(0, 3):
                check_date = now - timedelta(days=days_back)
                date_str = check_date.strftime('%Y-%m-%d')
                
                for signal_type in ["discovery", "alpha"]:
                    remote_path = f"analytics/{signal_type}/daily/{date_str}.json"
                    local_path = DATA_DIR / f"daily_{date_str}_{signal_type}.json"
                    
                    try:
                        logger.debug(f"Downloading {remote_path}...")
                        result = download_file(str(local_path), remote_path, bucket=BUCKET_NAME)
                        
                        if result is not None:
                            # File was downloaded successfully
                            files_downloaded += 1
                            logger.debug(f"✅ Downloaded {remote_path}")
                            
                            if os.path.exists(local_path):
                                with open(local_path, 'r') as f:
                                    daily_data = json.load(f)
                                    
                                if daily_data and "tokens" in daily_data:
                                    for token in daily_data["tokens"]:
                                        # CRITICAL: Only include tokens matching this signal type
                                        if token.get("signal_type") != signal_type:
                                            continue
                                        
                                        ath_roi = token.get("ath_roi")
                                        if ath_roi is not None and isinstance(ath_roi, (int, float)):
                                            roi_val = float(ath_roi)
                                            # Filter out tokens with 0 ATH ROI, but include all others (wins and losses)
                                            if roi_val != 0:
                                                # Round to nearest multiple of 5
                                                rounded_roi = round(roi_val / 5) * 5
                                                ath_roi_by_type[signal_type].append(rounded_roi)
                                                tokens_processed[signal_type] += 1
                                
                                # Clean up local file to save space
                                try:
                                    os.remove(local_path)
                                except:
                                    pass
                        else:
                            files_failed += 1
                            logger.debug(f"❌ Failed to download {remote_path}")
                    
                    except Exception as e:
                        files_failed += 1
                        logger.debug(f"Error processing {remote_path}: {e}")
            
            logger.info(f"Downloaded {files_downloaded} files, {files_failed} failed | Processed discovery: {tokens_processed['discovery']} tokens, alpha: {tokens_processed['alpha']} tokens")
            
            # Calculate metrics for each signal type
            for signal_type in ["discovery", "alpha"]:
                roi_values = ath_roi_by_type[signal_type]
                
                if roi_values:
                    median_val = statistics.median(roi_values)
                    mean_val = statistics.mean(roi_values)
                    
                    # Calculate Smart ATH (Tail ROI / 25th percentile)
                    # For a list of 1, quantiles fails or is just the value. statistics.quantiles needs n >= 2
                    if len(roi_values) >= 2:
                        smart_val = statistics.quantiles(roi_values, n=4)[0]
                    else:
                        smart_val = roi_values[0]
                    
                    # Round each ATH ROI to nearest multiple of 5 for mode
                    rounded_values = [round(v / 5) * 5 for v in roi_values]
                    try:
                        mode_val = statistics.mode(rounded_values)
                    except statistics.StatisticsError:
                        mode_val = median_val # Fallback if no unique mode
                    
                    # Default if any metric calculated is 0 or negative
                    if median_val <= 0: median_val = 40.0
                    if mean_val <= 0: mean_val = 40.0
                    if mode_val <= 0: mode_val = 40.0
                    if smart_val <= 0: smart_val = 35.0
                    
                    self.tp_metrics[signal_type] = {
                        "median_ath": round(median_val, 1),
                        "mean_ath": round(mean_val, 1),
                        "mode_ath": round(mode_val, 1),
                        "smart_ath": round(smart_val, 1)
                    }
                    
                    logger.info(f"✅ Updated TP metrics for {signal_type}: median={median_val:.1f}%, mean={mean_val:.1f}%, mode={mode_val:.1f}%, smart={smart_val:.1f}% (from {len(roi_values)} tokens)")
                else:
                    logger.warning(f"⚠️ No {signal_type} tokens found in past 3 days. Using defaults.")
                    self.tp_metrics[signal_type] = {
                        "median_ath": 40.0,
                        "mean_ath": 40.0,
                        "mode_ath": 40.0,
                        "smart_ath": 35.0
                    }
            
            self.tp_metrics["calculated_at"] = datetime.now(timezone.utc).isoformat() + "Z"
        
        except Exception as e:
            logger.error(f"Failed to calculate TP metrics: {e}")

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
        
        # Mark as initialized once we've had at least one save (even if empty initially)
        # but only if it was explicitly called (like during init_portfolio)
        if not self._is_initialized and len(self.portfolios) > 0:
            self._is_initialized = True
            
        if USE_SUPABASE and upload_file:
            if not self._is_initialized and len(self.portfolios) == 0:
                logger.warning("⚠️ Skipping Supabase upload for empty/uninitialized portfolio to prevent data loss.")
                return
                
            try:
                upload_file(str(self.file), bucket=BUCKET_NAME, remote_path=f"paper_trade/{self.file.name}")
                logger.debug(f"✅ Synced portfolio to Supabase: {self.file.name}")
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

    def init_portfolio(self, chat_id: str, capital: float):
        """Initialize a new portfolio for a user."""
        chat_id = str(chat_id)
        self.portfolios[chat_id] = {
            "capital_usd": float(capital),
            "starting_capital": float(capital),
            "positions": {},
            "trade_history": [],
            "blacklist": {},
            "stats": {
                "total_trades": 0, "wins": 0, "losses": 0, "total_pnl": 0.0,
                "best_trade": 0.0, "worst_trade": 0.0
            }
        }
        self.save()
        logger.info(f"Initialized portfolio for {chat_id} with ${capital}")

    def reset_portfolio(self, chat_id: str, capital: float):
        """Reset an existing portfolio."""
        self.init_portfolio(chat_id, capital)
        logger.info(f"Reset portfolio for {chat_id} to ${capital}")


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

    async def update_positions_with_live_prices(self, chat_id: str) -> Dict[str, float]:
        """Update all positions with live prices from analytics or Jupiter API.
        Returns a dict of mint -> current_price for all active positions."""
        portfolio = self.get_portfolio(chat_id)
        positions = portfolio.get("positions", {})
        live_prices = {}
        
        if not positions:
            return live_prices
        
        # Download active tracking once
        active_tracking = await self.download_active_tracking()
        
        for key, pos in positions.items():
            if pos.get("status") != "active":
                continue
            
            mint = pos.get("mint")
            signal_type = pos.get("signal_type")
            
            # Try to get price from active tracking
            analytics_key = f"{mint}_{signal_type}"
            data = active_tracking.get(analytics_key)
            
            if data and "current_price" in data:
                live_prices[mint] = float(data["current_price"])
            else:
                # Fallback to Jupiter API
                price = await self.fetch_current_price_fallback(mint)
                if price > 0:
                    live_prices[mint] = price
                else:
                    # Use last known price
                    live_prices[mint] = pos.get("current_price", pos["entry_price"])
        
        return live_prices

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
            token_balance = pos.get("token_amount", 0)
            entry_price = pos.get("entry_price", 0)
            avg_buy_price = pos.get("avg_buy_price", entry_price)
            
            # Skip if missing critical fields
            if not mint or token_balance <= 0 or entry_price <= 0:
                continue
            
            # Use provided live price or fallback to tracked price
            current_price = live_prices.get(mint, pos.get("current_price", entry_price))
            
            unrealized_pnl = token_balance * (current_price - avg_buy_price)
            cost_basis = token_balance * avg_buy_price
            
            unrealized_pct = ((current_price - avg_buy_price) / avg_buy_price) * 100 if avg_buy_price > 0 else 0
            
            total_unrealized_pnl += unrealized_pnl
            total_cost_basis += cost_basis
            
            positions_detail.append({
                "symbol": pos.get("symbol", "UNKNOWN"),
                "mint": mint,
                "signal_type": pos.get("signal_type", "unknown"),
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
        # ML Filtering: Only open positions if ML check passed
        if not token_data.get("ml_passed", False):
            logger.debug(f"⏭️ Skipping position for {token_data.get('mint', '')[:8]}... - ML_PASSED is False")
            return
            
        signal_type = token_data.get("signal_type", "discovery")
        prefs = user_manager.get_user_prefs(chat_id)
        
        # Win Probability Filtering
        ml_prediction = token_data.get("ml_prediction", {})
        probability = ml_prediction.get("probability", 0.0) if isinstance(ml_prediction, dict) else 0.0
        
        min_prob_key = "auto_min_prob_discovery" if signal_type == "discovery" else "auto_min_prob_alpha"
        min_prob_threshold = prefs.get(min_prob_key, 0.0)
        
        if probability < min_prob_threshold:
            logger.info(f"⏭️ [{chat_id}] Skipping trade for {token_data.get('mint', '')[:8]}... - Probability {probability:.2f} < Min {min_prob_threshold:.2f}")
            return
        
        portfolio = self.get_portfolio(chat_id)
        mint = token_data.get("mint")
        
        # Use data provided by analytics_monitoring
        entry_price = token_data.get("price")
        symbol = token_data.get("symbol", "Unknown")
        
        # CRITICAL: Use analytics provided end time
        tracking_end_time = token_data.get("tracking_end_time")
        
        # Fallback only if absolutely necessary
        if not tracking_end_time:
            # Tracking duration based on token age at signal time
            token_age_hours = token_data.get("token_age_hours", 0)
            hours = 168 if token_age_hours >= 12 else 24
            tracking_end_time = (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat() + "Z"

        # Defensive Freshness Check
        # Ensure we don't execute old signals even if they passed upstream checks
        entry_time_str = token_data.get("entry_time")
        if entry_time_str:
            try:
                # Handle standard ISO format with Z
                entry_dt = datetime.fromisoformat(entry_time_str.replace("Z", "+00:00"))
                age_seconds = (datetime.now(timezone.utc) - entry_dt).total_seconds()
                
                if age_seconds > SIGNAL_FRESHNESS_WINDOW:
                    logger.warning(f"🛑 Skipping stale signal in trade manager: {mint} ({age_seconds:.0f}s old > {SIGNAL_FRESHNESS_WINDOW}s limit)")
                    return
            except Exception as e:
                logger.warning(f"⚠️ Failed to parse entry_time for freshness check: {e}")

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
            # Use ATH ROI from tracking data (more accurate than current price)
            # This accounts for the 5-minute upload delay where ATH could have been hit
            other_roi = other_pos.get("ath_roi", 0.0)
            await self.exit_position(chat_id, other_key, "Signal Type Swap 🔄", app, user_manager, exit_roi=other_roi)

        # Validate capital with reserve and min trade size
        capital = portfolio["capital_usd"]
        reserve = prefs.get("reserve_balance", 0.0)
        min_trade = prefs.get("min_trade_size", 10.0)
        
        available = capital - reserve
        if available < min_trade:
            logger.info(f"Skipping trade - Available ${available:.2f} < Min ${min_trade:.2f}")
            return
        
        # Size: Respect user's trade_size_mode and trade_size_value settings
        trade_size_mode = prefs.get("trade_size_mode", "percent")
        trade_size_value = prefs.get("trade_size_value", 10)
        
        if trade_size_mode == "fixed":
            # Fixed dollar amount per trade
            size_usd = float(trade_size_value)
        else:
            # Percentage-based
            size_usd = available * (float(trade_size_value) / 100)
        
        # Enforce minimum trade size
        if size_usd < min_trade:
            size_usd = min_trade
        
        # Enforce maximum
        size_usd = min(size_usd, 150.0)
        
        # Final check: ensure we have enough capital
        if size_usd > available:
            logger.info(f"Skipping trade - Trade size ${size_usd:.2f} exceeds available ${available:.2f}")
            return
        
        token_amount = size_usd / entry_price
        
        # Get TP target for auto trades - supports mean/median/mode/custom percentage
        # Priority 1: Signal-type specific overrides (tp_discovery or tp_alpha)
        # Priority 2: Global tp_preference (median, mean, mode, or custom percentage)
        # Priority 3: Default based on signal type
        
        tp_target = None
        
        # Check for signal-type specific overrides first (highest priority)
        if signal_type == "discovery" and "tp_discovery" in prefs and prefs["tp_discovery"] is not None:
            override_val = prefs["tp_discovery"]
            # Try to convert to float first (fixed percentage)
            try:
                tp_target = float(override_val)
            except (ValueError, TypeError):
                # It's a string like "mode", "mean", "median", "smart" - look up from metrics
                if override_val in ["median", "mean", "mode", "smart"]:
                    tp_target = self.tp_metrics.get(signal_type, {}).get(f"{override_val}_ath", None)
        
        elif signal_type == "alpha" and "tp_alpha" in prefs and prefs["tp_alpha"] is not None:
            override_val = prefs["tp_alpha"]
            # Try to convert to float first (fixed percentage)
            try:
                tp_target = float(override_val)
            except (ValueError, TypeError):
                # It's a string like "mode", "mean", "median" - look up from metrics
                if override_val in ["median", "mean", "mode"]:
                    tp_target = self.tp_metrics.get(signal_type, {}).get(f"{override_val}_ath", None)
        
        # If no override, use global tp_preference
        if tp_target is None:
            tp_preference = prefs.get("tp_preference", "median")
            
            if tp_preference == "median":
                # Use median ATH from historical metrics
                tp_target = self.tp_metrics.get(signal_type, {}).get("median_ath", 50.0)
            elif tp_preference == "mean":
                # Use mean ATH from historical metrics
                tp_target = self.tp_metrics.get(signal_type, {}).get("mean_ath", 60.0)
            elif tp_preference == "mode":
                # Use mode (most frequent) ATH from historical metrics
                tp_target = self.tp_metrics.get(signal_type, {}).get("mode_ath", 40.0)
            elif tp_preference == "smart":
                # Use smart (Tail ROI) ATH from historical metrics
                tp_target = self.tp_metrics.get(signal_type, {}).get("smart_ath", 35.0)
            else:
                # Assume it's a fixed percentage
                try:
                    tp_target = float(tp_preference)
                except (ValueError, TypeError):
                    # Fallback to defaults if invalid
                    tp_target = 45.0 if signal_type == "discovery" else 50.0
        
        # Final safety check
        if tp_target is None or tp_target <= 0:
            tp_target = 45.0 if signal_type == "discovery" else 50.0
        
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
            # NOTE: sl_used intentionally NOT set for automatic signal-based trades.
            # Users can manually set SL via /set_sl command if desired.
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
            "avg_buy_price": entry_price,
            "ml_passed": token_data.get("ml_passed", False)
        }
        
        portfolio["capital_usd"] -= size_usd
        self.save()

        # Notification
        ml_action = token_data.get("ml_prediction", {}).get("action", "N/A")
        grade = token_data.get("grade", "N/A")
        
        # Get default SL if available
        default_sl = prefs.get("default_sl")
        sl_display = f"{abs(default_sl):.0f}%" if default_sl else "Not set"
        
        # Determine TP source for display
        tp_source = "default"
        
        # Check for signal-type specific overrides first
        if signal_type == "discovery" and "tp_discovery" in prefs and prefs["tp_discovery"] is not None:
            override_val = prefs["tp_discovery"]
            if override_val in ["median", "mean", "mode", "smart"]:
                tp_source = f"{override_val} ATH (discovery override)"
            else:
                try:
                    float(override_val)
                    tp_source = "discovery override"
                except:
                    tp_source = "discovery override"
        
        elif signal_type == "alpha" and "tp_alpha" in prefs and prefs["tp_alpha"] is not None:
            override_val = prefs["tp_alpha"]
            if override_val in ["median", "mean", "mode", "smart"]:
                tp_source = f"{override_val} ATH (alpha override)"
            else:
                try:
                    float(override_val)
                    tp_source = "alpha override"
                except:
                    tp_source = "alpha override"
        
        # If no override, check global preference
        elif "tp_preference" in prefs:
            pref = prefs["tp_preference"]
            if pref in ["median", "mean", "mode", "smart"]:
                tp_source = f"{pref} ATH"
            else:
                try:
                    float(pref)
                    tp_source = "custom"
                except:
                    tp_source = pref
        
        msg = (
            f"🟢 <b>PAPER TRADE OPENED</b>\n\n"
            f"<b>Token:</b> {symbol}\n"
            f"<b>Type:</b> {signal_type.upper()}\n"
            f"<b>Grade:</b> {grade}\n"
            f"<b>Entry:</b> ${entry_price:.8f}\n"
            f"<b>Size:</b> ${size_usd:.2f}\n"
            f"<b>Target TP:</b> +{tp_target:.0f}% ({tp_source})\n"
            f"<b>Stop Loss:</b> {sl_display}\n"
            # f"<b>ML Signal:</b> {ml_action}\n"
        )
        
        try:
            if prefs.get("trade_notifications_enabled", True):
                await app.bot.send_message(chat_id=chat_id, text=msg, parse_mode="HTML")
                logger.info(f"✅ [{chat_id}] Opened position: {symbol}")
            else:
                logger.info(f"🔇 [{chat_id}] Opened position (notifications disabled): {symbol}")
        except Exception:
            pass

    # --- EXIT LOGIC ---

    async def exit_position(self, chat_id: str, position_key: str, reason: str, app: Application, 
                           user_manager, exit_roi: float = 0.0):
        """Execute exit, update stats, remove position."""
        portfolio = self.get_portfolio(chat_id)
        pos = portfolio["positions"].get(position_key)
        if not pos: return

        # Get investment amount - calculate if missing for legacy positions
        if "investment_usd" in pos:
            investment = pos["investment_usd"]
        else:
            # Calculate from token amount and entry price
            investment = pos.get("token_amount", 0) * pos.get("entry_price", 0)
        
        # If still can't determine investment, skip this position
        if investment <= 0:
            logger.warning(f"Cannot determine investment for position {position_key}, skipping exit")
            return
        
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

        # History - Calculate hold duration
        # Robust timezone handling: strip all potential suffixes and add single UTC
        entry_time_str = pos.get("entry_time")
        if not entry_time_str:
            # If entry_time is missing, use current time as fallback
            entry_time = datetime.now(timezone.utc)
        else:
            # Remove Z and +00:00 (handle multiple occurrences)
            entry_time_str = entry_time_str.replace("Z", "").replace("+00:00", "")
            # Re-append single UTC timezone
            entry_time_str = entry_time_str + "+00:00"
            entry_time = datetime.fromisoformat(entry_time_str)
        exit_time = datetime.now(timezone.utc)
        hold_duration = exit_time - entry_time
        hold_duration_minutes = int(hold_duration.total_seconds() / 60)
        
        history_item = {
            "symbol": pos.get("symbol", "UNKNOWN"),
            "entry_price": pos.get("entry_price", 0),
            "exit_reason": reason,
            "pnl_usd": pnl_usd,
            "pnl_percent": exit_roi,
            "exit_time": exit_time.isoformat() + "Z",
            "signal_type": pos.get("signal_type", "unknown"),
            "hold_duration_minutes": hold_duration_minutes
        }
        portfolio["trade_history"].append(history_item)
        
        self.save()

        # Notify
        emoji = "🟢" if pnl_usd > 0 else "🔴"
        
        # Get TP/SL info
        tp = pos.get('tp_used')
        sl = pos.get('sl_used')
        tp_display = f"+{float(tp):.0f}%" if tp else "N/A"
        sl_display = f"{float(sl):.0f}%" if sl else "N/A"
        
        msg = (
            f"{emoji} <b>PAPER TRADE CLOSED</b>\n\n"
            f"<b>Token:</b> {pos.get('symbol', 'UNKNOWN')}\n"
            f"<b>Reason:</b> {reason}\n"
            f"<b>ROI:</b> {exit_roi:+.2f}%\n"
            f"<b>P/L:</b> ${pnl_usd:+.2f}\n"
            f"<b>Targets:</b> TP: {tp_display}, SL: {sl_display}\n"
            f"<b>Capital:</b> ${portfolio['capital_usd']:,.2f}"
        )
        try:
            # Check user preference for trade notifications
            prefs = user_manager.get_user_prefs(chat_id)
            if prefs.get("trade_notifications_enabled", True):
                await app.bot.send_message(chat_id=chat_id, text=msg, parse_mode="HTML")
                logger.info(f"✅ [{chat_id}] Notified trade close: {pos.get('symbol')}")
            else:
                logger.info(f"🔇 [{chat_id}] Trade closed (notifications disabled): {pos.get('symbol')}")
        except Exception as e:
            logger.error(f"Error sending trade close notification: {e}")
            pass

    def add_manual_position(self, chat_id: str, mint: str, symbol: str, price: float, amount_usd: float, tp_percent: float = 50.0, sl_percent: float = None, token_age_hours: float = None) -> bool:
        """Add a manually purchased position.
        
        Args:
            sl_percent: Stop loss percentage. If None, no SL is set and user must set it manually via /set_sl.
                       If provided, SL will be set to -sl_percent (negative value).
            token_age_hours: Age of token in hours at time of trade execution.
                            If None, will try to estimate or default to 24 hours.
        """
        portfolio = self.get_portfolio(chat_id)
        
        # Deduct capital
        if portfolio["capital_usd"] < amount_usd:
            return False
            
        portfolio["capital_usd"] -= amount_usd
        
        token_amount = amount_usd / price if price > 0 else 0
        
        position_key = f"{mint}_manual"
        
        # If position exists, average down/up
        if position_key in portfolio["positions"]:
            pos = portfolio["positions"][position_key]
            total_tokens = pos["token_amount"] + token_amount
            total_cost = (pos["token_amount"] * pos["entry_price"]) + amount_usd
            avg_price = total_cost / total_tokens
            
            pos["token_amount"] = total_tokens
            pos["entry_price"] = avg_price
            pos["avg_buy_price"] = avg_price
            pos["status"] = "active"
        else:
            # Create new position
            # Default token age to 24 hours if not provided
            if token_age_hours is None:
                token_age_hours = 24
            
            position_dict = {
                "mint": mint,
                "symbol": symbol,
                "signal_type": "manual",
                "entry_price": price,
                "avg_buy_price": price,
                "token_amount": token_amount,
                "entry_time": datetime.now(timezone.utc).isoformat() + "Z",
                "tracking_end_time": (datetime.now(timezone.utc) + timedelta(days=365)).isoformat() + "Z",
                "status": "active",
                "tp_used": float(tp_percent),
                "current_price": price,
                "current_roi": 0.0,
                "ath_price": price,
                "ath_roi": 0.0,
                "last_updated": datetime.now(timezone.utc).isoformat() + "Z",
                "manual_token_age_hours": float(token_age_hours)
            }
            
            # Only add sl_used if sl_percent is provided
            if sl_percent is not None:
                position_dict["sl_used"] = -abs(float(sl_percent))  # Ensure negative
            
            portfolio["positions"][position_key] = position_dict
            
        self.save()
        return True
    
    async def check_and_exit_positions(self, chat_id: str, app: Application, user_manager, active_tracking: Optional[Dict[str, Any]] = None):
        """
        Check analytics data and exit positions if TP hit or tracking ended.
        Updates position data with live analytics values.
        
        Args:
            chat_id: User chat ID
            app: Telegram application
            user_manager: UserManager instance for getting user preferences
            active_tracking: Optional pre-downloaded active_tracking data for efficiency
        """
        portfolio = self.get_portfolio(chat_id)
        
        # Download active tracking only if not provided
        if active_tracking is None:
            active_tracking = await self.download_active_tracking()
        
        now = datetime.now(timezone.utc)

        for key, pos in list(portfolio["positions"].items()):
            mint = pos.get("mint")
            signal_type = pos.get("signal_type")
            user_tp = pos.get("tp_used", 50.0)  # Default to 50% if not set
            
            # Skip positions with missing critical fields
            if not mint or not signal_type:
                logger.warning(f"Skipping position {key} - missing mint or signal_type")
                continue
            
            # Find in analytics
            analytics_key = f"{mint}_{signal_type}"
            data = active_tracking.get(analytics_key)
            
            # Get user's default SL if no position-specific SL is set
            prefs = user_manager.get_user_prefs(str(chat_id))
            position_sl = pos.get("sl_used")
            if position_sl is None:
                # Apply user's default SL if they have one set
                position_sl = prefs.get("default_sl")
            
            # --- 1. UPDATE LIVE DATA ---
            if signal_type == "manual":
                # For manual trades, respect token age-based fetch intervals
                # Tokens < 12 hours old: fetch every 5 seconds
                # Tokens >= 12 hours old: fetch every 4 minutes
                
                from alerts.price_fetcher import PriceFetcher
                
                entry_time_str = pos.get("entry_time")
                if not entry_time_str:
                    logger.warning(f"Position {key} missing entry_time, skipping update")
                    continue
                    
                entry_time = datetime.fromisoformat(entry_time_str.rstrip("Z")).replace(tzinfo=timezone.utc)
                time_since_entry = datetime.now(timezone.utc) - entry_time
                
                # Determine token age at entry time (store if not present)
                if "manual_token_age_hours" not in pos:
                    # If not explicitly stored, we can try to estimate
                    # In the future, this should be passed from the buy command
                    pos["manual_token_age_hours"] = 24  # Default assumption
                
                token_age_hours = pos.get("manual_token_age_hours", 24)
                
                # Determine fetch interval based on token age
                if token_age_hours < 12:
                    fetch_interval = 5  # 5 seconds for young tokens
                else:
                    fetch_interval = 240  # 4 minutes for older tokens
                
                # Check if enough time has passed since last update
                last_updated_str = pos.get("last_updated", pos.get("entry_time"))
                last_updated = datetime.fromisoformat(last_updated_str.rstrip("Z")).replace(tzinfo=timezone.utc)
                time_since_update = (datetime.now(timezone.utc) - last_updated).total_seconds()
                
                # Only fetch if enough time has passed
                if time_since_update >= fetch_interval:
                    token_info = await PriceFetcher.get_token_info(mint)
                    if token_info:
                        pos["current_price"] = token_info["price"]
                        pos["last_updated"] = datetime.now(timezone.utc).isoformat() + "Z"
                        
                        # Calculate ROI
                        entry_price = pos["entry_price"]
                        if entry_price > 0:
                            current_roi = ((pos["current_price"] - entry_price) / entry_price) * 100
                            pos["current_roi"] = current_roi
                            
                            # Update ATH
                            if pos["current_price"] > pos.get("ath_price", 0):
                                pos["ath_price"] = pos["current_price"]
                                pos["ath_roi"] = current_roi
            elif data:
                pos["current_price"] = data.get("current_price", pos["current_price"])
                pos["current_roi"] = data.get("current_roi", pos["current_roi"])
                pos["ath_price"] = data.get("ath_price", pos["ath_price"])
                pos["ath_roi"] = data.get("ath_roi", pos["ath_roi"])
                pos["last_updated"] = datetime.now(timezone.utc).isoformat() + "Z"
            
                        # --- 2. TP/SL CHECK ---
            # Check actual ATH from analytics against user TP
            ath_roi = float(pos.get("ath_roi", 0))
            current_roi = float(pos.get("current_roi", 0))
            
            # TP Check
            if ath_roi >= user_tp:
                # Exit at the actual peak recorded
                await self.exit_position(chat_id, key, "TP Hit 🎯", app, user_manager, exit_roi=ath_roi)
                continue
                
            # SL Check - apply position SL if set (either explicit or from user default)
            if position_sl is not None:
                sl_threshold = float(position_sl)
                if current_roi <= sl_threshold:
                    # Exit at current ROI
                    await self.exit_position(chat_id, key, "SL Hit 🛑", app, user_manager, exit_roi=current_roi)
                    continue
            
            # --- 3. EXPIRY CHECK ---
            tracking_end_time_str = pos.get("tracking_end_time")
            if not tracking_end_time_str:
                # If no tracking end time, skip expiry check for this position
                continue
                
            end_time = datetime.fromisoformat(tracking_end_time_str.rstrip("Z")).replace(tzinfo=timezone.utc)
            
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
                
                await self.exit_position(chat_id, key, "Tracking Ended ⏱️", app, user_manager, exit_roi=current_roi)

        self.save()