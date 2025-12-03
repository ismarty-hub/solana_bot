#!/usr/bin/env python3
"""
alerts/trade_monitor.py - Background monitoring for paper trade positions

This module handles automated position monitoring:
- Checks for TP hits
- Handles tracking expiry
- Updates position data silently (no notifications except trade open/close)
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from telegram.ext import Application

from config import DATA_DIR, BUCKET_NAME, USE_SUPABASE

logger = logging.getLogger(__name__)

# Import download_file for active tracking
try:
    from supabase_utils import download_file
except ImportError:
    download_file = None

ACTIVE_TRACKING_FILE = DATA_DIR / "active_tracking.json"
TRADE_MONITOR_INTERVAL = 60  # 60 seconds


async def download_active_tracking() -> dict:
    """Download active_tracking.json from Supabase."""
    if not USE_SUPABASE or not download_file:
        return {}
    
    remote_path = "analytics/active_tracking.json"
    try:
        if download_file(str(ACTIVE_TRACKING_FILE), remote_path, bucket=BUCKET_NAME):
            import json
            with open(ACTIVE_TRACKING_FILE, 'r') as f:
                data = json.load(f)
                logger.debug(f"Downloaded active_tracking.json with {len(data)} tokens")
                return data
    except Exception as e:
        logger.error(f"Failed to download active_tracking.json: {e}")
    return {}


async def trade_monitoring_loop(app: Application, user_manager, portfolio_manager):
    """
    Background loop to monitor all trading users' positions.
    
    - Downloads active_tracking.json ONCE per cycle
    - Iterates through all trading users
    - Checks for TP hits and expiry
    - Does NOT send PnL updates (only trade open/close notifications)
    """
    logger.info("📊 Trade monitoring loop started!")
    logger.info(f"⏰ Checking positions every {TRADE_MONITOR_INTERVAL} seconds")
    
    # Initial delay to let bot settle
    await asyncio.sleep(5)
    
    while True:
        try:
            # Download active tracking ONCE for this cycle
            active_tracking = await download_active_tracking()
            
            # Get all users with paper trading enabled
            trading_users = user_manager.get_trading_users()
            
            if not trading_users:
                logger.debug("No trading users found")
                await asyncio.sleep(TRADE_MONITOR_INTERVAL)
                continue
            
            logger.debug(f"Monitoring {len(trading_users)} trading users")
            
            # Check positions for each user
            for chat_id in trading_users:
                try:
                    portfolio = portfolio_manager.get_portfolio(chat_id)
                    positions = portfolio.get("positions", {})
                    
                    # Only process users with active positions
                    if positions:
                        # Pass shared active_tracking data to avoid redundant downloads
                        await portfolio_manager.check_and_exit_positions(
                            chat_id, 
                            app, 
                            active_tracking=active_tracking
                        )
                except Exception as e:
                    logger.exception(f"Error monitoring positions for user {chat_id}: {e}")
                    continue
            
            # Wait before next cycle
            await asyncio.sleep(TRADE_MONITOR_INTERVAL)
            
        except Exception as e:
            logger.exception(f"Trade monitoring loop error: {e}")
            await asyncio.sleep(TRADE_MONITOR_INTERVAL)
