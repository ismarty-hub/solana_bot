#!/usr/bin/env python3
"""
trade_engine.py
Standalone engine for processing trade signals and monitoring positions (Paper Trading).
"""

import asyncio
import logging
import os
from shared.engine_utils import get_standalone_app, initialize_logging
from alerts.analytics_monitoring import active_tracking_signal_loop
from alerts.trade_monitor import trade_monitoring_loop
from alerts.monitoring import tp_metrics_update_loop
from alerts.user_manager import UserManager
from alerts.portfolio_manager import PortfolioManager
from config import USER_PREFS_FILE, USER_STATS_FILE, PORTFOLIOS_FILE

# Set tag for isolation logging
os.environ["IS_ISOLATED_ENGINE"] = "TRADE"

logger = initialize_logging("TradeEngine")

async def main():
    logger.info("Initializing Standalone Trade Engine...")
    
    # Initialize managers
    user_manager = UserManager(USER_PREFS_FILE, USER_STATS_FILE)
    portfolio_manager = PortfolioManager(PORTFOLIOS_FILE)
    
    # Create app instance
    app = get_standalone_app()
    
    async with app:
        logger.info("Starting trade background loops...")
        
        # 1. Trade signal detection & entry
        asyncio.create_task(active_tracking_signal_loop(app, user_manager, portfolio_manager))
        
        # 2. Position monitoring (Exits, TP, SL)
        asyncio.create_task(trade_monitoring_loop(app, user_manager, portfolio_manager))
        
        # 3. TP Metrics update (Stats)
        asyncio.create_task(tp_metrics_update_loop(portfolio_manager))
        
        logger.info("Trade Engine is active and Monitoring positions.")
        
        # Keep process alive
        while True:
            await asyncio.sleep(3600)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Trade Engine stopped manually.")
    except Exception as e:
        logger.exception(f"Trade Engine failed: {e}")
