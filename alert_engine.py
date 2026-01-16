#!/usr/bin/env python3
"""
alert_engine.py
Standalone engine for processing and sending Telegram alerts (Discovery & Alpha).
"""

import asyncio
import logging
import os
from shared.engine_utils import get_standalone_app, initialize_logging
from alerts.monitoring import background_loop, monthly_expiry_notifier
from alerts.alpha_monitoring import alpha_monitoring_loop
from alerts.user_manager import UserManager
from trade_manager import PortfolioManager
from config import USER_PREFS_FILE, USER_STATS_FILE, PORTFOLIOS_FILE

# Logger initialized with specific name
logger = initialize_logging("AlertEngine")

async def main():
    logger.info("Initializing Standalone Alert Engine...")
    
    # Initialize managers
    user_manager = UserManager(USER_PREFS_FILE, USER_STATS_FILE)
    portfolio_manager = PortfolioManager(PORTFOLIOS_FILE)
    
    # Create app instance
    app = get_standalone_app()
    
    async with app:
        logger.info("Starting background loops...")
        
        # 1. Discovery alerts loop
        asyncio.create_task(background_loop(app, user_manager, portfolio_manager))
        
        # 2. Alpha alerts loop
        asyncio.create_task(alpha_monitoring_loop(app, user_manager))
        
        # 3. Monthly expiry notifier
        asyncio.create_task(monthly_expiry_notifier(app, user_manager))
        
        logger.info("Alert Engine is active and monitoring signals.")
        
        # Keep process alive
        while True:
            await asyncio.sleep(3600)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Alert Engine stopped manually.")
    except Exception as e:
        logger.exception(f"Alert Engine failed: {e}")
