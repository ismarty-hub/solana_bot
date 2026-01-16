#!/usr/bin/env python3
"""
shared/engine_utils.py
Common initialization for standalone alert and trade engines.
"""

import logging
import os
from telegram.ext import Application
from config import BOT_TOKEN

logger = logging.getLogger(__name__)

def get_standalone_app() -> Application:
    """
    Initialize a Telegram Application instance without polling.
    Suitable for scripts that only need to send messages.
    """
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN not found in environment")
        
    app = Application.builder().token(BOT_TOKEN).build()
    return app

def initialize_logging(name: str):
    """Setup standard logging for standalone scripts."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    # Silence noisy loggers
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('httpcore').setLevel(logging.WARNING)
    logging.getLogger('telegram').setLevel(logging.WARNING)
    
    return logging.getLogger(name)
