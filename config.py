#!/usr/bin/env python3
"""
config.py

Centralized configuration for the Solana Token Alert Bot.
"""

import os
import platform
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ----------------------
# Environment Detection
# ----------------------
IS_RAILWAY = os.getenv('RAILWAY_ENVIRONMENT') is not None
IS_LOCAL = platform.system() in ['Windows', 'Darwin'] and not IS_RAILWAY

# ----------------------
# Data Directory
# ----------------------
if IS_RAILWAY:
    DATA_DIR = Path("/data")
    POLL_INTERVAL_SECS = int(os.getenv("POLL_INTERVAL_SECS", "300"))  # 5 min for Railway
else:
    DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
    POLL_INTERVAL_SECS = int(os.getenv("POLL_INTERVAL_SECS", "60"))

DATA_DIR.mkdir(parents=True, exist_ok=True)

# ----------------------
# File Paths
# ----------------------
OVERLAP_FILE = DATA_DIR / "overlap_results.pkl"
USER_PREFS_FILE = DATA_DIR / "bot_user_prefs.pkl"
USER_STATS_FILE = DATA_DIR / "bot_user_stats.pkl"
ALERTS_STATE_FILE = DATA_DIR / "bot_alerts_state.pkl"

# ----------------------
# Bot Configuration
# ----------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_USER_ID = os.getenv("ADMIN_USER_ID")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required")

# ----------------------
# Alert Grades
# ----------------------
ALL_GRADES = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
VALID_GRADES = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]

# ----------------------
# Supabase Configuration
# ----------------------
BUCKET_NAME = os.getenv("SUPABASE_BUCKET", "monitor-data")

# Supabase feature flags (opt-in)
USE_SUPABASE = True
DOWNLOAD_OVERLAP_ON_STARTUP = True
SUPABASE_DAILY_SYNC = True

# ----------------------
# Optional Supabase Imports
# ----------------------
try:
    from supabase_utils import (
        download_overlap_results,
        upload_file,
        download_file,
    )
except Exception:
    download_overlap_results = None
    upload_file = None
    download_file = None

# ----------------------
# Logging Configuration
# ----------------------
import logging

# Configure logging based on environment
if IS_LOCAL:
    # Enable logging for local development
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
else:
    # Production: reduce logging noise but keep important messages
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

# Optionally silence specific noisy loggers
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)