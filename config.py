#!/usr/bin/env python3
"""
config.py - Configuration for Render deployment
"""

import os
import logging
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ----------------------
# Environment Detection
# ----------------------
IS_RENDER = os.getenv('RENDER') is not None
IS_LOCAL = not IS_RENDER

# ----------------------
# Data Directory (Render persistent disk)
# ----------------------
if IS_RENDER:
    # Render persistent disk mount point
    DATA_DIR = Path("/opt/render/project/data")
    POLL_INTERVAL_SECS = 60  # 1 minute for Render
else:
    # Local development
    DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
    POLL_INTERVAL_SECS = 60  # 1 minute local too

DATA_DIR.mkdir(parents=True, exist_ok=True)

# ----------------------
# File Paths
# ----------------------
OVERLAP_FILE = DATA_DIR / "overlap_results.pkl"
USER_PREFS_FILE = DATA_DIR / "bot_user_prefs.pkl"
USER_STATS_FILE = DATA_DIR / "bot_user_stats.pkl"
ALERTS_STATE_FILE = DATA_DIR / "bot_alerts_state.json"
ALPHA_ALERTS_STATE_FILE = DATA_DIR / "alerts_state_alpha.json"
GROUPS_FILE = DATA_DIR / "bot_groups.pkl"  
PORTFOLIOS_FILE = DATA_DIR / "bot_portfolios.pkl"
ACTIVATION_CODES_FILE = DATA_DIR / "activation_codes.json"

# ----------------------
# Analytics Tracking Configuration
# ----------------------
ACTIVE_TRACKING_FILE = DATA_DIR / "active_tracking.json"
ANALYTICS_POLL_INTERVAL = 300  # 5 minutes
SIGNAL_FRESHNESS_WINDOW = 420  # 7 minutes - only execute signals this fresh

# ----------------------
# Bot Configuration
# ----------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
raw_admins = os.getenv("ADMIN_USER_ID", "")
# Parse comma-separated string into list of integers
ADMIN_USER_ID = [int(x.strip()) for x in raw_admins.split(",") if x.strip().replace("-", "").isdigit()]

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required")

# ----------------------
# ML API Configuration (NEW)
# ----------------------
FASTAPI_ML_URL = os.getenv("ML_API_URL")
ML_API_TIMEOUT = int(os.getenv("ML_API_TIMEOUT", "30"))  # seconds

# ----------------------
# Alert Grades
# ----------------------
ALL_GRADES = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
VALID_GRADES = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]

# ----------------------
# Supabase Configuration
# ----------------------
BUCKET_NAME = os.getenv("SUPABASE_BUCKET", "monitor-data")

# 🔥 ALWAYS USE SUPABASE (no flags, just use it!)
USE_SUPABASE = True
DOWNLOAD_OVERLAP_ON_STARTUP = True
SUPABASE_DAILY_SYNC = True

# ----------------------
# Logging Configuration
# ----------------------
if IS_LOCAL:
    # Local - detailed logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
else:
    # Render - important logs only
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

# Enable monitoring logs (critical!)
logging.getLogger("alerts.monitoring").setLevel(logging.INFO)
logging.getLogger("bot").setLevel(logging.INFO)

# Silence noisy libraries
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)

print(f"✅ Config loaded: IS_RENDER={IS_RENDER}, DATA_DIR={DATA_DIR}, POLL={POLL_INTERVAL_SECS}s")
print(f"🤖 ML API configured: {FASTAPI_ML_URL}")