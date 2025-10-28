#!/usr/bin/env python3
"""
alerts/alpha_monitoring.py - Background monitoring for overlap_results_alpha.pkl
"""

import asyncio
import logging
import joblib
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

from telegram.ext import Application
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

# --- Configuration ---
from config import (
    DATA_DIR, BUCKET_NAME, USE_SUPABASE
)

# Define new state/config files
ALPHA_POLL_INTERVAL_SECS = 30  # Poll every 30 seconds as requested
ALPHA_OVERLAP_FILE = Path(DATA_DIR) / "overlap_results_alpha.pkl"
ALPHA_ALERTS_STATE_FILE = Path(DATA_DIR) / "alerts_state_alpha.json"

# --- NEW: Supabase remote file name for alpha state ---
ALPHA_ALERTS_STATE_REMOTE = "alerts_state_alpha.json"

# --- Imports ---
from shared.file_io import safe_load, safe_save
from alerts.user_manager import UserManager
from alerts.formatters import format_alpha_alert

# --- CORRECTED IMPORT: Now importing both download and generic upload functions ---
# Import the download and upload function from supabase_utils
try:
    from supabase_utils import download_alpha_overlap_results, upload_file
    logger = logging.getLogger(__name__)
    logger.info("✅ Successfully imported download_alpha_overlap_results and upload_file")
except ImportError:
    logger = logging.getLogger(__name__)
    logger.error("❌ FAILED to import required functions from supabase_utils!")
    download_alpha_overlap_results = None
    upload_file = None
# --- END CORRECTED IMPORT ---


def load_latest_alpha_tokens() -> Dict[str, Any] | None:
    """Load the latest alpha token data from the local PKL file."""
    if not ALPHA_OVERLAP_FILE.exists():
        logger.warning(f"File not found: {ALPHA_OVERLAP_FILE}. Attempting download...")
        if USE_SUPABASE and download_alpha_overlap_results:
            if download_alpha_overlap_results(str(ALPHA_OVERLAP_FILE), bucket=BUCKET_NAME):
                logger.info("✅ Downloaded alpha overlap file successfully.")
            else:
                logger.warning("❌ Failed to download alpha overlap file from Supabase.")
                return None
        else:
            return None

    try:
        # joblib is used for .pkl files
        with open(ALPHA_OVERLAP_FILE, 'rb') as f:
            data = joblib.load(f)
        return data
    except Exception as e:
        logger.error(f"❌ Failed to load alpha overlap data from PKL: {e}")
        return None

async def send_alpha_alert(app: Application, user_manager: UserManager, mint: str, entry: Dict[str, Any], alerted_tokens: Dict[str, Any]):
    """Format and send the alpha alert to all subscribed users."""
    try:
        # Get the latest entry from the history list
        latest_data = entry[-1]
        
        # Determine the users who are subscribed to alpha alerts
        alpha_subscribers = user_manager.get_alpha_subscribers()
        
        if not alpha_subscribers:
            logger.info(f"No alpha subscribers to notify for {mint}.")
            # Even if no one is subscribed, we still mark it as alerted to prevent re-alerting
            alerted_tokens[mint] = {"ts": datetime.now().isoformat(), "sent": False}
            return

        # Prepare the message and keyboard
        alert_msg = format_alpha_alert(latest_data)
        
        # Extract symbol for the keyboard button
        symbol = latest_data.get("result", {}).get("symbol", "TOKEN")
        
        keyboard = [
            [InlineKeyboardButton(f"🔄 Refresh Price ({symbol})", callback_data=f"alpha_refresh_{mint}")]
        ]

        # Send to all subscribers
        for chat_id in alpha_subscribers:
            try:
                await app.bot.send_message(
                    chat_id=chat_id,
                    text=alert_msg,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.warning(f"Failed to send alpha alert to {chat_id}: {e}")
                
        # Update the state to mark this token as alerted
        alerted_tokens[mint] = {"ts": datetime.now().isoformat(), "sent": True}

    except Exception as e:
        logger.error(f"❌ Error sending alpha alert for {mint}: {e}")
        # Mark as attempted even on error to avoid infinite loop
        alerted_tokens[mint] = {"ts": datetime.now().isoformat(), "sent": False}


async def alpha_monitoring_loop(app: Application, user_manager: UserManager):
    """
    Main background loop for alpha alert monitoring.
    Checks the overlap results file for new tokens periodically.
    """
    logger.info(f"🔄 Starting Alpha Monitoring Loop (Interval: {ALPHA_POLL_INTERVAL_SECS}s)")

    while True:
        try:
            # 1. Ensure the necessary files are available (and download if possible)
            if USE_SUPABASE and download_alpha_overlap_results:
                download_alpha_overlap_results(str(ALPHA_OVERLAP_FILE), bucket=BUCKET_NAME)
            
            # If local file is still missing after potential download, skip
            if not ALPHA_OVERLAP_FILE.exists():
                logger.warning(f"Alpha overlap file {ALPHA_OVERLAP_FILE} missing, skipping cycle.")
                await asyncio.sleep(ALPHA_POLL_INTERVAL_SECS)
                continue
            
            # 2. Load the local file
            latest_tokens = load_latest_alpha_tokens()
            if not latest_tokens:
                await asyncio.sleep(ALPHA_POLL_INTERVAL_SECS)
                continue
                
            # 3. Load the state of already-alerted tokens
            alerted_tokens = safe_load(ALPHA_ALERTS_STATE_FILE, {})
            
            new_tokens_found = False
            
            # 4. Process all tokens from the file
            for mint, entry in latest_tokens.items():
                if mint not in alerted_tokens:
                    logger.info(f"🚀 New Alpha Token Detected: {mint}")
                    new_tokens_found = True
                    
                    # 5. Send the alert and update the state (in-memory)
                    await send_alpha_alert(
                        app, user_manager, mint, entry, alerted_tokens
                    )
                    
            # 6. Save state back to disk and upload if new tokens were processed
            if new_tokens_found:
                logger.info(f"💾 Saving alpha alerts state with {len(alerted_tokens)} tokens.")
                safe_save(ALPHA_ALERTS_STATE_FILE, alerted_tokens)
                
                # --- NEW: Upload the state file to Supabase ---
                if USE_SUPABASE and upload_file:
                    logger.info("☁️ Uploading alerts_state_alpha.json to Supabase.")
                    upload_file(
                        str(ALPHA_ALERTS_STATE_FILE), 
                        BUCKET_NAME, 
                        ALPHA_ALERTS_STATE_REMOTE, 
                        debug=False
                    )
                # --- END NEW ---

        except Exception as e: 
            logger.exception(f"❌ Error in alpha monitoring loop: {e}")
        
        await asyncio.sleep(ALPHA_POLL_INTERVAL_SECS) 