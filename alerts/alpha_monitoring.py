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

# --- Imports ---
from shared.file_io import safe_load, safe_save
from alerts.user_manager import UserManager
from alerts.formatters import format_alpha_alert

# --- CORRECTED IMPORT ---
# Import the download function from supabase_utils
try:
    from supabase_utils import download_alpha_overlap_results
    logger = logging.getLogger(__name__)
    logger.info("✅ Successfully imported download_alpha_overlap_results")
except ImportError:
    logger = logging.getLogger(__name__)
    logger.error("❌ FAILED to import download_alpha_overlap_results from supabase_utils!")
    download_alpha_overlap_results = None

# --- Helper Functions ---

def download_latest_alpha_overlap() -> bool:
    """Download overlap_results_alpha.pkl from Supabase."""
    if not USE_SUPABASE or not download_alpha_overlap_results:
        logger.warning("Supabase download skipped (disabled or helper missing).")
        return False
    
    try:
        # logger.debug("⬇️ Downloading overlap_results_alpha.pkl...")
        if download_alpha_overlap_results(str(ALPHA_OVERLAP_FILE), bucket=BUCKET_NAME):
            # logger.debug("✅ Downloaded overlap_results_alpha.pkl")
            return True
        # logger.debug("File not found on Supabase yet.")
        return False
    except Exception as e:
        logger.error(f"❌ Alpha overlap download failed: {e}")
        return False

def load_latest_alpha_tokens() -> Dict[str, Dict[str, Any]]:
    """Load and parse the local overlap_results_alpha.pkl file."""
    if not ALPHA_OVERLAP_FILE.exists() or ALPHA_OVERLAP_FILE.stat().st_size == 0:
        return {}
    
    try:
        data = joblib.load(ALPHA_OVERLAP_FILE)
        latest_tokens = {}
        
        for token_id, history in data.items():
            if not history or not isinstance(history, list):
                continue
            
            # Get the most recent entry for this token
            latest_entry = history[-1]
            
            # Ensure it's a 'passed' token as per spec
            if latest_entry.get("security") == "passed":
                latest_tokens[token_id] = latest_entry
                
        return latest_tokens
    
    except Exception as e:
        logger.exception(f"❌ Failed to load alpha overlap file: {e}")
        return {}

async def send_alpha_alert(
    app: Application,
    user_manager: UserManager,
    mint: str,
    entry: Dict[str, Any],
    alerted_tokens_state: Dict[str, Any]
):
    """
    Format and send a new alpha alert to subscribed users.
    Updates the alerted_tokens_state with the token's initial data.
    """
    try:
        # 1. Format the alert message (fetches DexScreener data)
        message, initial_data = await format_alpha_alert(mint, entry)
        
        if not message or not initial_data:
            logger.warning(f"Failed to format alert for {mint}, skipping.")
            return

        # 2. Create the "Refresh" button
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("Refresh ↻", callback_data=f"refresh_alpha:{mint}")]
        ])
        
        # 3. Get all active, subscribed users who opted in
        active_users = user_manager.get_active_users()
        sent_count = 0
        
        for chat_id, prefs in active_users.items():
            # Check for alpha_alerts opt-in and subscription status
            if (
                prefs.get("alpha_alerts", False) and
                user_manager.is_subscribed(chat_id)
            ):
                try:
                    await app.bot.send_message(
                        chat_id=int(chat_id),
                        text=message,
                        parse_mode="HTML",
                        reply_markup=keyboard,
                        disable_web_page_preview=True
                    )
                    sent_count += 1
                except Exception as e:
                    logger.warning(f"⚠️ Failed to send alpha alert to {chat_id}: {e}")
                await asyncio.sleep(0.1) # Avoid rate limits
        
        if sent_count > 0:
            logger.info(f"📤 Sent alpha alert for {mint} to {sent_count} users.")
            # 4. Save the token's initial state to prevent re-alerting
            alerted_tokens_state[mint] = initial_data
            
    except Exception as e:
        logger.exception(f"❌ Critical error in send_alpha_alert for {mint}: {e}")

# --- Main Monitoring Loop ---

async def alpha_monitoring_loop(app: Application, user_manager: UserManager):
    """
    Main background loop to monitor overlap_results_alpha.pkl.
    Runs every 30 seconds.
    """
    logger.info("🔄 Starting Alpha Token monitoring loop...")
    await asyncio.sleep(10) # Stagger startup

    while True:
        try:
            # 1. Download the latest alpha file from Supabase
            if not download_latest_alpha_overlap():
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
                    
            # 6. Save state back to disk if new tokens were processed
            if new_tokens_found:
                logger.info(f"💾 Saving alpha alerts state with {len(alerted_tokens)} tokens.")
                safe_save(ALPHA_ALERTS_STATE_FILE, alerted_tokens)

        except Exception as e:
            logger.exception(f"❌ Error in alpha monitoring loop: {e}")
        
        await asyncio.sleep(ALPHA_POLL_INTERVAL_SECS)