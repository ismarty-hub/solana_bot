#!/usr/bin/env python3
"""
alerts/alpha_monitoring.py - Background monitoring for overlap_results_alpha.pkl

Features:
- Properly handles async formatter
- Saves initial_state correctly for refresh functionality
- NO DUPLICATE ALERTS on bot restart (checks 'sent' flag)
- Persistent state tracking across deployments (Now handled by monitoring.py)
"""

import asyncio
import logging
import joblib
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

from telegram.ext import Application
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest

# --- Configuration ---
from config import DATA_DIR, BUCKET_NAME, USE_SUPABASE

# Setup logger
logger = logging.getLogger(__name__)

# --- Constants ---
ALPHA_POLL_INTERVAL_SECS = 30
ALPHA_OVERLAP_FILE = Path(DATA_DIR) / "overlap_results_alpha.pkl"
ALPHA_ALERTS_STATE_FILE = Path(DATA_DIR) / "alerts_state_alpha.json"
# REMOVED: ALPHA_ALERTS_STATE_REMOTE (now handled by monitoring.py)

# --- Track if we've downloaded state on startup ---
# REMOVED: _state_downloaded_on_startup (now handled by bot.py on_startup)

# --- Imports ---
from shared.file_io import safe_load, safe_save
from alerts.user_manager import UserManager
from alerts.formatters import _format_alpha_alert_async

try:
    # Only import what's needed for this file
    from supabase_utils import download_alpha_overlap_results
    logger.info("✅ Successfully imported download_alpha_overlap_results")
except Exception:
    logger.exception("❌ FAILED to import required functions from supabase_utils!")
    download_alpha_overlap_results = None
    # REMOVED: upload_file and download_file imports


# REMOVED: download_alpha_state_from_supabase (now handled by monitoring.py)


def load_latest_alpha_tokens() -> Dict[str, Any] | None:
    """Load the latest alpha token data from the local PKL file."""
    if not ALPHA_OVERLAP_FILE.exists():
        logger.warning(f"File not found: {ALPHA_OVERLAP_FILE}. Attempting download...")
        if USE_SUPABASE and download_alpha_overlap_results:
            try:
                ok = download_alpha_overlap_results(str(ALPHA_OVERLAP_FILE), bucket=BUCKET_NAME)
                if ok:
                    logger.info("✅ Downloaded alpha overlap file successfully.")
                else:
                    logger.warning("❌ Failed to download alpha overlap file from Supabase.")
                    return None
            except Exception as e:
                logger.exception(f"❌ Exception while downloading alpha overlap file: {e}")
                return None
        else:
            return None

    try:
        with open(ALPHA_OVERLAP_FILE, 'rb') as f:
            data = joblib.load(f)
        return data
    except Exception as e:
        logger.exception(f"❌ Failed to load alpha overlap data from PKL: {e}")
        return None


async def send_alpha_alert(
    app: Application, 
    user_manager: UserManager, 
    mint: str, 
    entry: Dict[str, Any], 
    alerted_tokens: Dict[str, Any]
) -> bool:
    """
    Format and send the alpha alert to all subscribed users.
    Returns True if alert was successfully sent, False otherwise.
    """
    import html
    import re

    try:
        latest_data = entry[-1] if isinstance(entry, list) else entry

        logger.info("🔍 Checking for alpha subscribers...")
        alpha_subscribers = user_manager.get_alpha_subscribers()
        logger.info(f"📊 Alpha subscribers check complete: {len(alpha_subscribers)} users")

        if not alpha_subscribers:
            logger.warning(f"⚠️ No alpha subscribers to notify for {mint}.")
            # Mark as attempted but not sent
            alerted_tokens[mint] = {
                "ts": datetime.now().isoformat(),
                "sent": False,
                "subscriber_count": 0,
                "reason": "no_subscribers"
            }
            return False

        # --- Call the async formatter properly ---
        logger.info(f"📝 Formatting alert for {mint}...")
        try:
            alert_msg, alert_meta = await _format_alpha_alert_async(mint, latest_data)
        except Exception as e:
            logger.exception(f"❌ Failed to format alert for {mint}: {e}")
            alerted_tokens[mint] = {
                "ts": datetime.now().isoformat(),
                "sent": False,
                "error": f"Format error: {str(e)}"
            }
            return False

        if not alert_msg or alert_msg is None:
            logger.error(f"❌ Formatter returned None for {mint}")
            alerted_tokens[mint] = {
                "ts": datetime.now().isoformat(),
                "sent": False,
                "error": "Formatter returned None"
            }
            return False

        # Ensure message is a string
        if not isinstance(alert_msg, str):
            logger.error(f"❌ Formatter returned non-string: {type(alert_msg)}")
            alert_msg = str(alert_msg)

        # Log a snippet to verify format
        logger.info(f"📝 Message preview (first 200 chars): {alert_msg[:200]}")

        symbol = alert_meta.get("symbol", "TOKEN") if alert_meta else "TOKEN"
        keyboard = [[InlineKeyboardButton(f"🔄 Refresh Price ({symbol})", callback_data=f"refresh_alpha:{mint}")]]

        success_count = 0
        fail_count = 0

        for chat_id in alpha_subscribers:
            try:
                logger.info(f"📤 Sending alpha alert to user {chat_id}...")
                await app.bot.send_message(
                    chat_id=chat_id,
                    text=alert_msg,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML",
                    disable_web_page_preview=True
                )
                success_count += 1
                logger.info(f"✅ Alert sent successfully to {chat_id}")

            except BadRequest as e:
                err_text = str(e)
                # Detect Telegram entity parsing errors
                if "Can't parse entities" in err_text or "unexpected end of name token" in err_text:
                    logger.warning(f"❌ Failed to send alpha alert to {chat_id}: {err_text}")

                    # Try to extract byte offset for debugging
                    m = re.search(r'byte offset (\d+)', err_text)
                    if m:
                        try:
                            off = int(m.group(1))
                            start = max(0, off - 40)
                            end = min(len(alert_msg), off + 40)
                            snippet = alert_msg[start:end]
                            logger.warning(f"🔍 Parsing error near offset {off}: ...{snippet}...")
                        except Exception:
                            logger.debug("Could not extract snippet around offset")

                    # Escape HTML and retry
                    escaped = html.escape(alert_msg)
                    try:
                        await app.bot.send_message(
                            chat_id=chat_id,
                            text=escaped,
                            reply_markup=InlineKeyboardMarkup(keyboard),
                            parse_mode="HTML",
                            disable_web_page_preview=True
                        )
                        success_count += 1
                        logger.info(f"✅ Alert sent (escaped) successfully to {chat_id}")
                    except Exception as e2:
                        fail_count += 1
                        logger.warning(f"❌ Failed to send escaped alpha alert to {chat_id}: {e2}")
                else:
                    fail_count += 1
                    logger.warning(f"❌ Failed to send alpha alert to {chat_id}: {err_text}")

            except Exception as e:
                fail_count += 1
                logger.warning(f"❌ Failed to send alpha alert to {chat_id}: {e}")

        logger.info(f"📊 Alert delivery: {success_count} sent, {fail_count} failed")

        # Save the alert record WITH initial_state for refresh functionality
        alert_record = {
            "ts": datetime.now().isoformat(),
            "sent": success_count > 0,  # ✅ CRITICAL: Track if actually sent
            "subscriber_count": len(alpha_subscribers),
            "success_count": success_count,
            "fail_count": fail_count
        }
        
        # Merge with metadata (contains initial_state for refresh)
        if alert_meta:
            alert_record.update(alert_meta)
        
        alerted_tokens[mint] = alert_record
        logger.info(f"💾 Saved alert state for {mint} with sent={success_count > 0}")

        return success_count > 0

    except Exception as e:
        logger.exception(f"❌ Error sending alpha alert for {mint}: {e}")
        alerted_tokens[mint] = {
            "ts": datetime.now().isoformat(),
            "sent": False,
            "error": str(e)
        }
        return False


async def alpha_monitoring_loop(app: Application, user_manager: UserManager):
    """Main background loop for alpha alert monitoring with duplicate prevention."""
    # global _state_downloaded_on_startup # No longer needed
    
    logger.info(f"🔄 Starting Alpha Monitoring Loop (Interval: {ALPHA_POLL_INTERVAL_SECS}s)")

    # ✅ CRITICAL: Download is now handled by bot.py on_startup
    # REMOVED: Internal download logic
    # if USE_SUPABASE and not _state_downloaded_on_startup: ...

    try:
        initial_subs = user_manager.get_alpha_subscribers()
        logger.info(f"📊 Initial alpha subscriber count: {len(initial_subs)}")
        if not initial_subs:
            logger.warning("⚠️ NO ALPHA SUBSCRIBERS at startup!")
    except Exception as e:
        logger.exception(f"❌ Error checking initial subscribers: {e}")

    while True:
        try:
            # Download latest data from Supabase
            if USE_SUPABASE and download_alpha_overlap_results:
                try:
                    download_alpha_overlap_results(str(ALPHA_OVERLAP_FILE), bucket=BUCKET_NAME)
                except Exception:
                    logger.exception("Failed to download alpha overlap results during loop")

            if not ALPHA_OVERLAP_FILE.exists():
                logger.warning(f"Alpha overlap file {ALPHA_OVERLAP_FILE} missing, skipping cycle.")
                await asyncio.sleep(ALPHA_POLL_INTERVAL_SECS)
                continue

            # Load latest tokens from PKL
            latest_tokens = load_latest_alpha_tokens()
            if not latest_tokens:
                await asyncio.sleep(ALPHA_POLL_INTERVAL_SECS)
                continue

            # Load persistent state (which was downloaded on startup)
            alerted_tokens = safe_load(ALPHA_ALERTS_STATE_FILE, {})
            logger.debug(f"📂 Loaded state for {len(alerted_tokens)} previously tracked tokens")

            new_tokens_found = False
            alerts_sent_this_cycle = 0

            for mint, entry in latest_tokens.items():
                # Check if token exists in state
                if mint not in alerted_tokens:
                    # ✅ NEW TOKEN - Never seen before
                    logger.info(f"🆕 NEW Alpha Token Detected: {mint}")
                    success = await send_alpha_alert(app, user_manager, mint, entry, alerted_tokens)
                    if success:
                        alerts_sent_this_cycle += 1
                    new_tokens_found = True
                    
                else:
                    # ✅ EXISTING TOKEN - Check if alert was previously sent
                    existing_state = alerted_tokens[mint]
                    was_sent = existing_state.get("sent", False)
                    
                    if not was_sent:
                        # Token was tracked but alert failed previously - retry
                        logger.info(f"🔄 RETRY Alpha Alert (previous attempt failed): {mint}")
                        success = await send_alpha_alert(app, user_manager, mint, entry, alerted_tokens)
                        if success:
                            alerts_sent_this_cycle += 1
                        new_tokens_found = True
                    else:
                        # Alert was already sent successfully - skip
                        logger.debug(f"⏭️ SKIP {mint[:8]}... (alert already sent at {existing_state.get('ts')})")

            # Save state if any changes occurred
            if new_tokens_found:
                logger.info(f"💾 Saving alpha alerts state: {len(alerted_tokens)} tokens, {alerts_sent_this_cycle} sent this cycle")
                safe_save(ALPHA_ALERTS_STATE_FILE, alerted_tokens)

                # REMOVED: Internal upload logic
                # The periodic sync in monitoring.py will handle uploading this file.
                
            else:
                logger.debug("No new alpha tokens this cycle")

        except Exception as e:
            logger.exception(f"❌ Error in alpha monitoring loop: {e}")

        await asyncio.sleep(ALPHA_POLL_INTERVAL_SECS)
