#!/usr/bin/env python3
"""
alerts/alpha_monitoring.py - Background monitoring for overlap_results_alpha.pkl

Features:
- Properly handles async formatter
- Saves initial_state correctly for refresh functionality
- ✅ FIXED: State is NOW loaded by bot.py's on_startup. This loop ONLY loads
           the local file, preventing race conditions and re-alerts.
- ✅ FIXED: Only alerts on FIRST DETECTION or GRADE CHANGE.
- ✅ NEW: Sends a "Grade Change" alert if a token's grade changes.
"""

import asyncio
import logging
import joblib
import html
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

from telegram.ext import Application
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest

from config import (DATA_DIR, BUCKET_NAME, USE_SUPABASE, ALPHA_ALERTS_STATE_FILE)

# --- Constants ---
ALPHA_POLL_INTERVAL_SECS = 30
ALPHA_OVERLAP_FILE = Path(DATA_DIR) / "overlap_results_alpha.pkl"


# Setup logger
logger = logging.getLogger(__name__)

# --- Imports ---
from shared.file_io import safe_load, safe_save
from alerts.user_manager import UserManager
from alerts.formatters import _format_alpha_alert_async

try:
    from supabase_utils import download_alpha_overlap_results, download_file
    logger.info("✅ Successfully imported download_alpha_overlap_results")
except Exception:
    logger.exception("❌ FAILED to import required functions from supabase_utils!")
    download_alpha_overlap_results = None
    download_file = None

# Path to active_tracking.json for ML_PASSED crosscheck
ACTIVE_TRACKING_FILE = Path(DATA_DIR) / "active_tracking.json"


def download_active_tracking() -> Dict[str, Any]:
    """
    Download active_tracking.json from Supabase to check initial ML_PASSED status.
    Returns empty dict if download fails or file doesn't exist.
    """
    if USE_SUPABASE and download_file:
        try:
            remote_path = "analytics/active_tracking.json"
            ok = download_file(str(ACTIVE_TRACKING_FILE), remote_path, bucket=BUCKET_NAME)
            if ok and ACTIVE_TRACKING_FILE.exists():
                data = safe_load(ACTIVE_TRACKING_FILE, {})
                logger.debug(f"Downloaded active_tracking.json with {len(data)} tokens")
                return data
        except Exception as e:
            logger.warning(f"Failed to download active_tracking.json: {e}")
    
    # Fallback to local file
    if ACTIVE_TRACKING_FILE.exists():
        return safe_load(ACTIVE_TRACKING_FILE, {})
    
    return {}


def check_initial_ml_passed(mint: str, active_tracking: Dict[str, Any]) -> bool:
    """
    Check if a token's INITIAL ML_PASSED status was True from active_tracking.json.
    
    Returns:
        True if token NOT found in active_tracking (new token, allow alert)
        True if token found with ML_PASSED=True (allow alert)
        False if token found with ML_PASSED=False (block permanently)
    """
    # Check alpha entry first
    alpha_key = f"{mint}_alpha"
    if alpha_key in active_tracking:
        initial_ml_passed = active_tracking[alpha_key].get("ML_PASSED", False)
        if not initial_ml_passed:
            logger.debug(f"⛔ Token {mint[:8]}... had initial ML_PASSED=False in active_tracking")
            return False
        return True
    
    # Check discovery entry as fallback
    discovery_key = f"{mint}_discovery"
    if discovery_key in active_tracking:
        initial_ml_passed = active_tracking[discovery_key].get("ML_PASSED", False)
        if not initial_ml_passed:
            logger.debug(f"⛔ Token {mint[:8]}... had initial ML_PASSED=False in active_tracking")
            return False
        return True
    
    # Token not found in active_tracking - it's new, allow the alert
    logger.debug(f"✅ Token {mint[:8]}... not in active_tracking - allowing alert for new token")
    return True


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
    alerted_tokens: Dict[str, Any],
    previous_grade: Optional[str] = None  # <-- NEW: To handle grade changes
) -> bool:
    """
    Format and send the alpha alert to all subscribed users.
    Returns True if alert was successfully sent, False otherwise.
    
    If previous_grade is provided, prepends a "Grade Change" notice.
    Updates the alerted_tokens state with the latest grade and sent status.
    """
    import html
    import re

    try:
        # 1. Get latest data and current grade
        latest_data = entry[-1] if isinstance(entry, list) else entry
        current_grade = latest_data.get("result", {}).get("grade", "N/A")

        logger.info("📋 Checking for alpha subscribers...")
        alpha_subscribers = user_manager.get_alpha_subscribers()
        logger.info(f"📊 Alpha subscribers check complete: {len(alpha_subscribers)} users")

        if not alpha_subscribers:
            logger.warning(f"⚠️ No alpha subscribers to notify for {mint}.")
            # Mark as attempted but not sent
            alerted_tokens[mint] = {
                "ts": datetime.now().isoformat(),
                "sent": False,
                "subscriber_count": 0,
                "reason": "no_subscribers",
                "last_grade": current_grade
            }
            return False

        # 2. Call the async formatter
        logger.info(f"📝 Formatting alert for {mint}...")
        try:
            alert_msg, alert_meta, image_url = await _format_alpha_alert_async(mint, latest_data)
        except Exception as e:
            logger.exception(f"❌ Failed to format alert for {mint}: {e}")
            alerted_tokens[mint] = {
                "ts": datetime.now().isoformat(),
                "sent": False,
                "error": f"Format error: {str(e)}",
                "last_grade": current_grade
            }
            return False

        if not alert_msg or alert_msg is None:
            logger.error(f"❌ Formatter returned None for {mint}")
            alerted_tokens[mint] = {
                "ts": datetime.now().isoformat(),
                "sent": False,
                "error": "Formatter returned None",
                "last_grade": current_grade
            }
            return False

        if not isinstance(alert_msg, str):
            logger.error(f"❌ Formatter returned non-string: {type(alert_msg)}")
            alert_msg = str(alert_msg)

        # 3. --- NEW: Prepend Grade Change Header ---
        symbol = alert_meta.get("symbol", "TOKEN") if alert_meta else "TOKEN"
        if previous_grade:
            logger.info(f"Prepending grade change header: {previous_grade} -> {current_grade}")
            change_header = (
                f"🔔 <b>Alpha Grade Change: ${html.escape(symbol)}</b> 🔔\n"
                f"<b>Grade: {html.escape(previous_grade)} ➡️ {html.escape(current_grade)}</b>\n\n"
                "--- (Full Token Details Below) ---\n\n"
            )
            alert_msg = change_header + alert_msg
        
        logger.info(f"📄 Message preview (first 200 chars): {alert_msg[:200]}")
        if image_url:
            logger.info(f"🖼️ Token image URL available: {image_url[:50]}...")

        # 4. Define keyboard and send to all users
        keyboard = [[InlineKeyboardButton(f"🔄 Refresh Price ({symbol})", callback_data=f"refresh_alpha:{mint}")]]
        success_count = 0
        fail_count = 0

        for chat_id in alpha_subscribers:
            try:
                logger.info(f"📤 Sending alpha alert to user {chat_id}...")
                
                # Check message length for send_photo caption limit (1024)
                if image_url and len(alert_msg) < 1000:
                    try:
                        await app.bot.send_photo(
                            chat_id=chat_id,
                            photo=image_url,
                            caption=alert_msg,
                            reply_markup=InlineKeyboardMarkup(keyboard),
                            parse_mode="HTML"
                        )
                        success_count += 1
                        logger.info(f"✅ Photo alert sent successfully to {chat_id}")
                        continue
                    except Exception as photo_err:
                        logger.warning(f"📷 Photo send failed for {chat_id}, falling back to text: {photo_err}")
                
                # If message is long or send_photo failed, use "Invisible Link" trick
                # This makes the image appear as a preview even in a text message.
                final_text = alert_msg
                web_preview = True
                
                if image_url:
                    # Prepend an invisible link (zero-width joiner) to the message
                    # This tells Telegram to use this URL for the link preview.
                    hidden_link = f'<a href="{image_url}">&#8205;</a>'
                    final_text = hidden_link + alert_msg
                    web_preview = True
                    logger.debug(f"Inserted hidden image link for {chat_id}")

                # Regular message (supports up to 4096 chars)
                await app.bot.send_message(
                    chat_id=chat_id,
                    text=final_text,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML",
                    disable_web_page_preview=not web_preview
                )
                success_count += 1
                logger.info(f"✅ Alert sent successfully to {chat_id}")

            except BadRequest as e:
                err_text = str(e)
                if "Can't parse entities" in err_text or "unexpected end of name token" in err_text:
                    logger.warning(f"❌ Failed to send alpha alert to {chat_id}: {err_text}")
                    # ... (snippet extraction logic) ...
                    # Try to send escaped message
                    escaped = html.escape(alert_msg)
                    try:
                        await app.bot.send_message(
                            chat_id=chat_id,
                            text=f"⚠️ <i>HTML parse error, sending plain text:</i>\n\n{escaped}",
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

        # 5. --- CORRECTED: Save/Update State ---
        # Get existing state first to preserve initial_state
        alert_record = alerted_tokens.get(mint, {})

        # Update tracking fields
        alert_record.update({
            "ts": datetime.now().isoformat(),
            "sent": success_count > 0,
            "subscriber_count": len(alpha_subscribers),
            "success_count": success_count,
            "fail_count": fail_count,
            "last_grade": current_grade  # <-- ALWAYS update the last_grade
        })
        
        # ONLY add the metadata (initial_state) on the VERY FIRST alert
        if "first_alert_at" not in alert_record and alert_meta:
            logger.info(f"Saving *initial* metadata (first_alert_at etc.) for {mint}")
            alert_record.update(alert_meta)
        
        alerted_tokens[mint] = alert_record
        logger.info(f"💾 Saved alert state for {mint} with sent={success_count > 0}, grade={current_grade}")

        return success_count > 0

    except Exception as e:
        logger.exception(f"❌ CRITICAL Error in send_alpha_alert for {mint}: {e}")
        # Save error state
        alert_record = alerted_tokens.get(mint, {})
        alert_record.update({
            "ts": datetime.now().isoformat(),
            "sent": False,
            "error": str(e)
        })
        alerted_tokens[mint] = alert_record
        return False


async def alpha_monitoring_loop(app: Application, user_manager: UserManager):
    """
    Main background loop for alpha alert monitoring with duplicate prevention
    and grade-change detection.
    
    This loop assumes bot.py's on_startup has ALREADY downloaded the
    ALPHA_ALERTS_STATE_FILE from Supabase.
    """
    logger.info(f"🔄 Starting Alpha Monitoring Loop (Interval: {ALPHA_POLL_INTERVAL_SECS}s)")

    try:
        initial_subs = user_manager.get_alpha_subscribers()
        logger.info(f"📊 Initial alpha subscriber count: {len(initial_subs)}")
        if not initial_subs:
            logger.warning("⚠️ NO ALPHA SUBSCRIBERS at startup!")
    except Exception as e:
        logger.exception(f"❌ Error checking initial subscribers: {e}")

    # ---
    # ✅ FIX: Load the local state file ONCE.
    # We trust that bot.py's on_startup has already populated this
    # file with the latest data from Supabase, preventing re-alerts.
    # ---
    alerted_tokens = safe_load(ALPHA_ALERTS_STATE_FILE, {})
    logger.info(f"📂 Loaded alpha alert state: {len(alerted_tokens)} tokens tracked")
    logger.info(f"📂 State file location: {ALPHA_ALERTS_STATE_FILE}")

    while True:
        try:
            # Download latest *token data* (not state)
            if USE_SUPABASE and download_alpha_overlap_results:
                try:
                    download_alpha_overlap_results(str(ALPHA_OVERLAP_FILE), bucket=BUCKET_NAME)
                except Exception:
                    logger.exception("Failed to download alpha overlap results during loop")

            if not ALPHA_OVERLAP_FILE.exists():
                logger.warning(f"Alpha overlap file {ALPHA_OVERLAP_FILE} missing, skipping cycle.")
                await asyncio.sleep(ALPHA_POLL_INTERVAL_SECS)
                continue

            # Download active_tracking.json for ML_PASSED initial status crosscheck
            active_tracking = download_active_tracking()

            # Load latest tokens from PKL
            latest_tokens = load_latest_alpha_tokens()
            if not latest_tokens:
                logger.warning("No alpha tokens found in PKL file, skipping cycle.")
                await asyncio.sleep(ALPHA_POLL_INTERVAL_SECS)
                continue

            logger.debug(f"Checking {len(latest_tokens)} tokens against {len(alerted_tokens)} tracked tokens...")

            state_changed_this_cycle = False
            alerts_sent_this_cycle = 0

            for mint, entry in latest_tokens.items():
                
                # Get latest data and grade from the most recent entry
                latest_data = entry[-1] if isinstance(entry, list) else entry
                current_grade = latest_data.get("result", {}).get("grade", "N/A")
                ml_passed = latest_data.get("ML_PASSED", False)

                # ML Filtering: Only send alerts if ML check passed in overlap file
                if not ml_passed:
                    logger.debug(f"⏭️ Skipping alpha alert for {mint[:8]}... - ML_PASSED is False")
                    continue
                
                # CRITICAL: Crosscheck with active_tracking.json for INITIAL ML_PASSED status
                # This prevents alerts for tokens that initially failed ML but later passed
                if not check_initial_ml_passed(mint, active_tracking):
                    logger.info(f"⛔ BLOCKED alpha alert for {mint[:8]}... - initial ML_PASSED was False")
                    continue
                
                # Check if token exists in state
                existing_state = alerted_tokens.get(mint)

                if not existing_state:
                    # --- 1. NEW TOKEN ---
                    logger.info(f"🆕 NEW Alpha Token Detected: {mint}")
                    
                    # Send the first alert
                    logger.info(f"📢 Attempting to send first alert for {mint} (Grade: {current_grade})...")
                    success = await send_alpha_alert(app, user_manager, mint, entry, alerted_tokens)
                    if success:
                        alerts_sent_this_cycle += 1
                    
                    state_changed_this_cycle = True # We save the state regardless of success
                
                else:
                    # --- 2. EXISTING TOKEN ---
                    was_sent = existing_state.get("sent", False)
                    last_grade = existing_state.get("last_grade", "N/A")
                    
                    if not was_sent:
                        # --- 2a. RETRY FAILED ALERT ---
                        logger.info(f"🔄 RETRY Alpha Alert (previous attempt failed): {mint} (Grade: {current_grade})")
                        success = await send_alpha_alert(app, user_manager, mint, entry, alerted_tokens)
                        if success:
                            alerts_sent_this_cycle += 1
                        state_changed_this_cycle = True # Save the new state (e.g., sent=True)
                    
                    elif current_grade != last_grade:
                        # --- 2b. GRADE CHANGE DETECTED ---
                        logger.info(f"🔔 GRADE CHANGE for Alpha Token: {mint} | {last_grade} -> {current_grade}")
                        
                        success = await send_alpha_alert(
                            app, 
                            user_manager, 
                            mint, 
                            entry, 
                            alerted_tokens, 
                            previous_grade=last_grade # <-- Pass previous_grade
                        )
                        
                        if success:
                            alerts_sent_this_cycle += 1
                        state_changed_this_cycle = True # Save the new grade
                    
                    else:
                        # --- 2c. ALREADY SENT, NO CHANGE ---
                        logger.debug(f"⏭️ SKIP {mint[:8]}... (alert sent, grade {last_grade} unchanged)")

            # Save state if any changes occurred
            if state_changed_this_cycle:
                logger.info(f"💾 Saving alpha alerts state: {len(alerted_tokens)} tokens, {alerts_sent_this_cycle} sent this cycle")
                safe_save(ALPHA_ALERTS_STATE_FILE, alerted_tokens)
                
            else:
                logger.debug("No new alpha tokens or grade changes this cycle")

        except Exception as e:
            logger.exception(f"❌ Error in alpha monitoring loop: {e}")

        await asyncio.sleep(ALPHA_POLL_INTERVAL_SECS)
