#!/usr/bin/env python3
"""
alerts/monitoring.py - Background monitoring and alert sending
"""

import os
import time
import asyncio
import logging
import joblib
from datetime import datetime
from typing import Dict, Any, Optional
from telegram.ext import Application
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from config import (
    OVERLAP_FILE, USER_PREFS_FILE, USER_STATS_FILE, ALERTS_STATE_FILE,
    BUCKET_NAME, USE_SUPABASE, DOWNLOAD_OVERLAP_ON_STARTUP,
    SUPABASE_DAILY_SYNC, POLL_INTERVAL_SECS, VALID_GRADES, ALL_GRADES
)
from shared.file_io import safe_load, safe_save
from shared.utils import fetch_marketcap_and_fdv, truncate_address
from alerts.formatters import format_alert_html

# Optional supabase helpers
try:
    from supabase_utils import download_overlap_results, upload_file, download_file
except Exception:
    download_overlap_results = None
    upload_file = None
    download_file = None


# ----------------------
# Supabase sync functions
# ----------------------
_last_upload = 0

def upload_bot_data_to_supabase():
    """Upload bot data files to Supabase (opt-in)."""
    global _last_upload
    now = time.time()
    
    if not USE_SUPABASE or upload_file is None:
        logging.debug("Supabase upload skipped (disabled or helper missing).")
        return
    
    for file in [USER_PREFS_FILE, USER_STATS_FILE, ALERTS_STATE_FILE]:
        if file.exists():
            try:
                if now - _last_upload < 43200:  # Only once every 12 hrs
                    return
                upload_file(str(file), bucket=BUCKET_NAME)
                _last_upload = now
            except Exception as e:
                logging.exception(f"Failed to upload {file} to Supabase: {e}")


def download_bot_data_from_supabase():
    """Download bot data files from Supabase (opt-in)."""
    if not USE_SUPABASE or download_file is None:
        logging.debug("Supabase download skipped (disabled or helper missing).")
        return
    
    for file in [USER_PREFS_FILE, USER_STATS_FILE, ALERTS_STATE_FILE]:
        try:
            download_file(str(file), os.path.basename(file), bucket=BUCKET_NAME)
        except Exception as e:
            logging.debug(f"Could not download {file} from Supabase: {e}")


async def daily_supabase_sync():
    """Daily background task to sync data with Supabase."""
    if not (USE_SUPABASE and SUPABASE_DAILY_SYNC):
        logging.debug("Daily Supabase sync disabled by configuration.")
        return
    
    logging.info("Daily Supabase sync task started.")
    while True:
        try:
            upload_bot_data_to_supabase()
            logging.info("✅ Daily sync with Supabase complete")
        except Exception as e:
            logging.exception(f"Supabase daily sync failed: {e}")
        await asyncio.sleep(24 * 3600)


async def periodic_overlap_download():
    """Periodically refresh overlap_results.pkl from Supabase."""
    while True:
        try:
            logging.info("⬇ Refreshing overlap_results.pkl from Supabase...")
            if download_overlap_results:
                download_overlap_results(str(OVERLAP_FILE), bucket=BUCKET_NAME)
        except Exception as e:
            logging.error(f"❌ Failed to refresh overlap_results.pkl: {e}")
        await asyncio.sleep(180)  # 3 minutes


# ----------------------
# Token loading
# ----------------------
def load_latest_tokens_from_overlap() -> Dict[str, Dict[str, Any]]:
    """Load overlap_results.pkl from local disk."""
    if not OVERLAP_FILE.exists() or OVERLAP_FILE.stat().st_size == 0:
        return {}

    try:
        data = joblib.load(OVERLAP_FILE)
        latest_tokens = {}
        
        for token_id, history in data.items():
            if not history:
                continue
            
            result = history[-1].get("result", {})
            latest_tokens[token_id] = {
                "grade": result.get("grade", "NONE"),
                "token_metadata": {
                    "mint": token_id,
                    "name": result.get("token_metadata", {}).get("name"),
                    "symbol": result.get("token_metadata", {}).get("symbol", "")
                },
                "overlap_percentage": result.get("overlap_percentage", 0.0),
                "concentration": result.get("concentration", 0.0),
                "checked_at": result.get("checked_at")
            }
        
        return latest_tokens
    
    except Exception as e:
        logging.exception(f"Failed to load overlap file: {e}")
        return {}


# ----------------------
# Alert sending
# ----------------------
async def send_alert_to_subscribers(
    app: Application,
    token_data: Dict[str, Any],
    grade: str,
    user_manager,
    previous_grade: Optional[str] = None,
    initial_mc: Optional[float] = None,
    initial_fdv: Optional[float] = None,
    first_alert_at: Optional[str] = None
):
    """Send an alert to every active, subscribed user who subscribes to this grade."""
    active_users = user_manager.get_active_users()
    
    if not active_users:
        logging.debug("No active users to send alerts to.")
        return

    message = format_alert_html(
        token_data,
        "CHANGE" if previous_grade else "NEW",
        previous_grade,
        initial_mc=initial_mc,
        initial_fdv=initial_fdv,
        first_alert_at=first_alert_at
    )

    # Prepare keyboard
    mint = token_data.get("token_metadata", {}).get("mint") or token_data.get("token") or ""
    truncated = truncate_address(mint)
    buttons = []
    
    if mint:
        buttons.append(InlineKeyboardButton(f"📋 Copy {truncated}", callback_data=f"copy:{mint}"))
        buttons.append(InlineKeyboardButton("🔗 DexScreener", url=f"https://dexscreener.com/solana/{mint}"))

    keyboard = InlineKeyboardMarkup([buttons]) if buttons else None

    for chat_id, prefs in active_users.items():
        # Skip if subscription is invalid
        if not user_manager.is_subscribed(chat_id):
            logging.debug(f"Skipping alert for {chat_id}: not subscribed or expired")
            continue

        # Check if user wants this grade
        subscribed_grades = prefs.get("grades", ALL_GRADES.copy())
        if isinstance(subscribed_grades, (list, tuple)):
            if grade not in subscribed_grades:
                continue
        else:
            if grade not in ALL_GRADES:
                continue

        # Send the alert
        try:
            await app.bot.send_message(
                chat_id=int(chat_id),
                text=message,
                parse_mode="HTML",
                disable_web_page_preview=True,
                reply_markup=keyboard
            )
            user_manager.update_user_stats(chat_id, grade)
        except Exception as e:
            logging.warning(f"Failed to send alert to {chat_id}: {e}")

        await asyncio.sleep(0.1)


# ----------------------
# Monthly expiry notifier
# ----------------------
async def monthly_expiry_notifier(app: Application, user_manager):
    """Notify expired users once per month."""
    while True:
        try:
            prefs = safe_load(USER_PREFS_FILE, {})
            
            for chat_id, user in prefs.items():
                if user_manager.is_subscription_expired(chat_id):
                    last_notified = user.get("last_notified")
                    should_notify = True
                    
                    if last_notified:
                        try:
                            last_dt = datetime.fromisoformat(last_notified.rstrip("Z"))
                            if (datetime.utcnow() - last_dt).days < 30:
                                should_notify = False
                        except:
                            pass
                    
                    if should_notify:
                        try:
                            await app.bot.send_message(
                                chat_id=int(chat_id),
                                text="⚠️ Your subscription has expired. Please contact the admin to renew."
                            )
                            user_manager.mark_notified(chat_id)
                            logging.info(f"Notified expired user {chat_id}")
                        except Exception as e:
                            logging.warning(f"Failed to notify expired user {chat_id}: {e}")
        
        except Exception as e:
            logging.exception(f"Error in monthly_expiry_notifier: {e}")

        await asyncio.sleep(24 * 3600)


# ----------------------
# Background monitoring loop
# ----------------------
async def background_loop(app: Application, user_manager):
    """Main background loop for monitoring tokens and sending alerts."""
    logging.info("Background alert loop started...")

    # Load local state
    alerts_state = safe_load(ALERTS_STATE_FILE, {})

    # Try downloading latest state from Supabase
    if USE_SUPABASE and download_file:
        try:
            download_file(str(ALERTS_STATE_FILE), os.path.basename(ALERTS_STATE_FILE), bucket=BUCKET_NAME)
            alerts_state = safe_load(ALERTS_STATE_FILE, alerts_state)
            logging.info("✅ Downloaded latest alerts_state from Supabase")
        except Exception as e:
            logging.warning(f"⚠️ Could not fetch alerts_state from Supabase: {e}")

    first_run = True

    while True:
        try:
            tokens = load_latest_tokens_from_overlap()

            # Filter only today's tokens
            today = datetime.utcnow().date()
            fresh_tokens = {
                tid: t for tid, t in tokens.items()
                if t.get("checked_at") and datetime.fromisoformat(
                    t["checked_at"].rstrip("Z")
                ).date() >= today
            }

            if first_run:
                logging.info(f"DEBUG: Loaded {len(tokens)} tokens (today only: {len(fresh_tokens)})")
                sample_items = list(fresh_tokens.items())[:3]
                for tid, info in sample_items:
                    logging.info(f"DEBUG sample token: {tid} grade={info.get('grade')} checked_at={info.get('checked_at')}")
                first_run = False

            for token_id, token in fresh_tokens.items():
                grade = token.get("grade")
                if not grade:
                    continue

                current_state = alerts_state.get(token_id)
                last_grade = current_state.get("last_grade") if isinstance(current_state, dict) else None

                # Only proceed if grade changed
                if grade != last_grade:
                    logging.info(f"New/changed grade for {token_id}: {last_grade} -> {grade}")

                    if grade in VALID_GRADES:
                        # First time alert for this token
                        if last_grade is None:
                            mc, fdv, lqd = fetch_marketcap_and_fdv(token_id)
                            alerts_state[token_id] = {
                                "last_grade": grade,
                                "initial_marketcap": mc,
                                "initial_fdv": fdv,
                                "initial_liquidity": lqd,
                                "first_alert_at": datetime.utcnow().isoformat() + "Z"
                            }

                            safe_save(ALERTS_STATE_FILE, alerts_state)
                            
                            if USE_SUPABASE and upload_file:
                                try:
                                    upload_file(str(ALERTS_STATE_FILE), bucket=BUCKET_NAME)
                                    logging.info("✅ Uploaded alerts_state incrementally")
                                except Exception as e:
                                    logging.warning(f"⚠️ Failed incremental upload: {e}")

                            logging.info(f"Captured initial market data for {token_id}")
                        else:
                            alerts_state[token_id]["last_grade"] = grade

                        # Send alert
                        state = alerts_state.get(token_id, {})
                        await send_alert_to_subscribers(
                            app,
                            token,
                            grade,
                            user_manager,
                            previous_grade=last_grade,
                            initial_mc=state.get("initial_marketcap"),
                            initial_fdv=state.get("initial_fdv"),
                            first_alert_at=state.get("first_alert_at")
                        )
                    else:
                        logging.debug(f"Skipping alert save/upload for {token_id} with grade {grade}")

            # Persist full state after processing
            if any(entry.get("last_grade") in VALID_GRADES for entry in alerts_state.values()):
                safe_save(ALERTS_STATE_FILE, alerts_state)
                try:
                    if USE_SUPABASE and upload_file:
                        upload_file(str(ALERTS_STATE_FILE), bucket=BUCKET_NAME)
                        logging.info("✅ Synced alerts_state to Supabase")
                except Exception as e:
                    logging.warning(f"⚠️ Failed to upload alerts_state: {e}")

        except Exception as e:
            logging.exception(f"Error in background loop: {e}")

        await asyncio.sleep(POLL_INTERVAL_SECS)