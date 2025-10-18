#!/usr/bin/env python3
"""
alerts/monitoring.py - Background monitoring with Supabase polling
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
    OVERLAP_FILE, USER_PREFS_FILE, USER_STATS_FILE, ALERTS_STATE_FILE, GROUPS_FILE, PORTFOLIOS_FILE,
    BUCKET_NAME, USE_SUPABASE, POLL_INTERVAL_SECS, VALID_GRADES, ALL_GRADES
)
from shared.file_io import safe_load, safe_save
from shared.utils import fetch_marketcap_and_fdv, truncate_address
from alerts.formatters import format_alert_html

logger = logging.getLogger(__name__)

# Import Supabase helpers
try:
    from supabase_utils import download_overlap_results, upload_file, download_file
    logger.info("✅ Supabase utils loaded successfully")
except Exception as e:
    logger.error(f"❌ Failed to load supabase_utils: {e}")
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
        logger.debug("Supabase upload skipped (disabled or helper missing).")
        return
    
    # Upload user data files less frequently
    for file in [USER_PREFS_FILE, USER_STATS_FILE, GROUPS_FILE]:
        if file.exists():
            try:
                if now - _last_upload < 43200:  # Only once every 12 hrs
                    continue
                upload_file(str(file), bucket=BUCKET_NAME)
                _last_upload = now
            except Exception as e:
                logger.exception(f"Failed to upload {file} to Supabase: {e}")
    
    # Upload critical state files more often if needed (currently handled elsewhere)
    for file in [ALERTS_STATE_FILE, PORTFOLIOS_FILE]:
         if file.exists():
            try:
                # This upload can be more frequent, handled by managers
                pass
            except Exception as e:
                logger.exception(f"Failed to upload {file} to Supabase: {e}")


def download_bot_data_from_supabase():
    """Download bot data files from Supabase (opt-in)."""
    if not USE_SUPABASE or download_file is None:
        logger.debug("Supabase download skipped (disabled or helper missing).")
        return
    
    # Download user prefs, stats, and alert state
    for file in [USER_PREFS_FILE, USER_STATS_FILE, ALERTS_STATE_FILE]:
        try:
            download_file(str(file), os.path.basename(file), bucket=BUCKET_NAME)
        except Exception as e:
            logger.debug(f"Could not download {file} from Supabase: {e}")

    # Download portfolios into the correct folder structure
    try:
        remote_path = f"paper_trade/{PORTFOLIOS_FILE.name}"
        download_file(str(PORTFOLIOS_FILE), remote_path, bucket=BUCKET_NAME)
    except Exception as e:
        logger.debug(f"Could not download portfolios from Supabase: {e}")


def download_latest_overlap():
    """Download overlap_results.pkl from Supabase RIGHT NOW."""
    if not download_overlap_results:
        logger.warning("⚠️ download_overlap_results function not available!")
        return False
    
    try:
        logger.info("⬇️ Downloading overlap_results.pkl from Supabase...")
        result = download_overlap_results(str(OVERLAP_FILE), bucket=BUCKET_NAME)
        
        if OVERLAP_FILE.exists():
            size_kb = OVERLAP_FILE.stat().st_size / 1024
            logger.info(f"✅ Downloaded: {size_kb:.2f} KB")
            return True
        else:
            logger.error("❌ File not found after download!")
            return False
    except Exception as e:
        logger.error(f"❌ Download failed: {e}")
        return False

async def daily_supabase_sync():
    """Daily background task to sync data with Supabase."""
    if not (USE_SUPABASE):
        logger.debug("Daily Supabase sync disabled by configuration.")
        return
    
    logger.info("📅 Daily Supabase sync task started.")
    while True:
        await asyncio.sleep(24 * 3600)
        try:
            upload_bot_data_to_supabase()
            logger.info("✅ Daily sync with Supabase complete")
        except Exception as e:
            logger.exception(f"Supabase daily sync failed: {e}")

# ----------------------
# Token loading
# ----------------------
def load_latest_tokens_from_overlap() -> Dict[str, Dict[str, Any]]:
    """Load overlap_results.pkl from local disk (after download from Supabase)."""
    if not OVERLAP_FILE.exists() or OVERLAP_FILE.stat().st_size == 0:
        logger.info("ℹ️ No local overlap file yet (will be downloaded from Supabase)")
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
        
        logger.info(f"📊 Loaded {len(latest_tokens)} tokens from overlap file")
        return latest_tokens
    
    except Exception as e:
        logger.exception(f"❌ Failed to load overlap file: {e}")
        return {}

# ----------------------
# Alert sending (ONLY for users with "alerts" mode)
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
    """Send an alert notification to subscribed users who have 'alerts' mode enabled."""
    alerting_users = user_manager.get_alerting_users()
    
    if not alerting_users:
        logger.debug("No users with alerts mode enabled to send to.")
        return

    message = format_alert_html(
        token_data,
        "CHANGE" if previous_grade else "NEW",
        previous_grade,
        initial_mc=initial_mc,
        initial_fdv=initial_fdv,
        first_alert_at=first_alert_at
    )
    
    mint = token_data.get("token_metadata", {}).get("mint") or token_data.get("token") or ""
    buttons = []
    if mint:
        buttons.append(InlineKeyboardButton("🔗 Bonkbot", url=f"https://t.me/bonkbot_bot?start=ref_68ulj_ca_{mint}"))
        buttons.append(InlineKeyboardButton("🔗 Trojan", url=f"https://t.me/paris_trojanbot?start=r-ismarty1-{mint}"))
    keyboard = InlineKeyboardMarkup([buttons]) if buttons else None

    sent_count = 0
    for chat_id, prefs in alerting_users.items():
        if not user_manager.is_subscribed(chat_id):
            continue

        subscribed_grades = prefs.get("grades", ALL_GRADES)
        if grade not in subscribed_grades:
            continue

        try:
            await app.bot.send_message(
                chat_id=int(chat_id), text=message, parse_mode="HTML",
                disable_web_page_preview=True, reply_markup=keyboard
            )
            user_manager.update_user_stats(chat_id, grade)
            sent_count += 1
        except Exception as e:
            logger.warning(f"⚠️ Failed to send alert to {chat_id}: {e}")

        await asyncio.sleep(0.1)
    
    logger.info(f"📤 Sent {sent_count} alert notifications for grade {grade}")


# ----------------------
# Trade signal triggering (for paper trade engine - NO user notifications)
# ----------------------
async def trigger_trade_signals(
    token_data: Dict[str, Any],
    grade: str,
    user_manager,
    signal_queue: Dict[str, list]
):
    """
    Queue trade signals for users with 'papertrade' mode enabled.
    This does NOT send any messages to users - it just adds signals to the queue
    that signal_detection_loop will process.
    """
    trading_users = user_manager.get_trading_users()
    
    if not trading_users:
        logger.debug("No users with papertrade mode enabled.")
        return
    
    mint = token_data.get("token_metadata", {}).get("mint") or token_data.get("token") or ""
    if not mint:
        logger.warning("No mint address found in token_data for trade signal")
        return
    
    signals_queued = 0
    for chat_id in trading_users:
        if not user_manager.is_subscribed(chat_id):
            continue
        
        # Check if user's grade preferences include this grade
        user_prefs = user_manager.get_user_prefs(chat_id)
        subscribed_grades = user_prefs.get("grades", ALL_GRADES)
        if grade not in subscribed_grades:
            continue
        
        # Add signal to queue (will be picked up by signal_detection_loop)
        if mint not in signal_queue:
            signal_queue[mint] = {
                "symbol": token_data.get("token_metadata", {}).get("symbol", "Unknown"),
                "name": token_data.get("token_metadata", {}).get("name", "Unknown"),
                "grade": grade,
                "queued_at": datetime.utcnow().isoformat() + "Z",
                "queued_for_users": []
            }
        
        signal_queue[mint]["queued_for_users"].append(chat_id)
        signals_queued += 1
        
        logger.debug(f"🎯 Trade signal queued for {chat_id}: {grade} - {mint[:8]}...")
    
    if signals_queued > 0:
        logger.info(f"📊 Queued {signals_queued} trade signals for grade {grade} token {mint[:8]}...")


# ----------------------
# Monthly expiry notifier
# ----------------------
async def monthly_expiry_notifier(app: Application, user_manager):
    """Notify expired users once per month."""
    logger.info("📅 Starting monthly expiry notifier...")
    
    while True:
        await asyncio.sleep(24 * 3600)
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
                        except: pass
                    
                    if should_notify:
                        try:
                            await app.bot.send_message(
                                chat_id=int(chat_id),
                                text="⚠️ Your subscription has expired. Please contact the admin to renew."
                            )
                            user_manager.mark_notified(chat_id)
                            logger.info(f"Notified expired user {chat_id}")
                        except Exception as e:
                            logger.warning(f"Failed to notify {chat_id}: {e}")
        
        except Exception as e:
            logger.exception(f"Error in expiry notifier: {e}")

# ----------------------
# Main Background Loop (DECOUPLED alerts and trade signals)
# ----------------------
async def background_loop(app: Application, user_manager, portfolio_manager=None):
    """
    Main monitoring loop: Downloads from Supabase, checks for changes, 
    sends alerts to alert users AND triggers trades for paper trade users.
    """
    logger.info("🔄 Background alert loop started!")
    logger.info(f"⏰ Polling every {POLL_INTERVAL_SECS} seconds")

    alerts_state = safe_load(ALERTS_STATE_FILE, {})
    logger.info(f"📂 Loaded alert state: {len(alerts_state)} tokens tracked")
    
    signal_queue = {}

    while True:
        try:
            # STEP 1: DOWNLOAD FROM SUPABASE
            download_latest_overlap()
            
            # STEP 2: LOAD TOKENS
            tokens = load_latest_tokens_from_overlap()
            if not tokens:
                await asyncio.sleep(POLL_INTERVAL_SECS)
                continue

            alerts_sent_this_cycle = 0
            for token_id, token_info in tokens.items():
                grade = token_info.get("grade")
                if not grade or grade not in VALID_GRADES:
                    continue

                current_state = alerts_state.get(token_id)
                last_grade = current_state.get("last_grade") if isinstance(current_state, dict) else None

                is_new_token = (last_grade is None)
                is_grade_change = (grade != last_grade)
                
                # ✅ FIX: Initialize state for new tokens FIRST
                if is_new_token:
                    mc, fdv, lqd = fetch_marketcap_and_fdv(token_id)
                    alerts_state[token_id] = {
                        "last_grade": grade, 
                        "initial_marketcap": mc,
                        "initial_fdv": fdv, 
                        "first_alert_at": datetime.utcnow().isoformat() + "Z",
                        "broadcasted": False  # Initialize broadcast flag
                    }
                    logger.info(f"🆕 New token detected: {token_id[:8]}... | Grade: {grade}")
                
                # ✅ FIX: Broadcast to groups for ANY valid grade token (not just on grade change)
                # This should happen for new tokens with any grade (CRITICAL, HIGH, MEDIUM, LOW)
                state = alerts_state.get(token_id, {})
                should_broadcast = (
                    grade != "NONE" and  # Must have a valid grade
                    not state.get("broadcasted", False)  # Haven't broadcast yet
                )
                
                if should_broadcast:
                    mint_address = token_info.get("token_metadata", {}).get("mint", token_id)
                    try:
                        await broadcast_mint_to_groups(app, mint_address)
                        alerts_state[token_id]["broadcasted"] = True
                        logger.info(f"✅ Broadcasted to groups: {mint_address} (Grade: {grade})")
                    except Exception as e:
                        logger.error(f"❌ Broadcast failed for {mint_address}: {e}")
                
                # Continue with alert logic only if grade actually changed
                if is_grade_change:
                    logger.info(f"🔔 Grade change detected: {token_id[:8]}... | {last_grade} → {grade}")
                    
                    # Update grade in state
                    if not is_new_token:
                        alerts_state[token_id]["last_grade"] = grade

                    state = alerts_state.get(token_id, {})
                    
                    # ✅ SEND ALERT NOTIFICATIONS (only to users with "alerts" mode)
                    await send_alert_to_subscribers(
                        app, token_info, grade, user_manager,
                        previous_grade=last_grade, 
                        initial_mc=state.get("initial_marketcap"),
                        initial_fdv=state.get("initial_fdv"), 
                        first_alert_at=state.get("first_alert_at")
                    )
                    
                    # ✅ TRIGGER TRADE SIGNALS (for users with "papertrade" mode)
                    await trigger_trade_signals(
                        token_info, grade, user_manager, signal_queue
                    )
                    
                    alerts_sent_this_cycle += 1

            # Clean up old signals from queue
            current_time = datetime.utcnow()
            expired_signals = []
            for mint, signal_data in signal_queue.items():
                queued_at = datetime.fromisoformat(signal_data["queued_at"].rstrip("Z"))
                if (current_time - queued_at).total_seconds() > 300:
                    expired_signals.append(mint)
            
            for mint in expired_signals:
                del signal_queue[mint]
            
            if expired_signals:
                logger.debug(f"🧹 Cleaned up {len(expired_signals)} expired signals from queue")

            if alerts_sent_this_cycle > 0:
                logger.info(f"💾 Saving alert state after processing {alerts_sent_this_cycle} changes...")
                safe_save(ALERTS_STATE_FILE, alerts_state)
                if USE_SUPABASE and upload_file:
                    try:
                        upload_file(str(ALERTS_STATE_FILE), bucket=BUCKET_NAME)
                    except Exception as e:
                        logger.warning(f"⚠️ Failed to upload alerts_state: {e}")

            await asyncio.sleep(POLL_INTERVAL_SECS)

        except Exception as e:
            logger.exception(f"❌ Error in background loop: {e}")
            await asyncio.sleep(POLL_INTERVAL_SECS)


# ✅ IMPROVED: Enhanced broadcast function with better error handling
async def broadcast_mint_to_groups(app: Application, mint_address: str):
    """Broadcasts a message with the mint address and an inline button."""
    try:
        groups = safe_load(GROUPS_FILE, {})
        if not groups:
            logger.debug("No groups configured for broadcasting")
            return
        
        active_groups = {k: v for k, v in groups.items() if v.get("active", True)}
        if not active_groups:
            logger.debug("No active groups for broadcasting")
            return
        
        logger.info(f"📢 Broadcasting mint to {len(active_groups)} groups: {mint_address}")
        
        message_text = (
            f"🆕 <b>New Token Detected</b>\n\n"
            f"📋 Contract Address:\n"
            f"<code>{mint_address}</code>\n\n"
            f"👇 <i>Click below to analyze the C.A</i>"
        )
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔍 Analysis", callback_data=f"analyze_{mint_address}")],
            [
                InlineKeyboardButton("📊 Quick Trade (Bonkbot)", url=f"https://t.me/bonkbot_bot?start=ref_68ulj_ca_{mint_address}"),
                InlineKeyboardButton("🤖 Trojan Bot", url=f"https://t.me/paris_trojanbot?start=r-ismarty1-{mint_address}")
            ]
        ])

        sent_count = 0
        failed_count = 0
        
        for group_id, group_info in active_groups.items():
            try:
                await app.bot.send_message(
                    chat_id=int(group_id), 
                    text=message_text, 
                    reply_markup=keyboard,
                    parse_mode="HTML", 
                    disable_web_page_preview=True
                )
                sent_count += 1
                logger.info(f"✅ Sent mint to group {group_id} ({group_info.get('name', 'Unknown')})")
            except Exception as e:
                failed_count += 1
                error_msg = str(e).lower()
                logger.warning(f"⚠️ Failed to send to group {group_id}: {e}")
                
                # Deactivate group if bot was blocked or chat not found
                if any(keyword in error_msg for keyword in [
                    "bot was blocked", 
                    "chat not found", 
                    "forbidden", 
                    "bot is not a member",
                    "have no rights to send"
                ]):
                    groups[group_id]["active"] = False
                    safe_save(GROUPS_FILE, groups)
                    logger.info(f"🚫 Deactivated group {group_id} due to access error")
            
            await asyncio.sleep(0.1)
        
        logger.info(f"📊 Broadcast complete: {sent_count} sent, {failed_count} failed")
            
    except Exception as e:
        logger.exception(f"❌ Error in broadcast_mint_to_groups: {e}")