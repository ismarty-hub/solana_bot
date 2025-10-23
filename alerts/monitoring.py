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

try:
    from supabase_utils import download_overlap_results, upload_file, download_file
    logger.info("✅ Supabase utils loaded successfully")
except Exception as e:
    logger.error(f"❌ Failed to load supabase_utils: {e}")
    download_overlap_results = None
    upload_file = None
    download_file = None


def upload_all_bot_data_to_supabase():
    """Upload ALL bot data files to Supabase."""
    if not USE_SUPABASE or upload_file is None:
        logger.debug("Supabase upload skipped (disabled or helper missing).")
        return

    logger.info("☁️ Starting periodic upload of all bot data to Supabase...")
    
    files_to_upload = [
        USER_PREFS_FILE, 
        USER_STATS_FILE, 
        GROUPS_FILE, 
        ALERTS_STATE_FILE, 
        PORTFOLIOS_FILE
    ]
    
    uploaded_count = 0
    failed_count = 0
    
    for file in files_to_upload:
        if file.exists():
            try:
                remote_path = None
                if file == PORTFOLIOS_FILE:
                    remote_path = f"paper_trade/{PORTFOLIOS_FILE.name}"
                
                if upload_file(str(file), bucket=BUCKET_NAME, remote_path=remote_path):
                    uploaded_count += 1
                else:
                    failed_count += 1
            except Exception as e:
                failed_count += 1
                logger.exception(f"Failed to upload {file} to Supabase: {e}")
        else:
            logger.debug(f"Skipping upload for non-existent file: {file.name}")
    
    logger.info(f"☁️ Periodic sync complete: {uploaded_count} files uploaded, {failed_count} failed.")


def download_bot_data_from_supabase():
    """Download bot data files from Supabase (opt-in)."""
    if not USE_SUPABASE or download_file is None:
        logger.debug("Supabase download skipped (disabled or helper missing).")
        return
    
    for file in [USER_PREFS_FILE, USER_STATS_FILE, ALERTS_STATE_FILE, GROUPS_FILE]:
        try:
            download_file(str(file), os.path.basename(file), bucket=BUCKET_NAME)
        except Exception as e:
            logger.debug(f"Could not download {file} from Supabase: {e}")

    try:
        remote_path = f"paper_trade/{PORTFOLIOS_FILE.name}"
        download_file(str(PORTFOLIOS_FILE), remote_path, bucket=BUCKET_NAME)
    except Exception as e:
        logger.debug(f"Could not download portfolios from Supabase: {e}")


def download_latest_overlap():
    """Download overlap_results.pkl from Supabase."""
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


async def periodic_supabase_sync():
    """Periodic background task to sync all data with Supabase."""
    if not USE_SUPABASE:
        logger.debug("Periodic Supabase sync disabled by configuration.")
        return
    
    await asyncio.sleep(60)
    
    logger.info("📅 Starting periodic Supabase sync task (every 5 minutes).")
    while True:
        try:
            upload_all_bot_data_to_supabase()
            logger.info("✅ Periodic sync with Supabase complete")
        except Exception as e:
            logger.exception(f"Supabase periodic sync failed: {e}")
        
        await asyncio.sleep(300)


def load_latest_tokens_from_overlap() -> Dict[str, Dict[str, Any]]:
    """Load overlap_results.pkl from local disk."""
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
    """Send alert notification to subscribed users with 'alerts' mode enabled."""
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


async def trigger_trade_signals(
    token_data: Dict[str, Any],
    grade: str,
    user_manager,
    signal_queue: Dict[str, list]
):
    """
    Queue trade signals for users with 'papertrade' mode enabled.
    This does NOT send messages - signals are queued for processing.
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
        
        user_prefs = user_manager.get_user_prefs(chat_id)
        subscribed_grades = user_prefs.get("grades", ALL_GRADES)
        if grade not in subscribed_grades:
            continue
        
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
                        except: 
                            pass
                    
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

    # Exponential backoff starting interval
    INITIAL_RETRY_INTERVAL_SECS = 1

    while True:
        try:
            download_latest_overlap()
            
            tokens = load_latest_tokens_from_overlap()
            if not tokens:
                await asyncio.sleep(POLL_INTERVAL_SECS)
                continue

            alerts_sent_this_cycle = 0
            state_updated_this_cycle = 0

            for token_id, token_info in tokens.items():
                grade = token_info.get("grade")
                if not grade or grade not in VALID_GRADES:
                    continue

                current_state = alerts_state.get(token_id)
                last_grade = current_state.get("last_grade") if isinstance(current_state, dict) else None

                is_new_token = (last_grade is None)
                is_grade_change = (not is_new_token and grade != last_grade)
                
                # --- Initial State Saving (to Gate Alerts and Track Retries) ---
                if is_new_token:
                    mc, fdv, lqd = fetch_marketcap_and_fdv(token_id)
                    
                    # Determine if data is complete (MUST have MC AND Liquidity)
                    data_complete = (mc is not None and lqd is not None)
                    
                    alerts_state[token_id] = {
                        "last_grade": grade, 
                        "initial_marketcap": mc,
                        "initial_fdv": fdv,
                        "initial_liquidity": lqd, 
                        "first_alert_at": datetime.utcnow().isoformat() + "Z",
                        "broadcasted": False,
                        "data_complete": data_complete, 
                        "last_market_data_retry_at": None if data_complete else datetime.utcnow().isoformat() + "Z",
                        "market_data_retry_count": 0 if data_complete else 1
                    }
                    
                    logger.info(f"🆕 New token detected: {token_id[:8]}... | Grade: {grade} | Data Complete: {data_complete}")
                
                state = alerts_state.get(token_id, {})
                should_send_gated_alert = False 

                # --- GATED ALERT AND EXPONENTIAL SILENT RETRY ---
                # Only retry if token exists (not new) and data is incomplete
                if not is_new_token and state.get("data_complete") is False:
                    last_retry = state.get("last_market_data_retry_at")
                    retry_count = state.get("market_data_retry_count", 1)
                    current_time = datetime.utcnow()
                    should_retry = True
                    
                    if last_retry:
                        try:
                            last_dt = datetime.fromisoformat(last_retry.rstrip("Z"))
                            required_delay = INITIAL_RETRY_INTERVAL_SECS * (2 ** (retry_count - 1))
                            time_elapsed = (current_time - last_dt).total_seconds()
                            
                            if time_elapsed < required_delay:
                                should_retry = False
                                logger.debug(f"ℹ️ Skipping retry for {token_id[:8]}... Backoff active ({round(time_elapsed)}s elapsed, {required_delay}s required).")
                        except Exception as e:
                            logger.error(f"Error parsing timestamp for backoff: {e}")
                    
                    if should_retry:
                        new_mc, new_fdv, new_lqd = fetch_marketcap_and_fdv(token_id)
                        is_now_complete = (new_mc is not None and new_lqd is not None)
                        
                        alerts_state[token_id]["last_market_data_retry_at"] = current_time.isoformat() + "Z"

                        if is_now_complete:
                            logger.info(f"💰 GATED ALERT TRIGGERED: All market data found on retry #{retry_count} for {token_id[:8]}...")
                            
                            alerts_state[token_id]["initial_marketcap"] = new_mc
                            alerts_state[token_id]["initial_fdv"] = new_fdv
                            alerts_state[token_id]["initial_liquidity"] = new_lqd
                            alerts_state[token_id]["data_complete"] = True
                            
                            if "market_data_retry_count" in alerts_state[token_id]:
                                del alerts_state[token_id]["market_data_retry_count"]
                            
                            should_send_gated_alert = True
                            state_updated_this_cycle += 1
                        else:
                            alerts_state[token_id]["market_data_retry_count"] = retry_count + 1
                            logger.debug(f"ℹ️ Market data still incomplete (Retry #{retry_count + 1}) for {token_id[:8]}... Backoff set.")
                            state_updated_this_cycle += 1

                # --- Broadcasting Logic (unchanged) ---
                should_broadcast = (
                    grade != "NONE" and
                    not state.get("broadcasted", False)
                )
                
                if should_broadcast:
                    mint_address = token_info.get("token_metadata", {}).get("mint", token_id)
                    try:
                        await broadcast_mint_to_groups(app, mint_address)
                        alerts_state[token_id]["broadcasted"] = True
                        logger.info(f"✅ Broadcasted to groups: {mint_address} (Grade: {grade})")
                    except Exception as e:
                        logger.error(f"❌ Broadcast failed for {mint_address}: {e}")
                
                # --- FIXED: Alert Logic ---
                # Send alert on:
                # 1. Grade change (existing token)
                # 2. New token with complete data
                # 3. Gated token whose data just became complete
                is_alert_required = (
                    is_grade_change or 
                    (is_new_token and state.get("data_complete")) or
                    should_send_gated_alert
                )

                if is_alert_required:
                    if is_grade_change:
                        logger.info(f"🔔 Grade change detected: {token_id[:8]}... | {last_grade} → {grade}")
                        alerts_state[token_id]["last_grade"] = grade
                    elif is_new_token:
                        logger.info(f"🔔 Sending alert for new token: {token_id[:8]}... | Grade: {grade}")
                    else:  # should_send_gated_alert
                        logger.info(f"🔔 Sending gated alert (data now complete): {token_id[:8]}... | Grade: {grade}")

                    state = alerts_state.get(token_id, {})
                    
                    await send_alert_to_subscribers(
                        app, token_info, grade, user_manager,
                        previous_grade=last_grade if is_grade_change else None,
                        initial_mc=state.get("initial_marketcap"),
                        initial_fdv=state.get("initial_fdv"), 
                        first_alert_at=state.get("first_alert_at")
                    )
                    
                    await trigger_trade_signals(
                        token_info, grade, user_manager, signal_queue
                    )
                    
                    alerts_sent_this_cycle += 1

            # Clean up expired signals
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

            # Save state if changes occurred
            if alerts_sent_this_cycle > 0 or state_updated_this_cycle > 0:
                logger.info(f"💾 Saving alert state after processing {alerts_sent_this_cycle} alerts and {state_updated_this_cycle} state updates...")
                safe_save(ALERTS_STATE_FILE, alerts_state)

            await asyncio.sleep(POLL_INTERVAL_SECS)

        except Exception as e:
            logger.exception(f"❌ Error in background loop: {e}")
            await asyncio.sleep(POLL_INTERVAL_SECS)


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
            [
                InlineKeyboardButton(
                    "🔍 Analysis",
                    switch_inline_query_current_chat=mint_address
                )
            ],
            [
                InlineKeyboardButton(
                    "📊 Quick Trade (Bonkbot)",
                    url=f"https://t.me/bonkbot_bot?start=ref_68ulj_ca_{mint_address}"
                ),
                InlineKeyboardButton(
                    "🤖 Trojan Bot",
                    url=f"https://t.me/paris_trojanbot?start=r-ismarty1-{mint_address}"
                )
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