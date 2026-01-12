#!/usr/bin/env python3
"""
alerts/monitoring.py - Background monitoring with Supabase polling (ALERTS ONLY)

This version focuses solely on sending alerts to users, with all trading logic removed.
"""

import os
import asyncio
import logging
import joblib
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional
from telegram.ext import Application
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from config import (
    OVERLAP_FILE, USER_PREFS_FILE, USER_STATS_FILE, ALERTS_STATE_FILE, 
    GROUPS_FILE, PORTFOLIOS_FILE, BUCKET_NAME, USE_SUPABASE, 
    POLL_INTERVAL_SECS, VALID_GRADES, ALL_GRADES, ALPHA_ALERTS_STATE_FILE,
    DATA_DIR
)
from pathlib import Path
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
    # Check discovery entry first
    discovery_key = f"{mint}_discovery"
    if discovery_key in active_tracking:
        initial_ml_passed = active_tracking[discovery_key].get("ML_PASSED", True)
        if not initial_ml_passed:
            logger.debug(f"⛔ Token {mint[:8]}... had initial ML_PASSED=False in active_tracking")
            return False
        return True
    
    # Check alpha entry as fallback
    alpha_key = f"{mint}_alpha"
    if alpha_key in active_tracking:
        initial_ml_passed = active_tracking[alpha_key].get("ML_PASSED", True)
        if not initial_ml_passed:
            logger.debug(f"⛔ Token {mint[:8]}... had initial ML_PASSED=False in active_tracking")
            return False
        return True
    
    # Token not found in active_tracking - it's new, allow the alert
    return True


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
        PORTFOLIOS_FILE,
        ALPHA_ALERTS_STATE_FILE
    ]
    
    uploaded_count = 0
    failed_count = 0
    
    for file in files_to_upload:
        if file.exists():
            try:
                remote_path = None
                if file == PORTFOLIOS_FILE:
                    remote_path = f"paper_trade/{PORTFOLIOS_FILE.name}"
                elif file == ALPHA_ALERTS_STATE_FILE:
                    remote_path = ALPHA_ALERTS_STATE_FILE.name
                
                upload_file(str(file), bucket=BUCKET_NAME, remote_path=remote_path)
                uploaded_count += 1
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
    
    files_to_download = [
        USER_PREFS_FILE, 
        USER_STATS_FILE, 
        ALERTS_STATE_FILE, 
        GROUPS_FILE,
        ALPHA_ALERTS_STATE_FILE
    ]

    for file in files_to_download:
        try:
            remote_file_name = os.path.basename(file)
            download_file(str(file), remote_file_name, bucket=BUCKET_NAME)
        except Exception as e:
            logger.debug(f"Could not download {file} from Supabase: {e}")

    try:
        remote_path = f"paper_trade/{PORTFOLIOS_FILE.name}"
        logger.info(f"⬇️ Downloading portfolios from Supabase: {remote_path}")
        result = download_file(str(PORTFOLIOS_FILE), remote_path, bucket=BUCKET_NAME)
        if result:
            logger.info(f"✅ Successfully downloaded portfolios. Size: {len(result)} bytes")
        else:
            logger.warning("⚠️ Portfolio file not found on Supabase or download failed.")
    except Exception as e:
        logger.error(f"❌ Critical error downloading portfolios from Supabase: {e}")


def download_latest_overlap():
    """Download overlap_results.pkl from Supabase."""
    if not download_overlap_results:
        logger.warning("⚠️ download_overlap_results function not available!")
        return False
    
    try:
        logger.debug("⬇️ Downloading overlap_results.pkl from Supabase...")
        download_overlap_results(str(OVERLAP_FILE), bucket=BUCKET_NAME)
        
        if OVERLAP_FILE.exists():
            size_kb = OVERLAP_FILE.stat().st_size / 1024
            logger.debug(f"✅ Downloaded: {size_kb:.2f} KB")
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
            logger.debug("✅ Periodic sync with Supabase complete")
        except Exception as e:
            logger.exception(f"Supabase periodic sync failed: {e}")
        
        await asyncio.sleep(300)


def load_latest_tokens_from_overlap() -> Dict[str, Dict[str, Any]]:
    """
    Load overlap_results.pkl from local disk.
    Includes pre-fetched dexscreener and rugcheck data from token_monitor.py.
    ML_PASSED is stored at the top level of each entry.
    Includes ML prediction data for enhanced alerts.
    """
    if not OVERLAP_FILE.exists() or OVERLAP_FILE.stat().st_size == 0:
        logger.debug("ℹ️ No local overlap file yet (will be downloaded from Supabase)")
        return {}

    try:
        data = joblib.load(OVERLAP_FILE)
        latest_tokens = {}
        
        for token_id, history in data.items():
            if not history:
                continue
            
            last_entry = history[-1]
            if not isinstance(last_entry, dict):
                logger.warning(f"Skipping malformed entry for {token_id}")
                continue
                
            result = last_entry.get("result", {})
            dexscreener_data = last_entry.get("dexscreener", {})
            rugcheck_data = last_entry.get("rugcheck", {})
            ml_prediction_data = result.get("ml_prediction", {})
            
            latest_tokens[token_id] = {
                "grade": result.get("grade", "NONE"),
                "token_metadata": {
                    "mint": token_id,
                    "name": result.get("token_metadata", {}).get("name"),
                    "symbol": result.get("token_metadata", {}).get("symbol", "")
                },
                "overlap_percentage": result.get("overlap_percentage", 0.0),
                "concentration": result.get("concentration", 0.0),
                "checked_at": result.get("checked_at"),
                "dexscreener": dexscreener_data,
                "rugcheck": rugcheck_data,
                "ml_prediction": ml_prediction_data,
                "ml_passed": last_entry.get("ML_PASSED", False)
            }
        
        logger.debug(f"📊 Loaded {len(latest_tokens)} tokens from overlap file")
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
    """
    Send alert notification to subscribed users with 'alerts' mode enabled.
    Uses pre-fetched dexscreener/rugcheck data from token_data.
    """
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
            # Check Minimum Probability Filter
            min_prob = prefs.get("min_prob_discovery", 0.0)
            
            # Extract probability (default to 1.0 if missing to be safe, or 0.0?)
            # The structure is token_data["ml_prediction"]["probability"]
            ml_pred = token_data.get("ml_prediction", {})
            # If ml_prediction is just "passed" (bool) or empty, what do?
            # Based on previous file reads, it has "probability".
            prob = ml_pred.get("probability", 0.0) if isinstance(ml_pred, dict) else 0.0
            
            # Skip if probability is lower than user's threshold
            if prob < min_prob:
                continue

            await app.bot.send_message(
                chat_id=int(chat_id), 
                text=message, 
                parse_mode="HTML",
                disable_web_page_preview=True, 
                reply_markup=keyboard
            )
            user_manager.update_user_stats(chat_id, grade)
            sent_count += 1
        except Exception as e:
            logger.warning(f"⚠️ Failed to send alert to {chat_id}: {e}")

        await asyncio.sleep(0.1)
    
    if sent_count > 0:
        logger.info(f"📤 Sent {sent_count} alert notifications for grade {grade}")


async def broadcast_mint_to_groups(app: Application, mint_address: str):
    """Broadcasts a message with the mint address and inline buttons to all active groups."""
    try:
        groups = safe_load(GROUPS_FILE, {})
        if not groups:
            logger.debug("No groups configured for broadcasting")
            return
        
        active_groups = {k: v for k, v in groups.items() if v.get("active", True)}
        if not active_groups:
            logger.debug("No active groups for broadcasting")
            return
        
        logger.info(f"📢 Broadcasting mint to {len(active_groups)} groups: {mint_address[:8]}...")
        
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
                logger.debug(f"✅ Sent mint to group {group_id} ({group_info.get('name', 'Unknown')})")
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
                            if (datetime.now(timezone.utc) - last_dt.replace(tzinfo=timezone.utc)).days < 30:
                                should_notify = False
                        except Exception as e:
                            logger.debug(f"Error parsing last_notified date: {e}")
                    
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
    and sends alerts to subscribed users.
    
    Note: portfolio_manager parameter kept for compatibility but not used in alerts-only mode.
    """
    logger.info("🔄 Background alert loop started!")
    logger.info(f"⏰ Polling every {POLL_INTERVAL_SECS} seconds")

    alerts_state = safe_load(ALERTS_STATE_FILE, {})
    logger.info(f"📂 Loaded alert state: {len(alerts_state)} tokens tracked")

    # Exponential backoff starting interval
    INITIAL_RETRY_INTERVAL_SECS = 1

    while True:
        try:
            download_latest_overlap()
            
            tokens = load_latest_tokens_from_overlap()
            if not tokens:
                logger.debug("No tokens loaded, waiting for next cycle...")
                await asyncio.sleep(POLL_INTERVAL_SECS)
                continue

            # Download active_tracking.json for ML_PASSED initial status crosscheck
            active_tracking = download_active_tracking()

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
                    # Use pre-fetched data from overlap file
                    dex_data = token_info.get("dexscreener", {})
                    rugcheck_data = token_info.get("rugcheck", {})
                    
                    mc = dex_data.get("market_cap_usd")
                    lqd = rugcheck_data.get("total_liquidity_usd")
                    
                    # Determine if data is complete (MUST have MC AND Liquidity)
                    data_complete = (mc is not None and lqd is not None)
                    
                    # Fallback to live fetch if pre-fetched data is incomplete
                    if not data_complete:
                        _mc_live, _fdv_live, _lqd_live = fetch_marketcap_and_fdv(token_id)
                        final_mc = _mc_live if mc is None else mc
                        final_lqd = _lqd_live if lqd is None else lqd
                        final_fdv = _fdv_live
                        data_complete = (final_mc is not None and final_lqd is not None)
                    else:
                        final_mc = mc
                        final_lqd = lqd
                        # Try to get FDV from live fetch as it's not in pre-fetched data
                        _, final_fdv, _ = fetch_marketcap_and_fdv(token_id)

                    alerts_state[token_id] = {
                        "last_grade": grade, 
                        "initial_marketcap": final_mc,
                        "initial_fdv": final_fdv,
                        "initial_liquidity": final_lqd, 
                        "first_alert_at": datetime.now(timezone.utc).isoformat(),
                        "broadcasted": False,
                        "data_complete": data_complete, 
                        "last_market_data_retry_at": None if data_complete else datetime.now(timezone.utc).isoformat(),
                        "market_data_retry_count": 0 if data_complete else 1
                    }
                    
                    logger.info(f"🆕 New token detected: {token_id[:8]}... | Grade: {grade} | Data Complete: {data_complete}")
                    state_updated_this_cycle += 1
                
                state = alerts_state.get(token_id, {})
                should_send_gated_alert = False 

                # --- GATED ALERT AND EXPONENTIAL SILENT RETRY ---
                # For tokens initially detected with incomplete data
                if not is_new_token and state.get("data_complete") is False:
                    last_retry = state.get("last_market_data_retry_at")
                    retry_count = state.get("market_data_retry_count", 1)
                    current_time = datetime.now(timezone.utc)
                    should_retry = True
                    
                    if last_retry:
                        try:
                            last_dt = datetime.fromisoformat(last_retry.rstrip("Z")).replace(tzinfo=timezone.utc)
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
                        
                        alerts_state[token_id]["last_market_data_retry_at"] = current_time.isoformat()

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

                # --- Broadcasting Logic ---
                should_broadcast = (
                    grade != "NONE" and
                    not state.get("broadcasted", False) and
                    state.get("data_complete", False)
                )
                
                if should_broadcast:
                    # ML Filtering: Only broadcast if ML check passed
                    ml_passed = token_info.get("ml_passed")
                    logger.debug(f"🔍 Broadcast check for {token_id[:8]}... - ML_PASSED: {ml_passed}")
                    
                    if not ml_passed:
                        logger.debug(f"⏭️ Skipping broadcast for {token_id[:8]}... - ML_PASSED is False")
                        alerts_state[token_id]["broadcasted"] = True
                    # CRITICAL: Crosscheck with active_tracking.json for INITIAL ML_PASSED status
                    elif not check_initial_ml_passed(token_id, active_tracking):
                        logger.info(f"⛔ BLOCKED broadcast for {token_id[:8]}... - initial ML_PASSED was False")
                        alerts_state[token_id]["broadcasted"] = True
                    else:
                        mint_address = token_info.get("token_metadata", {}).get("mint", token_id)
                        try:
                            await broadcast_mint_to_groups(app, mint_address)
                            alerts_state[token_id]["broadcasted"] = True
                            logger.info(f"✅ Broadcasted to groups: {mint_address[:8]}... (Grade: {grade})")
                            state_updated_this_cycle += 1
                        except Exception as e:
                            logger.error(f"❌ Broadcast failed for {mint_address[:8]}...: {e}")
                
                # --- Alert Logic ---
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
                    # ML Filtering: Only send alerts if ML check passed
                    if not token_info.get("ml_passed"):
                        logger.debug(f"⏭️ Skipping alert for {token_id[:8]}... - ML_PASSED is False")
                        continue
                    
                    # CRITICAL: Crosscheck with active_tracking.json for INITIAL ML_PASSED status
                    if not check_initial_ml_passed(token_id, active_tracking):
                        logger.info(f"⛔ BLOCKED alert for {token_id[:8]}... - initial ML_PASSED was False")
                        continue

                    if is_grade_change:
                        logger.info(f"🔔 Grade change detected: {token_id[:8]}... | {last_grade} → {grade}")
                        alerts_state[token_id]["last_grade"] = grade
                        state_updated_this_cycle += 1
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
                    
                    alerts_sent_this_cycle += 1

            # Save state if changes occurred
            if alerts_sent_this_cycle > 0 or state_updated_this_cycle > 0:
                logger.info(f"💾 Saving alert state: {alerts_sent_this_cycle} alerts sent, {state_updated_this_cycle} state updates")
                safe_save(ALERTS_STATE_FILE, alerts_state)

                # ✅ IMMEDIATE SYNC to Supabase
                if USE_SUPABASE and upload_file:
                    try:
                        upload_file(str(ALERTS_STATE_FILE), bucket=BUCKET_NAME)
                        logger.info(f"☁️ Immediate sync complete: {ALERTS_STATE_FILE.name}")
                    except Exception as e:
                        logger.error(f"❌ Immediate sync failed for {ALERTS_STATE_FILE.name}: {e}")

            await asyncio.sleep(POLL_INTERVAL_SECS)

        except Exception as e:
            logger.exception(f"❌ Error in background loop: {e}")
            await asyncio.sleep(POLL_INTERVAL_SECS)


async def tp_metrics_update_loop(portfolio_manager):
    """
    Background loop: Calculates TP metrics (median, mean, mode)
    from the past 3 days of analytics daily files.
    
    Runs at 2 AM UTC daily, or on first startup.
    - Tokens are filtered by signal_type (discovery calculates from discovery tokens, etc.)
    - ATH ROI values are rounded to nearest multiple of 5 before calculating mode
    """
    logger.info("📈 TP Metrics update loop started! Will run every 8 hours.")
    
    # Run immediately on startup
    try:
        logger.info("🔄 Initial TP metrics calculation on startup...")
        await portfolio_manager.calculate_tp_metrics_from_daily_files()
        logger.info(f"✅ Initial TP metrics: {portfolio_manager.tp_metrics}")
    except Exception as e:
        logger.error(f"❌ Error on initial TP metrics calculation: {e}")
    
    while True:
        try:
            # Refresh every 8 hours
            interval_hours = 8
            logger.info(f"⏰ Sleeping for {interval_hours} hours until next TP metrics update...")
            await asyncio.sleep(interval_hours * 3600)
            
            # Run the calculation
            logger.info("🔄 Calculating TP metrics from past 3 days of analytics...")
            await portfolio_manager.calculate_tp_metrics_from_daily_files()
            logger.info(f"✅ TP metrics updated: {portfolio_manager.tp_metrics}")
        
        except Exception as e:
            logger.error(f"❌ Error updating TP metrics: {e}")
            # If something goes wrong, retry after 1 hour
            await asyncio.sleep(3600)

