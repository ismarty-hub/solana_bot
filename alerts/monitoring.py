#!/usr/bin/env python3
"""
alerts/monitoring.py - Background monitoring with Supabase polling
🔥 COMPLETE VERSION - Downloads from Supabase every 60 seconds!
UPDATED: Added group mint broadcasting functionality
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
    OVERLAP_FILE, USER_PREFS_FILE, USER_STATS_FILE, ALERTS_STATE_FILE, GROUPS_FILE,
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
    
    for file in [USER_PREFS_FILE, USER_STATS_FILE, ALERTS_STATE_FILE, GROUPS_FILE]:
        if file.exists():
            try:
                if now - _last_upload < 43200:  # Only once every 12 hrs
                    return
                upload_file(str(file), bucket=BUCKET_NAME)
                _last_upload = now
            except Exception as e:
                logger.exception(f"Failed to upload {file} to Supabase: {e}")


def download_bot_data_from_supabase():
    """Download bot data files from Supabase (opt-in)."""
    if not USE_SUPABASE or download_file is None:
        logger.debug("Supabase download skipped (disabled or helper missing).")
        return
    
    for file in [USER_PREFS_FILE, USER_STATS_FILE, ALERTS_STATE_FILE]:
        try:
            download_file(str(file), os.path.basename(file), bucket=BUCKET_NAME)
        except Exception as e:
            logger.debug(f"Could not download {file} from Supabase: {e}")


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


def upload_bot_data():
    """Upload bot state to Supabase."""
    if not upload_file:
        return
    
    try:
        for file in [ALERTS_STATE_FILE, USER_PREFS_FILE, USER_STATS_FILE]:
            if file.exists():
                upload_file(str(file), bucket=BUCKET_NAME)
        logger.info("✅ Uploaded bot data to Supabase")
    except Exception as e:
        logger.warning(f"⚠️ Upload failed: {e}")


async def daily_supabase_sync():
    """Daily background task to sync data with Supabase."""
    if not (USE_SUPABASE):
        logger.debug("Daily Supabase sync disabled by configuration.")
        return
    
    logger.info("📅 Daily Supabase sync task started.")
    while True:
        try:
            upload_bot_data_to_supabase()
            logger.info("✅ Daily sync with Supabase complete")
        except Exception as e:
            logger.exception(f"Supabase daily sync failed: {e}")
        await asyncio.sleep(24 * 3600)


async def periodic_overlap_download():
    """
    DEPRECATED: This function is replaced by download in main loop.
    Keeping it here for compatibility but it does nothing.
    """
    logger.info("⚠️ periodic_overlap_download is deprecated - using main loop download instead")
    await asyncio.sleep(999999)  # Sleep forever


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
# NEW: GROUP MINT BROADCASTING
# ----------------------

async def broadcast_mint_to_groups(app: Application, mint_address: str):
    """
    Broadcast only the mint address to all active groups.
    This is called for every new token alert, in parallel with user alerts.
    """
    try:
        # Load active groups
        groups = safe_load(GROUPS_FILE, {})
        
        if not groups:
            logger.debug("No groups configured for mint broadcasting")
            return
        
        active_groups = {k: v for k, v in groups.items() if v.get("active", True)}
        
        if not active_groups:
            logger.debug("No active groups for mint broadcasting")
            return
        
        logger.info(f"📢 Broadcasting mint to {len(active_groups)} groups: {mint_address}")
        
        # Simple message with just the mint address
        message = mint_address
        
        sent_count = 0
        failed_count = 0
        
        for group_id, group_info in active_groups.items():
            try:
                await app.bot.send_message(
                    chat_id=int(group_id),
                    text=message,
                    disable_web_page_preview=True
                )
                sent_count += 1
                logger.info(f"✅ Sent mint to group {group_id} ({group_info.get('name', 'Unknown')})")
                
            except Exception as e:
                failed_count += 1
                logger.warning(f"⚠️ Failed to send to group {group_id}: {e}")
                
                # If bot was removed/blocked, mark group as inactive
                if "bot was blocked" in str(e).lower() or "chat not found" in str(e).lower():
                    logger.warning(f"🚫 Bot removed from group {group_id}, marking as inactive")
                    groups[group_id]["active"] = False
                    safe_save(GROUPS_FILE, groups)
            
            # Small delay between sends
            await asyncio.sleep(0.1)
        
        logger.info(f"📊 Group broadcast complete: {sent_count} sent, {failed_count} failed")
        
    except Exception as e:
        logger.exception(f"❌ Error in broadcast_mint_to_groups: {e}")


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
    """Send an alert to subscribed users."""
    active_users = user_manager.get_active_users()
    
    if not active_users:
        logger.debug("No active users to send alerts to")
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
        buttons.append(InlineKeyboardButton("🔗 Bonkbot", url=f"https://t.me/bonkbot_bot?start=ref_68ulj_ca_{mint}"))
        buttons.append(InlineKeyboardButton("🔗 Trojan", url=f"https://t.me/paris_trojanbot?start=r-ismarty1-{mint}"))

    keyboard = InlineKeyboardMarkup([buttons]) if buttons else None

    sent_count = 0
    for chat_id, prefs in active_users.items():
        # Check subscription
        if not user_manager.is_subscribed(chat_id):
            logger.debug(f"Skipping alert for {chat_id}: not subscribed or expired")
            continue

        # Check if user wants this grade
        subscribed_grades = prefs.get("grades", ALL_GRADES.copy())
        if isinstance(subscribed_grades, (list, tuple)):
            if grade not in subscribed_grades:
                continue
        else:
            if grade not in ALL_GRADES:
                continue

        # Send alert
        try:
            await app.bot.send_message(
                chat_id=int(chat_id),
                text=message,
                parse_mode="HTML",
                disable_web_page_preview=True,
                reply_markup=keyboard
            )
            user_manager.update_user_stats(chat_id, grade)
            sent_count += 1
            logger.info(f"✅ Sent {grade} alert to {chat_id}")
        except Exception as e:
            logger.warning(f"⚠️ Failed to send alert to {chat_id}: {e}")

        await asyncio.sleep(0.1)
    
    logger.info(f"📤 Sent {sent_count} alerts for grade {grade}")


# ----------------------
# Monthly expiry notifier
# ----------------------
async def monthly_expiry_notifier(app: Application, user_manager):
    """Notify expired users once per month."""
    logger.info("📅 Starting monthly expiry notifier...")
    
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
                            logger.info(f"Notified expired user {chat_id}")
                        except Exception as e:
                            logger.warning(f"Failed to notify {chat_id}: {e}")
        
        except Exception as e:
            logger.exception(f"Error in expiry notifier: {e}")

        await asyncio.sleep(24 * 3600)


# ----------------------
# 🔥 BACKGROUND LOOP - DOWNLOADS EVERY 60 SECONDS!
# ----------------------
async def background_loop(app: Application, user_manager):
    """
    Main monitoring loop:
    1. Download overlap_results.pkl from Supabase
    2. Check for new/changed tokens
    3. Send alerts to users AND broadcast mints to groups
    4. Wait 60 seconds
    5. Repeat
    """
    logger.info("🔄 Background alert loop started!")
    logger.info(f"⏰ Polling every {POLL_INTERVAL_SECS} seconds")

    # Load alert state
    alerts_state = safe_load(ALERTS_STATE_FILE, {})
    logger.info(f"📂 Loaded alert state: {len(alerts_state)} tokens tracked")

    loop_count = 0

    while True:
        try:
            loop_count += 1
            logger.info(f"\n{'='*60}")
            logger.info(f"🔍 Loop #{loop_count} - {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
            logger.info(f"{'='*60}")
            
            # 🔥 STEP 1: DOWNLOAD FROM SUPABASE
            logger.info("🔥 Step 1: Downloading latest data from Supabase...")
            download_success = download_latest_overlap()
            
            if not download_success:
                logger.warning("⚠️ Download failed, using cached data if available")
            
            # 🔥 STEP 2: LOAD TOKENS
            logger.info("📂 Step 2: Loading tokens from file...")
            tokens = load_latest_tokens_from_overlap()
            
            if not tokens:
                logger.warning("⚠️ No tokens loaded! Will retry next cycle.")
                await asyncio.sleep(POLL_INTERVAL_SECS)
                continue

            # 🔥 STEP 3: FILTER TODAY'S TOKENS
            logger.info("📅 Step 3: Filtering today's tokens...")
            today = datetime.utcnow().date()
            fresh_tokens = {
                tid: t for tid, t in tokens.items()
                if t.get("checked_at") and datetime.fromisoformat(
                    t["checked_at"].rstrip("Z")
                ).date() >= today
            }
            
            logger.info(f"🆕 Found {len(fresh_tokens)} fresh tokens from today")
            
            if loop_count <= 3 and fresh_tokens:
                # Show samples for first 3 loops
                for i, (tid, info) in enumerate(list(fresh_tokens.items())[:3]):
                    logger.info(f"  Sample {i+1}: {tid[:8]}... | {info.get('grade')} | {info.get('checked_at')[:16]}")

            # 🔥 STEP 4: PROCESS TOKENS AND SEND ALERTS
            logger.info("🔔 Step 4: Checking for alerts...")
            alerts_sent = 0

            for token_id, token in fresh_tokens.items():
                grade = token.get("grade")
                
                # Skip if no grade or NONE
                if not grade or grade == "NONE":
                    continue

                # Check current state
                current_state = alerts_state.get(token_id)
                last_grade = current_state.get("last_grade") if isinstance(current_state, dict) else None

                # Check if this is new or changed
                if grade != last_grade:
                    logger.info(f"🔔 Alert trigger: {token_id[:8]}... | {last_grade} → {grade}")

                    if grade in VALID_GRADES:
                        # First time seeing this token
                        if last_grade is None:
                            logger.info(f"🆕 NEW TOKEN: {token_id[:8]}...")
                            mc, fdv, lqd = fetch_marketcap_and_fdv(token_id)
                            alerts_state[token_id] = {
                                "last_grade": grade,
                                "initial_marketcap": mc,
                                "initial_fdv": fdv,
                                "initial_liquidity": lqd,
                                "first_alert_at": datetime.utcnow().isoformat() + "Z"
                            }
                            logger.info(f"💰 Market data: MC={mc}, FDV={fdv}, Liq={lqd}")
                            
                            # Save state immediately after new token
                            safe_save(ALERTS_STATE_FILE, alerts_state)
                            if USE_SUPABASE and upload_file:
                                try:
                                    upload_file(str(ALERTS_STATE_FILE), bucket=BUCKET_NAME)
                                    logger.info("✅ Uploaded alerts_state after new token")
                                except Exception as e:
                                    logger.warning(f"⚠️ Failed to upload alerts_state: {e}")
                        else:
                            # Grade changed
                            logger.info(f"🔄 GRADE CHANGE: {token_id[:8]}...")
                            alerts_state[token_id]["last_grade"] = grade

                        # 🔥 SEND THE ALERT TO USERS!
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
                        
                        # 🔥 NEW: BROADCAST MINT ADDRESS TO GROUPS!
                        mint_address = token.get("token_metadata", {}).get("mint") or token_id
                        await broadcast_mint_to_groups(app, mint_address)
                        
                        alerts_sent += 1

            logger.info(f"✅ Alerts sent this cycle: {alerts_sent}")

            # 🔥 STEP 5: SAVE STATE
            if alerts_sent > 0:
                logger.info("💾 Saving alert state...")
                safe_save(ALERTS_STATE_FILE, alerts_state)
                if USE_SUPABASE and upload_file:
                    try:
                        upload_file(str(ALERTS_STATE_FILE), bucket=BUCKET_NAME)
                        logger.info("✅ Synced alerts_state to Supabase")
                    except Exception as e:
                        logger.warning(f"⚠️ Failed to upload alerts_state: {e}")

            # 🔥 STEP 6: WAIT FOR NEXT CYCLE
            logger.info(f"⏰ Sleeping for {POLL_INTERVAL_SECS} seconds...")
            await asyncio.sleep(POLL_INTERVAL_SECS)

        except Exception as e:
            logger.exception(f"❌ Error in background loop: {e}")
            logger.info(f"⏰ Sleeping {POLL_INTERVAL_SECS}s before retry...")
            await asyncio.sleep(POLL_INTERVAL_SECS)