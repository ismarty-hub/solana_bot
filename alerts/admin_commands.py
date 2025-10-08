#!/usr/bin/env python3
"""
alerts/admin_commands.py - Admin-only bot commands
UPDATED: Added debug_system_cmd to diagnose alert issues
"""

import asyncio
import logging
from datetime import datetime
from telegram import Update
from telegram.ext import ContextTypes

from config import ADMIN_USER_ID, OVERLAP_FILE, ALERTS_STATE_FILE, USER_PREFS_FILE
from shared.file_io import safe_load
from alerts.monitoring import upload_bot_data_to_supabase 
from config import USE_SUPABASE


def is_admin_update(update: Update) -> bool:
    """Check if the update is from an admin user."""
    if not ADMIN_USER_ID:
        return False
    return str(update.effective_user.id) == ADMIN_USER_ID


async def admin_stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager):
    """Handle /admin command - show platform statistics."""
    if not is_admin_update(update):
        await update.message.reply_text("Access denied.")
        return
    
    platform_stats = user_manager.get_all_stats()
    prefs = safe_load(user_manager.prefs_file, {})
    
    inactive_users = len([u for u in prefs.values() if not u.get("active", True)])
    recent_users = len([
        u for u in prefs.values() 
        if u.get("created_at") and 
        (datetime.utcnow() - datetime.fromisoformat(u["created_at"].rstrip("Z"))).days <= 7
    ])
    
    msg = (
        f"👑 <b>Admin Dashboard</b>\n\n"
        f"• Total registered: {platform_stats['total_users']}\n"
        f"• Active users: {platform_stats['active_users']}\n"
        f"• Inactive users: {inactive_users}\n"
        f"• New users (7 days): {recent_users}\n\n"
        f"• Total alerts sent: {platform_stats['total_alerts_sent']}\n"
    )
    
    await update.message.reply_html(msg)


async def force_download_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager):
    """Admin command: Force download overlap_results.pkl from Supabase."""
    if not is_admin_update(update):
        await update.message.reply_text("⛔ Access denied. Admins only.")
        return
    
    await update.message.reply_text("⬇️ Downloading overlap_results.pkl from Supabase...")
    
    try:
        from supabase_utils import download_overlap_results
        from config import BUCKET_NAME
        
        # Force download
        success = download_overlap_results(str(OVERLAP_FILE), bucket=BUCKET_NAME)
        
        if success and OVERLAP_FILE.exists():
            size_kb = OVERLAP_FILE.stat().st_size / 1024
            modified = datetime.fromtimestamp(OVERLAP_FILE.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
            
            await update.message.reply_html(
                f"✅ <b>Download successful!</b>\n\n"
                f"• Size: {size_kb:.2f} KB\n"
                f"• Modified: {modified}\n\n"
                f"Run /debugsystem to verify contents."
            )
        else:
            await update.message.reply_text("❌ Download failed or file is empty.")
            
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")


async def broadcast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager):
    """Handle /broadcast command - send message to all active users."""
    if not is_admin_update(update):
        await update.message.reply_text("Access denied.")
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /broadcast <message>")
        return
    
    message = " ".join(context.args)
    active_users = user_manager.get_active_users()
    sent = 0
    failed = 0
    
    for chat_id in active_users.keys():
        try:
            await context.bot.send_message(
                chat_id=int(chat_id),
                text=f"📢 <b>Announcement</b>\n\n{message}",
                parse_mode="HTML"
            )
            sent += 1
        except Exception as e:
            logging.warning(f"Failed broadcast to {chat_id}: {e}")
            failed += 1
        
        await asyncio.sleep(0.1)
    
    await update.message.reply_html(f"✅ Broadcast complete!\n• Sent: {sent}\n• Failed: {failed}")


async def adduser_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager):
    """Admin command: add a user with expiry in days."""
    if not is_admin_update(update):
        await update.message.reply_text("⛔ Access denied. Admins only.")
        return
    
    if len(context.args) < 2:
        await update.message.reply_text("⚠️ Usage: /adduser <chat_id> <days>")
        return

    try:
        chat_id = str(context.args[0])
        days = int(context.args[1])
        
        logging.info(f"🔧 Admin adding user {chat_id} with {days} days validity")
        
        # Add user and get expiry date
        expiry_date_str = user_manager.add_user_with_expiry(chat_id, days)

        # Optional: check subscription status if needed
        is_sub_after = user_manager.is_subscribed(chat_id)

        # If Supabase sync is enabled
        if USE_SUPABASE:
            try:
                upload_bot_data_to_supabase()
                await update.message.reply_text(
                    f"✅ User {chat_id} added/updated.\n"
                    f"Expires on: `{expiry_date_str}`\n"
                    f"Data synced to Supabase.",
                    parse_mode="Markdown"
                )
            except Exception as e:
                logging.error(f"⚠️ Supabase sync failed: {e}")
                await update.message.reply_text(
                    f"⚠️ User {chat_id} added locally, but Supabase sync failed.\n"
                    f"Expires on: `{expiry_date_str}`",
                    parse_mode="Markdown"
                )
        else:
            await update.message.reply_text(
                f"✅ User {chat_id} added/updated locally.\n"
                f"Expires on: `{expiry_date_str}`",
                parse_mode="Markdown"
            )

    except Exception as e:
        logging.exception("❌ Error in /adduser:")
        await update.message.reply_text(f"❌ Failed to add user: {e}")


async def debug_user_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager):
    """Debug command to check user status."""
    if not is_admin_update(update):
        await update.message.reply_text("⛔ Access denied. Admins only.")
        return
    
    if not context.args:
        chat_id = str(update.effective_chat.id)
    else:
        chat_id = str(context.args[0])
    
    prefs = safe_load(user_manager.prefs_file, {})
    user_data = prefs.get(chat_id, {})
    
    is_sub = user_manager.is_subscribed(chat_id)
    is_expired = user_manager.is_subscription_expired(chat_id)
    
    debug_msg = (
        f"🔍 <b>Debug User {chat_id}</b>\n\n"
        f"<b>Raw data:</b>\n"
        f"• Found in prefs: {chat_id in prefs}\n"
        f"• subscribed: {user_data.get('subscribed', 'NOT SET')}\n"
        f"• active: {user_data.get('active', 'NOT SET')}\n"
        f"• expires_at: {user_data.get('expires_at', 'NOT SET')}\n\n"
        f"<b>Function results:</b>\n"
        f"• is_subscribed(): {is_sub}\n"
        f"• is_subscription_expired(): {is_expired}\n\n"
        f"<b>All user data:</b>\n"
        f"<code>{user_data}</code>"
    )
    
    await update.message.reply_html(debug_msg)


async def debug_system_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager):
    """Debug command to check system status and why alerts aren't being sent."""
    if not is_admin_update(update):
        await update.message.reply_text("⛔ Access denied. Admins only.")
        return
    
    # Import here to avoid circular imports
    from alerts.monitoring import load_latest_tokens_from_overlap
    
    # Check files
    overlap_exists = OVERLAP_FILE.exists()
    overlap_size = OVERLAP_FILE.stat().st_size if overlap_exists else 0
    overlap_modified = datetime.fromtimestamp(OVERLAP_FILE.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S") if overlap_exists else "N/A"
    
    alerts_state = safe_load(ALERTS_STATE_FILE, {})
    prefs = safe_load(USER_PREFS_FILE, {})
    
    # Load tokens
    try:
        tokens = load_latest_tokens_from_overlap()
        today = datetime.utcnow().date()
        fresh_tokens = {
            tid: t for tid, t in tokens.items()
            if t.get("checked_at") and datetime.fromisoformat(
                t["checked_at"].rstrip("Z")
            ).date() >= today
        }
        
        # Count by grade
        grade_counts = {}
        for token in fresh_tokens.values():
            grade = token.get("grade", "NONE")
            grade_counts[grade] = grade_counts.get(grade, 0) + 1
            
    except Exception as e:
        tokens = {}
        fresh_tokens = {}
        grade_counts = {"ERROR": str(e)}
        
    active_users = [k for k, v in prefs.items() if v.get("active", False)]
    subscribed_users = [k for k in active_users if user_manager.is_subscribed(k)]
    
    msg = (
        f"🔍 <b>System Debug Info</b>\n\n"
        f"<b>📁 Files:</b>\n"
        f"• overlap_results.pkl: {'✅' if overlap_exists else '❌'}\n"
        f"  Size: {overlap_size / 1024:.2f} KB\n"
        f"  Modified: {overlap_modified}\n\n"
        f"<b>🪙 Tokens (Today):</b>\n"
        f"• Total loaded: {len(tokens)}\n"
        f"• Fresh (today): {len(fresh_tokens)}\n"
        f"• Grade breakdown:\n"
    )
    
    for grade in ["CRITICAL", "HIGH", "MEDIUM", "LOW", "NONE", "ERROR"]:
        count = grade_counts.get(grade, 0)
        if count > 0:
            msg += f"  - {grade}: {count}\n"
    
    msg += (
        f"\n<b>👥 Users:</b>\n"
        f"• Total registered: {len(prefs)}\n"
        f"• Active: {len(active_users)}\n"
        f"• Subscribed (valid): {len(subscribed_users)}\n\n"
        f"<b>🔔 Alert State:</b>\n"
        f"• Tokens tracked: {len(alerts_state)}\n"
    )
    
    # Show sample of fresh tokens
    if fresh_tokens:
        msg += f"\n<b>📋 Sample Fresh Tokens (first 3):</b>\n"
        for i, (tid, info) in enumerate(list(fresh_tokens.items())[:3]):
            grade = info.get("grade", "NONE")
            checked = info.get("checked_at", "N/A")[:16]
            msg += f"{i+1}. {tid[:8]}... | {grade} | {checked}\n"
    else:
        msg += f"\n⚠️ <b>No fresh tokens found!</b>\n"
    
    # Check if token is in alert state
    if fresh_tokens and alerts_state:
        msg += f"\n<b>🔍 Alert State Check:</b>\n"
        for tid in list(fresh_tokens.keys())[:3]:
            if tid in alerts_state:
                last_grade = alerts_state[tid].get("last_grade", "NONE")
                msg += f"• {tid[:8]}... tracked as {last_grade}\n"
            else:
                msg += f"• {tid[:8]}... NOT tracked yet\n"
    
    await update.message.reply_html(msg)