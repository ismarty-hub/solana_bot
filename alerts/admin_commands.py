#!/usr/bin/env python3
"""
alerts/admin_commands.py - Admin-only bot commands
"""

import asyncio
import logging
from datetime import datetime
from telegram import Update
from telegram.ext import ContextTypes

from config import ADMIN_USER_ID
from shared.file_io import safe_load


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
        
        expiry_date = user_manager.add_user_with_expiry(chat_id, days)
        is_sub_after = user_manager.is_subscribed(chat_id)
        
        await update.message.reply_text(
            f"✅ User {chat_id} added/updated with expiry {expiry_date}\n"
            f"🔍 Subscription check: {is_sub_after}\n"
            f"💬 Tell user to try /start now"
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