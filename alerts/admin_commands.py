#!/usr/bin/env python3
"""
alerts/admin_commands.py - Admin-only bot commands
UPDATED: Added group management commands for mint broadcasting
"""

import asyncio
import logging
from datetime import datetime
from telegram import Update
from telegram.ext import ContextTypes

from config import ADMIN_USER_ID, OVERLAP_FILE, ALERTS_STATE_FILE, USER_PREFS_FILE, GROUPS_FILE
from shared.file_io import safe_load, safe_save
from alerts.monitoring import periodic_supabase_sync 
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
                periodic_supabase_sync()
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
        f"<b>📢 Alert State:</b>\n"
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
        msg += f"\n<b>🔎 Alert State Check:</b>\n"
        for tid in list(fresh_tokens.keys())[:3]:
            if tid in alerts_state:
                last_grade = alerts_state[tid].get("last_grade", "NONE")
                msg += f"• {tid[:8]}... tracked as {last_grade}\n"
            else:
                msg += f"• {tid[:8]}... NOT tracked yet\n"
    
    await update.message.reply_html(msg)


# ----------------------
# NEW: GROUP MANAGEMENT COMMANDS
# ----------------------

async def notify_new_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Sends a notification to the admin when the bot is added to a new group.
    """
    if not ADMIN_USER_ID:
        logging.warning("ADMIN_USER_ID is not configured. Cannot send new group notification.")
        return

    # Check if the update is from a group chat
    if not update.effective_chat.type in ["group", "supergroup"]:
        return

    chat = update.effective_chat
    bot_user = await context.bot.get_me()

    # Check if the bot itself is among the new members
    is_bot_added = False
    if update.message and update.message.new_chat_members:
        for member in update.message.new_chat_members:
            if member.id == bot_user.id:
                is_bot_added = True
                break
    
    if not is_bot_added:
        return

    group_name = chat.title
    group_id_str = str(chat.id)
    
    # Escape HTML special characters in the group name
    group_name_escaped = group_name.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    message_text = (
        "🆕 <b>Bot Added to New Group</b>\n\n"
        f"• Name: <b>{group_name_escaped}</b>\n"
        f"• ID: <code>{group_id_str}</code>\n\n"
        f"Use /addgroup {group_id_str} to enable mint broadcasts."
    )

    try:
        await context.bot.send_message(
            chat_id=ADMIN_USER_ID, 
            text=message_text, 
            parse_mode='HTML',
            disable_web_page_preview=True
        )
        logging.info(f"✅ Notified admin about new group: {group_name} ({group_id_str})")
    except Exception as e:
        logging.error(f"❌ Failed to notify admin about new group {group_id_str}: {e}")

async def addgroup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: Add a group to receive mint broadcasts."""
    if not is_admin_update(update):
        await update.message.reply_text("⛔ Access denied. Admins only.")
        return
    
    if not context.args:
        await update.message.reply_text(
            "⚠️ Usage: /addgroup <group_chat_id>\n\n"
            "Example: /addgroup -1001234567890"
        )
        return
    
    try:
        group_id = str(context.args[0])
        
        # Validate it looks like a group ID (starts with -)
        if not group_id.startswith("-"):
            await update.message.reply_text(
                "⚠️ Group IDs typically start with a minus sign (e.g., -1001234567890)\n"
                "Are you sure this is correct? Proceeding anyway..."
            )
        
        # Load existing groups
        groups = safe_load(GROUPS_FILE, {})
        
        # Try to get group info
        try:
            chat = await context.bot.get_chat(int(group_id))
            group_name = chat.title or "Unknown"
        except Exception as e:
            logging.warning(f"Could not fetch group info: {e}")
            group_name = "Unknown"
        
        # Add/update group
        groups[group_id] = {
            "active": True,
            "name": group_name,
            "added_at": datetime.utcnow().isoformat() + "Z"
        }
        
        safe_save(GROUPS_FILE, groups)
        
        # Sync to Supabase
        if USE_SUPABASE:
            try:
                periodic_supabase_sync()
                sync_msg = "\nData synced to Supabase."
            except Exception as e:
                logging.error(f"⚠️ Supabase sync failed: {e}")
                sync_msg = "\n⚠️ Supabase sync failed (saved locally only)."
        else:
            sync_msg = ""
        
        await update.message.reply_html(
            f"✅ <b>Group added!</b>\n\n"
            f"• ID: <code>{group_id}</code>\n"
            f"• Name: {group_name}\n"
            f"• Status: Active\n\n"
            f"New token mints will now be posted to this group.{sync_msg}"
        )
        
        logging.info(f"✅ Admin added group {group_id} ({group_name})")
        
    except Exception as e:
        logging.exception("❌ Error in /addgroup:")
        await update.message.reply_text(f"❌ Failed to add group: {e}")

async def removegroup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: Remove a group from mint broadcasts."""
    if not is_admin_update(update):
        await update.message.reply_text("⛔ Access denied. Admins only.")
        return
    
    if not context.args:
        await update.message.reply_text(
            "⚠️ Usage: /removegroup <group_chat_id>\n\n"
            "Example: /removegroup -1001234567890"
        )
        return
    
    try:
        group_id = str(context.args[0])
        
        # Load existing groups
        groups = safe_load(GROUPS_FILE, {})
        
        if group_id not in groups:
            await update.message.reply_text(
                f"⚠️ Group {group_id} is not in the active list.\n"
                f"Use /listgroups to see active groups."
            )
            return
        
        # Remove group
        group_name = groups[group_id].get("name", "Unknown")
        del groups[group_id]
        
        safe_save(GROUPS_FILE, groups)
        
        await update.message.reply_html(
            f"✅ <b>Group removed!</b>\n\n"
            f"• ID: <code>{group_id}</code>\n"
            f"• Name: {group_name}\n\n"
            f"This group will no longer receive mint broadcasts."
        )
        
        logging.info(f"✅ Admin removed group {group_id} ({group_name})")
        
    except Exception as e:
        logging.exception("❌ Error in /removegroup:")
        await update.message.reply_text(f"❌ Failed to remove group: {e}")

async def listgroups_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: List all active groups."""
    if not is_admin_update(update):
        await update.message.reply_text("⛔ Access denied. Admins only.")
        return
    
    try:
        groups = safe_load(GROUPS_FILE, {})
        
        if not groups:
            await update.message.reply_html(
                "🔭 <b>No groups configured yet.</b>\n\n"
                "Use /addgroup &lt;group_id&gt; to add a group."
            )
            return
        
        active_groups = {k: v for k, v in groups.items() if v.get("active", True)}
        
        if not active_groups:
            await update.message.reply_html(
                "🔭 <b>No active groups.</b>\n\n"
                "All groups have been deactivated. Use /addgroup &lt;group_id&gt; to add a new group."
            )
            return
        
        msg = f"📋 <b>Active Groups ({len(active_groups)})</b>\n\n"
        
        for group_id, info in active_groups.items():
            name = info.get("name", "Unknown")
            added = info.get("added_at", "N/A")[:10]
            # Escape HTML special characters in name
            name_escaped = name.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            msg += f"• <b>{name_escaped}</b>\n"
            msg += f"  ID: <code>{group_id}</code>\n"
            msg += f"  Added: {added}\n\n"
        
        msg += "Use /removegroup &lt;group_id&gt; to remove a group."
        
        await update.message.reply_html(msg)
        
    except Exception as e:
        logging.exception("❌ Error in /listgroups:")
        await update.message.reply_text(f"❌ Failed to list groups: {e}")