#!/usr/bin/env python3
"""
bot.py - Main entry point for the modular Telegram bot
FIXED: Proper logging, background tasks, and Supabase integration
UPDATED: Added group management system for mint broadcasting
"""

import logging
import asyncio
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, Defaults, ChatMemberHandler
from telegram import Update, ChatMemberUpdated
from telegram.ext import ContextTypes

from config import BOT_TOKEN, DATA_DIR, USER_PREFS_FILE, USER_STATS_FILE, ALERTS_STATE_FILE, GROUPS_FILE
from config import USE_SUPABASE, DOWNLOAD_OVERLAP_ON_STARTUP, SUPABASE_DAILY_SYNC, OVERLAP_FILE, BUCKET_NAME

from shared.file_io import safe_load, safe_save
from alerts.user_manager import UserManager
from alerts.commands import (
    start_cmd, setalerts_cmd, myalerts_cmd, stop_cmd,
    help_cmd, stats_cmd, testalert_cmd, button_handler
)
from alerts.admin_commands import (
    admin_stats_cmd, broadcast_cmd, adduser_cmd, debug_user_cmd, 
    debug_system_cmd, force_download_cmd,
    addgroup_cmd, removegroup_cmd, listgroups_cmd,  
    is_admin_update
)
from alerts.monitoring import (
    background_loop, monthly_expiry_notifier,
    download_bot_data_from_supabase, daily_supabase_sync,
    periodic_overlap_download
)

# ----------------------
# Logger for this module
# ----------------------
logger = logging.getLogger(__name__)

# ----------------------
# Optional supabase helpers
# ----------------------
try:
    from supabase_utils import download_overlap_results
except Exception:
    download_overlap_results = None

# ----------------------
# Initialize User Manager
# ----------------------
user_manager = UserManager(USER_PREFS_FILE, USER_STATS_FILE)


# ----------------------
# Group Chat Detection Handler
# ----------------------
async def handle_my_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Automatically detect when the bot is added to a new group.
    This logs the event but does NOT auto-enable posting (admin must use /addgroup).
    """
    try:
        result = update.my_chat_member
        
        if not result:
            return
        
        chat = result.chat
        new_status = result.new_chat_member.status
        old_status = result.old_chat_member.status
        
        # Only care about group/supergroup chats
        if chat.type not in ["group", "supergroup"]:
            return
        
        # Bot was added to group
        if old_status in ["left", "kicked"] and new_status == "member":
            group_id = str(chat.id)
            group_name = chat.title or "Unknown Group"
            
            logger.info(f"🆕 Bot added to new group: {group_name} (ID: {group_id})")
            
            # Load groups file
            groups = safe_load(GROUPS_FILE, {})
            
            # Add group but mark as INACTIVE (admin must enable it)
            if group_id not in groups:
                groups[group_id] = {
                    "active": False,  # Inactive by default
                    "name": group_name,
                    "detected_at": update.my_chat_member.date.isoformat() + "Z"
                }
                safe_save(GROUPS_FILE, groups)
                
                logger.info(f"📝 Group {group_id} registered (inactive). Admin must use /addgroup to enable.")
            
            # Optionally notify admin
            from config import ADMIN_USER_ID
            if ADMIN_USER_ID:
                try:
                    await context.bot.send_message(
                        chat_id=int(ADMIN_USER_ID),
                        text=(
                            f"🆕 <b>Bot Added to New Group</b>\n\n"
                            f"• Name: {group_name}\n"
                            f"• ID: <code>{group_id}</code>\n\n"
                            f"Use /addgroup {group_id} to enable mint broadcasts."
                        ),
                        parse_mode="HTML"
                    )
                except Exception as e:
                    logger.warning(f"Could not notify admin: {e}")
        
        # Bot was removed from group
        elif old_status == "member" and new_status in ["left", "kicked"]:
            group_id = str(chat.id)
            group_name = chat.title or "Unknown Group"
            
            logger.info(f"🚫 Bot removed from group: {group_name} (ID: {group_id})")
            
            # Mark as inactive
            groups = safe_load(GROUPS_FILE, {})
            if group_id in groups:
                groups[group_id]["active"] = False
                safe_save(GROUPS_FILE, groups)
                logger.info(f"📝 Group {group_id} marked as inactive")
    
    except Exception as e:
        logger.exception(f"Error in handle_my_chat_member: {e}")


# ----------------------
# Command wrapper functions (inject user_manager)
# ----------------------
async def start_wrapper(update, context):
    await start_cmd(update, context, user_manager)

async def setalerts_wrapper(update, context):
    await setalerts_cmd(update, context, user_manager)

async def myalerts_wrapper(update, context):
    await myalerts_cmd(update, context, user_manager)

async def stop_wrapper(update, context):
    await stop_cmd(update, context, user_manager)

async def stats_wrapper(update, context):
    is_admin = is_admin_update(update)
    await stats_cmd(update, context, user_manager, is_admin)

async def admin_stats_wrapper(update, context):
    await admin_stats_cmd(update, context, user_manager)

async def broadcast_wrapper(update, context):
    await broadcast_cmd(update, context, user_manager)

async def adduser_wrapper(update, context):
    await adduser_cmd(update, context, user_manager)

async def debug_user_wrapper(update, context):
    await debug_user_cmd(update, context, user_manager)

async def debug_system_wrapper(update, context):
    await debug_system_cmd(update, context, user_manager)

async def force_download_wrapper(update, context):
    await force_download_cmd(update, context, user_manager)

async def button_wrapper(update, context):
    await button_handler(update, context, user_manager)

# NEW: Group management command wrappers
async def addgroup_wrapper(update, context):
    await addgroup_cmd(update, context)

async def removegroup_wrapper(update, context):
    await removegroup_cmd(update, context)

async def listgroups_wrapper(update, context):
    await listgroups_cmd(update, context)


# ----------------------
# Startup hook
# ----------------------
async def on_startup(app: Application):
    """Initialize bot on startup - ALWAYS downloads from Supabase first."""
    logger.info("🔧 Initializing bot startup sequence...")
    
    # Ensure data directory and baseline files exist
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    safe_save(USER_PREFS_FILE, safe_load(USER_PREFS_FILE, {}))
    safe_save(ALERTS_STATE_FILE, safe_load(ALERTS_STATE_FILE, {}))
    safe_save(USER_STATS_FILE, safe_load(USER_STATS_FILE, {}))
    safe_save(GROUPS_FILE, safe_load(GROUPS_FILE, {}))  # NEW
    logger.info(f"✅ Data directory initialized: {DATA_DIR}")

    # 🔥 ALWAYS download from Supabase on startup (both local and Render)
    logger.info("☁️ Supabase integration enabled - downloading all data...")
    
    # Download bot data (user prefs, stats, alerts state)
    from alerts.monitoring import download_bot_data_from_supabase
    download_bot_data_from_supabase()
    
    # 🔥 FORCE download overlap_results.pkl
    try:
        from supabase_utils import download_overlap_results
        logger.info("⬇️ Downloading overlap_results.pkl from Supabase...")
        download_overlap_results(str(OVERLAP_FILE), bucket=BUCKET_NAME)
        
        if OVERLAP_FILE.exists():
            size_kb = OVERLAP_FILE.stat().st_size / 1024
            logger.info(f"✅ Downloaded overlap_results.pkl ({size_kb:.2f} KB)")
        else:
            logger.error("❌ overlap_results.pkl not found after download!")
    except Exception as e:
        logger.error(f"❌ Startup overlap download failed: {e}")
        logger.info("⚠️ Bot will keep trying to download in background loop")

    # Start background loops
    logger.info("🔄 Starting background monitoring tasks...")
    asyncio.create_task(background_loop(app, user_manager))
    asyncio.create_task(monthly_expiry_notifier(app, user_manager))

    # Start daily sync if enabled
    if USE_SUPABASE and SUPABASE_DAILY_SYNC:
        logger.info("🔄 Starting daily Supabase sync task...")
        asyncio.create_task(daily_supabase_sync())

    logger.info("🚀 Bot startup complete. Monitoring for token alerts...")


async def main():
    """Main entry point for the bot - can be called from FastAPI or standalone."""
    logger.info("🤖 Initializing Telegram bot application...")
    
    defaults = Defaults(parse_mode="HTML")
    app = Application.builder().token(BOT_TOKEN).defaults(defaults).build()

    # Register user commands
    app.add_handler(CommandHandler("start", start_wrapper))
    app.add_handler(CommandHandler("setalerts", setalerts_wrapper))
    app.add_handler(CommandHandler("myalerts", myalerts_wrapper))
    app.add_handler(CommandHandler("stop", stop_wrapper))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("stats", stats_wrapper))
    app.add_handler(CommandHandler("testalert", testalert_cmd))
    
    # Register admin commands
    app.add_handler(CommandHandler("admin", admin_stats_wrapper))
    app.add_handler(CommandHandler("broadcast", broadcast_wrapper))
    app.add_handler(CommandHandler("adduser", adduser_wrapper))
    app.add_handler(CommandHandler("debuguser", debug_user_wrapper))
    app.add_handler(CommandHandler("debugsystem", debug_system_wrapper))
    app.add_handler(CommandHandler("forcedownload", force_download_wrapper))
    
    # NEW: Register group management commands
    app.add_handler(CommandHandler("addgroup", addgroup_wrapper))
    app.add_handler(CommandHandler("removegroup", removegroup_wrapper))
    app.add_handler(CommandHandler("listgroups", listgroups_wrapper))
    
    # NEW: Register chat member handler for auto-detection
    app.add_handler(ChatMemberHandler(handle_my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))
    
    # Register callback handler
    app.add_handler(CallbackQueryHandler(button_wrapper))
    
    logger.info("✅ All command handlers registered (including group management)")

    logger.info("🔌 Starting bot...")
    
    # This is the standard way to run an application with startup logic
    async with app:
        # ✅ MANUALLY CALL THE STARTUP FUNCTION HERE
        await on_startup(app)
        
        # Start the polling after setup is complete
        await app.start()
        await app.updater.start_polling()
        logger.info("🚀 Bot is now polling for updates.")
        
        # Keep the bot running
        await asyncio.Event().wait()

        # Stop the bot gracefully on shutdown
        logger.info("🛑 Shutting down bot...")
        await app.updater.stop()
        await app.stop()

if __name__ == "__main__":
    # For standalone execution (not via FastAPI)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot stopped manually.")