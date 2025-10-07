#!/usr/bin/env python3
"""
bot.py - Main entry point for the modular Telegram bot
"""

import logging
import asyncio
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, Defaults

from config import BOT_TOKEN, DATA_DIR, USER_PREFS_FILE, USER_STATS_FILE, ALERTS_STATE_FILE
from config import USE_SUPABASE, DOWNLOAD_OVERLAP_ON_STARTUP, SUPABASE_DAILY_SYNC, OVERLAP_FILE, BUCKET_NAME

from shared.file_io import safe_load, safe_save
from alerts.user_manager import UserManager
from alerts.commands import (
    start_cmd, setalerts_cmd, myalerts_cmd, stop_cmd,
    help_cmd, stats_cmd, testalert_cmd, button_handler
)
from alerts.admin_commands import (
    admin_stats_cmd, broadcast_cmd, adduser_cmd, debug_user_cmd, is_admin_update
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

async def button_wrapper(update, context):
    await button_handler(update, context, user_manager)


# ----------------------
# Startup hook
# ----------------------
async def on_startup(app: Application):
    """Initialize bot on startup."""
    logger.info("🔧 Initializing bot startup sequence...")
    
    # Ensure data directory and baseline files exist
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    safe_save(USER_PREFS_FILE, safe_load(USER_PREFS_FILE, {}))
    safe_save(ALERTS_STATE_FILE, safe_load(ALERTS_STATE_FILE, {}))
    safe_save(USER_STATS_FILE, safe_load(USER_STATS_FILE, {}))
    logger.info(f"✅ Data directory initialized: {DATA_DIR}")

    # Optional: download bot data & overlap once at startup
    if USE_SUPABASE:
        logger.info("☁️ Supabase integration enabled. Attempting startup downloads...")
        download_bot_data_from_supabase()
        
        if DOWNLOAD_OVERLAP_ON_STARTUP and download_overlap_results is not None:
            try:
                download_overlap_results(str(OVERLAP_FILE), bucket=BUCKET_NAME)
                logger.info("✅ Downloaded overlap_results.pkl at startup")
            except Exception as e:
                logger.warning(f"Startup overlap download failed: {e}")

    # Start background loops
    logger.info("🔄 Starting background monitoring tasks...")
    asyncio.create_task(background_loop(app, user_manager))
    asyncio.create_task(monthly_expiry_notifier(app, user_manager))

    # Start daily supabase sync & overlap refresh if enabled
    if USE_SUPABASE and SUPABASE_DAILY_SYNC:
        logger.info("🔄 Starting daily Supabase sync tasks...")
        asyncio.create_task(daily_supabase_sync())
        asyncio.create_task(periodic_overlap_download())

    logger.info("🚀 Bot startup complete. Monitoring for token alerts...")


# ----------------------
# Main function
# ----------------------
async def main():
    """Main entry point for the bot."""
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
    
    # Register callback handler
    app.add_handler(CallbackQueryHandler(button_wrapper))
    
    logger.info("✅ All command handlers registered")

    # Set startup hook
    app.post_init = on_startup

    logger.info("🔌 Starting bot polling...")
    await app.run_polling(allowed_updates=None, poll_interval=1.0)

if __name__ == "__main__":
    asyncio.run(main())