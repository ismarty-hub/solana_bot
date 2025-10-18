#!/usr/bin/env python3
"""
bot.py - Main entry point for the modular Telegram bot
"""

import logging
import asyncio
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, Defaults
from telegram import Update
from telegram.ext import ContextTypes


from config import BOT_TOKEN, DATA_DIR, USER_PREFS_FILE, USER_STATS_FILE, ALERTS_STATE_FILE, GROUPS_FILE, PORTFOLIOS_FILE
from config import USE_SUPABASE, OVERLAP_FILE, BUCKET_NAME

from shared.file_io import safe_load, safe_save
from alerts.user_manager import UserManager
from trade_manager import PortfolioManager, trade_monitoring_loop, signal_detection_loop

from alerts.commands import (
    start_cmd, setalerts_cmd, myalerts_cmd, stop_cmd,
    help_cmd, stats_cmd, testalert_cmd, button_handler,
    papertrade_cmd, portfolio_cmd, pnl_cmd, history_cmd,  # Enhanced trade commands
    performance_cmd, watchlist_cmd, resetcapital_cmd  # New trade commands
)
from alerts.admin_commands import (
    admin_stats_cmd, broadcast_cmd, adduser_cmd, debug_user_cmd, 
    debug_system_cmd, force_download_cmd,
    addgroup_cmd, removegroup_cmd, listgroups_cmd,  
    is_admin_update
)
from alerts.monitoring import (
    background_loop, monthly_expiry_notifier,
    download_bot_data_from_supabase, 
    periodic_supabase_sync  # ✅ --- FIX: Import renamed task ---
)

logger = logging.getLogger(__name__)

# ----------------------
# Initialize Managers
# ----------------------
# UserManager is initialized after data download
user_manager = None
# PortfolioManager is initialized after data download
portfolio_manager = None


# ----------------------
# Command wrapper functions (inject managers)
# ----------------------
async def start_wrapper(update, context): await start_cmd(update, context, user_manager)
async def setalerts_wrapper(update, context): await setalerts_cmd(update, context, user_manager)
async def myalerts_wrapper(update, context): await myalerts_cmd(update, context, user_manager)
async def stop_wrapper(update, context): await stop_cmd(update, context, user_manager)
async def stats_wrapper(update, context): await stats_cmd(update, context, user_manager, is_admin_update(update))
async def admin_stats_wrapper(update, context): await admin_stats_cmd(update, context, user_manager)
async def broadcast_wrapper(update, context): await broadcast_cmd(update, context, user_manager)
async def adduser_wrapper(update, context): await adduser_cmd(update, context, user_manager)
async def debug_user_wrapper(update, context): await debug_user_cmd(update, context, user_manager)
async def debug_system_wrapper(update, context): await debug_system_cmd(update, context, user_manager)
async def force_download_wrapper(update, context): await force_download_cmd(update, context, user_manager)
async def button_wrapper(update, context): await button_handler(update, context, user_manager)
async def addgroup_wrapper(update, context): await addgroup_cmd(update, context)
async def removegroup_wrapper(update, context): await removegroup_cmd(update, context)
async def listgroups_wrapper(update, context): await listgroups_cmd(update, context)

# Trading command wrappers
async def papertrade_wrapper(update, context): 
    await papertrade_cmd(update, context, user_manager, portfolio_manager)

async def portfolio_wrapper(update, context): 
    await portfolio_cmd(update, context, user_manager, portfolio_manager)

async def pnl_wrapper(update, context): 
    await pnl_cmd(update, context, user_manager, portfolio_manager)

async def history_wrapper(update, context): 
    await history_cmd(update, context, user_manager, portfolio_manager)

async def performance_wrapper(update, context): 
    await performance_cmd(update, context, user_manager, portfolio_manager)

async def watchlist_wrapper(update, context): 
    await watchlist_cmd(update, context, user_manager, portfolio_manager)

async def resetcapital_wrapper(update, context): 
    await resetcapital_cmd(update, context, user_manager, portfolio_manager)


# ----------------------
# Startup hook
# ----------------------
async def on_startup(app: Application):
    """Initialize bot on startup."""
    global user_manager, portfolio_manager
    logger.info("🔧 Initializing bot startup sequence...")
    
    # Set bot commands for better UX
    try:
        from telegram import BotCommand, BotCommandScopeDefault, BotCommandScopeChat
        
        # Default commands for all users
        user_commands = [
            BotCommand("start", "Configure bot mode (Alerts/Trading)"),
            BotCommand("help", "Show all available commands"),
            BotCommand("myalerts", "View your settings and stats"),
            BotCommand("setalerts", "Set alert grades (e.g. CRITICAL HIGH)"),
            BotCommand("stop", "Unsubscribe from all services"),
            BotCommand("papertrade", "Enable trading (e.g. /papertrade 1000)"),
            BotCommand("portfolio", "View trading portfolio"),
            BotCommand("pnl", "Check unrealized P/L"),
            BotCommand("history", "View trades (e.g. /history 20)"),
            BotCommand("performance", "Detailed trading stats"),
            BotCommand("watchlist", "Tokens being watched"),
            BotCommand("resetcapital", "Reset capital (e.g. /resetcapital 5000)"),
            BotCommand("stats", "View usage statistics"),
        ]
        
        # Set commands for all users
        await app.bot.set_my_commands(user_commands, scope=BotCommandScopeDefault())
        logger.info("✅ User commands menu configured")
        
        # Admin commands (includes all user commands + admin-only)
        admin_commands = user_commands + [
            BotCommand("admin", "View platform statistics"),
            BotCommand("broadcast", "Send message to all users"),
            BotCommand("adduser", "Add new subscriber"),
            BotCommand("debuguser", "Debug user data"),
            BotCommand("debugsystem", "System diagnostics"),
            BotCommand("forcedownload", "Force download from Supabase"),
            BotCommand("addgroup", "Add authorized group"),
            BotCommand("removegroup", "Remove authorized group"),
            BotCommand("listgroups", "List authorized groups"),
        ]
        
        # Set admin commands for each admin (if ADMIN_IDS exists)
        try:
            from config import ADMIN_IDS
            for admin_id in ADMIN_IDS:
                try:
                    await app.bot.set_my_commands(
                        admin_commands, 
                        scope=BotCommandScopeChat(chat_id=admin_id)
                    )
                except Exception as e:
                    logger.warning(f"Could not set admin commands for {admin_id}: {e}")
            logger.info(f"✅ Admin commands configured for {len(ADMIN_IDS)} admins")
        except ImportError:
            logger.info("ℹ️ No ADMIN_IDS found in config - skipping admin command setup")
        
    except Exception as e:
        logger.error(f"Failed to set bot commands: {e}")
    
    # Ensure data directory and files exist
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    # Define default files and their initial content
    default_files = {
        USER_PREFS_FILE: {}, ALERTS_STATE_FILE: {}, USER_STATS_FILE: {},
        GROUPS_FILE: {}, PORTFOLIOS_FILE: {}
    }
    # Initialize local files if they don't exist
    for file_path, default_content in default_files.items():
        if not file_path.exists():
            safe_save(file_path, default_content)
    logger.info(f"✅ Data directory initialized: {DATA_DIR}")

    if USE_SUPABASE:
        logger.info("☁️ Supabase enabled - downloading all bot data...")
        download_bot_data_from_supabase() # This now handles all bot files including portfolios
        try:
            from supabase_utils import download_overlap_results
            logger.info("⬇️ Downloading overlap_results.pkl from Supabase...")
            if download_overlap_results(str(OVERLAP_FILE), bucket=BUCKET_NAME):
                logger.info("✅ Downloaded overlap_results.pkl")
            else:
                 logger.error("❌ overlap_results.pkl not found after download!")
        except Exception as e:
            logger.error(f"❌ Startup overlap download failed: {e}")

    # --- Initialize managers AFTER data has been potentially downloaded ---
    logger.info("🔧 Initializing managers...")
    user_manager = UserManager(USER_PREFS_FILE, USER_STATS_FILE)
    portfolio_manager = PortfolioManager(PORTFOLIOS_FILE)
    logger.info("✅ Managers initialized.")

    # --- Start ALL background loops ---
    logger.info("🔄 Starting background tasks...")
    # 1. Original alert monitoring loop - NOW PASSES portfolio_manager
    asyncio.create_task(background_loop(app, user_manager, portfolio_manager))
    # 2. Monthly expiry notifier
    asyncio.create_task(monthly_expiry_notifier(app, user_manager))
    # 3. Periodic Supabase sync
    if USE_SUPABASE:
        # ✅ --- FIX: Start the renamed, frequent task ---
        asyncio.create_task(periodic_supabase_sync())
        # ✅ --- END FIX ---
    # 4. Paper trading signal detection loop
    asyncio.create_task(signal_detection_loop(app, user_manager, portfolio_manager))
    # 5. Paper trading high-frequency monitoring loop
    asyncio.create_task(trade_monitoring_loop(app, user_manager, portfolio_manager))

    logger.info("🚀 Bot startup complete.")

async def main():
    """Main entry point for the bot."""
    logger.info("🤖 Initializing Telegram bot application...")
    
    defaults = Defaults(parse_mode="HTML")
    app = Application.builder().token(BOT_TOKEN).defaults(defaults).build()

    # Register core commands
    app.add_handler(CommandHandler("start", start_wrapper))
    app.add_handler(CommandHandler("setalerts", setalerts_wrapper))
    app.add_handler(CommandHandler("myalerts", myalerts_wrapper))
    app.add_handler(CommandHandler("stop", stop_wrapper))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("stats", stats_wrapper))
    app.add_handler(CommandHandler("testalert", testalert_cmd))
    
    # Register ALL trading commands
    app.add_handler(CommandHandler("papertrade", papertrade_wrapper))
    app.add_handler(CommandHandler("portfolio", portfolio_wrapper))
    app.add_handler(CommandHandler("pnl", pnl_wrapper))
    app.add_handler(CommandHandler("history", history_wrapper))
    app.add_handler(CommandHandler("performance", performance_wrapper))
    app.add_handler(CommandHandler("watchlist", watchlist_wrapper))
    app.add_handler(CommandHandler("resetcapital", resetcapital_wrapper))
    
    # Register admin commands
    app.add_handler(CommandHandler("admin", admin_stats_wrapper))
    app.add_handler(CommandHandler("broadcast", broadcast_wrapper))
    app.add_handler(CommandHandler("adduser", adduser_wrapper))
    app.add_handler(CommandHandler("debuguser", debug_user_wrapper))
    app.add_handler(CommandHandler("debugsystem", debug_system_wrapper))
    app.add_handler(CommandHandler("forcedownload", force_download_wrapper))
    app.add_handler(CommandHandler("addgroup", addgroup_wrapper))
    app.add_handler(CommandHandler("removegroup", removegroup_wrapper))
    app.add_handler(CommandHandler("listgroups", listgroups_wrapper))
    
    # Register callback query handler for inline buttons
    app.add_handler(CallbackQueryHandler(button_wrapper))
    
    logger.info("✅ All command handlers registered (including new trading commands).")
    
    # Run application with startup logic
    async with app:
        await on_startup(app)
        logger.info("🔌 Starting bot polling...")
        await app.start()
        await app.updater.start_polling()
        await asyncio.Event().wait() # Keep running indefinitely
        logger.info("🛑 Shutting down bot...")
        await app.updater.stop()
        await app.stop()

if __name__ == "__main__":
    # Basic logging setup
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot stopped manually.")