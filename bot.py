#!/usr/bin/env python3
"""
bot.py - Main entry point for the modular Telegram bot
"""

import logging
import asyncio
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, Defaults, MessageHandler, filters
from telegram import Update
from telegram.ext import ContextTypes
from telegram.error import TimedOut, BadRequest, NetworkError

# --- Updated Config Imports ---
# Now imports ALL settings from the new config.py
from config import (
    BOT_TOKEN, DATA_DIR, USER_PREFS_FILE, USER_STATS_FILE, 
    ALERTS_STATE_FILE, GROUPS_FILE, PORTFOLIOS_FILE,
    USE_SUPABASE, OVERLAP_FILE, BUCKET_NAME,
    ALPHA_ALERTS_STATE_FILE, ADMIN_USER_ID
)
# --- End Updated Config Imports ---

from shared.file_io import safe_load, safe_save
from alerts.user_manager import UserManager
from trade_manager import PortfolioManager#, trade_monitoring_loop
from alerts.analytics_monitoring import active_tracking_signal_loop


# --- Updated Command Imports ---
from alerts.commands import (
    start_cmd, setalerts_cmd, myalerts_cmd, stop_cmd,
    help_cmd, stats_cmd, testalert_cmd, button_handler,
    papertrade_cmd, portfolio_cmd, pnl_cmd, history_cmd,
    performance_cmd, watchlist_cmd, resetcapital_cmd,
    # Add new alpha commands
    alpha_subscribe_cmd, alpha_unsubscribe_cmd,
    # --- NEW: Add ML commands ---
    predict_cmd, predict_batch_cmd,

    set_tp_cmd, set_tp_discovery_cmd, set_tp_alpha_cmd,

    closeposition_cmd, closeall_cmd, confirmcloseall_cmd
)

# --- End Updated Command Imports ---

from alerts.admin_commands import (
    admin_stats_cmd, broadcast_cmd, adduser_cmd, genactivation_cmd, debug_user_cmd, 
    debug_system_cmd, force_download_cmd,
    addgroup_cmd, removegroup_cmd, listgroups_cmd,  
    is_admin_update, notify_new_group
)

# --- Updated Monitoring Imports ---
from alerts.monitoring import (
    background_loop, monthly_expiry_notifier,
    download_bot_data_from_supabase, 
    periodic_supabase_sync, tp_metrics_update_loop
)
# Add new alpha monitoring loop
from alerts.alpha_monitoring import alpha_monitoring_loop, ALPHA_OVERLAP_FILE
# Add trade monitoring loop
from alerts.trade_monitor import trade_monitoring_loop
# --- End Updated Monitoring Imports ---

# --- New Import for closing session ---
from alerts.formatters import _close_http_session
# --- End New Import ---


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
async def genactivation_wrapper(update, context): await genactivation_cmd(update, context, user_manager)
async def debug_user_wrapper(update, context): await debug_user_cmd(update, context, user_manager)
async def debug_system_wrapper(update, context): await debug_system_cmd(update, context, user_manager)
async def force_download_wrapper(update, context): await force_download_cmd(update, context, user_manager)
async def button_wrapper(update, context): await button_handler(update, context, user_manager, portfolio_manager)
async def addgroup_wrapper(update, context): await addgroup_cmd(update, context)
async def removegroup_wrapper(update, context): await removegroup_cmd(update, context)
async def listgroups_wrapper(update, context): await listgroups_cmd(update, context)

# --- New Alpha Command Wrappers ---
async def alpha_subscribe_wrapper(update, context): 
    await alpha_subscribe_cmd(update, context, user_manager)

async def alpha_unsubscribe_wrapper(update, context): 
    await alpha_unsubscribe_cmd(update, context, user_manager)
# --- End New Alpha Command Wrappers ---

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

async def closeposition_wrapper(update, context): 
    await closeposition_cmd(update, context, user_manager, portfolio_manager)

async def closeall_wrapper(update, context): 
    await closeall_cmd(update, context, user_manager, portfolio_manager)

async def confirmcloseall_wrapper(update, context): 
    await confirmcloseall_cmd(update, context, user_manager, portfolio_manager)

# --- NEW: ML Command Wrappers ---
# We pass user_manager to check for subscription status
async def predict_wrapper(update, context): 
    await predict_cmd(update, context, user_manager)

async def predict_batch_wrapper(update, context): 
    await predict_batch_cmd(update, context, user_manager)
# --- END ML Command Wrappers ---

async def set_tp_wrapper(update, context): await set_tp_cmd(update, context, user_manager)
async def set_tp_discovery_wrapper(update, context): await set_tp_discovery_cmd(update, context, user_manager)
async def set_tp_alpha_wrapper(update, context): await set_tp_alpha_cmd(update, context, user_manager)

# ----------------------
# Global Error Handler
# ----------------------
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle errors gracefully and show user-friendly messages."""
    error = context.error
    
    # Log the error
    logger.error(f"Exception while handling an update: {error}")
    
    # Determine a user-friendly message based on the error type
    if isinstance(error, TimedOut):
        user_message = "⚠️ Connection timed out. Please try again."
    elif isinstance(error, BadRequest):
        if "Query is too old" in str(error):
            user_message = "⚠️ This button has expired. Please try again, or use /start to refresh the menu."
        else:
            user_message = f"❌ Request error: {str(error)[:50]}..."
    elif isinstance(error, NetworkError):
        user_message = "📡 Network error. Please check your connection and try again."
    else:
        user_message = "❌ An unexpected error occurred. Please try again later."
        # Log full traceback for unexpected errors
        logger.exception(f"Unhandled exception: {error}")
    
    # Try to notify the user
    try:
        if update and update.effective_message:
            await update.effective_message.reply_text(user_message)
        elif update and update.callback_query:
            try:
                await update.callback_query.answer(user_message, show_alert=True)
            except Exception:
                pass  # Query might already be answered or expired
    except Exception as notify_error:
        logger.warning(f"Could not notify user of error: {notify_error}")

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
            # Add new alpha commands to the list
            BotCommand("alpha_subscribe", "Subscribe to Alpha Alerts"),
            BotCommand("alpha_unsubscribe", "Unsubscribe from Alpha Alerts"),
            # --- NEW: Add ML commands to menu ---
            BotCommand("predict", "🤖 Get ML prediction for a token"),
            BotCommand("predict_batch", "🤖 Get ML predictions for multiple tokens"),
        ]
        
        # Set commands for all users
        await app.bot.set_my_commands(user_commands, scope=BotCommandScopeDefault())
        logger.info("✅ User commands menu configured")
        
        # Admin commands (includes all user commands + admin-only)
        admin_commands = user_commands + [
            BotCommand("admin", "View platform statistics"),
            BotCommand("broadcast", "Send message to all users"),
            BotCommand("adduser", "Add new subscriber"),
            BotCommand("genactivation", "Generate activation code"),
            BotCommand("debuguser", "Debug user data"),
            BotCommand("debugsystem", "System diagnostics"),
            BotCommand("forcedownload", "Force download from Supabase"),
            BotCommand("addgroup", "Add authorized group"),
            BotCommand("removegroup", "Remove authorized group"),
            BotCommand("listgroups", "List authorized groups"),
        ]
        
        # Set admin commands for each admin
        if ADMIN_USER_ID:
            for admin_id in ADMIN_USER_ID:
                try:
                    await app.bot.set_my_commands(
                        admin_commands, 
                        scope=BotCommandScopeChat(chat_id=admin_id)
                    )
                except Exception as e:
                    logger.warning(f"Could not set admin commands for {admin_id}: {e}")
            logger.info(f"✅ Admin commands configured for {len(ADMIN_USER_ID)} admins")
        else:
            logger.info("ℹ️ No ADMIN_IDS found in config - skipping admin command setup")
        
    except Exception as e:
        logger.error(f"Failed to set bot commands: {e}")
    
    # Ensure data directory exists
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"✅ Data directory initialized: {DATA_DIR}")

    if USE_SUPABASE:
        logger.info("☁️ Supabase enabled - downloading all bot data...")
        # This function now handles ALL bot files, including portfolios AND alpha state
        download_bot_data_from_supabase() 
        try:
            # Import all required downloaders
            from supabase_utils import download_overlap_results, download_alpha_overlap_results

            logger.info("⬇️ Downloading overlap_results.pkl from Supabase...")
            if download_overlap_results(str(OVERLAP_FILE), bucket=BUCKET_NAME):
                logger.info("✅ Downloaded overlap_results.pkl")
            else:
                 logger.error("❌ overlap_results.pkl not found after download!")

            # Download alpha results
            logger.info("⬇️ Downloading overlap_results_alpha.pkl from Supabase...")
            if download_alpha_overlap_results(str(ALPHA_OVERLAP_FILE), bucket=BUCKET_NAME):
                logger.info("✅ Downloaded overlap_results_alpha.pkl")
            else:
                 logger.warning("ℹ️ overlap_results_alpha.pkl not found (may be new bot)")

        except Exception as e:
            logger.error(f"❌ Startup overlap download failed: {e}")

    # Initialize local files if they don't exist (ONLY after download attempt)
    default_files = {
        USER_PREFS_FILE: {}, ALERTS_STATE_FILE: {}, USER_STATS_FILE: {},
        GROUPS_FILE: {}, PORTFOLIOS_FILE: {},
        ALPHA_ALERTS_STATE_FILE: {}
    }
    for file_path, default_content in default_files.items():
        if not file_path.exists():
            logger.info(f"🆕 Creating new default file: {file_path.name}")
            safe_save(file_path, default_content)

    # --- Initialize managers AFTER data has been potentially downloaded ---
    logger.info("🔧 Initializing managers...")
    user_manager = UserManager(USER_PREFS_FILE, USER_STATS_FILE)
    portfolio_manager = PortfolioManager(PORTFOLIOS_FILE)
    logger.info("✅ Managers initialized.")
    # Start ALL background loops
    logger.info("🔄 Starting background tasks...")
    # 1. Original alert monitoring loop - ONLY sends alerts
    asyncio.create_task(background_loop(app, user_manager, portfolio_manager))
    # 2. Monthly expiry notifier
    asyncio.create_task(monthly_expiry_notifier(app, user_manager))
    # 3. Periodic Supabase sync
    if USE_SUPABASE:
        asyncio.create_task(periodic_supabase_sync())
    # 4. 🆕 ANALYTICS-DRIVEN SIGNAL DETECTION (replaces old signal_detection_loop)
    asyncio.create_task(active_tracking_signal_loop(app, user_manager, portfolio_manager))
    # 5. 📊 Paper trading position monitoring (checks exits, TP, stop loss)
    asyncio.create_task(trade_monitoring_loop(app, user_manager, portfolio_manager))
    # 6. Alpha alerts monitoring
    asyncio.create_task(alpha_monitoring_loop(app, user_manager))
    # 7. 📈 TP Metrics calculation (from past 3 days of analytics)
    asyncio.create_task(tp_metrics_update_loop(portfolio_manager))

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

    # --- Register New Alpha Commands ---
    app.add_handler(CommandHandler("alpha_subscribe", alpha_subscribe_wrapper))
    app.add_handler(CommandHandler("alpha_unsubscribe", alpha_unsubscribe_wrapper))
    # --- End New Alpha Commands ---
    
    # --- NEW: Register ML Commands ---
    app.add_handler(CommandHandler("predict", predict_wrapper))
    app.add_handler(CommandHandler("predict_batch", predict_batch_wrapper))
    # --- END NEW ML Commands ---

    # Register admin commands
    app.add_handler(CommandHandler("admin", admin_stats_wrapper))
    app.add_handler(CommandHandler("broadcast", broadcast_wrapper))
    app.add_handler(CommandHandler("adduser", adduser_wrapper))
    app.add_handler(CommandHandler("genactivation", genactivation_wrapper))
    app.add_handler(CommandHandler("debuguser", debug_user_wrapper))
    app.add_handler(CommandHandler("debugsystem", debug_system_wrapper))
    app.add_handler(CommandHandler("forcedownload", force_download_wrapper))
    app.add_handler(CommandHandler("addgroup", addgroup_wrapper))
    app.add_handler(CommandHandler("removegroup", removegroup_wrapper))
    app.add_handler(CommandHandler("listgroups", listgroups_wrapper))
    
    # Register callback query handler for inline buttons
    app.add_handler(CallbackQueryHandler(button_wrapper))
    
    # Register text message handler for menu inputs
    from alerts.message_handler import handle_text_message
    
    async def text_message_wrapper(update, context):
        await handle_text_message(update, context, user_manager, portfolio_manager)
    
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, text_message_wrapper)
    )
    
    app.add_handler(
        MessageHandler(
            filters.StatusUpdate.NEW_CHAT_MEMBERS & filters.ChatType.GROUPS,
            notify_new_group
        )
    )

    app.add_handler(CommandHandler("set_tp", set_tp_wrapper))
    app.add_handler(CommandHandler("set_tp_discovery", set_tp_discovery_wrapper))
    app.add_handler(CommandHandler("set_tp_alpha", set_tp_alpha_wrapper))

    app.add_handler(CommandHandler("closeposition", closeposition_wrapper))
    app.add_handler(CommandHandler("closeall", closeall_wrapper))
    app.add_handler(CommandHandler("confirmcloseall", confirmcloseall_wrapper))
    
    logger.info("✅ All command handlers registered (including new alpha and ML commands).")
    
    # Register global error handler
    app.add_error_handler(error_handler)
    logger.info("✅ Global error handler registered.")
    
    # Run application with startup logic
    try:
        async with app:
            await on_startup(app)
            logger.info("🔌 Starting bot polling...")
            await app.start()
            await app.updater.start_polling()
            await asyncio.Event().wait() # Keep running indefinitely

    except Exception as e:
        logger.exception(f"Bot failed to run: {e}")
    
    finally:
        # --- New: Graceful Shutdown ---
        logger.info("🛑 Shutting down bot...")
        await _close_http_session() # Close the aiohttp session
        if app.updater and app.updater.running:
            await app.updater.stop()
        if app.running:
            await app.stop()
        logger.info("👋 Bot shut down successfully.")
        # --- End New Shutdown Logic ---

if __name__ == "__main__":
    # Basic logging setup
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    # Silence noisy loggers
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('httpcore').setLevel(logging.WARNING)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot stopped manually.")