#!/usr/bin/env python3
"""
alerts/commands.py - User-facing bot commands
"""

import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from config import ALL_GRADES
from alerts.user_manager import UserManager
from trade_manager import PortfolioManager

def get_mode_status_text(user_prefs: dict) -> str:
    """Generates a status line for user's current modes."""
    modes = user_prefs.get("modes", [])
    status = []
    if "alerts" in modes:
        status.append("🔔 Alerts")
    if "papertrade" in modes:
        status.append("📈 Paper Trading")
    
    if not status:
        return "No active modes."
    return " & ".join(status)

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager):
    """Handle /start command."""
    chat_id = str(update.effective_chat.id)
    logging.info(f"🚀 User {chat_id} started bot")

    if not user_manager.is_subscribed(chat_id):
        await update.message.reply_html(
            f"👋 Welcome!\n\n"
            f"❌ You are not subscribed to alerts.\n"
            f"Please contact the admin to activate your subscription."
        )
        return

    user_manager.activate_user(chat_id)
    user_prefs = user_manager.get_user_prefs(chat_id)
    
    keyboard = [
        [InlineKeyboardButton("🔔 Alerts Only", callback_data="mode_alerts")],
        [InlineKeyboardButton("📈 Paper Trading Only", callback_data="mode_papertrade")],
        [InlineKeyboardButton("🚀 Both Modes", callback_data="mode_both")],
        [InlineKeyboardButton("⚙️ Configure Alert Grades", callback_data="config_grades")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    welcome_msg = (
        f"👋 <b>Welcome!</b>\n\n"
        f"Please choose your desired mode of operation. You can receive token alerts, "
        f"let the bot automatically paper trade signals, or do both.\n\n"
        f"<b>Current Mode:</b> {get_mode_status_text(user_prefs)}\n"
        f"<b>Alert Grades:</b> {', '.join(user_prefs.get('grades', ['Not Set']))}"
    )
    await update.message.reply_html(welcome_msg, reply_markup=reply_markup)

async def setalerts_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager):
    """Handle /setalerts command."""
    chat_id = str(update.effective_chat.id)
    
    if not user_manager.is_subscribed(chat_id):
        await update.message.reply_text("⛔ You are not subscribed. Please contact the admin.")
        return
    
    args = context.args or []
    valid = set(ALL_GRADES)
    chosen = [a.upper() for a in args if a.upper() in valid]

    if not chosen:
        keyboard = [
            [InlineKeyboardButton("🔴 CRITICAL", callback_data="preset_critical"),
             InlineKeyboardButton("🔥 CRITICAL + HIGH", callback_data="preset_critical_high")],
            [InlineKeyboardButton("📊 All Grades", callback_data="preset_all")]
        ]
        await update.message.reply_html(
            "⚠️ Usage: /setalerts GRADE1 GRADE2 ...\nAvailable: CRITICAL, HIGH, MEDIUM, LOW",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    success = user_manager.update_user_prefs(chat_id, {"grades": chosen})
    
    if success:
        await update.message.reply_html(f"✅ Alert grades updated! You will now receive: <b>{', '.join(chosen)}</b>")
    else:
        await update.message.reply_text("❌ Failed to save preferences. Please try again.")

async def myalerts_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager):
    """Handle /myalerts command, now shows modes as well."""
    chat_id = str(update.effective_chat.id)
    
    if not user_manager.is_subscribed(chat_id):
        await update.message.reply_text("⛔ You are not subscribed. Please contact the admin.")
        return
    
    prefs = user_manager.get_user_prefs(chat_id)
    stats = user_manager.get_user_stats(chat_id)

    total_alerts = stats.get("alerts_received", 0)
    last_alert = stats.get("last_alert_at")
    last_alert_str = "Never" if not last_alert else f"<i>{last_alert[:10]}</i>"

    msg = (
        f"📊 <b>Your Settings</b>\n\n"
        f"<b>Active Modes:</b> {get_mode_status_text(prefs)}\n"
        f"<b>Subscribed Grades:</b> {', '.join(prefs.get('grades', ALL_GRADES))}\n"
        f"<b>Total alerts received:</b> {total_alerts}\n"
        f"<b>Last alert:</b> {last_alert_str}\n\n"
        f"Use /start to change your mode or /setalerts to change alert grades."
    )
    await update.message.reply_html(msg)

async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager):
    """Handle /stop command."""
    chat_id = str(update.effective_chat.id)
    user_manager.deactivate_user(chat_id)
    await update.message.reply_html("😔 You have been unsubscribed from all alerts and services. Use /start to reactivate.")

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /help command."""
    help_text = (
        "🤖 <b>Bot Help & Commands</b>\n\n"
        "<b>--- Core Commands ---</b>\n"
        "• /start - Change bot mode (Alerts/Trading)\n"
        "• /myalerts - View your current settings & stats\n"
        "• /setalerts - Set which grade alerts you receive\n"
        "• /stop - Unsubscribe from everything\n\n"
        "<b>--- Paper Trading ---</b>\n"
        "• /papertrade [capital] - Set trading capital (also enables paper trading mode). E.g., <code>/papertrade 1000</code>\n"
        "• /portfolio - View your trading portfolio and P&L\n\n"
        "<b>--- General ---</b>\n"
        "• /help - Show this help message"
    )
    await update.message.reply_html(help_text)

# --- NEW TRADING COMMANDS ---

async def papertrade_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager, portfolio_manager: PortfolioManager):
    """Enable paper trading mode and configure capital."""
    chat_id = str(update.effective_chat.id)
    if not user_manager.is_subscribed(chat_id):
        await update.message.reply_text("⛔ You must be a subscribed user to enable paper trading.")
        return
    
    capital = 1000.0 # Default capital
    if context.args:
        try:
            capital = float(context.args[0])
            if capital <= 0:
                await update.message.reply_text("Please provide a positive number for capital.")
                return
        except ValueError:
            await update.message.reply_text("Invalid capital amount. Please use a number.")
            return
            
    user_manager.enable_papertrade_mode(chat_id)
    portfolio_manager.set_capital(chat_id, capital)
    
    await update.message.reply_html(
        f"📈 <b>Paper Trading Enabled!</b>\n\n"
        f"Your virtual portfolio has been set up with <b>${capital:,.2f} USD</b>.\n\n"
        f"The bot will now automatically trade signals based on its predefined strategy. "
        f"Use /portfolio to track your performance."
    )

async def portfolio_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager, portfolio_manager: PortfolioManager):
    """Display user's paper trading portfolio."""
    chat_id = str(update.effective_chat.id)
    prefs = user_manager.get_user_prefs(chat_id)
    
    if "papertrade" not in prefs.get("modes", []):
        await update.message.reply_html(
            "Paper trading is not enabled. Use <code>/papertrade [capital]</code> or /start to enable it."
        )
        return
        
    portfolio = portfolio_manager.get_portfolio(chat_id)
    
    capital = portfolio['capital_usd']
    positions = portfolio['positions']
    history = portfolio['trade_history']
    
    total_pnl = sum(trade['pnl_usd'] for trade in history)
    
    msg = (
        f"💼 <b>Your Paper Trading Portfolio</b>\n\n"
        f"Available Capital: <b>${capital:,.2f} USD</b>\n"
        f"Total Realized P/L: <b>${total_pnl:,.2f} USD</b>\n"
        f"Total Trades: {len(history)}\n\n"
        f"<b>--- Open Positions ({len(positions)}) ---</b>\n"
    )

    if not positions:
        msg += "<i>No open positions.</i>"
    else:
        for mint, pos in positions.items():
            msg += (
                f"<b>${pos['symbol']}</b> - Invested: ${pos['investment_usd']:,.2f} "
                f"at ${pos['entry_price']:,.6f}\n"
            )
            
    await update.message.reply_html(msg)

async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager, is_admin: bool = False):
    """Handle /stats command."""
    chat_id = str(update.effective_chat.id)
    user_stats = user_manager.get_user_stats(chat_id)
    
    msg = (
        f"📊 <b>Your Statistics</b>\n\n"
        f"📬 Total alerts received: <b>{user_stats.get('alerts_received', 0)}</b>\n"
        f"📅 Member since: <i>{user_stats.get('joined_at', 'Unknown')[:10] if user_stats.get('joined_at') else 'Unknown'}</i>\n"
    )
    
    if is_admin:
        platform_stats = user_manager.get_all_stats()
        msg += (
            f"\n🏢 <b>Platform Statistics (Admin)</b>\n"
            f"• Total users: <b>{platform_stats['total_users']}</b>\n"
            f"• Active users: <b>{platform_stats['active_users']}</b>\n"
            f"• Total alerts sent: <b>{platform_stats['total_alerts_sent']}</b>\n"
        )
    
    await update.message.reply_html(msg)

async def testalert_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send a test alert for a known token."""
    from alerts.formatters import format_alert_html
    token_data = {
        "token": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263", "grade": "CRITICAL",
        "token_metadata": {"name": "TestToken", "symbol": "TEST"},
        "overlap_percentage": 75.0, "concentration": 50.0
    }
    message = format_alert_html(token_data, "NEW")
    await update.message.reply_html(f"🔔 Test Alert\n\n{message}")

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager):
    """Handle inline keyboard button callbacks."""
    query = update.callback_query
    await query.answer()
    chat_id = str(query.from_user.id)
    
    if not user_manager.is_subscribed(chat_id):
        await query.answer("⛔ You are not subscribed.", show_alert=True)
        return

    data = query.data
    
    # --- Mode Selection ---
    if data == "mode_alerts":
        user_manager.set_modes(chat_id, ["alerts"])
        await query.edit_message_text("✅ Mode set to <b>🔔 Alerts Only</b>.", parse_mode="HTML")
    elif data == "mode_papertrade":
        user_manager.set_modes(chat_id, ["papertrade"])
        await query.edit_message_text("✅ Mode set to <b>📈 Paper Trading Only</b>.\nUse <code>/papertrade [capital]</code> to set your starting funds.", parse_mode="HTML")
    elif data == "mode_both":
        user_manager.set_modes(chat_id, ["alerts", "papertrade"])
        await query.edit_message_text("✅ Mode set to <b>🚀 Both Alerts & Paper Trading</b>.", parse_mode="HTML")

    # --- Grade Configuration ---
    elif data == "config_grades":
        keyboard = [
            [InlineKeyboardButton("🔴 CRITICAL", callback_data="preset_critical"),
             InlineKeyboardButton("🔥 CRITICAL + HIGH", callback_data="preset_critical_high")],
            [InlineKeyboardButton("📊 All Grades", callback_data="preset_all")]
        ]
        await query.edit_message_text("Please select a preset for your <b>alert grades</b> or use <code>/setalerts</code> for a custom list.", 
                                      reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
    elif data == "preset_critical":
        user_manager.update_user_prefs(chat_id, {"grades": ["CRITICAL"]})
        await query.edit_message_text("✅ Alert grades updated: <b>CRITICAL</b> only.", parse_mode="HTML")
    elif data == "preset_critical_high":
        user_manager.update_user_prefs(chat_id, {"grades": ["CRITICAL", "HIGH"]})
        await query.edit_message_text("✅ Alert grades updated: <b>CRITICAL + HIGH</b>.", parse_mode="HTML")
    elif data == "preset_all":
        user_manager.update_user_prefs(chat_id, {"grades": ALL_GRADES.copy()})
        await query.edit_message_text("✅ Alert grades updated: <b>ALL</b> grades.", parse_mode="HTML")