#!/usr/bin/env python3
"""
alerts/commands.py - User-facing bot commands with enhanced portfolio management
"""

import logging
import html
import aiohttp
import asyncio
from pathlib import Path  # Added back for path construction
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from datetime import datetime

# Imports from new config.py
# REVERTED: We import DATA_DIR instead of ALPHA_ALERTS_STATE_FILE to ensure path accuracy
from config import ALL_GRADES, FASTAPI_ML_URL, DATA_DIR
from alerts.user_manager import UserManager
from trade_manager import PortfolioManager

# --- Imports for Alpha Alerts ---
from shared.file_io import safe_load
# Import the refresh formatter and HTTP session manager
from .formatters import format_alpha_refresh, _get_http_session, _close_http_session

logger = logging.getLogger(__name__)

# --- FIX: Explicitly define path to ensure it matches where the writer saves data ---
ALPHA_ALERTS_STATE_FILE = Path(DATA_DIR) / "alerts_state_alpha.json"


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

# --- ALPHA ALERTS COMMANDS ---

async def alpha_subscribe_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager):
    """Handle /alpha_subscribe command to opt-in to high-priority alpha alerts."""
    chat_id = str(update.effective_chat.id)
    
    # Check subscription status
    if not user_manager.is_subscribed(chat_id):
        await update.message.reply_text("⛔ You are not subscribed. Please contact the admin.")
        return
    
    # Get current preferences to check current state
    user_prefs = user_manager.get_user_prefs(chat_id)
    already_subscribed = user_prefs.get("alpha_alerts", False)
    
    if already_subscribed:
        await update.message.reply_html(
            "ℹ️ <b>Already Subscribed!</b>\n\n"
            "You are already receiving Alpha Alerts.\n"
            "Use /myalerts to view your settings."
        )
        return
    
    # Set the user preference for alpha_alerts to True
    success = user_manager.update_user_prefs(chat_id, {"alpha_alerts": True})
    
    if success:
        await update.message.reply_html(
            "🚀 <b>Alpha Alerts Activated!</b>\n\n"
            "✅ You will now receive high-priority Alpha Alerts.\n"
            "<i>Use /myalerts to confirm your settings</i>"
        )
    else:
        await update.message.reply_html(
            "❌ <b>Failed to Subscribe</b>\n\n"
            "There was an error updating your preferences. Please try again in a moment."
        )


async def alpha_unsubscribe_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager):
    """Handle /alpha_unsubscribe command to opt-out of alpha alerts."""
    chat_id = str(update.effective_chat.id)
    
    if not user_manager.is_subscribed(chat_id):
        await update.message.reply_text("⛔ You are not subscribed. Please contact the admin.")
        return
    
    # Get current preferences to check current state
    user_prefs = user_manager.get_user_prefs(chat_id)
    currently_subscribed = user_prefs.get("alpha_alerts", False)
    
    if not currently_subscribed:
        await update.message.reply_html(
            "ℹ️ <b>Not Subscribed</b>\n\n"
            "You are not currently receiving Alpha Alerts.\n"
            "Use /alpha_subscribe to enable them."
        )
        return
    
    # Set the user preference for alpha_alerts to False
    success = user_manager.update_user_prefs(chat_id, {"alpha_alerts": False})
    
    if success:
        await update.message.reply_html(
            "😴 <b>Alpha Alerts Disabled</b>\n\n"
            "✅ You will no longer receive high-priority Alpha Alerts.\n"
            "<i>Use /myalerts to confirm your settings</i>"
        )
    else:
        await update.message.reply_html(
            "❌ <b>Failed to Unsubscribe</b>\n\n"
            "There was an error updating your preferences. Please try again in a moment."
        )

# --- END ALPHA ALERTS COMMANDS ---

async def setalerts_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager):
    """Handle /setalerts command with improved UX."""
    chat_id = str(update.effective_chat.id)
    
    if not user_manager.is_subscribed(chat_id):
        await update.message.reply_text("⛔ You are not subscribed. Please contact the admin.")
        return
    
    args = context.args or []
    valid = set(ALL_GRADES)
    chosen = [a.upper() for a in args if a.upper() in valid]

    if not chosen:
        # Show interactive buttons AND usage instructions
        keyboard = [
            [InlineKeyboardButton("🔴 CRITICAL", callback_data="preset_critical"),
             InlineKeyboardButton("🔥 CRITICAL + HIGH", callback_data="preset_critical_high")],
            [InlineKeyboardButton("📊 All Grades", callback_data="preset_all")]
        ]
        
        current_grades = user_manager.get_user_prefs(chat_id).get('grades', [])
        current_str = ', '.join(current_grades) if current_grades else "None set"
        
        await update.message.reply_html(
            f"⚙️ <b>Configure Alert Grades</b>\n\n"
            f"<b>Current Setting:</b> {current_str}\n\n"
            f"<b>📝 Manual Setup:</b>\n"
            f"Usage: <code>/setalerts GRADE1 GRADE2 ...</code>\n\n"
            f"<b>Available Grades:</b>\n"
            f"• CRITICAL - Highest priority signals\n"
            f"• HIGH - Strong signals\n"
            f"• MEDIUM - Moderate signals\n"
            f"• LOW - All signals\n\n"
            f"<b>Examples:</b>\n"
            f"<code>/setalerts CRITICAL</code>\n"
            f"<code>/setalerts CRITICAL HIGH</code>\n"
            f"<code>/setalerts CRITICAL HIGH MEDIUM LOW</code>\n\n"
            f"<b>Or choose a preset below:</b>",
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

    # --- New: Check Alpha Alert Status ---
    alpha_status = "✅ Subscribed" if prefs.get("alpha_alerts", False) else "❌ Not Subscribed"
    # --- End New ---
    
    msg = (
        f"📊 <b>Your Settings</b>\n\n"
        f"<b>Active Modes:</b> {get_mode_status_text(prefs)}\n"
        f"<b>Subscribed Grades:</b> {', '.join(prefs.get('grades', ALL_GRADES))}\n"
        f"<b>🚀 Alpha Alerts:</b> {alpha_status}\n\n"
        f"<b>Total alerts received:</b> {total_alerts}\n"
        f"<b>Last alert:</b> {last_alert_str}\n\n"
        f"Use /start to change your mode.\n"
        f"Use /setalerts to change alert grades.\n"
        f"Use /alpha_subscribe or /alpha_unsubscribe to manage alpha alerts."
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
        "<b>--- 🔥 Alpha Alerts ---</b>\n"
        "• /alpha_subscribe - Opt-in to high-priority alpha alerts\n"
        "• /alpha_unsubscribe - Opt-out of alpha alerts\n\n"
        
        "<b>--- 🤖 ML Predictions (NEW) ---</b>\n"
        "• /predict [mint] - Get ML prediction for one token\n"
        "• /predict_batch [mints...] - Get ML predictions for multiple tokens\n\n"

        "<b>--- Paper Trading ---</b>\n"
        "• /papertrade [capital] - Set trading capital and enable paper trading\n"
        "  Example: <code>/papertrade 1000</code>\n"
        "• /portfolio - View detailed portfolio with all positions\n"
        "• /pnl - Get current unrealized P/L update\n"
        "• /history [limit] - View trade history (default: last 10)\n"
        "• /performance - View detailed trading performance stats\n"
        "• /watchlist - View tokens being watched for entry\n"
        "• /resetcapital [amount] - Reset trading capital\n\n"
        "<b>--- General ---</b>\n"
        "• /help - Show this help message\n"
        "• /stats - View your usage statistics"
    )
    await update.message.reply_html(help_text)

# --- ENHANCED TRADING COMMANDS ---

async def papertrade_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager, portfolio_manager: PortfolioManager):
    """Enable paper trading mode and configure capital with improved UX."""
    chat_id = str(update.effective_chat.id)
    if not user_manager.is_subscribed(chat_id):
        await update.message.reply_text("⛔ You must be a subscribed user to enable paper trading.")
        return
    
    # Check if already enabled
    prefs = user_manager.get_user_prefs(chat_id)
    is_already_enabled = "papertrade" in prefs.get("modes", [])
    
    if not context.args:
        # Show helpful prompt with current status
        portfolio = portfolio_manager.get_portfolio(chat_id)
        current_capital = portfolio.get('capital_usd', 0)
        
        status_msg = ""
        if is_already_enabled:
            status_msg = f"<b>Current Status:</b> ✅ Enabled\n<b>Current Capital:</b> ${current_capital:,.2f}\n\n"
        else:
            status_msg = "<b>Current Status:</b> ❌ Not enabled\n\n"
        
        await update.message.reply_html(
            f"📈 <b>Paper Trading Setup</b>\n\n"
            f"{status_msg}"
            f"<b>📝 How to use:</b>\n"
            f"<code>/papertrade [amount]</code>\n\n"
            f"<b>Examples:</b>\n"
            f"<code>/papertrade 1000</code> - Start with $1,000\n"
            f"<code>/papertrade 5000</code> - Start with $5,000\n"
            f"<code>/papertrade 10000</code> - Start with $10,000\n\n"
            f"<b>Requirements:</b>\n"
            f"• Minimum: $100\n"
            f"• Maximum: $1,000,000\n\n"
            f"💡 <i>Tip: Start with $1,000-$5,000 for realistic results</i>"
        )
        return
    
    capital = 1000.0  # Default capital
    try:
        capital = float(context.args[0])
        if capital <= 0:
            await update.message.reply_html(
                "❌ <b>Invalid Amount</b>\n\n"
                "Please provide a positive number.\n"
                "Example: <code>/papertrade 1000</code>"
            )
            return
        if capital < 100:
            await update.message.reply_html(
                "❌ <b>Amount Too Low</b>\n\n"
                "Minimum capital is <b>$100 USD</b>.\n"
                "Example: <code>/papertrade 100</code>"
            )
            return
        if capital > 1000000:
            await update.message.reply_html(
                "❌ <b>Amount Too High</b>\n\n"
                "Maximum capital is <b>$1,000,000 USD</b>.\n"
                "Example: <code>/papertrade 10000</code>"
            )
            return
    except ValueError:
        await update.message.reply_html(
            "❌ <b>Invalid Format</b>\n\n"
            "Please provide a valid number.\n\n"
            "<b>Examples:</b>\n"
            "<code>/papertrade 1000</code>\n"
            "<code>/papertrade 5000</code>"
        )
        return
            
    user_manager.enable_papertrade_mode(chat_id)
    portfolio_manager.set_capital(chat_id, capital)
    
    action_word = "updated to" if is_already_enabled else "set up with"
    
    await update.message.reply_html(
        f"📈 <b>Paper Trading {'Updated' if is_already_enabled else 'Enabled'}!</b>\n\n"
        f"Your virtual portfolio has been {action_word} <b>${capital:,.2f} USD</b>.\n\n"
        f"<b>🎯 Strategy Overview:</b>\n"
        f"• Position Size: 8-12% per trade (max $150)\n"
        f"• Partial Profits: 40% @ +40%, 30% @ +80%, 20% @ +150%\n"
        f"• Trailing Stop: Dynamic 15-25% from peak\n"
        f"• Liquidity Protection: Exit on 40% drain\n"
        f"• Max Hold: 4 hours\n\n"
        f"<b>📊 Track Your Performance:</b>\n"
        f"• /portfolio - View positions\n"
        f"• /pnl - Check unrealized P/L\n"
        f"• /performance - Detailed stats\n"
        f"• /history - Trade log\n\n"
        f"The bot will now automatically trade signals. Good luck! 🚀"
    )

async def portfolio_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager, portfolio_manager: PortfolioManager):
    """Display user's comprehensive paper trading portfolio."""
    chat_id = str(update.effective_chat.id)
    prefs = user_manager.get_user_prefs(chat_id)
    
    if "papertrade" not in prefs.get("modes", []):
        await update.message.reply_html(
            "❌ Paper trading is not enabled. Use <code>/papertrade [capital]</code> to enable it."
        )
        return
        
    portfolio = portfolio_manager.get_portfolio(chat_id)
    
    capital = portfolio['capital_usd']
    positions = portfolio['positions']
    watchlist = portfolio.get('watchlist', {})
    reentry = portfolio.get('reentry_candidates', {})
    blacklist = portfolio.get('blacklist', {})
    history = portfolio['trade_history']
    stats = portfolio.get('stats', {})
    
    # Calculate totals
    total_realized_pnl = stats.get('total_pnl', 0)
    wins = stats.get('wins', 0)
    losses = stats.get('losses', 0)
    total_trades = stats.get('total_trades', 0)
    win_rate = (wins / total_trades * 100) if total_trades > 0 else 0
    
    # Calculate invested capital
    invested = sum(
        pos['investment_usd'] * (pos.get('remaining_percentage', 100) / 100.0)
        for pos in positions.values()
        if pos.get('status') == 'active'
    )
    
    total_portfolio_value = capital + invested
    
    msg = (
        f"💼 <b>Paper Trading Portfolio</b>\n\n"
        f"<b>💰 Capital Summary:</b>\n"
        f"• Available: <b>${capital:,.2f}</b>\n"
        f"• Invested: <b>${invested:,.2f}</b>\n"
        f"• Total Value: <b>${total_portfolio_value:,.2f}</b>\n\n"
        f"<b>📊 Performance:</b>\n"
        f"• Realized P/L: <b>${total_realized_pnl:,.2f}</b>\n"
        f"• Win Rate: <b>{win_rate:.1f}%</b> ({wins}W / {losses}L)\n"
        f"• Total Trades: <b>{total_trades}</b>\n\n"
    )

    # Open Positions
    msg += f"<b>📈 Open Positions ({len(positions)}):</b>\n"
    if not positions:
        msg += "<i>No open positions</i>\n"
    else:
        for mint, pos in list(positions.items())[:5]:  # Show top 5
            if pos.get('status') != 'active':
                continue
            
            remaining_pct = pos.get('remaining_percentage', 100)
            locked = pos.get('locked_profit_usd', 0)
            remaining_note = f" ({remaining_pct:.0f}%)" if remaining_pct < 100 else ""
            locked_note = f" | 💰${locked:.0f}" if locked > 0 else ""
            
            msg += (
                f"• <b>{pos['symbol']}</b>{remaining_note}\n"
                f"  Entry: ${pos['entry_price']:.6f}\n"
                f"  Invested: ${pos['investment_usd'] * (remaining_pct/100):.2f}{locked_note}\n"
            )
        
        if len(positions) > 5:
            msg += f"<i>...and {len(positions) - 5} more positions</i>\n"
    
    msg += "\n"
    
    # Watchlist
    if watchlist:
        msg += f"<b>👀 Watchlist ({len(watchlist)}):</b>\n"
        for mint, item in list(watchlist.items())[:3]:
            msg += f"• {item['symbol']} @ ${item['signal_price']:.6f}\n"
        if len(watchlist) > 3:
            msg += f"<i>...and {len(watchlist) - 3} more</i>\n"
        msg += "\n"
    
    # Re-entry candidates
    if reentry:
        msg += f"<b>🔄 Re-entry Watch ({len(reentry)}):</b>\n"
        for mint, cand in list(reentry.items())[:3]:
            msg += f"• {cand['symbol']} (exits: {cand.get('reentry_attempts', 0)}/2)\n"
        if len(reentry) > 3:
            msg += f"<i>...and {len(reentry) - 3} more</i>\n"
        msg += "\n"
    
    # Blacklist info
    if blacklist:
        msg += f"<b>🚫 Blacklisted:</b> {len(blacklist)} tokens\n\n"
    
    msg += (
        f"<i>Use /pnl for live unrealized P/L</i>\n"
        f"<i>Use /performance for detailed stats</i>\n"
        f"<i>Use /history to see trade log</i>"
    )
    
    await update.message.reply_html(msg)

async def pnl_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager, portfolio_manager: PortfolioManager):
    """Get current unrealized P/L for all open positions."""
    chat_id = str(update.effective_chat.id)
    prefs = user_manager.get_user_prefs(chat_id)
    
    if "papertrade" not in prefs.get("modes", []):
        await update.message.reply_html("❌ Paper trading is not enabled.")
        return
    
    portfolio = portfolio_manager.get_portfolio(chat_id)
    positions = portfolio.get('positions', {})
    
    if not positions:
        await update.message.reply_html("📊 No open positions to calculate P/L.")
        return
    
    msg = (
        f"📊 <b>Unrealized P/L Summary</b>\n\n"
        f"<i>Note: For live P/L with current prices, the monitoring loop sends automatic updates every 5 minutes.</i>\n\n"
        f"<b>Open Positions:</b> {len(positions)}\n\n"
        f"To see detailed P/L calculations, wait for the next automatic update or use /portfolio to view position details.\n\n"
        f"<i>Automatic P/L updates are sent:</i>\n"
        f"• Every 5 minutes for all positions\n"
        f"• At profit milestones: +25%, +50%, +100%, +200%, +500%"
    )
    
    await update.message.reply_html(msg)

async def history_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager, portfolio_manager: PortfolioManager):
    """View trade history with optional limit and improved UX."""
    chat_id = str(update.effective_chat.id)
    prefs = user_manager.get_user_prefs(chat_id)
    
    if "papertrade" not in prefs.get("modes", []):
        await update.message.reply_html("❌ Paper trading is not enabled.")
        return
    
    portfolio = portfolio_manager.get_portfolio(chat_id)
    history = portfolio.get('trade_history', [])
    
    # Parse limit argument
    limit = 10  # Default limit
    if context.args:
        try:
            limit = int(context.args[0])
            if limit < 1:
                await update.message.reply_html(
                    "❌ <b>Invalid Limit</b>\n\n"
                    "Please provide a number greater than 0.\n"
                    "Example: <code>/history 20</code>"
                )
                return
            if limit > 50:
                await update.message.reply_html(
                    "⚠️ <b>Limit Too High</b>\n\n"
                    "Maximum is 50 trades at a time.\n"
                    "Showing last 50 trades instead..."
                )
                limit = 50
        except ValueError:
            await update.message.reply_html(
                "❌ <b>Invalid Format</b>\n\n"
                "Please provide a valid number.\n\n"
                "<b>Usage:</b> <code>/history [number]</code>\n\n"
                "<b>Examples:</b>\n"
                "<code>/history</code> - Last 10 trades (default)\n"
                "<code>/history 20</code> - Last 20 trades\n"
                "<code>/history 50</code> - Last 50 trades"
            )
            return
    
    if not history:
        await update.message.reply_html(
            "📜 <b>No Trade History</b>\n\n"
            "You haven't closed any trades yet.\n\n"
            "Start trading to see your results here!\n"
            "Use <code>/portfolio</code> to see open positions."
        )
        return
    
    # Get most recent trades
    recent_trades = history[-limit:]
    recent_trades.reverse()  # Most recent first
    
    msg = f"📜 <b>Trade History (Last {len(recent_trades)}/{len(history)})</b>\n\n"
    
    for i, trade in enumerate(recent_trades, 1):
        pnl_symbol = "🟢" if trade.get('total_pnl_usd', trade.get('pnl_usd', 0)) > 0 else "🔴"
        pnl_usd = trade.get('total_pnl_usd', trade.get('pnl_usd', 0))
        pnl_pct = trade.get('total_pnl_percent', trade.get('pnl_percent', 0))
        
        exit_reason = trade.get('exit_reason', trade.get('reason', 'Unknown'))
        hold_time = trade.get('hold_duration_minutes', 0)
        
        msg += (
            f"{i}. {pnl_symbol} <b>{trade['symbol']}</b>\n"
            f"   P/L: ${pnl_usd:,.2f} ({pnl_pct:+.1f}%)\n"
            f"   Hold: {hold_time}m | {exit_reason}\n\n"
        )
    
    total_pnl = sum(t.get('total_pnl_usd', t.get('pnl_usd', 0)) for t in history)
    stats = portfolio.get('stats', {})
    
    msg += (
        f"<b>Overall Statistics:</b>\n"
        f"Total P/L: <b>${total_pnl:,.2f}</b>\n"
        f"Win Rate: <b>{(stats.get('wins', 0) / max(stats.get('total_trades', 1), 1) * 100):.1f}%</b>\n"
        f"Best Trade: <b>+{stats.get('best_trade', 0):.1f}%</b>\n"
        f"Worst Trade: <b>{stats.get('worst_trade', 0):.1f}%</b>\n\n"
    )
    
    if len(history) > limit:
        msg += f"<i>💡 Use /history {min(limit + 10, 50)} to see more</i>"
    
    await update.message.reply_html(msg)

async def performance_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager, portfolio_manager: PortfolioManager):
    """View detailed trading performance statistics."""
    chat_id = str(update.effective_chat.id)
    prefs = user_manager.get_user_prefs(chat_id)
    
    if "papertrade" not in prefs.get("modes", []):
        await update.message.reply_html("❌ Paper trading is not enabled.")
        return
    
    portfolio = portfolio_manager.get_portfolio(chat_id)
    stats = portfolio.get('stats', {})
    history = portfolio.get('trade_history', [])
    
    if not history:
        await update.message.reply_html("📊 No trades yet. Performance stats will appear after your first closed trade.")
        return
    
    total_trades = stats.get('total_trades', 0)
    wins = stats.get('wins', 0)
    losses = stats.get('losses', 0)
    win_rate = (wins / total_trades * 100) if total_trades > 0 else 0
    
    total_pnl = stats.get('total_pnl', 0)
    best_trade = stats.get('best_trade', 0)
    worst_trade = stats.get('worst_trade', 0)
    
    # Calculate average trade metrics
    winning_trades = [t for t in history if t.get('total_pnl_usd', t.get('pnl_usd', 0)) > 0]
    losing_trades = [t for t in history if t.get('total_pnl_usd', t.get('pnl_usd', 0)) <= 0]
    
    avg_win = sum(t.get('total_pnl_usd', t.get('pnl_usd', 0)) for t in winning_trades) / len(winning_trades) if winning_trades else 0
    avg_loss = sum(t.get('total_pnl_usd', t.get('pnl_usd', 0)) for t in losing_trades) / len(losing_trades) if losing_trades else 0
    
    # Calculate hold times
    avg_hold_time = sum(t.get('hold_duration_minutes', 0) for t in history) / len(history) if history else 0
    
    # Re-entry stats
    reentry_trades = stats.get('reentry_trades', 0)
    reentry_wins = stats.get('reentry_wins', 0)
    reentry_rate = (reentry_wins / reentry_trades * 100) if reentry_trades > 0 else 0
    
    # Starting capital vs current
    starting_capital = 1000.0  # Default, could be tracked
    current_capital = portfolio.get('capital_usd', 0)
    invested = sum(
        pos['investment_usd'] * (pos.get('remaining_percentage', 100) / 100.0)
        for pos in portfolio.get('positions', {}).values()
        if pos.get('status') == 'active'
    )
    total_value = current_capital + invested
    roi = ((total_value - starting_capital) / starting_capital * 100) if starting_capital > 0 else 0
    
    msg = (
        f"📊 <b>Trading Performance Report</b>\n\n"
        f"<b>💰 Capital:</b>\n"
        f"• Starting: ${starting_capital:,.2f}\n"
        f"• Current Total: ${total_value:,.2f}\n"
        f"• ROI: <b>{roi:+.2f}%</b>\n\n"
        f"<b>📈 Trade Statistics:</b>\n"
        f"• Total Trades: <b>{total_trades}</b>\n"
        f"• Wins: <b>{wins}</b> | Losses: <b>{losses}</b>\n"
        f"• Win Rate: <b>{win_rate:.1f}%</b>\n"
        f"• Total P/L: <b>${total_pnl:,.2f}</b>\n\n"
        f"<b>💵 Trade Metrics:</b>\n"
        f"• Best Trade: <b>+{best_trade:.1f}%</b>\n"
        f"• Worst Trade: <b>{worst_trade:.1f}%</b>\n"
        f"• Avg Win: <b>${avg_win:,.2f}</b>\n"
        f"• Avg Loss: <b>${avg_loss:,.2f}</b>\n"
        f"• Avg Hold Time: <b>{avg_hold_time:.0f} minutes</b>\n\n"
    )
    
    if reentry_trades > 0:
        msg += (
            f"<b>🔄 Re-entry Stats:</b>\n"
            f"• Re-entry Trades: <b>{reentry_trades}</b>\n"
            f"• Re-entry Wins: <b>{reentry_wins}</b>\n"
            f"• Re-entry Win Rate: <b>{reentry_rate:.1f}%</b>\n\n"
        )
    
    # Exit reason breakdown
    exit_reasons = {}
    for trade in history:
        reason = trade.get('exit_reason', trade.get('reason', 'Unknown'))
        exit_reasons[reason] = exit_reasons.get(reason, 0) + 1
    
    if exit_reasons:
        msg += f"<b>📤 Exit Breakdown:</b>\n"
        for reason, count in sorted(exit_reasons.items(), key=lambda x: x[1], reverse=True)[:5]:
            msg += f"• {reason}: {count}\n"
    
    await update.message.reply_html(msg)

async def watchlist_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager, portfolio_manager: PortfolioManager):
    """View current watchlist and re-entry candidates."""
    chat_id = str(update.effective_chat.id)
    prefs = user_manager.get_user_prefs(chat_id)
    
    if "papertrade" not in prefs.get("modes", []):
        await update.message.reply_html("❌ Paper trading is not enabled.")
        return
    
    portfolio = portfolio_manager.get_portfolio(chat_id)
    watchlist = portfolio.get('watchlist', {})
    reentry = portfolio.get('reentry_candidates', {})
    
    if not watchlist and not reentry:
        await update.message.reply_html("👀 No tokens currently being watched. Waiting for new signals...")
        return
    
    msg = f"👀 <b>Watchlist & Re-entry Candidates</b>\n\n"
    
    if watchlist:
        msg += f"<b>🎯 Waiting for Entry ({len(watchlist)}):</b>\n"
        for mint, item in watchlist.items():
            signal_time = item.get('signal_time', '')
            time_ago = "recently"
            if signal_time:
                try:
                    signal_dt = datetime.fromisoformat(signal_time.rstrip('Z'))
                    minutes_ago = int((datetime.utcnow() - signal_dt).total_seconds() / 60)
                    time_ago = f"{minutes_ago}m ago"
                except:
                    pass
            
            msg += (
                f"• <b>{item['symbol']}</b>\n"
                f"  Signal: ${item['signal_price']:.6f} ({time_ago})\n"
            )
        msg += "\n"
    
    if reentry:
        msg += f"<b>🔄 Re-entry Watch ({len(reentry)}):</b>\n"
        for mint, cand in reentry.items():
            attempts = cand.get('reentry_attempts', 0)
            best_pnl = cand.get('best_pnl_pct', 0)
            
            msg += (
                f"• <b>{cand['symbol']}</b>\n"
                f"  Previous: {best_pnl:+.1f}% | Attempts: {attempts}/2\n"
            )
    
    await update.message.reply_html(msg)

async def resetcapital_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager, portfolio_manager: PortfolioManager):
    """Reset trading capital with improved UX and confirmation."""
    chat_id = str(update.effective_chat.id)
    prefs = user_manager.get_user_prefs(chat_id)
    
    if "papertrade" not in prefs.get("modes", []):
        await update.message.reply_html("❌ Paper trading is not enabled.")
        return
    
    portfolio = portfolio_manager.get_portfolio(chat_id)
    current_capital = portfolio.get('capital_usd', 0)
    open_positions = len([p for p in portfolio.get('positions', {}).values() if p.get('status') == 'active'])
    
    if not context.args:
        # Show detailed prompt with current status
        await update.message.reply_html(
            f"⚠️ <b>Reset Trading Capital</b>\n\n"
            f"<b>Current Status:</b>\n"
            f"• Capital: ${current_capital:,.2f}\n"
            f"• Open Positions: {open_positions}\n\n"
            f"⚠️ <b>Warning:</b> This will:\n"
            f"• Close ALL open positions\n"
            f"• Clear watchlist and re-entry candidates\n"
            f"• Reset your capital to a new amount\n"
            f"• Preserve your trade history\n\n"
            f"<b>📝 Usage:</b>\n"
            f"<code>/resetcapital [amount]</code>\n\n"
            f"<b>Examples:</b>\n"
            f"<code>/resetcapital 1000</code> - Reset to $1,000\n"
            f"<code>/resetcapital 5000</code> - Reset to $5,000\n"
            f"<code>/resetcapital 10000</code> - Reset to $10,000\n\n"
            f"<b>Requirements:</b>\n"
            f"• Min: $100 | Max: $1,000,000"
        )
        return
    
    try:
        capital = float(context.args[0])
        if capital < 100 or capital > 1000000:
            await update.message.reply_html(
                "❌ <b>Invalid Amount</b>\n\n"
                "Capital must be between <b>$100</b> and <b>$1,000,000</b>.\n\n"
                "<b>Examples:</b>\n"
                "<code>/resetcapital 1000</code>\n"
                "<code>/resetcapital 5000</code>"
            )
            return
    except ValueError:
        await update.message.reply_html(
            "❌ <b>Invalid Format</b>\n\n"
            "Please provide a valid number.\n\n"
            "<b>Usage:</b> <code>/resetcapital [amount]</code>\n\n"
            "<b>Examples:</b>\n"
            "<code>/resetcapital 1000</code>\n"
            "<code>/resetcapital 5000</code>"
        )
        return
    
    # Perform reset
    old_positions = len(portfolio.get('positions', {}))
    
    # Clear everything except history
    portfolio['positions'] = {}
    portfolio['watchlist'] = {}
    portfolio['reentry_candidates'] = {}
    portfolio['blacklist'] = {}
    portfolio['capital_usd'] = capital
    
    portfolio_manager.save()
    
    await update.message.reply_html(
        f"✅ <b>Portfolio Reset Complete</b>\n\n"
        f"<b>Changes:</b>\n"
        f"• Closed: {old_positions} position(s)\n"
        f"• New Capital: <b>${capital:,.2f}</b>\n"
        f"• Trade History: ✅ Preserved\n\n"
        f"<i>Ready to start fresh! 🚀</i>\n\n"
        f"The bot will now watch for new signals with your updated capital."
    )

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


# --- NEW: ML PREDICTION COMMANDS ---

def _format_prediction_result(mint: str, data: dict) -> str:
    """Helper to format the prediction data into an HTML string."""
    try:
        # The data from batch and single endpoints has a slightly different structure
        # This handles both
        if 'prediction' in data:
            pred = data['prediction']
        else:
            pred = data
            
        win_prob = pred.get('win_probability', 0) * 100
        confidence = pred.get('confidence', 'N/A')
        risk_tier = pred.get('risk_tier', 'N/A')
        
        lines = [
            f"🤖 <b>ML Prediction for Token</b>",
            f"<code>{mint}</code>\n",
            f"<b>Win Probability:</b> {win_prob:.2f}%",
            f"<b>Confidence:</b> {html.escape(confidence)}",
            f"<b>Risk Tier:</b> {html.escape(risk_tier)}",
        ]
        
        # --- Helper to format numbers compactly (e.g., 100K, 2.5M) ---
        def fmt_num(val):
            if val is None: return "N/A"
            if val >= 1_000_000: return f"${val/1_000_000:.1f}M"
            if val >= 1_000: return f"${val/1_000:.1f}K"
            return f"${val:,.2f}"

        # Add Key Metrics with Friendly Names and Grouping
        key_metrics = pred.get('key_metrics', {})
        if key_metrics:
            lines.append("\n--- <b>Key Metrics</b> ---")
            
            # 1. Market Stats (Liquidity, Volume, FDV)
            liq = key_metrics.get('liquidity_usd', 0)
            vol = key_metrics.get('volume_h24_usd', 0)
            fdv = key_metrics.get('market_cap_usd', 0)
            
            lines.append(f"💧 <b>Liquidity:</b> {fmt_num(liq)}")
            lines.append(f"📊 <b>24h Volume:</b> {fmt_num(vol)}")
            if fdv > 0:
                lines.append(f"💰 <b>Market Cap:</b> {fmt_num(fdv)}")

            # 2. Age & Price Action
            age_hours = key_metrics.get('token_age_hours', 0)
            price_change = key_metrics.get('price_change_h24_pct', 0)
            
            # Smart Age Formatting (Corrected for Days/Hours)
            if age_hours >= 24:
                age_str = f"{age_hours/24:.1f} days"
            else:
                age_str = f"{age_hours:.1f}h"
                
            lines.append(f"⏰ <b>Token Age:</b> {age_str}")
            
            # Price Change with Direction
            pch_emoji = "🟢" if price_change > 0 else "🔴"
            lines.append(f"{pch_emoji} <b>24h Change:</b> {price_change:+.2f}%")

            # 3. Holders & Supply
            insider = key_metrics.get('insider_supply_pct', 0)
            top10 = key_metrics.get('top_10_holders_pct', 0)
            
            lines.append(f"👥 <b>Insider Holdings:</b> {insider:.1f}%")
            lines.append(f"🐳 <b>Top 10 Holders:</b> {top10:.1f}%")
            
            # 4. Risk & Health (Interpretations)
            risk_score = key_metrics.get('pump_dump_risk_score', 0)
            health_score = key_metrics.get('market_health_score', 0)
            
            # Interpret Risk (Lower is better)
            if risk_score <= 20:
                risk_text = "Low (Safe)"
                risk_emoji = "🟢"
            elif risk_score <= 50:
                risk_text = "Moderate"
                risk_emoji = "🟡"
            else:
                risk_text = "High Risk"
                risk_emoji = "🔴"
            
            # Interpret Health (Higher is better)
            if health_score >= 80:
                health_text = "Strong"
                health_emoji = "💪"
            elif health_score >= 40:
                health_text = "Average"
                health_emoji = "⚠️"
            else:
                health_text = "Weak"
                health_emoji = "🤒"
            
            lines.append(f"{risk_emoji} <b>Risk Level:</b> {risk_text} ({risk_score:.0f})")
            lines.append(f"{health_emoji} <b>Market Health:</b> {health_text} ({health_score:.0f})")

        # Add Warnings
        warnings = pred.get('warnings', [])
        if warnings:
            lines.append("\n--- <b>Warnings</b> ---")
            for warning in warnings:
                # Clean up specific common warning text if needed, otherwise just display
                clean_warning = html.escape(warning)
                lines.append(f"• {clean_warning}")
        
        return "\n".join(lines)
        
    except Exception as e:
        logger.error(f"Error formatting prediction: {e}")
        return f"❌ Error formatting prediction for <code>{mint}</code>."

async def predict_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager):
    """Handle /predict [mint] command."""
    chat_id = str(update.effective_chat.id)
    if not user_manager.is_subscribed(chat_id):
        await update.message.reply_text("⛔ You are not subscribed. Please contact the admin.")
        return

    if not context.args or len(context.args) != 1:
        await update.message.reply_html(
            "<b>Usage:</b> <code>/predict [mint_address]</code>\n\n"
            "<b>Example:</b> <code>/predict DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263</code>"
        )
        return

    mint = context.args[0].strip()
    loading_msg = await update.message.reply_html(f"🤖 Analyzing token <code>{mint}</code>... Please wait.")
    
    url = f"{FASTAPI_ML_URL}/token/{mint}/predict"
    
    try:
        session = await _get_http_session()
        async with session.get(url, timeout=120) as resp:
            if resp.status == 200:
                data = await resp.json()
                result_msg = _format_prediction_result(mint, data)
                await loading_msg.edit_text(result_msg, parse_mode="HTML", disable_web_page_preview=True)
            else:
                error_data = await resp.json()
                error_msg = error_data.get('detail', 'Unknown error')
                logger.warning(f"Prediction failed for {mint}, status {resp.status}: {error_msg}")
                await loading_msg.edit_text(
                    f"❌ <b>Analysis Failed for <code>{mint}</code></b>\n\n"
                    f"<b>Error:</b> {html.escape(error_msg)}",
                    parse_mode="HTML"
                )
                
    except asyncio.TimeoutError:
        logger.error(f"Timeout during /predict for {mint}")
        await loading_msg.edit_text(f"❌ <b>Request Timed Out</b>\n\nThe prediction service is taking too long to respond.")
    except aiohttp.ClientError as e:
        logger.error(f"HTTP error during /predict: {e}")
        await loading_msg.edit_text(f"❌ <b>Connection Error</b>\n\nFailed to connect to the prediction service.")
    except Exception as e:
        logger.exception(f"Error in /predict command: {e}")
        await loading_msg.edit_text(f"❌ <b>An Unexpected Error Occurred</b>\n\nPlease try again later.")

async def predict_batch_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager):
    """Handle /predict_batch [mint1] [mint2] ... command."""
    chat_id = str(update.effective_chat.id)
    if not user_manager.is_subscribed(chat_id):
        await update.message.reply_text("⛔ You are not subscribed. Please contact the admin.")
        return

    mints = [arg.strip() for arg in context.args]
    
    if not mints:
        await update.message.reply_html(
            "<b>Usage:</b> <code>/predict_batch [mint1] [mint2] ...</code>\n\n"
            "<b>Example:</b> <code>/predict_batch mint1 mint2 mint3</code>\n\n"
            "<b>Note:</b> Maximum 10 tokens per batch"
        )
        return
        
    if len(mints) > 10:
        await update.message.reply_html("❌ <b>Error:</b> Maximum of 10 tokens per batch request.")
        return

    loading_msg = await update.message.reply_html(
        f"🤖 Analyzing <b>{len(mints)} tokens</b> in a batch... This may take a moment."
    )
    
    # FIXED: Send as JSON array in request body, NOT inside a dict
    url = f"{FASTAPI_ML_URL}/token/predict/batch"
    
    try:
        session = await _get_http_session()
        
        # CRITICAL FIX: Send mints as a JSON array directly
        async with session.post(
            url, 
            json=mints,  # ← This is now a list, not {"mints": mints}
            params={"threshold": 0.70},
            timeout=120
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                predictions = data.get('predictions', [])
                
                result_lines = [f"<b>Batch Analysis Complete ({len(predictions)} tokens)</b>\n"]
                
                for item in predictions:
                    mint = item.get('mint')
                    mint_short = mint[:8] + "..." if len(mint) > 12 else mint
                    
                    if item.get('success'):
                        # Format successful prediction
                        pred_data = item['prediction']
                        action = pred_data.get('action', 'N/A')
                        win_prob = pred_data.get('win_probability', 0) * 100
                        confidence = pred_data.get('confidence', 'N/A')
                        risk = pred_data.get('risk_tier', 'N/A')
                        
                        # Action emoji
                        action_emoji = {
                            "BUY": "🟢",
                            "CONSIDER": "🟡",
                            "SKIP": "🟠",
                            "AVOID": "🔴"
                        }.get(action, "⚪")
                        
                        result_lines.append(
                            f"{action_emoji} <b>{mint_short}</b>\n"
                            f"   Action: <b>{action}</b>\n"
                            f"   Win: {win_prob:.1f}% | {confidence}\n"
                            f"   Risk: {risk}\n"
                        )
                    else:
                        # Format error
                        error = item.get('error', 'Unknown error')
                        result_lines.append(
                            f"❌ <b>{mint_short}</b>\n"
                            f"   Error: {html.escape(error)}\n"
                        )
                
                # Summary
                successful = data.get('successful_predictions', 0)
                failed = data.get('failed_predictions', 0)
                buy_signals = data.get('buy_signals', 0)
                
                result_lines.append(
                    f"\n<b>Summary:</b>\n"
                    f"✅ Success: {successful} | ❌ Failed: {failed}\n"
                    f"🟢 Buy Signals: {buy_signals}"
                )
                
                final_msg = "\n".join(result_lines)
                
                # Check Telegram message length limit
                if len(final_msg) > 4096:
                    final_msg = final_msg[:4090] + "...\n<i>(truncated)</i>"
                    
                await loading_msg.edit_text(final_msg, parse_mode="HTML", disable_web_page_preview=True)
                
            else:
                error_data = await resp.json()
                error_msg = error_data.get('detail', 'Unknown error')
                logger.warning(f"Batch prediction failed, status {resp.status}: {error_msg}")
                await loading_msg.edit_text(
                    f"❌ <b>Batch Analysis Failed</b>\n\n"
                    f"<b>Error:</b> {html.escape(error_msg)}",
                    parse_mode="HTML"
                )
                
    except asyncio.TimeoutError:
        logger.error(f"Timeout during /predict_batch for {len(mints)} mints")
        await loading_msg.edit_text(
            "❌ <b>Request Timed Out</b>\n\n"
            "The prediction service is taking too long to respond.\n"
            "Try reducing the number of tokens or try again later."
        )
    except aiohttp.ClientError as e:
        logger.error(f"HTTP error during /predict_batch: {e}")
        await loading_msg.edit_text(
            "❌ <b>Connection Error</b>\n\n"
            "Failed to connect to the prediction service.\n"
            "Please try again in a moment."
        )
    except Exception as e:
        logger.exception(f"Error in /predict_batch command: {e}")
        await loading_msg.edit_text(
            "❌ <b>An Unexpected Error Occurred</b>\n\n"
            "Please try again later or contact support."
        )

# --- END ML PREDICTION COMMANDS ---


# --- MODIFIED: button_handler ---
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager):
    """Handle inline keyboard button callbacks."""
    query = update.callback_query
    
    # Acknowledge the click immediately
    # For refresh, show a loading text
    if query.data.startswith("refresh_alpha:"):
        await query.answer("Refreshing data...")
    else:
        await query.answer() 
    
    chat_id = str(query.from_user.id)
    data = query.data

    # --- Handle Alpha Refresh Button ---
    if data.startswith("refresh_alpha:"):
        try:
            mint = data.split(":", 1)[1]
            
            # Load the initial state using the correctly constructed path
            alerts_state = safe_load(ALPHA_ALERTS_STATE_FILE, {})
            initial_state = alerts_state.get(mint)
            
            if not initial_state:
                # If not found, it might be because the alert is too old or path issue persisting
                await query.edit_message_text(
                    "Error: Initial data not found for this token. It may be too old.",
                    reply_markup=None
                )
                return
            
            # --- Reply vs. Edit Logic ---
            is_refresh_message = query.message.text.startswith("🔄 <b>Refresh:")
            
            if is_refresh_message:
                # --- EDIT LOGIC (for subsequent clicks) ---
                symbol = initial_state.get("symbol", "N/A")
                loading_message = f"""🔄 <b>Refreshing: ${symbol}</b>

<i>Please wait, fetching live data...</i> 
"""
                loading_keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("Refreshing... ↻", callback_data=f"refresh_alpha:{mint}")]
                ])
                
                try:
                    if query.message.text != loading_message:
                        await query.edit_message_text(
                            text=loading_message,
                            parse_mode="HTML",
                            reply_markup=loading_keyboard,
                            disable_web_page_preview=True
                        )
                except Exception as e:
                    if "message is not modified" in str(e).lower():
                        await query.answer("Already refreshing...")
                        return 
                    raise 
                
                message = await format_alpha_refresh(mint, initial_state)
                final_keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("Refresh ↻", callback_data=f"refresh_alpha:{mint}")]
                ])

                await query.edit_message_text(
                    text=message,
                    parse_mode="HTML",
                    reply_markup=final_keyboard,
                    disable_web_page_preview=True
                )
            
            else:
                # --- REPLY LOGIC (for first click on original alert) ---
                message = await format_alpha_refresh(mint, initial_state)
                final_keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("Refresh ↻", callback_data=f"refresh_alpha:{mint}")]
                ])
                
                await query.message.reply_html(
                    text=message,
                    reply_markup=final_keyboard,
                    disable_web_page_preview=True
                )
                
        except Exception as e:
            if "message is not modified" in str(e).lower():
                await query.answer("Data is already up to date.")
            else:
                logger.error(f"Failed to refresh alpha alert: {e}")
                await query.answer("Error during refresh.", show_alert=True)
        return

    # --- Handle CA Analysis Button in Groups ---
    if data.startswith("analyze_"):
        if query.message.chat.type in ["group", "supergroup"]:
            try:
                mint_address = data.split("_", 1)[1]
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text=f"<code>{mint_address}</code>",
                    parse_mode="HTML"
                )
                return 
            except Exception as e:
                logging.warning(f"Failed to send CA in group: {e}")
                try:
                    await query.answer("Error sending address.", show_alert=True)
                except:
                    pass
                return 
        else:
            try:
                mint_address = data.split("_", 1)[1]
                await query.message.reply_text(f"<code>{mint_address}</code>", parse_mode="HTML")
            except Exception as e:
                logging.warning(f"Failed to send CA in private chat: {e}")
            return

    # --- Subscription Check (for all other user-specific commands) ---
    if not user_manager.is_subscribed(chat_id):
        await query.answer("⛔ You are not subscribed.", show_alert=True)
        return
    
    # --- Mode Selection ---
    if data == "mode_alerts":
        user_manager.set_modes(chat_id, ["alerts"])
        await query.edit_message_text("✅ Mode set to <b>🔔 Alerts Only</b>.", parse_mode="HTML")
    elif data == "mode_papertrade":
        user_manager.set_modes(chat_id, ["papertrade"])
        await query.edit_message_text(
            "✅ Mode set to <b>📈 Paper Trading Only</b>.\n\n"
            "Use <code>/papertrade [capital]</code> to set your starting funds.\n"
            "Example: <code>/papertrade 1000</code>", 
            parse_mode="HTML"
        )
    elif data == "mode_both":
        user_manager.set_modes(chat_id, ["alerts", "papertrade"])
        await query.edit_message_text(
            "✅ Mode set to <b>🚀 Both Alerts & Paper Trading</b>.\n\n"
            "You'll receive alerts AND auto-trade signals.\n"
            "Use <code>/papertrade [capital]</code> to configure your trading capital.", 
            parse_mode="HTML"
        )

    # --- Grade Configuration ---
    elif data == "config_grades":
        keyboard = [
            [InlineKeyboardButton("🔴 CRITICAL", callback_data="preset_critical"),
             InlineKeyboardButton("🔥 CRITICAL + HIGH", callback_data="preset_critical_high")],
            [InlineKeyboardButton("📊 All Grades", callback_data="preset_all")]
        ]
        await query.edit_message_text(
            "Please select a preset for your <b>alert grades</b> or use <code>/setalerts</code> for a custom list.", 
            reply_markup=InlineKeyboardMarkup(keyboard), 
            parse_mode="HTML"
        )
    elif data == "preset_critical":
        user_manager.update_user_prefs(chat_id, {"grades": ["CRITICAL"]})
        await query.edit_message_text("✅ Alert grades updated: <b>CRITICAL</b> only.", parse_mode="HTML")
    elif data == "preset_critical_high":
        user_manager.update_user_prefs(chat_id, {"grades": ["CRITICAL", "HIGH"]})
        await query.edit_message_text("✅ Alert grades updated: <b>CRITICAL + HIGH</b>.", parse_mode="HTML")
    elif data == "preset_all":
        user_manager.update_user_prefs(chat_id, {"grades": ALL_GRADES.copy()})
        await query.edit_message_text("✅ Alert grades updated: <b>ALL</b> grades.", parse_mode="HTML")