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
PAGE_SIZE = 5



def get_mode_status_text(user_prefs: dict) -> str:
    """Generates a status line for user's current modes."""
    modes = user_prefs.get("modes", [])
    status = []
    if "alerts" in modes:
        status.append("\U0001F514 Notifications")
    if "papertrade" in modes:
        status.append("\U0001F4C8 Paper Trading")
    
    if not status:
        return "No active modes."
    return " & ".join(status)

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager):
    """Handle /start command - Show main menu."""
    from alerts.menu_navigation import show_main_menu
    
    chat_id = str(update.effective_chat.id)
    logging.info(f"\U0001F680 User {chat_id} started bot")

    # Ensure user exists and is active
    user_manager.activate_user(chat_id)
    
    # Show the main menu (subscription status will be displayed there)
    from alerts.menu_navigation import show_main_menu
    await show_main_menu(update.message, user_manager, chat_id)

# --- ALPHA ALERTS COMMANDS ---

async def alpha_subscribe_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager):
    """Handle /alpha_subscribe command to opt-in to high-priority alpha alerts."""
    chat_id = str(update.effective_chat.id)
    
    # Check subscription status
    if not user_manager.is_subscribed(chat_id):
        if user_manager.is_subscription_expired(chat_id):
            await update.message.reply_text("â›” Your subscription has expired. Please contact the admin to renew.")
        else:
            await update.message.reply_text("â›” You are not subscribed. Please contact the admin.")
        return
    
    # Get current preferences to check current state
    user_prefs = user_manager.get_user_prefs(chat_id)
    already_subscribed = user_prefs.get("alpha_alerts", False)
    
    if already_subscribed:
        await update.message.reply_html(
            "â„¹ï¸ <b>Already Subscribed!</b>\n\n"
            "You are already receiving Alpha Alerts.\n"
            "Use /myalerts to view your settings."
        )
        return
    
    # Set the user preference for alpha_alerts to True
    success = user_manager.update_user_prefs(chat_id, {"alpha_alerts": True})
    
    if success:
        await update.message.reply_html(
            "ðŸš€ <b>Alpha Alerts Activated!</b>\n\n"
            "\u2705 You will now receive high-priority Alpha Alerts.\n"
            "<i>Use /myalerts to confirm your settings</i>"
        )
    else:
        await update.message.reply_html(
            "âŒ <b>Failed to Subscribe</b>\n\n"
            "There was an error updating your preferences. Please try again in a moment."
        )


async def alpha_unsubscribe_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager):
    """Handle /alpha_unsubscribe command to opt-out of alpha alerts."""
    chat_id = str(update.effective_chat.id)
    
    if not user_manager.is_subscribed(chat_id):
        if user_manager.is_subscription_expired(chat_id):
            await update.message.reply_text("â›” Your subscription has expired. Please contact the admin to renew.")
        else:
            await update.message.reply_text("â›” You are not subscribed. Please contact the admin.")
        return
    
    # Get current preferences to check current state
    user_prefs = user_manager.get_user_prefs(chat_id)
    currently_subscribed = user_prefs.get("alpha_alerts", False)
    
    if not currently_subscribed:
        await update.message.reply_html(
            "â„¹ï¸ <b>Not Subscribed</b>\n\n"
            "You are not currently receiving Alpha Alerts.\n"
            "Use /alpha_subscribe to enable them."
        )
        return
    
    # Set the user preference for alpha_alerts to False
    success = user_manager.update_user_prefs(chat_id, {"alpha_alerts": False})
    
    if success:
        await update.message.reply_html(
            "ðŸ˜´ <b>Alpha Alerts Disabled</b>\n\n"
            "\u2705 You will no longer receive high-priority Alpha Alerts.\n"
            "<i>Use /myalerts to confirm your settings</i>"
        )
    else:
        await update.message.reply_html(
            "âŒ <b>Failed to Unsubscribe</b>\n\n"
            "There was an error updating your preferences. Please try again in a moment."
        )

# --- END ALPHA ALERTS COMMANDS ---

async def setalerts_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager):
    """Handle /setalerts command with improved UX."""
    chat_id = str(update.effective_chat.id)
    
    if not user_manager.is_subscribed(chat_id):
        if user_manager.is_subscription_expired(chat_id):
            await update.message.reply_text("â›” Your subscription has expired. Please contact the admin to renew.")
        else:
            await update.message.reply_text("â›” You are not subscribed. Please contact the admin.")
        return
    
    args = context.args or []
    valid = set(ALL_GRADES)
    chosen = [a.upper() for a in args if a.upper() in valid]

    if not chosen:
        # Show interactive buttons AND usage instructions
        keyboard = [
            [InlineKeyboardButton("ðŸ”´ CRITICAL", callback_data="preset_critical"),
             InlineKeyboardButton("ðŸ”¥ CRITICAL + HIGH", callback_data="preset_critical_high")],
            [InlineKeyboardButton("ðŸ❌Š All Grades", callback_data="preset_all")]
        ]
        
        current_grades = user_manager.get_user_prefs(chat_id).get('grades', [])
        current_str = ', '.join(current_grades) if current_grades else "None set"
        
        await update.message.reply_html(
            f"âš™ï¸ <b>Configure Alert Grades</b>\n\n"
            f"<b>Current Setting:</b> {current_str}\n\n"
            f"<b>ðŸ❌ Manual Setup:</b>\n"
            f"Usage: <code>/setalerts GRADE1 GRADE2 ...</code>\n\n"
            f"<b>Available Grades:</b>\n"
            f"\u2022 CRITICAL - Highest priority signals\n"
            f"\u2022 HIGH - Strong signals\n"
            f"\u2022 MEDIUM - Moderate signals\n"
            f"\u2022 LOW - All signals\n\n"
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
        await update.message.reply_html(f"\u2705 Alert grades updated! You will now receive: <b>{', '.join(chosen)}</b>")
    else:
        await update.message.reply_text("âŒ Failed to save preferences. Please try again.")

async def myalerts_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager):
    """Handle /myalerts command, now shows modes as well."""
    chat_id = str(update.effective_chat.id)
    # Allowed for all users
    
    prefs = user_manager.get_user_prefs(chat_id)
    stats = user_manager.get_user_stats(chat_id)

    total_alerts = stats.get("alerts_received", 0)
    last_alert = stats.get("last_alert_at")
    last_alert_str = "Never" if not last_alert else f"<i>{last_alert[:10]}</i>"

    # --- New: Check Alpha Alert Status ---
    alpha_status = "\u2705 Subscribed" if prefs.get("alpha_alerts", False) else "\u274C Not Subscribed"
    # --- End New ---
    
    msg = (
        f"\U0001F4CA <b>Your Settings</b>\n\n"
        f"<b>Active Modes:</b> {get_mode_status_text(prefs)}\n"
        f"<b>Subscribed Grades:</b> {', '.join(prefs.get('grades', ALL_GRADES))}\n"
        f"<b>\U0001F680 Alpha Notifications:</b> {alpha_status}\n\n"
        f"<b>Total signals received:</b> {total_alerts}\n"
        f"<b>Last signal:</b> {last_alert_str}\n\n"
        f"Use /start to change your mode.\n"
        f"Use /setalerts to change notification grades.\n"
        f"Use /alpha_subscribe or /alpha_unsubscribe to manage alpha notifications."
    )
    await update.message.reply_html(msg)

async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager):
    """Handle /stop command."""
    chat_id = str(update.effective_chat.id)
    user_manager.deactivate_user(chat_id)
    await update.message.reply_html("\U0001F614 You have been unsubscribed from all notifications and services. Use /start to reactivate.")

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /help command."""
    help_text = (
        "\U0001F916 <b>Bot Help & Commands</b>\n\n"
        "<b>--- Core Commands ---</b>\n"
        "\u2022 /start - Change bot mode (Notifications/Trading)\n"
        "\u2022 /myalerts - View your current settings & stats\n"
        "\u2022 /setalerts - Set which grade notifications you receive\n"
        "\u2022 /stop - Unsubscribe from everything\n\n"
        "<b>--- \U0001F525 Alpha Notifications ---</b>\n"
        "\u2022 /alpha_subscribe - Opt-in to high-priority alpha notifications\n"
        "\u2022 /alpha_unsubscribe - Opt-out of alpha notifications\n"
        "\u2022 /set_min_prob_discovery [0-100] - Set min win chance for Discovery\n"
        "\u2022 /set_min_prob_alpha [0-100] - Set min win chance for Alpha\n\n"
        
        "<b>--- \U0001F916 ML Predictions (NEW) ---</b>\n"
        "\u2022 /predict [mint] - Get ML prediction for one token\n"
        "\u2022 /predict_batch [mints...] - Get ML predictions for multiple tokens\n\n"

        "<b>--- Paper Trading ---</b>\n"
        "\u2022 /papertrade [capital] - Set trading capital and enable paper trading\n"
        "  Example: <code>/papertrade 1000</code>\n"
        "\u2022 /portfolio - View detailed portfolio with all positions\n"
        "\u2022 /pnl - Get current unrealized P/L update\n"
        "\u2022 /history [limit] - View trade history (default: last 10)\n"
        "\u2022 /performance - View detailed trading performance stats\n"
        "\u2022 /watchlist - View tokens being watched for entry\n"
        "\u2022 /resetcapital [amount] - Reset trading capital\n\n"
        "<b>--- General ---</b>\n"
        "\u2022 /help - Show this help message\n"
        "\u2022 /stats - View your usage statistics"
    )
    await update.message.reply_html(help_text)

# --- ENHANCED TRADING COMMANDS ---

async def papertrade_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager, portfolio_manager: PortfolioManager):
    """Enable paper trading mode and configure capital with improved UX."""
    # Allowed for all users
    chat_id = str(update.effective_chat.id)
    
    
    # Check if already enabled
    prefs = user_manager.get_user_prefs(chat_id)
    is_already_enabled = "papertrade" in prefs.get("modes", [])
    
    if not context.args:
        # Show helpful prompt with current status
        portfolio = portfolio_manager.get_portfolio(chat_id)
        current_capital = portfolio.get('capital_usd', 0)
        
        status_msg = ""
        if is_already_enabled:
            status_msg = f"<b>Current Status:</b> \u2705 Enabled\n<b>Current Capital:</b> ${current_capital:,.2f}\n\n"
        else:
            status_msg = "<b>Current Status:</b> \u274C Not enabled\n\n"
        
        await update.message.reply_html(
            f"\U0001F4C8 <b>Paper Trading Setup</b>\n\n"
            f"{status_msg}"
            f"<b>\u270F\uFE0F How to use:</b>\n"
            f"<code>/papertrade [amount]</code>\n\n"
            f"<b>Examples:</b>\n"
            f"<code>/papertrade 1000</code> - Start with $1,000\n"
            f"<code>/papertrade 5000</code> - Start with $5,000\n"
            f"<code>/papertrade 10000</code> - Start with $10,000\n\n"
            f"<b>Requirements:</b>\n"
            f"\u2022 Minimum: $100\n"
            f"\u2022 Maximum: $1,000,000\n\n"
            f"\U0001F4A1 <i>Tip: Start with $1,000-$5,000 for realistic results</i>"
        )
        return
    
    capital = 1000.0  # Default capital
    try:
        capital = float(context.args[0])
        if capital <= 0:
            await update.message.reply_html(
                "âŒ <b>Invalid Amount</b>\n\n"
                "Please provide a positive number.\n"
                "Example: <code>/papertrade 1000</code>"
            )
            return
        if capital < 100:
            await update.message.reply_html(
                "âŒ <b>Amount Too Low</b>\n\n"
                "Minimum capital is <b>$100 USD</b>.\n"
                "Example: <code>/papertrade 100</code>"
            )
            return
        if capital > 1000000:
            await update.message.reply_html(
                "âŒ <b>Amount Too High</b>\n\n"
                "Maximum capital is <b>$1,000,000 USD</b>.\n"
                "Example: <code>/papertrade 10000</code>"
            )
            return
    except ValueError:
        await update.message.reply_html(
            "âŒ <b>Invalid Format</b>\n\n"
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
        f"\U0001F4C8 <b>Paper Trading {'Updated' if is_already_enabled else 'Enabled'}!</b>\n\n"
        f"Your virtual portfolio has been {action_word} <b>${capital:,.2f} USD</b>.\n\n"
        f"<b>\U0001F3AF Strategy Overview:</b>\n"
        f"\u2022 Position Size: 8-12% per trade (max $150)\n"
        f"\u2022 Partial Profits: 40% @ +40%, 30% @ +80%, 20% @ +150%\n"
        f"\u2022 Trailing Stop: Dynamic 15-25% from peak\n"
        f"\u2022 Liquidity Protection: Exit on 40% drain\n"
        f"\u2022 Max Hold: 4 hours\n\n"
        f"<b>\U0001F4CA Track Your Performance:</b>\n"
        f"\u2022 /portfolio - View positions\n"
        f"\u2022 /pnl - Check unrealized P/L\n"
        f"\u2022 /performance - Detailed stats\n"
        f"\u2022 /history - Trade log\n\n"
        f"The bot will now automatically trade signals. Good luck! \U0001F680"
    )

async def portfolio_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager, portfolio_manager: PortfolioManager, page: int = 0):
    """Display user's comprehensive paper trading portfolio with pagination and sell buttons."""
    from alerts.trading_buttons import send_portfolio_page
    
    chat_id = str(update.effective_chat.id)
    prefs = user_manager.get_user_prefs(chat_id)
    if "papertrade" not in prefs.get("modes", []):
        await update.message.reply_html("❌ Paper trading is not enabled. Use /papertrade [capital] to enable it.")
        return
    
    portfolio = portfolio_manager.get_portfolio(chat_id)
    await send_portfolio_page(update.message, chat_id, portfolio, page=page)

async def pnl_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager, portfolio_manager: PortfolioManager, page: int = 0):
    """Get current unrealized P/L for all open positions with live prices and interactive buttons."""
    from alerts.trading_buttons import send_pnl_page
    
    chat_id = str(update.effective_chat.id)
    prefs = user_manager.get_user_prefs(chat_id)
    if "papertrade" not in prefs.get("modes", []):
        await update.message.reply_html("❌ Paper trading is not enabled.")
        return
    
    # Fetch live prices and calculate PnL
    try:
        live_prices = await portfolio_manager.update_positions_with_live_prices(chat_id)
        pnl_data = portfolio_manager.calculate_unrealized_pnl(chat_id, live_prices)
    except Exception as e:
        logger.exception(f"Error calculating PnL for {chat_id}: {e}")
        await update.message.reply_html("❌ Error fetching live prices. Please try again.")
        return
    
    portfolio = portfolio_manager.get_portfolio(chat_id)
    await send_pnl_page(update.message, chat_id, portfolio, pnl_data, page=page)


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
                    "âŒ <b>Invalid Limit</b>\n\n"
                    "Please provide a number greater than 0.\n"
                    "Example: <code>/history 20</code>"
                )
                return
            if limit > 50:
                await update.message.reply_html(
                    "\u26A0\uFE0F <b>Limit Too High</b>\n\n"
                    "Maximum is 50 trades at a time.\n"
                    "Showing last 50 trades instead..."
                )
                limit = 50
        except ValueError:
            await update.message.reply_html(
                "âŒ <b>Invalid Format</b>\n\n"
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
            "\U0001F4DC <b>No Trade History</b>\n\n"
            "You haven't closed any trades yet.\n\n"
            "Start trading to see your results here!\n"
            "Use <code>/portfolio</code> to see open positions."
        )
        return
    
    # Get most recent trades
    recent_trades = history[-limit:]
    recent_trades.reverse()  # Most recent first
    
    msg = f"\U0001F4DC <b>Trade History (Last {len(recent_trades)}/{len(history)})</b>\n\n"
    
    for i, trade in enumerate(recent_trades, 1):
        pnl_symbol = "\U0001F7E2" if trade.get('total_pnl_usd', trade.get('pnl_usd', 0)) > 0 else "\U0001F534"
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
        msg += f"<i>\U0001F4A1 Use /history {min(limit + 10, 50)} to see more</i>"
    
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
        await update.message.reply_html("\U0001F4CA No trades yet. Performance stats will appear after your first closed trade.")
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
        f"\U0001F4CA <b>Trading Performance Report</b>\n\n"
        f"<b>\U0001F4B0 Capital:</b>\n"
        f"\u2022 Starting: ${starting_capital:,.2f}\n"
        f"\u2022 Current Total: ${total_value:,.2f}\n"
        f"\u2022 ROI: <b>{roi:+.2f}%</b>\n\n"
        f"<b>ðŸ❌ˆ Trade Statistics:</b>\n"
        f"\u2022 Total Trades: <b>{total_trades}</b>\n"
        f"\u2022 Wins: <b>{wins}</b> | Losses: <b>{losses}</b>\n"
        f"\u2022 Win Rate: <b>{win_rate:.1f}%</b>\n"
        f"\u2022 Total P/L: <b>${total_pnl:,.2f}</b>\n\n"
        f"<b>\U0001F4B5 Trade Metrics:</b>\n"
        f"\u2022 Best Trade: <b>+{best_trade:.1f}%</b>\n"
        f"\u2022 Worst Trade: <b>{worst_trade:.1f}%</b>\n"
        f"\u2022 Avg Win: <b>${avg_win:,.2f}</b>\n"
        f"\u2022 Avg Loss: <b>${avg_loss:,.2f}</b>\n"
        f"\u2022 Avg Hold Time: <b>{avg_hold_time:.0f} minutes</b>\n\n"
    )
    
    if reentry_trades > 0:
        msg += (
            f"<b>\U0001F504 Re-entry Stats:</b>\n"
            f"\u2022 Re-entry Trades: <b>{reentry_trades}</b>\n"
            f"\u2022 Re-entry Wins: <b>{reentry_wins}</b>\n"
            f"\u2022 Re-entry Win Rate: <b>{reentry_rate:.1f}%</b>\n\n"
        )
    
    # Exit reason breakdown
    exit_reasons = {}
    for trade in history:
        reason = trade.get('exit_reason', trade.get('reason', 'Unknown'))
        exit_reasons[reason] = exit_reasons.get(reason, 0) + 1
    
    if exit_reasons:
        msg += f"<b>\U0001F4E4 Exit Breakdown:</b>\n"
        for reason, count in sorted(exit_reasons.items(), key=lambda x: x[1], reverse=True)[:5]:
            msg += f"\u2022 {reason}: {count}\n"
    
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
        await update.message.reply_html("ðŸ‘€ No tokens currently being watched. Waiting for new signals...")
        return
    
    msg = f"\U0001F440 <b>Watchlist & Re-entry Candidates</b>\n\n"
    
    if watchlist:
        msg += f"<b>\U0001F3AF Waiting for Entry ({len(watchlist)}):</b>\n"
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
                f"\u2022 <b>{item['symbol']}</b>\n"
                f"  Signal: ${item['signal_price']:.6f} ({time_ago})\n"
            )
        msg += "\n"
    
    if reentry:
        msg += f"<b>ðŸ”„ Re-entry Watch ({len(reentry)}):</b>\n"
        for mint, cand in reentry.items():
            attempts = cand.get('reentry_attempts', 0)
            best_pnl = cand.get('best_pnl_pct', 0)
            
            msg += (
                f"\u2022 <b>{cand['symbol']}</b>\n"
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
            f"\u26A0\uFE0F <b>Reset Trading Capital</b>\n\n"
            f"<b>Current Status:</b>\n"
            f"\u2022 Capital: ${current_capital:,.2f}\n"
            f"\u2022 Open Positions: {open_positions}\n\n"
            f"\u26A0\uFE0F <b>Warning:</b> This will:\n"
            f"\u2022 Close ALL open positions\n"
            f"\u2022 Clear watchlist and re-entry candidates\n"
            f"\u2022 Reset your capital to a new amount\n"
            f"\u2022 Preserve your trade history\n\n"
            f"<b>ðŸ❌ Usage:</b>\n"
            f"<code>/resetcapital [amount]</code>\n\n"
            f"<b>Examples:</b>\n"
            f"<code>/resetcapital 1000</code> - Reset to $1,000\n"
            f"<code>/resetcapital 5000</code> - Reset to $5,000\n"
            f"<code>/resetcapital 10000</code> - Reset to $10,000\n\n"
            f"<b>Requirements:</b>\n"
            f"\u2022 Min: $100 | Max: $1,000,000"
        )
        return
    
    try:
        capital = float(context.args[0])
        if capital < 100 or capital > 1000000:
            await update.message.reply_html(
                "âŒ <b>Invalid Amount</b>\n\n"
                "Capital must be between <b>$100</b> and <b>$1,000,000</b>.\n\n"
                "<b>Examples:</b>\n"
                "<code>/resetcapital 1000</code>\n"
                "<code>/resetcapital 5000</code>"
            )
            return
    except ValueError:
        await update.message.reply_html(
            "âŒ <b>Invalid Format</b>\n\n"
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
        f"\u2705 <b>Portfolio Reset Complete</b>\n\n"
        f"<b>Changes:</b>\n"
        f"\u2022 Closed: {old_positions} position(s)\n"
        f"\u2022 New Capital: <b>${capital:,.2f}</b>\n"
        f"\u2022 Trade History: \u2705 Preserved\n\n"
        f"<i>Ready to start fresh! ðŸš€</i>\n\n"
        f"The bot will now watch for new signals with your updated capital."
    )

async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager, is_admin: bool = False):
    """Handle /stats command."""
    chat_id = str(update.effective_chat.id)
    user_stats = user_manager.get_user_stats(chat_id)
    
    msg = (
        f"\U0001F4CA <b>Your Statistics</b>\n\n"
        f"\U0001F4EC Total alerts received: <b>{user_stats.get('alerts_received', 0)}</b>\n"
        f"\U0001F4C5 Member since: <i>{user_stats.get('joined_at', 'Unknown')[:10] if user_stats.get('joined_at') else 'Unknown'}</i>\n"
    )
    
    if is_admin:
        platform_stats = user_manager.get_all_stats()
        
        # Get segment counts
        subs_count = len(user_manager.get_users_by_segment('subs'))
        expired_count = len(user_manager.get_users_by_segment('expired'))
        free_count = len(user_manager.get_users_by_segment('free'))
        
        msg += (
            f"\n\U0001F3E2 <b>Platform Statistics (Admin)</b>\n"
            f"\u2022 Total users: <b>{platform_stats['total_users']}</b>\n"
            f"\u2022 Active users: <b>{platform_stats['active_users']}</b>\n\n"
            
            f"<b>\U0001F4CA User Segments:</b>\n"
            f"\u2022 \U0001F48E Subscribers: <b>{subs_count}</b>\n"
            f"\u2022 \U0001F570 Expired: <b>{expired_count}</b>\n"
            f"\u2022 \U0001F193 Free: <b>{free_count}</b>\n\n"
            
            f"\u2022 Total alerts sent: <b>{platform_stats['total_alerts_sent']}</b>\n"
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
    await update.message.reply_html(f"ðŸ”” Test Alert\n\n{message}")


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
            f"\U0001F916 <b>ML Prediction for Token</b>",
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
            
            lines.append(f"\U0001F4A7 <b>Liquidity:</b> {fmt_num(liq)}")
            lines.append(f"\U0001F4CA <b>24h Volume:</b> {fmt_num(vol)}")
            if fdv > 0:
                lines.append(f"\U0001F4B0 <b>Market Cap:</b> {fmt_num(fdv)}")

            # 2. Age & Price Action
            age_hours = key_metrics.get('token_age_hours', 0)
            price_change = key_metrics.get('price_change_h24_pct', 0)
            
            # Smart Age Formatting (Corrected for Days/Hours)
            if age_hours >= 24:
                age_str = f"{age_hours/24:.1f} days"
            else:
                age_str = f"{age_hours:.1f}h"
                
            lines.append(f"\U000023F0 <b>Token Age:</b> {age_str}")
            
            # Price Change with Direction
            pch_emoji = "\U0001F7E2" if price_change > 0 else "\U0001F534"
            lines.append(f"{pch_emoji} <b>24h Change:</b> {price_change:+.2f}%")

            # 3. Holders & Supply
            insider = key_metrics.get('insider_supply_pct', 0)
            top10 = key_metrics.get('top_10_holders_pct', 0)
            
            lines.append(f"\U0001F465 <b>Insider Holdings:</b> {insider:.1f}%")
            lines.append(f"\U0001F3F3 <b>Top 10 Holders:</b> {top10:.1f}%")
            
            # 4. Risk & Health (Interpretations)
            risk_score = key_metrics.get('pump_dump_risk_score', 0)
            health_score = key_metrics.get('market_health_score', 0)
            
            # Interpret Risk (Lower is better)
            if risk_score <= 20:
                risk_text = "Low (Safe)"
                risk_emoji = "\U0001F7E2"
            elif risk_score <= 50:
                risk_text = "Moderate"
                risk_emoji = "\U0001F7E1"
            else:
                risk_text = "High Risk"
                risk_emoji = "\U0001F534"
            
            # Interpret Health (Higher is better)
            if health_score >= 80:
                health_text = "Strong"
                health_emoji = "\U0001F4AA"
            elif health_score >= 40:
                health_text = "Average"
                health_emoji = "\u26A0\uFE0F"
            else:
                health_text = "Weak"
                health_emoji = "\U0001F912"
            
            lines.append(f"{risk_emoji} <b>Risk Level:</b> {risk_text} ({risk_score:.0f})")
            lines.append(f"{health_emoji} <b>Market Health:</b> {health_text} ({health_score:.0f})")

        # Add Warnings
        warnings = pred.get('warnings', [])
        if warnings:
            lines.append("\n--- <b>Warnings</b> ---")
            for warning in warnings:
                # Clean up specific common warning text if needed, otherwise just display
                clean_warning = html.escape(warning)
                lines.append(f"\u2022 {clean_warning}")
        
        return "\n".join(lines)
        
    except Exception as e:
        logger.error(f"Error formatting prediction: {e}")
        return f"âŒ Error formatting prediction for <code>{mint}</code>."

async def predict_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager):
    """Handle /predict [mint] command."""
    chat_id = str(update.effective_chat.id)
    # Allowed for all users
    

    if not context.args or len(context.args) != 1:
        await update.message.reply_html(
            "<b>Usage:</b> <code>/predict [mint_address]</code>\n\n"
            "<b>Example:</b> <code>/predict DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263</code>"
        )
        return

    mint = context.args[0].strip()
    loading_msg = await update.message.reply_html(f"\U0001F916 Analyzing token <code>{mint}</code>... Please wait.")
    
    url = f"{FASTAPI_ML_URL}/token/{mint}/predict"
    
    try:
        session = await _get_http_session()
        async with session.get(url, timeout=300) as resp:
            if resp.status == 200:
                data = await resp.json()
                result_msg = _format_prediction_result(mint, data)
                await loading_msg.edit_text(result_msg, parse_mode="HTML", disable_web_page_preview=True)
            else:
                error_data = await resp.json()
                error_msg = error_data.get('detail', 'Unknown error')
                logger.warning(f"Prediction failed for {mint}, status {resp.status}: {error_msg}")
                await loading_msg.edit_text(
                    f"\u274C <b>Analysis Failed for <code>{mint}</code></b>\n\n"
                    f"<b>Error:</b> {html.escape(error_msg)}",
                    parse_mode="HTML"
                )
                
    except asyncio.TimeoutError:
        logger.error(f"Timeout during /predict for {mint}")
        await loading_msg.edit_text(f"âŒ <b>Request Timed Out</b>\n\nThe prediction service is taking too long to respond.")
    except aiohttp.ClientError as e:
        logger.error(f"HTTP error during /predict: {e}")
        await loading_msg.edit_text(f"âŒ <b>Connection Error</b>\n\nFailed to connect to the prediction service.")
    except Exception as e:
        logger.exception(f"Error in /predict command: {e}")
        await loading_msg.edit_text(f"âŒ <b>An Unexpected Error Occurred</b>\n\nPlease try again later.")

async def predict_batch_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager):
    """Handle /predict_batch [mint1] [mint2] ... command."""
    chat_id = str(update.effective_chat.id)
    # Allowed for all users
    

    mints = [arg.strip() for arg in context.args]
    
    if not mints:
        await update.message.reply_html(
            "<b>Usage:</b> <code>/predict_batch [mint1] [mint2] ...</code>\n\n"
            "<b>Example:</b> <code>/predict_batch mint1 mint2 mint3</code>\n\n"
            "<b>Note:</b> Maximum 10 tokens per batch"
        )
        return
        
    if len(mints) > 10:
        await update.message.reply_html("âŒ <b>Error:</b> Maximum of 10 tokens per batch request.")
        return

    loading_msg = await update.message.reply_html(
        f"\U0001F916 Analyzing <b>{len(mints)} tokens</b> in a batch... This may take a moment."
    )
    
    # FIXED: Send as JSON array in request body, NOT inside a dict
    url = f"{FASTAPI_ML_URL}/token/predict/batch"
    
    try:
        session = await _get_http_session()
        
        # CRITICAL FIX: Send mints as a JSON array directly
        async with session.post(
            url, 
            json=mints,  # â† This is now a list, not {"mints": mints}
            params={"threshold": 0.70},
            timeout=300
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
                            "BUY": "\U0001F7E2",
                            "CONSIDER": "\U0001F7E1",
                            "SKIP": "\U0001F7E0",
                            "AVOID": "\U0001F534"
                        }.get(action, "\u26AA")
                        
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
                            f"\u274C <b>{mint_short}</b>\n"
                            f"   Error: {html.escape(error)}\n"
                        )
                
                # Summary
                successful = data.get('successful_predictions', 0)
                failed = data.get('failed_predictions', 0)
                buy_signals = data.get('buy_signals', 0)
                
                result_lines.append(
                    f"\n<b>Summary:</b>\n"
                    f"\u2705 Success: {successful} | \u274C Failed: {failed}\n"
                    f"\U0001F7E2 Buy Signals: {buy_signals}"
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
                    f"âŒ <b>Batch Analysis Failed</b>\n\n"
                    f"<b>Error:</b> {html.escape(error_msg)}",
                    parse_mode="HTML"
                )
                
    except asyncio.TimeoutError:
        logger.error(f"Timeout during /predict_batch for {len(mints)} mints")
        await loading_msg.edit_text(
            "âŒ <b>Request Timed Out</b>\n\n"
            "The prediction service is taking too long to respond.\n"
            "Try reducing the number of tokens or try again later."
        )
    except aiohttp.ClientError as e:
        logger.error(f"HTTP error during /predict_batch: {e}")
        await loading_msg.edit_text(
            "âŒ <b>Connection Error</b>\n\n"
            "Failed to connect to the prediction service.\n"
            "Please try again in a moment."
        )
    except Exception as e:
        logger.exception(f"Error in /predict_batch command: {e}")
        await loading_msg.edit_text(
            "âŒ <b>An Unexpected Error Occurred</b>\n\n"
            "Please try again later or contact support."
        )

# --- MODIFIED: button_handler ---
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager, portfolio_manager=None):
    """Handle inline keyboard button callbacks."""
    from alerts.trading_buttons import (
        handle_pnl_page_callback, handle_portfolio_page_callback,
        handle_sell_confirm_callback, handle_sell_execute_callback,
        handle_sell_all_confirm_callback, handle_sell_all_execute_callback,
        handle_sell_cancel_callback
    )
    from alerts.menu_handler import handle_menu_callback
    
    query = update.callback_query
    data = query.data
    chat_id = str(query.from_user.id)
    
    # --- Handle Menu Navigation Callbacks First ---
    if data.startswith("menu_") or data.startswith("init_capital:") or data.startswith("reset_capital") or \
       data.startswith("custom_capital") or data.startswith("settings_") or data.startswith("alpha_") or \
       data.startswith("setalerts_") or data.startswith("tp_") or data.startswith("predict_") or \
       data.startswith("help_") or data.startswith("myalerts_") or data.startswith("history_") or \
       data.startswith("performance_") or data == "watchlist_direct" or data == "portfolio_direct" or \
       data == "pnl_direct" or data.startswith("resetcapital_") or data == "grades_done" or \
       data.startswith("enable_") or data == "mysettings_direct" or data == "set_reserve_menu" or \
       data == "set_mintrade_menu" or data.startswith("set_reserve:") or data.startswith("set_mintrade:") or \
       data == "set_reserve_custom" or data == "set_mintrade_custom" or data == "set_default_sl_custom" or \
       data.startswith("set_default_sl:") or data.startswith("set_trade_size_mode_select:") or \
       data.startswith("trade_") or data.startswith("set_trade_") or \
       data.startswith("grade_") or \
       data == "min_prob_menu" or data.startswith("set_prob_"):
        if portfolio_manager:
            await handle_menu_callback(update, context, user_manager, portfolio_manager)
        return
    
    # --- Handle Mode Selection (Direct) ---
    if data == "mode_alerts":
        await query.answer()
        user_manager.set_modes(chat_id, ["alerts"])
        await query.edit_message_text("✅ Mode set to <b>🔔 Alerts Only</b>.", parse_mode="HTML")
        return
    elif data == "mode_papertrade":
        await query.answer()
        user_manager.set_modes(chat_id, ["papertrade"])
        await query.edit_message_text(
            "✅ Mode set to <b>📈 Paper Trading Only</b>.\n\n"
            "Use <code>/papertrade [capital]</code> to set your starting funds.", 
            parse_mode="HTML"
        )
        return
    elif data == "mode_both":
        await query.answer()
        user_manager.set_modes(chat_id, ["alerts", "papertrade"])
        await query.edit_message_text(
            "✅ Mode set to <b>🚀 Both Alerts & Paper Trading</b>.", 
            parse_mode="HTML"
        )
        return

    # --- Grade Configuration Presets ---
    elif data == "config_grades":
        if not user_manager.is_subscribed(chat_id):
            if user_manager.is_subscription_expired(chat_id):
                await query.answer("\u26A0\uFE0F Subscription expired.", show_alert=True)
            else:
                await query.answer("\u26A0\uFE0F Active subscription required.", show_alert=True)
            return
        await query.answer()
        keyboard = [
            [InlineKeyboardButton("ðŸ”´ CRITICAL", callback_data="preset_critical"),
             InlineKeyboardButton("ðŸ”¥ CRITICAL + HIGH", callback_data="preset_critical_high")],
            [InlineKeyboardButton("ðŸ❌Š All Grades", callback_data="preset_all")]
        ]
        await query.edit_message_text(
            "Please select a preset for your <b>alert grades</b> or use <code>/setalerts</code> for a custom list.", 
            reply_markup=InlineKeyboardMarkup(keyboard), 
            parse_mode="HTML"
        )
        return
    elif data == "preset_critical":
        if not user_manager.is_subscribed(chat_id):
            if user_manager.is_subscription_expired(chat_id):
                await query.answer("\u26A0\uFE0F Subscription expired.", show_alert=True)
            else:
                await query.answer("\u26A0\uFE0F Active subscription required.", show_alert=True)
            return
        await query.answer()
        user_manager.update_user_prefs(chat_id, {"grades": ["CRITICAL"]})
        await query.edit_message_text("\u2705 Alert grades updated: <b>CRITICAL</b> only.", parse_mode="HTML")
        return
    elif data == "preset_critical_high":
        if not user_manager.is_subscribed(chat_id):
            if user_manager.is_subscription_expired(chat_id):
                await query.answer("\u26A0\uFE0F Subscription expired.", show_alert=True)
            else:
                await query.answer("\u26A0\uFE0F Active subscription required.", show_alert=True)
            return
        await query.answer()
        user_manager.update_user_prefs(chat_id, {"grades": ["CRITICAL", "HIGH"]})
        await query.edit_message_text("\u2705 Alert grades updated: <b>CRITICAL + HIGH</b>.", parse_mode="HTML")
        return
    elif data == "preset_all":
        if not user_manager.is_subscribed(chat_id):
            if user_manager.is_subscription_expired(chat_id):
                await query.answer("\u26A0\uFE0F Subscription expired.", show_alert=True)
            else:
                await query.answer("\u26A0\uFE0F Active subscription required.", show_alert=True)
            return
        await query.answer()
        user_manager.update_user_prefs(chat_id, {"grades": ALL_GRADES.copy()})
        await query.edit_message_text("\u2705 Alert grades updated: <b>ALL</b> grades.", parse_mode="HTML")
        return

    # --- Handle Trading Button Callbacks ---
    if (data.startswith("buy_amount:") or data.startswith("buy_custom:") or 
        data.startswith("buy_tp:") or data.startswith("buy_sl:") or
        data == "buy_tp_custom" or data == "buy_sl_custom"):
        if portfolio_manager:
            await buy_token_callback_handler(update, context, user_manager, portfolio_manager)
        return
    
    # Handle message deletion
    if data == "delete_msg":
        await query.answer()
        await query.message.delete()
        return
    
    # Pagination
    if data.startswith("pnl_page:"):
        if portfolio_manager:
            await handle_pnl_page_callback(update, context, user_manager, portfolio_manager)
        return
    elif data.startswith("portfolio_page:"):
        if portfolio_manager:
            await handle_portfolio_page_callback(update, context, user_manager, portfolio_manager)
        return
    
    # Position management
    elif data.startswith("sell_confirm:"):
        if portfolio_manager:
            await handle_sell_confirm_callback(update, context, user_manager, portfolio_manager)
        return
    elif data.startswith("sell_execute:"):
        if portfolio_manager:
            await handle_sell_execute_callback(update, context, user_manager, portfolio_manager)
        return
    elif data == "sell_all_confirm":
        if portfolio_manager:
            await handle_sell_all_confirm_callback(update, context, user_manager, portfolio_manager)
        return
    elif data == "sell_all_execute":
        if portfolio_manager:
            await handle_sell_all_execute_callback(update, context, user_manager, portfolio_manager)
        return
    elif data == "sell_cancel":
        await handle_sell_cancel_callback(update, context)
        return
    
    # --- Handle Alpha Refresh Button ---
    if data.startswith("refresh_alpha:"):
        try:
            mint = data.split(":", 1)[1]
            from shared.file_io import safe_load
            alerts_state = safe_load(ALPHA_ALERTS_STATE_FILE, {})
            initial_state = alerts_state.get(mint)
            if not initial_state:
                await query.answer("Record not found", show_alert=True)
                return
            message = await format_alpha_refresh(mint, initial_state)
            final_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("\U0001F504 Refresh", callback_data=f"refresh_alpha:{mint}")]])
            if query.message.text.startswith("\U0001F504 Refresh:"):
                await query.edit_message_text(text=message, parse_mode="HTML", reply_markup=final_keyboard)
            else:
                await query.message.reply_html(text=message, reply_markup=final_keyboard)
            await query.answer("Refreshed!")
        except Exception as e:
            logger.error(f"Refresh error: {e}")
            await query.answer("Refresh failed", show_alert=True)
        return

    # --- Handle CA Analysis Button ---
    if data.startswith("analyze_"):
        await query.answer()
        mint_address = data.split("_", 1)[1]
        await query.message.reply_text(f"<code>{mint_address}</code>", parse_mode="HTML")
        return

async def set_tp_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager):
    """Set global TP preference."""
    chat_id = str(update.effective_chat.id)
    if not context.args:
        await update.message.reply_html(
            "<b>Usage:</b> <code>/set_tp [median|mean|mode|smart|number]</code>\n"
            "\u2022 median: Use median historical ATH (Middle value)\n"
            "\u2022 mean: Use average historical ATH (Aggressive - higher)\n"
            "\u2022 mode: Use most frequent ATH\n"
            "\u2022 smart: Use TP targets statistically reached 75% of the time\n"
            "\u2022 number: Fixed percentage (e.g., 50)"
        )
        return
        
    val = context.args[0].lower()
    if val not in ["median", "mean", "mode", "smart"]:
        try:
            float(val)
        except ValueError:
            await update.message.reply_text("âŒ Invalid option.")
            return

    user_manager.update_user_prefs(chat_id, {"tp_preference": val})
    await update.message.reply_html(f"\u2705 Global TP preference set to: <b>{val}</b>")

async def set_tp_discovery_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager):
    """Override TP for discovery signals."""
    chat_id = str(update.effective_chat.id)
    if not context.args:
        await update.message.reply_html("Usage: <code>/set_tp_discovery [median|mean|mode|smart|number]</code>")
        return
    
    val = context.args[0].lower()
    if val in ["median", "mean", "mode", "smart"]:
        user_manager.update_user_prefs(chat_id, {"tp_discovery": val})
        await update.message.reply_text(f"\u2705 Discovery TP set to {val} ATH")
        return
        
    try:
        val_float = float(val)
        user_manager.update_user_prefs(chat_id, {"tp_discovery": val_float})
        await update.message.reply_text(f"\u2705 Discovery TP fixed at {val_float}%")
    except: await update.message.reply_text("\u274C Invalid option or number")

async def set_tp_alpha_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager):
    """Override TP for alpha signals."""
    chat_id = str(update.effective_chat.id)
    if not context.args:
        await update.message.reply_html("Usage: <code>/set_tp_alpha [median|mean|mode|smart|number]</code>")
        return
    
    val = context.args[0].lower()
    if val in ["median", "mean", "mode", "smart"]:
        user_manager.update_user_prefs(chat_id, {"tp_alpha": val})
        await update.message.reply_text(f"\u2705 Alpha TP set to {val} ATH")
        return
        
    try:
        val_float = float(val)
        user_manager.update_user_prefs(chat_id, {"tp_alpha": val_float})
        await update.message.reply_text(f"\u2705 Alpha TP fixed at {val_float}%")
    except: await update.message.reply_text("\u274C Invalid option or number")


async def view_tp_settings_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager):
    """View current TP settings."""
    chat_id = str(update.effective_chat.id)
    prefs = user_manager.get_user_prefs(chat_id)
    
    tp_global = prefs.get("tp_preference", "median")
    tp_discovery = prefs.get("tp_discovery", "Default (Global)")
    tp_alpha = prefs.get("tp_alpha", "Default (Global)")
    
    msg = (
        f"\U0001F3AF <b>Take Profit Settings</b>\n\n"
        f"<b>Global Preference:</b> {tp_global}\n"
        f"<i>Used when no specific override is set.</i>\n\n"
        f"<b>Overrides:</b>\n"
        f"\u2022 Discovery Signals: <b>{tp_discovery}</b>\n"
        f"\u2022 Alpha Signals: <b>{tp_alpha}</b>\n\n"
        f"Use the buttons in Settings > Take Profit to change these."
    )
    
    # Check if we should edit or reply (based on how it's called)
    # Since this is a command, we usually reply, but if called from menu handler with a new update object
    # that wraps a callback query message, we might want to edit if possible.
    # However, standard commands reply. The menu handler can handle editing if we return the text?
    # No, let's just reply for now, or use the edit logic if we update the handler.
    
    await update.message.reply_html(msg)


async def buy_token_process(update: Update, context: ContextTypes.DEFAULT_TYPE, 
                          user_manager: UserManager, portfolio_manager: PortfolioManager, mint: str):
    """
    Process a manual token buy request (from implicit mint message).
    Fetches token info and asks for amount.
    """
    from alerts.price_fetcher import PriceFetcher
    
    status_msg = await update.message.reply_text("\U0001F50E Fetching token info...")
    
    # Fetch token info
    token_info = await PriceFetcher.get_token_info(mint)
    
    # Fetch RugCheck security analysis
    rugcheck_data = await PriceFetcher.get_rugcheck_analysis(mint)
    
    if not token_info:
        await status_msg.edit_text("\u274C Could not find token info for this mint address.")
        return
        
    symbol = token_info.get("symbol", "UNKNOWN")
    name = token_info.get("name", "Unknown Token")
    price = token_info.get("price", 0.0)
    source = token_info.get("source", "unknown")
    
    # Allow purchasing anytime, regardless of mode setting
    # Users can trade whenever they want by sending mint addresses
    
    # Ask for amount
    keyboard = [
        [
            InlineKeyboardButton("$100", callback_data=f"buy_amount:{mint}:100"),
            InlineKeyboardButton("$500", callback_data=f"buy_amount:{mint}:500"),
            InlineKeyboardButton("$1000", callback_data=f"buy_amount:{mint}:1000")
        ],
        [
            InlineKeyboardButton("Custom Amount", callback_data=f"buy_custom:{mint}")
        ],
        [InlineKeyboardButton("\u274C Cancel", callback_data="delete_msg")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Format detailed metrics (if available from DexScreener)
    fdv = token_info.get("fdv", 0)
    volume24h = token_info.get("volume24h", 0)
    liquidity = token_info.get("liquidity", 0)
    price_change_24h = token_info.get("price_change_24h", 0)
    
    msg = (
        f"\U0001F48E <b>Found {name} ({symbol})</b>\n"
        f"<code>{mint}</code>\n\n"
        f"<b>Price:</b> ${price:.6f}\n"
    )
    
    # Add detailed metrics if available
    if fdv > 0:
        msg += f"<b>Market Cap:</b> ${fdv:,.0f}\n"
    if volume24h > 0:
        msg += f"<b>24h Volume:</b> ${volume24h:,.0f}\n"
    if liquidity > 0:
        msg += f"<b>Liquidity:</b> ${liquidity:,.0f}\n"
    if price_change_24h != 0:
        change_emoji = "\U0001F4C8" if price_change_24h > 0 else "\U0001F4C9"
        msg += f"<b>24h Change:</b> {change_emoji} {price_change_24h:+.2f}%\n"
    
    # Add Educational Security Insights (White-labeled)
    if rugcheck_data:
        score = rugcheck_data.get("score", 0)
        # Interpret score (Lower is better in RugCheck, usually)
        # Wait, RugCheck score: 0 is good, high is bad? 
        # Actually, RugCheck usually gives a risk score where lower is better.
        # Let's assume standard risk score: < 1000. 
        # Based on user input: "score": 500 (warn). 
        # Let's use the score logic we had but refined.
        
        # Safe/Risk assessment
        is_safe = score < 400  # Arbitrary threshold based on "warn" at 500
        risk_level = "LOW" if score < 200 else "MEDIUM" if score < 500 else "HIGH"
        risk_emoji = "\U0001F7E2" if score < 200 else "\U0001F7E1" if score < 500 else "\U0001F534"
        
        msg += f"\n\n<b>\U0001F6E1\uFE0F SECURITY INSIGHTS</b>\n"
        msg += f"Risk Level: {risk_emoji} {risk_level} ({score})\n\n"
        
        # 1. Authority Analysis
        mint_auth = rugcheck_data.get("mint_authority")
        freeze_auth = rugcheck_data.get("freeze_authority")
        mutable = rugcheck_data.get("is_mutable", True)
        
        msg += "<b>👮 Authority Status:</b>\n"
        msg += f"• Mint Authority: {'✅ Disabled' if not mint_auth else '⚠️ Enabled'}\n"
        msg += f"• Freeze Authority: {'✅ Disabled' if not freeze_auth else '⚠️ Enabled'}\n"
        msg += f"• Metadata Mutable: {'⚠️ Yes' if mutable else '✅ No'}\n"
        
        # 2. Liquidity Analysis
        liq_locked = rugcheck_data.get("liquidity_locked_pct", 0)
        msg += f"\n<b>💧 Liquidity Status:</b>\n"
        msg += f"• Locked: {liq_locked:.1f}% {'✅' if liq_locked > 90 else '⚠️'}\n"
        
        # 3. Holder Analysis
        top_holders = rugcheck_data.get("top_holders_pct", 0)
        top_holder = rugcheck_data.get("top_holder_pct", 0)
        insider_count = rugcheck_data.get("insider_wallets_count", 0)
        
        msg += f"\n<b>👥 Holder Analysis:</b>\n"
        msg += f"• Top 10 Hold: {top_holders:.1f}% {'✅' if top_holders < 30 else '⚠️'}\n"
        msg += f"• Top 1 Holder: {top_holder:.1f}%\n"
        if insider_count > 0:
            msg += f"• Insider Wallets: {insider_count} ⚠️\n"
        
        # 4. Critical Warnings
        dev_sold = rugcheck_data.get("dev_sold", False)
        risks = rugcheck_data.get("risks", [])
        
        warnings = []
        if dev_sold: warnings.append("Dev/Creator has sold tokens")
        if mint_auth: warnings.append("Mint Authority enabled (Supply can increase)")
        if freeze_auth: warnings.append("Freeze Authority enabled (Wallets can be frozen)")
        if liq_locked < 80: warnings.append(f"Low Liquidity Lock ({liq_locked:.1f}%)")
        
        if warnings:
            msg += "\n<b>\u26A0\uFE0F CRITICAL WARNINGS:</b>\n"
            for warn in warnings:
                msg += f"\u2022 {warn}\n"
    
        
        # 5. Detailed Risks (from API)
        if risks:
            msg += "\n<b>\u26A0\uFE0F Potential Risks:</b>\n"
            for risk in risks:
                r_name = risk.get("name", "Unknown")
                r_desc = risk.get("description", "")
                if r_desc:
                    msg += f"\u2022 {r_name}: {r_desc}\n"
                else:
                    msg += f"\u2022 {r_name}\n"
    msg += f"\n\U0001F4B0 <b>Select Amount to Buy:</b>"
    await status_msg.edit_text(msg, reply_markup=reply_markup, parse_mode="HTML")



async def ask_buy_tp(update, context, mint, amount, portfolio_manager=None, user_manager=None):
    """Step 2: Ask for Take Profit percentage."""
    # Store mint and amount in context to avoid callback_data length limit (64 bytes)
    context.user_data["buy_mint"] = mint
    context.user_data["buy_amount"] = amount
    
    # Validate available capital
    chat_id = str(update.effective_user.id)
    
    if portfolio_manager:
        portfolio = portfolio_manager.get_portfolio(chat_id)
        
        capital = portfolio.get('capital_usd', 0)
        reserve = portfolio.get('reserve_balance', 0)
        # Check user preferences for reserve balance if not in portfolio
        if reserve == 0 and user_manager:
            prefs = user_manager.get_user_prefs(chat_id)
            reserve = prefs.get("reserve_balance", 0.0)
        
        available = capital - reserve
        amount_float = float(amount)
        
        # Check if amount exceeds available capital
        if amount_float > available:
            error_msg = (
                f"\u274C <b>Insufficient Capital</b>\n\n"
                f"<b>Amount Requested:</b> ${amount_float:,.2f}\n"
                f"<b>Available Capital:</b> ${available:,.2f}\n"
                f"<b>Total Capital:</b> ${capital:,.2f}\n"
                f"<b>Reserve:</b> ${reserve:,.2f}\n\n"
                f"You don't have enough available capital for this trade."
            )
            
            if update.callback_query:
                await update.callback_query.answer("\u274C Insufficient capital!", show_alert=True)
                await update.callback_query.message.edit_text(error_msg, parse_mode="HTML")
            else:
                await update.message.reply_html(error_msg)
            return
    
    keyboard = [
        [
            InlineKeyboardButton("25%", callback_data="buy_tp:25"),
            InlineKeyboardButton("50%", callback_data="buy_tp:50"),
            InlineKeyboardButton("100%", callback_data="buy_tp:100")
        ],
        [
        ],
        [
            InlineKeyboardButton("Custom", callback_data="buy_tp_custom"),
            InlineKeyboardButton("Skip (No TP)", callback_data="buy_tp:99999")
        ],
        [InlineKeyboardButton("âŒ Cancel", callback_data="delete_msg")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    msg = (
        f"\U0001F4B0 <b>Amount Set:</b> ${float(amount):.2f}\n\n"
        f"\U0001F3AF <b>Select Take Profit (TP)</b>\n"
        f"At what percentage gain should the bot sell?"
    )
    
    if update.callback_query:
        await update.callback_query.answer()
        try:
            await update.callback_query.edit_message_text(msg, reply_markup=reply_markup, parse_mode="HTML")
        except Exception as e:
            # If edit fails, send new message
            await update.callback_query.message.reply_html(msg, reply_markup=reply_markup)
    else:
        await update.message.reply_html(msg, reply_markup=reply_markup)

async def ask_buy_sl(update, context, mint, amount, tp):
    """Step 3: Ask for Stop Loss percentage."""
    # Store tp in context (mint and amount already stored)
    context.user_data["buy_tp"] = tp
    
    # Store tp in context (mint and amount already stored)
    context.user_data["buy_tp"] = tp
    
    keyboard = [
        [
            InlineKeyboardButton("10%", callback_data="buy_sl:10"),
            InlineKeyboardButton("20%", callback_data="buy_sl:20"),
            InlineKeyboardButton("30%", callback_data="buy_sl:30")
        ],
        [
            InlineKeyboardButton("Custom", callback_data="buy_sl_custom"),
            InlineKeyboardButton("Skip (No SL)", callback_data="buy_sl:-999")
        ],
        [InlineKeyboardButton("âŒ Cancel", callback_data="delete_msg")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    tp_display = "None" if float(tp) >= 99999 else f"{tp}%"
    msg = (
        f"\U0001F4B0 <b>Amount:</b> ${float(amount):.2f}\n"
        f"\U0001F3AF <b>TP:</b> {tp_display}\n\n"
        f"\U0001F6D1 <b>Select Stop Loss (SL)</b>\n"
        f"At what percentage loss should the bot sell?"
    )
    
    if update.callback_query:
        await update.callback_query.answer()
        try:
            await update.callback_query.edit_message_text(msg, reply_markup=reply_markup, parse_mode="HTML")
        except Exception as e:
            # If edit fails, send new message
            await update.callback_query.message.reply_html(msg, reply_markup=reply_markup)
    else:
        await update.message.reply_html(msg, reply_markup=reply_markup)

async def buy_token_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE,
                                   user_manager: UserManager, portfolio_manager: PortfolioManager):
    """Handle buy amount and TP/SL selection callbacks."""
    query = update.callback_query
    data = query.data
    chat_id = str(query.from_user.id)
    
    # Step 1: Amount Selected -> Ask TP
    if data.startswith("buy_amount:"):
        _, mint, amount_str = data.split(":")
        await ask_buy_tp(update, context, mint, amount_str, portfolio_manager, user_manager)
        
    # Step 2: TP Selected -> Ask SL
    elif data.startswith("buy_tp:"):
        tp = data.split(":")[1]
        mint = context.user_data.get("buy_mint")
        amount = context.user_data.get("buy_amount")
        if mint and amount:
            await ask_buy_sl(update, context, mint, amount, tp)

    # Step 3: SL Selected -> Execute
    elif data.startswith("buy_sl:"):
        sl = data.split(":")[1]
        mint = context.user_data.get("buy_mint")
        amount = context.user_data.get("buy_amount")
        tp = context.user_data.get("buy_tp")
        if mint and amount and tp:
            await _execute_manual_buy(update, context, user_manager, portfolio_manager, mint, float(amount), float(tp), float(sl))

    # Custom Inputs
    elif data.startswith("buy_custom:"):
        _, mint = data.split(":")
        await query.message.reply_text(
            "\U0001F4B0 <b>Enter Custom Amount</b>\n\n"
            f"Send the amount in USD to buy {mint}\n"
            "Example: <code>250</code>",
            parse_mode="HTML"
        )
        context.user_data["awaiting_buy_custom"] = mint
        await query.answer()
        
    elif data == "buy_tp_custom":
        mint = context.user_data.get("buy_mint")
        amount = context.user_data.get("buy_amount")
        await query.message.reply_text(
            "\U0001F3AF <b>Enter Custom Take Profit</b>\n\n"
            "Send the percentage (e.g., 150):",
            parse_mode="HTML"
        )
        context.user_data["awaiting_tp_custom"] = {"mint": mint, "amount": amount}
        await query.answer()
        
    elif data == "buy_sl_custom":
        mint = context.user_data.get("buy_mint")
        amount = context.user_data.get("buy_amount")
        tp = context.user_data.get("buy_tp")
        await query.message.reply_text(
            "\U0001F6D1 <b>Enter Custom Stop Loss</b>\n\n"
            "Send the percentage (e.g., 25):",
            parse_mode="HTML"
        )
        context.user_data["awaiting_sl_custom"] = {"mint": mint, "amount": amount, "tp": tp}
        await query.answer()

async def _execute_manual_buy(update, context, user_manager, portfolio_manager, mint, amount, tp=50.0, sl=None):
    """Execute the trade and confirm."""
    from alerts.price_fetcher import PriceFetcher
    
    # Convert -999 sentinel value to None (no SL)
    if sl == -999:
        sl = None
    
    # Re-fetch price to be accurate at execution time
    token_info = await PriceFetcher.get_token_info(mint)
    if not token_info:
        if update.callback_query:
            await update.callback_query.message.edit_text("âŒ Failed to fetch latest price. Try again.")
        else:
            await update.message.reply_text("âŒ Failed to fetch latest price. Try again.")
        return
        
    price = token_info.get("price", 0.0)
    symbol = token_info.get("symbol", "UNKNOWN")
    token_age_hours = token_info.get("token_age_hours")  # May be None if not available
    
    # Add position
    chat_id = str(update.effective_chat.id)
    
    success = portfolio_manager.add_manual_position(chat_id, mint, symbol, price, amount, tp, sl, token_age_hours=token_age_hours)
    
    # Format TP and SL for display
    tp_display = f"{float(tp):.0f}%" if float(tp) < 99999 else "None"
    sl_display = f"-{abs(float(sl)):.0f}%" if sl and sl != -999 else "None"
    
    msg = (
        f"\u2705 <b>Buy Successful!</b>\n\n"
        f"\U0001F48E <b>Token:</b> {symbol}\n"
        f"\U0001F4B5 <b>Amount:</b> ${amount:,.2f}\n"
        f"\U0001F4B2 <b>Entry Price:</b> ${price:.6f}\n\n"
        f"<b>Trading Parameters:</b>\n"
        f"\U0001F3AF <b>Take Profit:</b> +{tp_display}\n"
        f"\U0001F6D1 <b>Stop Loss:</b> {sl_display}\n\n"
        f"Position added to portfolio."
    )
    
    if update.callback_query:
        await update.callback_query.message.edit_text(msg, parse_mode="HTML")
    else:
        await update.message.reply_html(msg)


async def closeposition_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, 
                            user_manager: UserManager, portfolio_manager: PortfolioManager):
    """
    Close a specific position by symbol.
    Usage: /closeposition TOKEN
    """
    chat_id = str(update.effective_chat.id)
    
    if not context.args:
        await update.message.reply_text(
            "âŒ Usage: /closeposition <SYMBOL>\n"
            "Example: /closeposition SOL"
        )
        return
    
    symbol = context.args[0].upper()
    
    portfolio = portfolio_manager.get_portfolio(chat_id)
    if not portfolio or not portfolio.get("positions"):
        await update.message.reply_text("âŒ No open positions.")
        return
    
    # Find position
    position_key = None
    position = None
    for key, pos in portfolio["positions"].items():
        if pos.get("symbol", "").upper() == symbol:
            position_key = key
            position = pos
            break
    
    if not position_key:
        await update.message.reply_text(f"âŒ Position {symbol} not found.")
        return
    
    # Get current ROI
    active_tracking = await portfolio_manager.download_active_tracking()
    analytics_key = f"{position['mint']}_{position['signal_type']}"
    data = active_tracking.get(analytics_key)
    
    current_roi = 0.0
    if data:
        current_roi = float(data.get("current_roi", 0))
    else:
        # Fallback
        curr_price = await portfolio_manager.fetch_current_price_fallback(position["mint"])
        if curr_price > 0:
            current_roi = ((curr_price - position["entry_price"]) / position["entry_price"]) * 100
    
    await portfolio_manager.exit_position(
        chat_id, position_key, 
        "Manual Close \U0001F464", 
        context.application,  
        exit_roi=current_roi
    )

async def closeall_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, 
                       user_manager: UserManager, portfolio_manager: PortfolioManager):
    """
    Close ALL open positions with confirmation.
    """
    chat_id = str(update.effective_chat.id)
    
    portfolio = portfolio_manager.get_portfolio(chat_id)
    if not portfolio or not portfolio.get("positions"):
        await update.message.reply_text("âŒ No open positions.")
        return
    
    positions = portfolio["positions"]
    count = len(positions)
    
    msg = f"\u26A0\uFE0F <b>Close All Positions?</b>\n\n"
    msg += f"You have {count} open position(s):\n\n"
    
    for key, pos in positions.items():
        msg += f"\u2022 {pos.get('symbol', 'N/A')} ({pos.get('signal_type', 'N/A')})\n"
    
    msg += f"\nType <code>/confirmcloseall</code> to proceed."
    
    await update.message.reply_html(msg)

async def confirmcloseall_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, 
                              user_manager: UserManager, portfolio_manager: PortfolioManager):
    """
    Confirmation for /closeall
    """
    chat_id = str(update.effective_chat.id)
    
    portfolio = portfolio_manager.get_portfolio(chat_id)
    if not portfolio or not portfolio.get("positions"):
        await update.message.reply_text("\u274C No open positions to close.")
        return
    
    active_tracking = await portfolio_manager.download_active_tracking()
    
    closed_count = 0
    # Create list of keys to avoid runtime error during deletion
    keys_to_close = list(portfolio["positions"].keys())

    for key in keys_to_close:
        pos = portfolio["positions"].get(key)
        if not pos: continue

        analytics_key = f"{pos['mint']}_{pos['signal_type']}"
        data = active_tracking.get(analytics_key)
        
        current_roi = 0.0
        if data:
            current_roi = float(data.get("current_roi", 0))
        else:
            curr_price = await portfolio_manager.fetch_current_price_fallback(pos["mint"])
            if curr_price > 0:
                current_roi = ((curr_price - pos["entry_price"]) / pos["entry_price"]) * 100
        
        await portfolio_manager.exit_position(
            chat_id, key, 
            "Manual Close All \U0001F464", 
            context.application, 
            exit_roi=current_roi
        )
        closed_count += 1
    
    await update.message.reply_text(f"\u2705 Closed {closed_count} position(s).")


async def set_min_prob_discovery_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager):
    """Set minimum probability for Discovery alerts."""
    chat_id = str(update.effective_chat.id)
    args = context.args

    if not args:
        current_val = user_manager.get_user_prefs(chat_id).get("min_prob_discovery", 0.0)
        await update.message.reply_html(
            f"🎯 <b>Min Discovery Probability</b>\n\n"
            f"<b>Current:</b> {current_val * 100:.0f}%\n\n"
            f"<b>Usage:</b> <code>/set_min_prob_discovery [0-100]</code>\n"
            f"<i>Example: /set_min_prob_discovery 60 (for 60%+)</i>"
        )
        return

    try:
        val = float(args[0])
        if val < 0 or val > 100:
            raise ValueError
        
        # Convert 0-100 to 0.0-1.0
        prob = val / 100.0
        user_manager.update_user_prefs(chat_id, {"min_prob_discovery": prob})
        await update.message.reply_html(f"✅ Discovery alerts minimum probability set to <b>{val:.0f}%</b>")

    except ValueError:
        await update.message.reply_text("❌ Please enter a number between 0 and 100.")


async def set_min_prob_alpha_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager):
    """Set minimum probability for Alpha alerts."""
    chat_id = str(update.effective_chat.id)
    args = context.args

    if not args:
        current_val = user_manager.get_user_prefs(chat_id).get("min_prob_alpha", 0.0)
        await update.message.reply_html(
            f"🎯 <b>Min Alpha Probability</b>\n\n"
            f"<b>Current:</b> {current_val * 100:.0f}%\n\n"
            f"<b>Usage:</b> <code>/set_min_prob_alpha [0-100]</code>\n"
            f"<i>Example: /set_min_prob_alpha 75 (for 75%+)</i>"
        )
        return

    try:
        val = float(args[0])
        if val < 0 or val > 100:
            raise ValueError
        
        # Convert 0-100 to 0.0-1.0
        prob = val / 100.0
        user_manager.update_user_prefs(chat_id, {"min_prob_alpha": prob})
        await update.message.reply_html(f"✅ Alpha alerts minimum probability set to <b>{val:.0f}%</b>")

    except ValueError:
        await update.message.reply_text("❌ Please enter a number between 0 and 100.")
