#!/usr/bin/env python3
"""
alerts/commands.py - User-facing bot commands with enhanced portfolio management
"""

import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from datetime import datetime

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
    """Enable paper trading mode and configure capital."""
    chat_id = str(update.effective_chat.id)
    if not user_manager.is_subscribed(chat_id):
        await update.message.reply_text("⛔ You must be a subscribed user to enable paper trading.")
        return
    
    capital = 1000.0  # Default capital
    if context.args:
        try:
            capital = float(context.args[0])
            if capital <= 0:
                await update.message.reply_text("❌ Please provide a positive number for capital.")
                return
            if capital < 100:
                await update.message.reply_text("❌ Minimum capital is $100 USD.")
                return
            if capital > 1000000:
                await update.message.reply_text("❌ Maximum capital is $1,000,000 USD.")
                return
        except ValueError:
            await update.message.reply_text("❌ Invalid capital amount. Please use a number.")
            return
            
    user_manager.enable_papertrade_mode(chat_id)
    portfolio_manager.set_capital(chat_id, capital)
    
    await update.message.reply_html(
        f"📈 <b>Paper Trading Enabled!</b>\n\n"
        f"Your virtual portfolio has been set up with <b>${capital:,.2f} USD</b>.\n\n"
        f"<b>Strategy Overview:</b>\n"
        f"• Position Size: 8-12% per trade (max $150)\n"
        f"• Partial Profits: 40% @ +40%, 30% @ +80%, 20% @ +150%\n"
        f"• Trailing Stop: Dynamic 15-25% from peak\n"
        f"• Liquidity Protection: Exit on 40% drain\n"
        f"• Max Hold: 4 hours\n\n"
        f"<b>Commands:</b>\n"
        f"• /portfolio - View your positions\n"
        f"• /pnl - Check unrealized P/L\n"
        f"• /performance - See detailed stats\n\n"
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
    
    # We need current prices - this is a simplified version
    # In production, you'd fetch live prices here
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
    """View trade history with optional limit."""
    chat_id = str(update.effective_chat.id)
    prefs = user_manager.get_user_prefs(chat_id)
    
    if "papertrade" not in prefs.get("modes", []):
        await update.message.reply_html("❌ Paper trading is not enabled.")
        return
    
    limit = 10  # Default limit
    if context.args:
        try:
            limit = int(context.args[0])
            limit = max(1, min(limit, 50))  # Between 1 and 50
        except ValueError:
            pass
    
    portfolio = portfolio_manager.get_portfolio(chat_id)
    history = portfolio.get('trade_history', [])
    
    if not history:
        await update.message.reply_html("📜 No trade history yet. Start trading to see your results here!")
        return
    
    # Get most recent trades
    recent_trades = history[-limit:]
    recent_trades.reverse()  # Most recent first
    
    msg = f"📜 <b>Trade History (Last {len(recent_trades)})</b>\n\n"
    
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
        f"<b>Overall:</b>\n"
        f"Total P/L: <b>${total_pnl:,.2f}</b>\n"
        f"Win Rate: <b>{(stats.get('wins', 0) / max(stats.get('total_trades', 1), 1) * 100):.1f}%</b>\n"
        f"Best Trade: <b>+{stats.get('best_trade', 0):.1f}%</b>\n"
        f"Worst Trade: <b>{stats.get('worst_trade', 0):.1f}%</b>"
    )
    
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
    """Reset trading capital (closes all positions)."""
    chat_id = str(update.effective_chat.id)
    prefs = user_manager.get_user_prefs(chat_id)
    
    if "papertrade" not in prefs.get("modes", []):
        await update.message.reply_html("❌ Paper trading is not enabled.")
        return
    
    if not context.args:
        await update.message.reply_html(
            "⚠️ <b>Reset Capital</b>\n\n"
            "This will close all open positions and reset your portfolio.\n\n"
            "Usage: <code>/resetcapital [amount]</code>\n"
            "Example: <code>/resetcapital 2000</code>"
        )
        return
    
    try:
        capital = float(context.args[0])
        if capital < 100 or capital > 1000000:
            await update.message.reply_html("❌ Capital must be between $100 and $1,000,000.")
            return
    except ValueError:
        await update.message.reply_html("❌ Invalid amount. Please provide a number.")
        return
    
    portfolio = portfolio_manager.get_portfolio(chat_id)
    old_positions = len(portfolio.get('positions', {}))
    
    # Clear everything except history
    portfolio['positions'] = {}
    portfolio['watchlist'] = {}
    portfolio['reentry_candidates'] = {}
    portfolio['blacklist'] = {}
    portfolio['capital_usd'] = capital
    
    portfolio_manager.save()
    
    await update.message.reply_html(
        f"🔄 <b>Portfolio Reset</b>\n\n"
        f"• Closed {old_positions} position(s)\n"
        f"• New capital: <b>${capital:,.2f}</b>\n"
        f"• Trade history preserved\n\n"
        f"<i>Ready to start fresh! 🚀</i>"
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