#!/usr/bin/env python3
"""
alerts/menu_navigation.py - Comprehensive button-based menu system for bot

This module provides a hierarchical menu structure for users to navigate
the bot's features using buttons instead of commands, while keeping commands
functional for advanced users.

Menu Structure (REORGANIZED):
- Main Menu
  ├─ 📊 Dashboard & Trading
  │  ├─ View Portfolio
  │  ├─ View P&L
  │  ├─ Trade History
  │  ├─ Performance Stats
  │  └─ Watchlist
  ├─ 🔔 Alerts
  │  ├─ Configure Alert Grades
  │  ├─ Alpha Alerts (Subscribe/Unsubscribe)
  │  └─ View Alert Settings
  ├─ ⚙️ Settings
  │  ├─ Bot Modes (Alerts/Trading)
  │  ├─ Paper Trading Settings
  │  │  ├─ Enable/Initialize Trading
  │  │  ├─ Reset Capital
  │  │  ├─ Set Reserve Balance
  │  │  ├─ Set Min Trade Size
  │  │  └─ Set Stop Loss (SL) ⭐ NEW
  │  ├─ Alert Settings (TP Targets)
  │  │  ├─ Discovery Signal TP
  │  │  └─ Alpha Signal TP
  │  └─ View All Settings
  ├─ 🤖 ML Predictions
  │  ├─ Predict Single Token
  │  └─ Batch Prediction
  └─ ℹ️ Help
"""

import logging
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes
from alerts.user_manager import UserManager
from trade_manager import PortfolioManager
from config import ALL_GRADES

logger = logging.getLogger(__name__)

# ============================================================================
# MAIN MENU
# ============================================================================

async def show_main_menu(message, user_manager: UserManager, chat_id: str, edit=False):
    """Display the main navigation menu with reorganized sections."""
    user_prefs = user_manager.get_user_prefs(chat_id)
    modes = user_prefs.get("modes", [])
    
    # Determine subscription status
    is_subbed = user_manager.is_subscribed(chat_id)
    is_expired = user_manager.is_subscription_expired(chat_id)
    
    if is_subbed:
        sub_status = "✅ Active"
    elif is_expired:
        sub_status = "❌ Expired"
    else:
        sub_status = "❌ Inactive"
        
    # Get expiry date if available
    expires_at = user_prefs.get("expires_at") or "N/A"
    if expires_at and expires_at != "N/A" and "Z" in expires_at:
        expires_at = expires_at.replace("Z", "").replace("T", " ")[:16]

    # Determine active mode indicators
    alerts_active = "✅" if "alerts" in modes else "⭕"
    trading_active = "✅" if "papertrade" in modes else "⭕"

    keyboard = [
        [InlineKeyboardButton("📊 Dashboard & Trading", callback_data="menu_dashboard")],
        [InlineKeyboardButton(f"🔔 Notifications {alerts_active}", callback_data="menu_alerts")],
        [InlineKeyboardButton("⚙️ Settings", callback_data="menu_settings")],
        [InlineKeyboardButton("🤖 ML Predictions", callback_data="menu_ml")],
        [InlineKeyboardButton("ℹ️ Help", callback_data="menu_help")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    menu_text = (
        f"📱 <b>Main Menu</b>\n\n"
        f"<b>Subscription:</b> {sub_status}\n"
        f"<b>Expires:</b> <code>{expires_at}</code>\n\n"
        f"Welcome! Use the buttons to navigate all features.\n"
        f"Commands also work if you prefer typing.\n\n"
        f"<b>Active Modes:</b>\n"
        f"• 🔔 Notifications: {alerts_active}\n"
        f"• 📈 Trading: {trading_active}\n\n"
        f"<b>Pro Tip:</b> Type /help anytime for all commands."
    )
    
    if not is_subbed:
        menu_text += "\n\n⚠️ <i>Alerts are disabled without active subscription.</i>"
    
    if edit:
        await message.edit_text(menu_text, reply_markup=reply_markup, parse_mode="HTML")
    else:
        await message.reply_html(menu_text, reply_markup=reply_markup)


# ============================================================================
# DASHBOARD & TRADING MENU
# ============================================================================

async def show_dashboard_menu(message, user_manager: UserManager, portfolio_manager: PortfolioManager, chat_id: str, edit=False):
    """Display dashboard and trading overview menu."""
    user_prefs = user_manager.get_user_prefs(chat_id)
    is_enabled = "papertrade" in user_prefs.get("modes", [])
    
    if not is_enabled:
        keyboard = [
            [InlineKeyboardButton("▶️ Enable Paper Trading", callback_data="enable_trading")],
            [InlineKeyboardButton("◀️ Back", callback_data="menu_main")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        menu_text = (
            f"📊 <b>Dashboard & Trading</b>\n\n"
            f"<b>Status:</b> ❌ Trading Disabled\n\n"
            f"Enable paper trading to start simulating trades.\n"
            f"No real money is used - perfect for learning!"
        )
    else:
        portfolio = portfolio_manager.get_portfolio(chat_id)
        capital = portfolio.get('capital_usd', 0)
        positions = len([p for p in portfolio.get('positions', {}).values() if p.get('status') == 'active'])
        
        keyboard = [
            [InlineKeyboardButton("💼 View Portfolio", callback_data="portfolio_direct")],
            [InlineKeyboardButton("📊 View P&L", callback_data="pnl_direct")],
            [InlineKeyboardButton("📜 Trade History", callback_data="history_direct")],
            [InlineKeyboardButton("📈 Performance Stats", callback_data="performance_direct")],
            [InlineKeyboardButton("👀 Watchlist", callback_data="watchlist_direct")],
            [InlineKeyboardButton("◀️ Back", callback_data="menu_main")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        reserve = user_prefs.get("reserve_balance", 0.0)
        available = capital - reserve
        
        menu_text = (
            f"📊 <b>Dashboard & Trading</b>\n\n"
            f"<b>Status:</b> ✅ Trading Enabled\n"
            f"<b>Capital:</b> ${capital:,.2f}\n"
            f"<b>Available:</b> ${available:,.2f}\n"
            f"<b>Open Positions:</b> {positions}\n\n"
            f"<b>Quick Actions:</b>\n"
            f"View your portfolio, trades, and performance below."
        )
    
    if edit:
        await message.edit_text(menu_text, reply_markup=reply_markup, parse_mode="HTML")
    else:
        await message.reply_html(menu_text, reply_markup=reply_markup)


# ============================================================================
# ALERTS MENU
# ============================================================================

async def show_alerts_menu(message, user_manager: UserManager, chat_id: str, edit=False):
    """Display alerts configuration menu."""
    user_prefs = user_manager.get_user_prefs(chat_id)
    modes = user_prefs.get("modes", [])
    alert_grades = user_prefs.get("grades", [])
    alpha_alerts = "✅" if user_prefs.get("alpha_alerts", False) else "❌"
    
    alert_text = ", ".join(alert_grades) if alert_grades else "Not configured"
    
    keyboard = [
        [InlineKeyboardButton("🎯 Set Notification Grades", callback_data="setalerts_menu")],
        [InlineKeyboardButton(f"🔔 Notifications {'✅' if 'alerts' in modes else '⭕'}", callback_data="toggle_alerts")],
        [InlineKeyboardButton(f"🌟 Alpha Notifications {alpha_alerts}", callback_data="alpha_menu")],
        [InlineKeyboardButton("📋 View Active Filters", callback_data="myalerts_direct")],
        [InlineKeyboardButton("◀️ Back", callback_data="menu_main")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    menu_text = (
        f"🔔 <b>Notification Menu</b>\n\n"
        f"<b>Current Notification Grades:</b>\n"
        f"{alert_text}\n\n"
        f"<b>Alpha Notifications:</b> {alpha_alerts}\n\n"
        f"Configure which signals you want to receive as messages."
    )
    
    if edit:
        await message.edit_text(menu_text, reply_markup=reply_markup, parse_mode="HTML")
    else:
        await message.reply_html(menu_text, reply_markup=reply_markup)


async def show_alert_grades_menu(message, user_manager, chat_id, edit=False):
    """Display menu for selecting alert grades with current indicators."""
    user_prefs = user_manager.get_user_prefs(chat_id)
    selected_grades = user_prefs.get("grades", [])
    
    def get_btn_text(grade_name, emoji):
        return f"{'✅ ' if grade_name in selected_grades else ''}{emoji} {grade_name}"

    keyboard = [
        [
            InlineKeyboardButton(get_btn_text("CRITICAL", "🔴"), callback_data="grade_critical"),
            InlineKeyboardButton(get_btn_text("HIGH", "🟠"), callback_data="grade_high")
        ],
        [
            InlineKeyboardButton(get_btn_text("MEDIUM", "🟡"), callback_data="grade_medium"),
            InlineKeyboardButton(get_btn_text("LOW", "🟢"), callback_data="grade_low")
        ],
        [InlineKeyboardButton("🔄 Done Selecting", callback_data="grades_done")],
        [InlineKeyboardButton("◀️ Back", callback_data="menu_alerts")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    menu_text = (
        f"🎯 <b>Notification Grades</b>\n\n"
        f"Click each grade to toggle it on/off.\n"
        f"When done, click 'Done Selecting'.\n\n"
        f"<b>Grades:</b>\n"
        f"🔴 CRITICAL - Highest priority notifications\n"
        f"🟠 HIGH - Important signals\n"
        f"🟡 MEDIUM - Regular notifications\n"
        f"🟢 LOW - All signals"
    )
    
    if edit:
        await message.edit_text(menu_text, reply_markup=reply_markup, parse_mode="HTML")
    else:
        await message.reply_html(menu_text, reply_markup=reply_markup)


async def show_alpha_alerts_menu(message, user_manager: UserManager, chat_id: str, edit=False):
    """Display alpha notifications subscription menu."""
    user_prefs = user_manager.get_user_prefs(chat_id)
    is_subscribed = user_prefs.get("alpha_alerts", False)
    
    status = "✅ Subscribed" if is_subscribed else "❌ Not Subscribed"
    action_text = "Unsubscribe" if is_subscribed else "Subscribe"
    action_data = "alpha_unsubscribe_menu" if is_subscribed else "alpha_subscribe_menu"
    
    keyboard = [
        [InlineKeyboardButton(f"🌟 {action_text}", callback_data=action_data)],
        [InlineKeyboardButton("◀️ Back", callback_data="menu_alerts")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    menu_text = (
        f"🌟 <b>Alpha Notifications</b>\n\n"
        f"<b>Status:</b> {status}\n\n"
        f"Alpha Notifications are high-priority, curated token opportunities with "
        f"advanced security analysis and ML insights.\n\n"
        f"<b>Benefits:</b>\n"
        f"• 🔍 Advanced security analysis\n"
        f"• 🤖 ML win probability\n"
        f"• ⚠️ Top 5 risks highlighted\n"
        f"• 📊 Detailed market metrics"
    )
    
    if edit:
        await message.edit_text(menu_text, reply_markup=reply_markup, parse_mode="HTML")
    else:
        await message.reply_html(menu_text, reply_markup=reply_markup)


# ============================================================================
# SETTINGS MENU (Reorganized)
# ============================================================================

async def show_settings_menu(message, user_manager: UserManager, portfolio_manager: PortfolioManager, chat_id: str, edit=False):
    """Display main settings menu with organized subsections."""
    user_prefs = user_manager.get_user_prefs(chat_id)
    modes = user_prefs.get("modes", [])
    is_trading_enabled = "papertrade" in modes
    
    keyboard = [
        [InlineKeyboardButton("🔄 Bot Modes", callback_data="settings_mode")],
        [InlineKeyboardButton("📈 Paper Trading Settings", callback_data="settings_trading")],
        [InlineKeyboardButton("👤 View All Settings", callback_data="mysettings_direct")],
        [InlineKeyboardButton("◀️ Back", callback_data="menu_main")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    mode_text = " & ".join([f"✅ {m.upper()}" for m in modes]) if modes else "⭕ No modes active"
    
    menu_text = (
        f"⚙️ <b>Settings Menu</b>\n\n"
        f"<b>Current Modes:</b>\n"
        f"{mode_text}\n\n"
        f"<b>Customize Your Experience:</b>\n"
        f"• Bot modes and behavior\n"
        f"• Trading parameters\n"
        f"• Alert preferences"
    )
    
    if edit:
        await message.edit_text(menu_text, reply_markup=reply_markup, parse_mode="HTML")
    else:
        await message.reply_html(menu_text, reply_markup=reply_markup)


async def show_mode_selection_menu(message, edit=False):
    """Display mode selection menu."""
    keyboard = [
        [InlineKeyboardButton("🔔 Alerts Only", callback_data="mode_alerts_set")],
        [InlineKeyboardButton("📈 Trading Only", callback_data="mode_papertrade_set")],
        [InlineKeyboardButton("🚀 Both Modes", callback_data="mode_both_set")],
        [InlineKeyboardButton("◀️ Back", callback_data="menu_settings")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    menu_text = (
        f"🔄 <b>Select Bot Modes</b>\n\n"
        f"<b>🔔 Alerts Only</b>\n"
        f"Receive token alerts with analysis.\n\n"
        f"<b>📈 Trading Only</b>\n"
        f"Paper trade without alerts.\n\n"
        f"<b>🚀 Both Modes</b>\n"
        f"Get alerts AND paper trade them.\n\n"
        f"<b>Tip:</b> You can change this anytime."
    )
    
    if edit:
        await message.edit_text(menu_text, reply_markup=reply_markup, parse_mode="HTML")
    else:
        await message.reply_html(menu_text, reply_markup=reply_markup)


# ============================================================================
# PAPER TRADING SETTINGS SUBMENU
# ============================================================================

async def show_trading_settings_menu(message, user_manager: UserManager, portfolio_manager: PortfolioManager, chat_id: str, edit=False):
    """Display paper trading settings submenu with SL settings."""
    user_prefs = user_manager.get_user_prefs(chat_id)
    is_enabled = "papertrade" in user_prefs.get("modes", [])
    
    if not is_enabled:
        keyboard = [
            [InlineKeyboardButton("▶️ Enable Paper Trading", callback_data="enable_trading")],
            [InlineKeyboardButton("◀️ Back", callback_data="menu_settings")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        menu_text = (
            f"📈 <b>Paper Trading Settings</b>\n\n"
            f"<b>Status:</b> ❌ Disabled\n\n"
            f"Enable paper trading to configure trading parameters.\n"
            f"Select an initial capital amount to get started."
        )
    else:
        portfolio = portfolio_manager.get_portfolio(chat_id)
        capital = portfolio.get('capital_usd', 0)
        reserve = user_prefs.get("reserve_balance", 0.0)
        min_trade = user_prefs.get("min_trade_size", 10.0)
        available = capital - reserve
        
        # Get default SL value if exists
        default_sl = user_prefs.get("default_sl", None)
        sl_display = f"{abs(default_sl):.0f}%" if default_sl else "None (User Choice)"
        
        # Get trade size mode if exists
        trade_size_mode = user_prefs.get("trade_size_mode", "percent")
        trade_size_value = user_prefs.get("trade_size_value", 10)
        
        if trade_size_mode == "percent":
            trade_size_display = f"📊 {trade_size_value}% of portfolio"
        else:
            trade_size_display = f"💵 ${trade_size_value:,.2f} per trade"
        
        # Get TP preference
        tp_pref = user_prefs.get("tp_preference", "median")
        tp_display = f"🎯 {tp_pref.capitalize()}" if tp_pref in ["median", "mean", "mode", "smart"] else f"🎯 {tp_pref}%"
        
        keyboard = [
            [InlineKeyboardButton("💰 Reset Capital", callback_data="resetcapital_menu")],
            [InlineKeyboardButton("💵 Reserve Balance", callback_data="set_reserve_menu")],
            [InlineKeyboardButton("📏 Min Trade Size", callback_data="set_mintrade_menu")],
            [InlineKeyboardButton("📊 Trade Size", callback_data="settings_trade_size_menu")],
            [InlineKeyboardButton("🚜 Auto-Trade Filters", callback_data="settings_trade_filters")],
            [InlineKeyboardButton("🎯 Take Profit (TP)", callback_data="settings_tp")],
            [InlineKeyboardButton("🛑 Stop Loss (SL)", callback_data="settings_sl_menu")],
            [InlineKeyboardButton("◀️ Back", callback_data="menu_settings")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        menu_text = (
            f"📈 <b>Paper Trading Settings</b>\n\n"
            f"<b>Status:</b> ✅ Enabled\n\n"
            f"<b>Capital Overview:</b>\n"
            f"• Total Capital: ${capital:,.2f}\n"
            f"• Reserve: ${reserve:,.2f}\n"
            f"• Available: ${available:,.2f}\n"
            f"• Min Trade: ${min_trade:,.2f}\n"
            f"• Trade Size Mode: {trade_size_display}\n"
            f"• Take Profit: {tp_display}\n"
            f"• Stop Loss: {sl_display}\n\n"
            f"<b>Adjust settings below:</b>"
        )
    
    if edit:
        await message.edit_text(menu_text, reply_markup=reply_markup, parse_mode="HTML")
    else:
        await message.reply_html(menu_text, reply_markup=reply_markup)


# ============================================================================
# STOP LOSS SETTINGS MENU (NEW)
# ============================================================================

async def show_sl_settings_menu(message, user_manager: UserManager, chat_id: str, edit=False):
    """Display stop loss settings menu."""
    user_prefs = user_manager.get_user_prefs(chat_id)
    current_sl = user_prefs.get("default_sl", None)
    sl_display = f"{abs(current_sl):.0f}%" if current_sl else "None (User Choice)"
    
    keyboard = [
        [
            InlineKeyboardButton("No SL (Manual)", callback_data="set_default_sl:none"),
            InlineKeyboardButton("10%", callback_data="set_default_sl:10")
        ],
        [
            InlineKeyboardButton("20%", callback_data="set_default_sl:20"),
            InlineKeyboardButton("30%", callback_data="set_default_sl:30")
        ],
        [
            InlineKeyboardButton("50%", callback_data="set_default_sl:50"),
            InlineKeyboardButton("Custom", callback_data="set_default_sl_custom")
        ],
        [InlineKeyboardButton("◀️ Back", callback_data="settings_trading")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    menu_text = (
        f"🛑 <b>Stop Loss (SL) Settings</b>\n\n"
        f"<b>Current Setting:</b> {sl_display}\n\n"
        f"<b>What is Stop Loss?</b>\n"
        f"Automatically exits trades if they drop below this percentage.\n\n"
        f"<b>Examples:</b>\n"
        f"• 20% SL = Exit if trade drops -20%\n"
        f"• No SL = Never auto-exit (manual only)\n\n"
        f"<b>Tip:</b> You can still manually set SL on individual trades."
    )
    
    if edit:
        await message.edit_text(menu_text, reply_markup=reply_markup, parse_mode="HTML")
    else:
        await message.reply_html(menu_text, reply_markup=reply_markup)


# ============================================================================
# TRADE SIZE MODE MENU (NEW)
# ============================================================================

async def show_trade_size_mode_menu(message, user_manager: UserManager, chat_id: str, edit=False):
    """Display trade size mode and custom value selection menu."""
    user_prefs = user_manager.get_user_prefs(chat_id)
    current_mode = user_prefs.get("trade_size_mode", "percent")
    current_value = user_prefs.get("trade_size_value", 10)
    
    keyboard = [
        [InlineKeyboardButton("📊 Percentage-Based", callback_data="set_trade_size_mode_select:percent")],
        [InlineKeyboardButton("💵 Fixed Amount", callback_data="set_trade_size_mode_select:fixed")],
        [InlineKeyboardButton("◀️ Back", callback_data="settings_trading")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if current_mode == "percent":
        current_display = f"📊 Percentage-Based ({current_value}% of portfolio)"
    else:
        current_display = f"💵 Fixed Amount (${current_value} per trade)"
    
    menu_text = (
        f"📊 <b>Trade Size Settings</b>\n\n"
        f"<b>Current Setting:</b> {current_display}\n\n"
        f"<b>Choose Mode:</b>\n\n"
        f"📊 <b>Percentage-Based</b>\n"
        f"• Example: 50 (means 50% of portfolio)\n"
        f"• Scales with your capital\n\n"
        f"💵 <b>Fixed Amount</b>\n"
        f"• Example: 50 (means $50 per trade)\n"
        f"• Constant size regardless of capital\n\n"
        f"After selecting a mode, you will be prompted to enter your custom value."
    )
    
    if edit:
        await message.edit_text(menu_text, reply_markup=reply_markup, parse_mode="HTML")
    else:
        await message.reply_html(menu_text, reply_markup=reply_markup)


# ============================================================================
# ALERT SETTINGS SUBMENU
# ============================================================================

async def show_alert_settings_menu(message, edit=False):
    """Display alert settings submenu (Take Profit settings)."""
    keyboard = [
        [InlineKeyboardButton("🎯 Global TP", callback_data="tp_global_menu")],
        [InlineKeyboardButton("🔍 Discovery TP", callback_data="tp_discovery_menu")],
        [InlineKeyboardButton("⭐ Alpha TP", callback_data="tp_alpha_menu")],
        [InlineKeyboardButton("👀 View Current TP", callback_data="tp_view")],
        [InlineKeyboardButton("◀️ Back", callback_data="menu_settings")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    menu_text = (
        f"📢 <b>Alert Settings</b>\n\n"
        f"Configure Take Profit (TP) targets:\n\n"
        f"🎯 <b>Global TP:</b> Applies to all trades (including manual and paper trading) unless overridden.\n\n"
        f"<b>Signal Overrides:</b> (Subscriber Only)\n"
        f"• 🔍 Discovery - Custom TP for discovery alerts\n"
        f"• ⭐ Alpha - Custom TP for alpha alerts\n\n"
        f"Options: median, mean, mode, <b>smart</b>, or a custom number."
    )
    
    if edit:
        await message.edit_text(menu_text, reply_markup=reply_markup, parse_mode="HTML")
    else:
        await message.reply_html(menu_text, reply_markup=reply_markup)


# ============================================================================
# PAPER TRADING MENU (Legacy Support)
# ============================================================================

async def show_trading_menu(message, user_manager: UserManager, portfolio_manager: PortfolioManager, chat_id: str, edit=False):
    """Display paper trading menu - redirects to new settings."""
    # Redirect to new trading settings menu for backward compatibility
    await show_trading_settings_menu(message, user_manager, portfolio_manager, chat_id, edit)


async def show_enable_trading_menu(message, edit=False):
    """Display menu for enabling paper trading."""
    keyboard = [
        [
            InlineKeyboardButton("💵 $100", callback_data="init_capital:100"),
            InlineKeyboardButton("💵 $500", callback_data="init_capital:500")
        ],
        [
            InlineKeyboardButton("💵 $1000", callback_data="init_capital:1000"),
            InlineKeyboardButton("💵 $5000", callback_data="init_capital:5000")
        ],
        [InlineKeyboardButton("💵 Custom Amount", callback_data="custom_capital")],
        [InlineKeyboardButton("◀️ Back", callback_data="menu_settings")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    menu_text = (
        f"▶️ <b>Enable Paper Trading</b>\n\n"
        f"Select an initial capital amount:\n\n"
        f"This is your simulated trading budget.\n"
        f"No real money is used."
    )
    
    if edit:
        await message.edit_text(menu_text, reply_markup=reply_markup, parse_mode="HTML")
    else:
        await message.reply_html(menu_text, reply_markup=reply_markup)


async def show_reset_capital_menu(message, edit=False):
    """Display menu for resetting capital."""
    keyboard = [
        [
            InlineKeyboardButton("💵 $100", callback_data="reset_capital:100"),
            InlineKeyboardButton("💵 $500", callback_data="reset_capital:500")
        ],
        [
            InlineKeyboardButton("💵 $1000", callback_data="reset_capital:1000"),
            InlineKeyboardButton("💵 $5000", callback_data="reset_capital:5000")
        ],
        [InlineKeyboardButton("💵 Custom Amount", callback_data="reset_capital_custom")],
        [InlineKeyboardButton("◀️ Back", callback_data="settings_trading")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    menu_text = (
        f"💰 <b>Reset Capital</b>\n\n"
        f"Select a new capital amount:\n\n"
        f"⚠️ This will reset your trading account.\n"
        f"All positions will be closed."
    )
    
    if edit:
        await message.edit_text(menu_text, reply_markup=reply_markup, parse_mode="HTML")
    else:
        await message.reply_html(menu_text, reply_markup=reply_markup)


# ============================================================================
# ML PREDICTIONS MENU
# ============================================================================

async def show_ml_menu(message, edit=False):
    """Display ML predictions menu."""
    keyboard = [
        [InlineKeyboardButton("🎯 Single Token", callback_data="predict_single")],
        [InlineKeyboardButton("📊 Batch Prediction", callback_data="predict_batch_menu")],
        [InlineKeyboardButton("◀️ Back", callback_data="menu_main")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    menu_text = (
        f"🤖 <b>ML Predictions Menu</b>\n\n"
        f"Get AI-powered win probability for tokens.\n\n"
        f"<b>What is ML Prediction?</b>\n"
        f"Analyze tokens using machine learning models to predict "
        f"win probability based on security, market, and behavioral metrics."
    )
    
    if edit:
        await message.edit_text(menu_text, reply_markup=reply_markup, parse_mode="HTML")
    else:
        await message.reply_html(menu_text, reply_markup=reply_markup)


# ============================================================================
# CAPITAL MANAGEMENT MENUS
# ============================================================================

async def show_reserve_balance_menu(message, edit=False):
    """Display menu for setting reserve balance."""
    keyboard = [
        [
            InlineKeyboardButton("$0", callback_data="set_reserve:0"),
            InlineKeyboardButton("$50", callback_data="set_reserve:50")
        ],
        [
            InlineKeyboardButton("$100", callback_data="set_reserve:100"),
            InlineKeyboardButton("$200", callback_data="set_reserve:200")
        ],
        [InlineKeyboardButton("💵 Custom Amount", callback_data="set_reserve_custom")],
        [InlineKeyboardButton("◀️ Back", callback_data="settings_trading")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    menu_text = (
        f"💵 <b>Set Reserve Balance</b>\n\n"
        f"Reserve balance is the minimum capital that the bot will NOT use for trading.\n\n"
        f"<b>Current:</b> Check /portfolio\n\n"
        f"Select a preset or enter custom amount:"
    )
    
    if edit:
        await message.edit_text(menu_text, reply_markup=reply_markup, parse_mode="HTML")
    else:
        await message.reply_html(menu_text, reply_markup=reply_markup)


async def show_min_trade_size_menu(message, edit=False):
    """Display menu for setting minimum trade size."""
    keyboard = [
        [
            InlineKeyboardButton("$10", callback_data="set_mintrade:10"),
            InlineKeyboardButton("$20", callback_data="set_mintrade:20")
        ],
        [
            InlineKeyboardButton("$50", callback_data="set_mintrade:50"),
            InlineKeyboardButton("$100", callback_data="set_mintrade:100")
        ],
        [InlineKeyboardButton("💵 Custom Amount", callback_data="set_mintrade_custom")],
        [InlineKeyboardButton("◀️ Back", callback_data="settings_trading")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    menu_text = (
        f"📏 <b>Set Minimum Trade Size</b>\n\n"
        f"Minimum USD amount per trade. Bot will skip trades smaller than this.\n\n"
        f"<b>Current:</b> Check settings\n\n"
        f"Select a preset or enter custom amount:"
    )
    
    if edit:
        await message.edit_text(menu_text, reply_markup=reply_markup, parse_mode="HTML")
    else:
        await message.reply_html(menu_text, reply_markup=reply_markup)


# ============================================================================
# HELP MENU
# ============================================================================

async def show_help_menu(message, edit=False):
    """Display help menu."""
    keyboard = [
        [InlineKeyboardButton("📖 Getting Started", callback_data="help_getting_started")],
        [InlineKeyboardButton("🔔 About Alerts", callback_data="help_alerts")],
        [InlineKeyboardButton("📈 About Trading", callback_data="help_trading")],
        [InlineKeyboardButton("🤖 About ML", callback_data="help_ml")],
        [InlineKeyboardButton("◀️ Back", callback_data="menu_main")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    menu_text = (
        f"ℹ️ <b>Help Menu</b>\n\n"
        f"Learn how to use the bot effectively."
    )
    
    if edit:
        await message.edit_text(menu_text, reply_markup=reply_markup, parse_mode="HTML")
    else:
        await message.reply_html(menu_text, reply_markup=reply_markup)


async def show_help_topic(message, topic: str):
    """Display help for a specific topic."""
    help_texts = {
        "getting_started": (
            f"🚀 <b>Getting Started</b>\n\n"
            f"<b>Step 1: Choose Your Mode</b>\n"
            f"Use Settings menu → Bot Modes\n"
            f"• 🔔 Alerts - Receive token notifications\n"
            f"• 📈 Trading - Paper trade simulation\n"
            f"• 🚀 Both - Get alerts and trade them\n\n"
            f"<b>Step 2: Configure Alerts</b>\n"
            f"Go to Alerts menu → Set Alert Grades\n"
            f"Choose which priority levels you want.\n\n"
            f"<b>Step 3: Configure Trading</b>\n"
            f"Go to Settings → Paper Trading Settings\n"
            f"Set capital, reserve, min trade size, and SL.\n\n"
            f"<b>Step 4: Start!</b>\n"
            f"View Dashboard & Trading for live portfolio.\n\n"
            f"<b>Quick Tips:</b>\n"
            f"• Use /help anytime for command list\n"
            f"• Click ◀️ Back to go to previous menu\n"
            f"• Commands work alongside buttons"
        ),
        "alerts": (
            f"🔔 <b>About Alerts</b>\n\n"
            f"<b>What are Alerts?</b>\n"
            f"Notifications when new tokens match your criteria.\n\n"
            f"<b>Alert Grades:</b>\n"
            f"🔴 CRITICAL - Top priority tokens\n"
            f"🟠 HIGH - Important opportunities\n"
            f"🟡 MEDIUM - Regular tokens\n"
            f"🟢 LOW - All tokens\n\n"
            f"<b>Alpha Alerts:</b>\n"
            f"Premium alerts with advanced analysis:\n"
            f"• 🔍 Security deep-dive\n"
            f"• 🤖 ML win probability\n"
            f"• ⚠️ Risk assessment\n"
            f"• 📊 Market metrics"
        ),
        "trading": (
            f"📈 <b>About Paper Trading</b>\n\n"
            f"<b>What is Paper Trading?</b>\n"
            f"Simulate real trades with fake money.\n"
            f"Perfect for learning without risk!\n\n"
            f"<b>How It Works:</b>\n"
            f"1. Set initial capital ($100-$5000+)\n"
            f"2. Get token alerts\n"
            f"3. Auto-trade or manual entries\n"
            f"4. Track P&L and performance\n\n"
            f"<b>Key Settings:</b>\n"
            f"• Reserve - Minimum capital to keep aside\n"
            f"• Min Trade - Minimum trade size\n"
            f"• Stop Loss - Auto-exit on loss %\n\n"
            f"<b>Key Metrics:</b>\n"
            f"• Portfolio Value - Total capital\n"
            f"• Unrealized P&L - Current profit/loss\n"
            f"• Win Rate - % of winning trades\n"
            f"• Max ROI - Best single trade"
        ),
        "ml": (
            f"🤖 <b>About ML Predictions</b>\n\n"
            f"<b>What is ML Prediction?</b>\n"
            f"AI models analyze tokens to predict win probability.\n\n"
            f"<b>How It Works:</b>\n"
            f"Analyzes:\n"
            f"• 🛡️ Security metrics\n"
            f"• 📊 Market data\n"
            f"• 👥 Holder distribution\n"
            f"• 📈 Volume &amp; price trends\n\n"
            f"<b>Win Probability Tiers:</b>\n"
            f"🟢 70%+ - Strong buy signal\n"
            f"🟡 50-70% - Moderate opportunity\n"
            f"🔴 &lt;50% - Wait for better signal"
        )
    }
    
    text = help_texts.get(topic, "Help topic not found.")
    
    keyboard = [
        [InlineKeyboardButton("◀️ Back", callback_data="menu_help")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await message.edit_text(text, parse_mode="HTML", reply_markup=reply_markup)


# ============================================================================
# AUTO-TRADE FILTER SUBMENUS
# ============================================================================

async def show_trade_filters_menu(message, edit=False):
    """Display submenu for choosing auto-trade filter type (Grades or Alpha)."""
    keyboard = [
        [InlineKeyboardButton("🔍 Discovery Grades", callback_data="set_trade_grades_menu")],
        [InlineKeyboardButton("⭐ Alpha Auto-Trade", callback_data="trade_alpha_menu")],
        [InlineKeyboardButton("◀️ Back", callback_data="settings_trading")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    menu_text = (
        "🚜 <b>Auto-Trade Filters</b>\n\n"
        "Control which signals the bot automatically trades.\n\n"
        "• <b>Discovery Grades:</b> Choose which signal qualities to trade.\n"
        "• <b>Alpha Auto-Trade:</b> Toggle trading for Alpha signals."
    )
    
    if edit:
        await message.edit_text(menu_text, reply_markup=reply_markup, parse_mode="HTML")
    else:
        await message.reply_html(menu_text, reply_markup=reply_markup)


async def show_trade_grades_menu(message, user_manager: UserManager, chat_id: str, edit=False):
    """Display menu for toggling trade-specific discovery grades."""
    user_prefs = user_manager.get_user_prefs(chat_id)
    trade_grades = user_prefs.get("trade_grades", ALL_GRADES)
    
    keyboard = []
    for grade in ALL_GRADES:
        status = "✅" if grade in trade_grades else "❌"
        keyboard.append([InlineKeyboardButton(f"{status} {grade}", callback_data=f"trade_grade_{grade}")])
        
    keyboard.append([InlineKeyboardButton("◀️ Back", callback_data="settings_trade_filters")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    menu_text = (
        "🔍 <b>Auto-Trade Grades</b>\n\n"
        "Select which discovery signal grades will trigger a trade.\n"
        "Trades will only be opened for the grades marked with ✅."
    )
    
    if edit:
        await message.edit_text(menu_text, reply_markup=reply_markup, parse_mode="HTML")
    else:
        await message.reply_html(menu_text, reply_markup=reply_markup)


async def show_trade_alpha_menu(message, user_manager: UserManager, chat_id: str, edit=False):
    """Display menu for toggling trade-specific alpha alerts."""
    user_prefs = user_manager.get_user_prefs(chat_id)
    trade_alpha = user_prefs.get("trade_alpha_alerts", False)
    
    status = "✅ ENABLED" if trade_alpha else "❌ DISABLED"
    toggle_text = "❌ Disable Alpha Trading" if trade_alpha else "✅ Enable Alpha Trading"
    
    keyboard = [
        [InlineKeyboardButton(toggle_text, callback_data="trade_alpha_toggle")],
        [InlineKeyboardButton("◀️ Back", callback_data="settings_trade_filters")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    menu_text = (
        "⭐ <b>Alpha Auto-Trade</b>\n\n"
        f"<b>Current Status:</b> {status}\n\n"
        "If enabled, the bot will automatically trade curated Alpha signals."
    )
    
    if edit:
        await message.edit_text(menu_text, reply_markup=reply_markup, parse_mode="HTML")
    else:
        await message.reply_html(menu_text, reply_markup=reply_markup)

