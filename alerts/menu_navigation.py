#!/usr/bin/env python3
"""
alerts/menu_navigation.py - Comprehensive button-based menu system for bot

This module provides a hierarchical menu structure for users to navigate
the bot's features using buttons instead of commands, while keeping commands
functional for advanced users.

Menu Structure:
- Main Menu
  ├─ 🔔 Alerts
  │  ├─ Configure Alert Grades
  │  ├─ View Alert Settings
  │  └─ Alpha Alerts (Subscribe/Unsubscribe)
  ├─ 📈 Paper Trading
  │  ├─ Enable Trading
  │  ├─ View Portfolio
  │  ├─ View P&L
  │  ├─ View History
  │  ├─ Performance Stats
  │  ├─ Watchlist
  │  └─ Reset Capital
  ├─ 🤖 ML Predictions
  │  ├─ Predict Single Token
  │  └─ Predict Batch
  ├─ ⚙️ Settings
  │  ├─ Mode Selection
  │  ├─ Take Profit Settings
  │  └─ View Current Settings
  └─ ℹ️ Help & Info
"""

import logging
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes
from alerts.user_manager import UserManager
from trade_manager import PortfolioManager

logger = logging.getLogger(__name__)

# ============================================================================
# MAIN MENU
# ============================================================================

async def show_main_menu(message, user_manager: UserManager, chat_id: str):
    """Display the main navigation menu."""
    user_prefs = user_manager.get_user_prefs(chat_id)
    modes = user_prefs.get("modes", [])
    
    # Determine active mode indicators
    alerts_active = "✅" if "alerts" in modes else "⭕"
    trading_active = "✅" if "papertrade" in modes else "⭕"
    
    keyboard = [
        [InlineKeyboardButton(f"🔔 Alerts {alerts_active}", callback_data="menu_alerts")],
        [InlineKeyboardButton(f"📈 Paper Trading {trading_active}", callback_data="menu_trading")],
        [InlineKeyboardButton("🤖 ML Predictions", callback_data="menu_ml")],
        [InlineKeyboardButton("⚙️ Settings", callback_data="menu_settings")],
        [InlineKeyboardButton("ℹ️ Help", callback_data="menu_help")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    menu_text = (
        f"📱 <b>Main Menu</b>\n\n"
        f"Welcome! Use the buttons below to navigate.\n"
        f"You can also use text commands if you prefer.\n\n"
        f"<b>Active Modes:</b>\n"
        f"• 🔔 Alerts: {alerts_active}\n"
        f"• 📈 Trading: {trading_active}\n\n"
        f"<b>Tip:</b> Type /help to see all available commands anytime."
    )
    
    await message.reply_html(menu_text, reply_markup=reply_markup)


# ============================================================================
# ALERTS MENU
# ============================================================================

async def show_alerts_menu(message, user_manager: UserManager, chat_id: str):
    """Display alerts configuration menu."""
    user_prefs = user_manager.get_user_prefs(chat_id)
    alert_grades = user_prefs.get("grades", [])
    alpha_alerts = "✅" if user_prefs.get("alpha_alerts", False) else "❌"
    
    alert_text = ", ".join(alert_grades) if alert_grades else "Not configured"
    
    keyboard = [
        [InlineKeyboardButton("🎯 Set Alert Grades", callback_data="setalerts_menu")],
        [InlineKeyboardButton("📋 View Current Settings", callback_data="myalerts_direct")],
        [InlineKeyboardButton(f"🌟 Alpha Alerts {alpha_alerts}", callback_data="alpha_menu")],
        [InlineKeyboardButton("◀️ Back", callback_data="menu_main")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    menu_text = (
        f"🔔 <b>Alerts Menu</b>\n\n"
        f"<b>Current Alert Grades:</b>\n"
        f"{alert_text}\n\n"
        f"<b>Alpha Alerts:</b> {alpha_alerts}\n\n"
        f"Configure which token grades you want to receive alerts for."
    )
    
    await message.reply_html(menu_text, reply_markup=reply_markup)


async def show_alert_grades_menu(message):
    """Display menu for selecting alert grades."""
    keyboard = [
        [
            InlineKeyboardButton("🔴 CRITICAL", callback_data="grade_critical"),
            InlineKeyboardButton("🟠 HIGH", callback_data="grade_high")
        ],
        [
            InlineKeyboardButton("🟡 MEDIUM", callback_data="grade_medium"),
            InlineKeyboardButton("🟢 LOW", callback_data="grade_low")
        ],
        [InlineKeyboardButton("🔄 Done Selecting", callback_data="grades_done")],
        [InlineKeyboardButton("◀️ Back", callback_data="menu_alerts")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    menu_text = (
        f"🎯 <b>Select Alert Grades</b>\n\n"
        f"Click each grade to toggle it on/off.\n"
        f"When done, click 'Done Selecting'.\n\n"
        f"<b>Grades:</b>\n"
        f"🔴 CRITICAL - Highest priority alerts\n"
        f"🟠 HIGH - Important tokens\n"
        f"🟡 MEDIUM - Regular alerts\n"
        f"🟢 LOW - All tokens"
    )
    
    await message.reply_html(menu_text, reply_markup=reply_markup)


async def show_alpha_alerts_menu(message, user_manager: UserManager, chat_id: str):
    """Display alpha alerts subscription menu."""
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
        f"🌟 <b>Alpha Alerts</b>\n\n"
        f"<b>Status:</b> {status}\n\n"
        f"Alpha Alerts are high-priority, curated token opportunities with "
        f"advanced security analysis and ML insights.\n\n"
        f"<b>Benefits:</b>\n"
        f"• 🔍 Advanced security analysis\n"
        f"• 🤖 ML win probability\n"
        f"• ⚠️ Top 5 risks highlighted\n"
        f"• 📊 Detailed market metrics"
    )
    
    await message.reply_html(menu_text, reply_markup=reply_markup)


# ============================================================================
# PAPER TRADING MENU
# ============================================================================

async def show_trading_menu(message, user_manager: UserManager, portfolio_manager: PortfolioManager, chat_id: str):
    """Display paper trading menu."""
    user_prefs = user_manager.get_user_prefs(chat_id)
    is_enabled = "papertrade" in user_prefs.get("modes", [])
    
    if not is_enabled:
        keyboard = [
            [InlineKeyboardButton("▶️ Enable Paper Trading", callback_data="enable_trading")],
            [InlineKeyboardButton("◀️ Back", callback_data="menu_main")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        menu_text = (
            f"📈 <b>Paper Trading Menu</b>\n\n"
            f"<b>Status:</b> ❌ Disabled\n\n"
            f"Enable paper trading to start simulating token trades.\n"
            f"No real money is used."
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
            [InlineKeyboardButton("💰 Reset Capital", callback_data="resetcapital_menu")],
            [InlineKeyboardButton("◀️ Back", callback_data="menu_main")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        menu_text = (
            f"📈 <b>Paper Trading Menu</b>\n\n"
            f"<b>Status:</b> ✅ Enabled\n"
            f"<b>Capital:</b> ${capital:,.2f}\n"
            f"<b>Open Positions:</b> {positions}\n\n"
            f"Manage your paper trading portfolio below."
        )
    
    await message.reply_html(menu_text, reply_markup=reply_markup)


async def show_enable_trading_menu(message):
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
        [InlineKeyboardButton("◀️ Back", callback_data="menu_trading")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    menu_text = (
        f"▶️ <b>Enable Paper Trading</b>\n\n"
        f"Select an initial capital amount:\n\n"
        f"This is your simulated trading budget.\n"
        f"No real money is used."
    )
    
    await message.reply_html(menu_text, reply_markup=reply_markup)


async def show_reset_capital_menu(message):
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
        [InlineKeyboardButton("◀️ Back", callback_data="menu_trading")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    menu_text = (
        f"💰 <b>Reset Capital</b>\n\n"
        f"Select a new capital amount:\n\n"
        f"⚠️ This will reset your trading account.\n"
        f"All positions will be closed."
    )
    
    await message.reply_html(menu_text, reply_markup=reply_markup)


# ============================================================================
# ML PREDICTIONS MENU
# ============================================================================

async def show_ml_menu(message):
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
    
    await message.reply_html(menu_text, reply_markup=reply_markup)


# ============================================================================
# SETTINGS MENU
# ============================================================================

async def show_settings_menu(message, user_manager: UserManager, chat_id: str):
    """Display settings menu."""
    user_prefs = user_manager.get_user_prefs(chat_id)
    modes = user_prefs.get("modes", [])
    
    keyboard = [
        [InlineKeyboardButton("🔄 Mode Selection", callback_data="settings_mode")],
        [InlineKeyboardButton("🎯 Take Profit Settings", callback_data="settings_tp")],
        [InlineKeyboardButton("👤 View My Settings", callback_data="mysettings_direct")],
        [InlineKeyboardButton("◀️ Back", callback_data="menu_main")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    mode_text = " & ".join([f"✅ {m.upper()}" for m in modes]) if modes else "⭕ No modes active"
    
    menu_text = (
        f"⚙️ <b>Settings Menu</b>\n\n"
        f"<b>Current Modes:</b>\n"
        f"{mode_text}\n\n"
        f"Customize your bot experience."
    )
    
    await message.reply_html(menu_text, reply_markup=reply_markup)


async def show_mode_selection_menu(message):
    """Display mode selection menu."""
    keyboard = [
        [InlineKeyboardButton("🔔 Alerts Only", callback_data="mode_alerts_set")],
        [InlineKeyboardButton("📈 Paper Trading Only", callback_data="mode_papertrade_set")],
        [InlineKeyboardButton("🚀 Both Modes", callback_data="mode_both_set")],
        [InlineKeyboardButton("◀️ Back", callback_data="menu_settings")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    menu_text = (
        f"🔄 <b>Select Bot Modes</b>\n\n"
        f"<b>🔔 Alerts Only</b>\n"
        f"Receive token alerts with security analysis.\n\n"
        f"<b>📈 Paper Trading Only</b>\n"
        f"Simulate trading without real money.\n\n"
        f"<b>🚀 Both Modes</b>\n"
        f"Get alerts AND paper trade them."
    )
    
    await message.reply_html(menu_text, reply_markup=reply_markup)


async def show_tp_settings_menu(message):
    """Display take profit settings menu."""
    keyboard = [
        [InlineKeyboardButton("🔍 Discovery Signals TP", callback_data="tp_discovery_menu")],
        [InlineKeyboardButton("⭐ Alpha Signals TP", callback_data="tp_alpha_menu")],
        [InlineKeyboardButton("👀 View Current TP", callback_data="tp_view")],
        [InlineKeyboardButton("◀️ Back", callback_data="menu_settings")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    menu_text = (
        f"🎯 <b>Take Profit Settings</b>\n\n"
        f"Set target profit percentages for automatic position closing.\n\n"
        f"<b>Discovery Signals:</b> Regular token alerts\n"
        f"<b>Alpha Signals:</b> Premium curated alerts"
    )
    
    await message.reply_html(menu_text, reply_markup=reply_markup)


# ============================================================================
# HELP MENU
# ============================================================================

async def show_help_menu(message):
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
    
    await message.reply_html(menu_text, reply_markup=reply_markup)


async def show_help_topic(message, topic: str):
    """Display help for a specific topic."""
    help_texts = {
        "getting_started": (
            f"🚀 <b>Getting Started</b>\n\n"
            f"<b>Step 1: Choose Your Mode</b>\n"
            f"Use the Settings menu to select between:\n"
            f"• 🔔 Alerts - Receive token notifications\n"
            f"• 📈 Trading - Paper trade simulation\n"
            f"• 🚀 Both - Get alerts and trade them\n\n"
            f"<b>Step 2: Configure Alerts</b>\n"
            f"Go to Alerts menu → Set Alert Grades\n"
            f"Choose which priority levels you want.\n\n"
            f"<b>Step 3: Start Trading</b>\n"
            f"Enable paper trading with initial capital.\n"
            f"Use the Trading menu to manage positions.\n\n"
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
            f"• 📈 Volume & price trends\n\n"
            f"<b>Win Probability Tiers:</b>\n"
            f"🟢 70%+ - Strong buy signal\n"
            f"🟡 50-70% - Moderate opportunity\n"
            f"🔴 <50% - Wait for better signal"
        )
    }
    
    text = help_texts.get(topic, "Help topic not found.")
    
    keyboard = [
        [InlineKeyboardButton("◀️ Back", callback_data="menu_help")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await message.reply_html(text, reply_markup=reply_markup)
