#!/usr/bin/env python3
"""
alerts/menu_handler.py - Callback handler for menu navigation

Handles all menu-related button callbacks and routes them to command functions.
"""

import logging
from telegram import Update
from telegram.ext import ContextTypes
from alerts.user_manager import UserManager
from trade_manager import PortfolioManager
from alerts.menu_navigation import (
    show_main_menu, show_alerts_menu, show_alert_grades_menu, show_alpha_alerts_menu,
    show_trading_menu, show_enable_trading_menu, show_reset_capital_menu,
    show_ml_menu, show_settings_menu, show_mode_selection_menu, show_tp_settings_menu,
    show_help_menu, show_help_topic
)

logger = logging.getLogger(__name__)


async def handle_menu_callback(
    update: Update, 
    context: ContextTypes.DEFAULT_TYPE, 
    user_manager: UserManager, 
    portfolio_manager: PortfolioManager
):
    """Route menu callbacks to appropriate handlers."""
    from alerts.commands import (
        myalerts_cmd, history_cmd, performance_cmd, watchlist_cmd,
        portfolio_cmd, pnl_cmd, predict_cmd, predict_batch_cmd,
        papertrade_cmd, resetcapital_cmd, setalerts_cmd, 
        set_tp_cmd, set_tp_discovery_cmd, set_tp_alpha_cmd,
        alpha_subscribe_cmd, alpha_unsubscribe_cmd, testalert_cmd
    )
    
    query = update.callback_query
    data = query.data
    chat_id = str(query.from_user.id)
    
    await query.answer()
    
    # ========================================================================
    # MAIN MENU
    # ========================================================================
    if data == "menu_main":
        await show_main_menu(query.message, user_manager, chat_id)
        return
    
    # ========================================================================
    # ALERTS MENU
    # ========================================================================
    elif data == "menu_alerts":
        await show_alerts_menu(query.message, user_manager, chat_id)
        return
    
    elif data == "setalerts_menu":
        await show_alert_grades_menu(query.message)
        return
    
    elif data == "myalerts_direct":
        # Call the actual myalerts command
        update.message = query.message
        await myalerts_cmd(update, context, user_manager)
        return
    
    elif data == "alpha_menu":
        await show_alpha_alerts_menu(query.message, user_manager, chat_id)
        return
    
    elif data == "alpha_subscribe_menu":
        update.message = query.message
        # Create a fake context with no args for the command
        from types import SimpleNamespace
        temp_context = SimpleNamespace(args=[])
        temp_context.__dict__.update(context.__dict__)
        await alpha_subscribe_cmd(update, temp_context, user_manager)
        return
    
    elif data == "alpha_unsubscribe_menu":
        update.message = query.message
        from types import SimpleNamespace
        temp_context = SimpleNamespace(args=[])
        temp_context.__dict__.update(context.__dict__)
        await alpha_unsubscribe_cmd(update, temp_context, user_manager)
        return
    
    # ========================================================================
    # TRADING MENU
    # ========================================================================
    elif data == "menu_trading":
        await show_trading_menu(query.message, user_manager, portfolio_manager, chat_id)
        return
    
    elif data == "enable_trading":
        await show_enable_trading_menu(query.message)
        return
    
    elif data.startswith("init_capital:"):
        amount = float(data.split(":")[1])
        user_manager.update_user_prefs(chat_id, {"modes": ["papertrade"]})
        portfolio_manager.init_portfolio(chat_id, amount)
        await query.message.reply_html(
            f"✅ <b>Paper Trading Enabled!</b>\n\n"
            f"<b>Initial Capital:</b> ${amount:,.2f}\n"
            f"You can now start trading.\n\n"
            f"Use the Trading menu to manage your positions."
        )
        return
    
    elif data == "custom_capital":
        await query.message.reply_html(
            "💰 <b>Enter Custom Capital Amount</b>\n\n"
            "Send a number (e.g., 2500)\n"
            "Example: <code>2500</code>"
        )
        context.user_data['awaiting_capital'] = True
        return
    
    elif data == "portfolio_direct":
        update.message = query.message
        await portfolio_cmd(update, context, user_manager, portfolio_manager)
        return
    
    elif data == "pnl_direct":
        update.message = query.message
        await pnl_cmd(update, context, user_manager, portfolio_manager)
        return
    
    elif data == "history_direct":
        update.message = query.message
        await history_cmd(update, context, user_manager, portfolio_manager)
        return
    
    elif data == "performance_direct":
        update.message = query.message
        await performance_cmd(update, context, user_manager, portfolio_manager)
        return
    
    elif data == "watchlist_direct":
        update.message = query.message
        from types import SimpleNamespace
        temp_context = SimpleNamespace(args=[])
        temp_context.__dict__.update(context.__dict__)
        await watchlist_cmd(update, temp_context, user_manager, portfolio_manager)
        return
    
    elif data == "resetcapital_menu":
        await show_reset_capital_menu(query.message)
        return
    
    elif data.startswith("reset_capital:"):
        amount = float(data.split(":")[1])
        portfolio_manager.reset_portfolio(chat_id, amount)
        await query.message.reply_html(
            f"✅ <b>Capital Reset!</b>\n\n"
            f"<b>New Capital:</b> ${amount:,.2f}\n"
            f"All positions have been closed."
        )
        return
    
    elif data == "reset_capital_custom":
        await query.message.reply_html(
            "💰 <b>Enter Custom Capital Amount</b>\n\n"
            "Send a number (e.g., 2500)\n"
            "Example: <code>2500</code>"
        )
        context.user_data['awaiting_capital'] = True
        context.user_data['resetting_capital'] = True
        return
    
    # ========================================================================
    # ML MENU
    # ========================================================================
    elif data == "menu_ml":
        await show_ml_menu(query.message)
        return
    
    elif data == "predict_single":
        await query.message.reply_html(
            "🎯 <b>ML Prediction - Single Token</b>\n\n"
            "Send a token mint address or symbol:\n"
            "Example: <code>SOL</code> or <code>So11111111111111111111111111111111111111112</code>"
        )
        context.user_data['awaiting_predict'] = True
        return
    
    elif data == "predict_batch_menu":
        await query.message.reply_html(
            "📊 <b>ML Prediction - Batch</b>\n\n"
            "Send comma-separated tokens:\n"
            "Example: <code>SOL,BONK,RAY</code>"
        )
        context.user_data['awaiting_predict_batch'] = True
        return
    
    # ========================================================================
    # SETTINGS MENU
    # ========================================================================
    elif data == "menu_settings":
        await show_settings_menu(query.message, user_manager, chat_id)
        return
    
    elif data == "settings_mode":
        await show_mode_selection_menu(query.message)
        return
    
    elif data == "mode_alerts_set":
        user_manager.update_user_prefs(chat_id, {"modes": ["alerts"]})
        await query.message.reply_text("✅ Mode set to: Alerts Only")
        return
    
    elif data == "mode_papertrade_set":
        user_manager.update_user_prefs(chat_id, {"modes": ["papertrade"]})
        await query.message.reply_text("✅ Mode set to: Paper Trading Only")
        return
    
    elif data == "mode_both_set":
        user_manager.update_user_prefs(chat_id, {"modes": ["alerts", "papertrade"]})
        await query.message.reply_text("✅ Mode set to: Both Alerts & Trading")
        return
    
    elif data == "settings_tp":
        await show_tp_settings_menu(query.message)
        return
    
    elif data == "tp_discovery_menu":
        await query.message.reply_html(
            "🔍 <b>Discovery Take Profit</b>\n\n"
            "Send a number for take profit percentage:\n"
            "Example: <code>50</code> (for 50%)\n\n"
            "Or use special values:\n"
            "• <code>median</code> - Use median historical ATH\n"
            "• <code>mean</code> - Use average historical ATH"
        )
        context.user_data['awaiting_tp_discovery'] = True
        return
    
    elif data == "tp_alpha_menu":
        await query.message.reply_html(
            "⭐ <b>Alpha Take Profit</b>\n\n"
            "Send a number for take profit percentage:\n"
            "Example: <code>50</code> (for 50%)\n\n"
            "Or use special values:\n"
            "• <code>median</code> - Use median historical ATH\n"
            "• <code>mean</code> - Use average historical ATH"
        )
        context.user_data['awaiting_tp_alpha'] = True
        return
    
    elif data == "tp_view":
        update.message = query.message
        await myalerts_cmd(update, context, user_manager)
        return
    
    elif data == "mysettings_direct":
        update.message = query.message
        await myalerts_cmd(update, context, user_manager)
        return
    
    # ========================================================================
    # HELP MENU
    # ========================================================================
    elif data == "menu_help":
        await show_help_menu(query.message)
        return
    
    elif data == "help_getting_started":
        await show_help_topic(query.message, "getting_started")
        return
    
    elif data == "help_alerts":
        await show_help_topic(query.message, "alerts")
        return
    
    elif data == "help_trading":
        await show_help_topic(query.message, "trading")
        return
    
    elif data == "help_ml":
        await show_help_topic(query.message, "ml")
        return
    
    # ========================================================================
    # GRADE SELECTION
    # ========================================================================
    elif data.startswith("grade_"):
        grade = data.replace("grade_", "").upper()
        user_prefs = user_manager.get_user_prefs(chat_id)
        grades = user_prefs.get("grades", [])
        
        if grade in grades:
            grades.remove(grade)
        else:
            grades.append(grade)
        
        user_manager.update_user_prefs(chat_id, {"grades": grades})
        
        # Show updated menu
        await show_alert_grades_menu(query.message)
        return
    
    elif data == "grades_done":
        user_prefs = user_manager.get_user_prefs(chat_id)
        grades = user_prefs.get("grades", [])
        grades_text = ", ".join(grades) if grades else "None selected"
        await query.message.reply_html(
            f"✅ <b>Alerts Configured!</b>\n\n"
            f"<b>Selected Grades:</b>\n"
            f"{grades_text}\n\n"
            f"You'll now receive alerts for these grades."
        )
        return
    
    else:
        logger.warning(f"Unknown callback: {data}")
        await query.answer("❌ Unknown command", show_alert=True)
