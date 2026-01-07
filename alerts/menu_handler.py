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
    show_ml_menu, show_settings_menu, show_mode_selection_menu,
    show_help_menu, show_help_topic, show_reserve_balance_menu, show_min_trade_size_menu,
    show_dashboard_menu, show_trading_settings_menu, show_alert_settings_menu, show_sl_settings_menu,
    show_dashboard_menu, show_trading_settings_menu, show_alert_settings_menu, show_sl_settings_menu,
    show_trade_size_mode_menu, show_trade_filters_menu, show_trade_grades_menu, show_trade_alpha_menu,
    show_min_prob_menu
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
    
    logger.info(f"Processing menu callback: '{data}' from user {chat_id}")
    
    # query.answer() removed from here to avoid double-answering
    # It will be called within each handler block as needed.
    


    
    # ========================================================================
    # TRADING BUTTON HANDLERS
    # ========================================================================
    from alerts.trading_buttons import (
        handle_pnl_page_callback,
        handle_portfolio_page_callback,
        handle_sell_confirm_callback,
        handle_sell_execute_callback,
        handle_sell_all_confirm_callback,
        handle_sell_all_execute_callback,
        handle_sell_cancel_callback
    )

    if data.startswith("sc:") or data.startswith("sell_confirm:"):
        await handle_sell_confirm_callback(update, context, user_manager, portfolio_manager)
        return
        
    elif data.startswith("sx:") or data.startswith("sell_execute:"):
        await handle_sell_execute_callback(update, context, user_manager, portfolio_manager)
        return
        
    elif data.startswith("pnl_page:"):
        await handle_pnl_page_callback(update, context, user_manager, portfolio_manager)
        return
        
    elif data.startswith("portfolio_page:"):
        await handle_portfolio_page_callback(update, context, user_manager, portfolio_manager)
        return
        
    elif data == "sell_all_confirm":
        await handle_sell_all_confirm_callback(update, context, user_manager, portfolio_manager)
        return
        
    elif data == "sell_all_execute":
        await handle_sell_all_execute_callback(update, context, user_manager, portfolio_manager)
        return
        
    elif data == "sell_cancel":
        await handle_sell_cancel_callback(update, context)
        return

    # ========================================================================
    # MAIN MENU
    # ========================================================================
    if data == "menu_main":
        await query.answer()
        await show_main_menu(query.message, user_manager, chat_id, edit=True)
        return
    
    # ========================================================================
    # ALERTS MENU
    # ========================================================================
    elif data == "menu_alerts":
        await query.answer()
        await show_alerts_menu(query.message, user_manager, chat_id, edit=True)
        return
    
    elif data == "toggle_alerts":
        user_prefs = user_manager.get_user_prefs(chat_id)
        modes = user_prefs.get("modes", [])
        if "alerts" in modes:
            modes.remove("alerts")
            status_msg = "⭕ Notifications Disabled"
        else:
            modes.append("alerts")
            status_msg = "✅ Notifications Enabled"
        
        user_manager.update_user_prefs(chat_id, {"modes": modes})
        await query.answer(status_msg, show_alert=True)
        await show_alerts_menu(query.message, user_manager, chat_id, edit=True)
        return
    
    elif data == "setalerts_menu":
        if not user_manager.is_subscribed(chat_id):
            if user_manager.is_subscription_expired(chat_id):
                await query.answer("⚠️ Subscription expired.", show_alert=True)
            else:
                await query.answer("⚠️ Active subscription required for alert configuration.", show_alert=True)
            return
        await show_alert_grades_menu(query.message, user_manager, chat_id, edit=True)
        return
    
    elif data == "myalerts_direct":
        await query.answer()
        # Call the actual myalerts command
        # Create a new Update object with the message from callback_query
        from telegram import Update as TgUpdate
        new_update = TgUpdate(
            update_id=update.update_id,
            message=query.message
        )
        await myalerts_cmd(new_update, context, user_manager)
        return
    
    elif data == "alpha_menu":
        if not user_manager.is_subscribed(chat_id):
            if user_manager.is_subscription_expired(chat_id):
                await query.answer("⚠️ Subscription expired.", show_alert=True)
            else:
                await query.answer("⚠️ Active subscription required for Alpha Notifications.", show_alert=True)
            return
        await show_alpha_alerts_menu(query.message, user_manager, chat_id, edit=True)
        return
    
    elif data == "alpha_subscribe_menu":
        if not user_manager.is_subscribed(chat_id):
            if user_manager.is_subscription_expired(chat_id):
                await query.answer("⚠️ Subscription expired.", show_alert=True)
            else:
                await query.answer("⚠️ Active subscription required.", show_alert=True)
            return
        # Create a new Update object with the message from callback_query
        from telegram import Update as TgUpdate
        from types import SimpleNamespace
        new_update = TgUpdate(
            update_id=update.update_id,
            message=query.message
        )
        temp_context = SimpleNamespace(args=[])
        temp_context.__dict__.update(context.__dict__)
        await alpha_subscribe_cmd(new_update, temp_context, user_manager)
        return
    
    elif data == "alpha_unsubscribe_menu":
        if not user_manager.is_subscribed(chat_id):
            if user_manager.is_subscription_expired(chat_id):
                await query.answer("⚠️ Subscription expired.", show_alert=True)
            else:
                await query.answer("⚠️ Active subscription required.", show_alert=True)
            return
        # Create a new Update object with the message from callback_query
        from telegram import Update as TgUpdate
        from types import SimpleNamespace
        new_update = TgUpdate(
            update_id=update.update_id,
            message=query.message
        )
        temp_context = SimpleNamespace(args=[])
        temp_context.__dict__.update(context.__dict__)
        await alpha_unsubscribe_cmd(new_update, temp_context, user_manager)
        return
        temp_context.__dict__.update(context.__dict__)
        await alpha_unsubscribe_cmd(new_update, temp_context, user_manager)
        return
    
    elif data == "min_prob_menu":
        if not user_manager.is_subscribed(chat_id):
            await query.answer("⚠️ Active subscription required.", show_alert=True)
            return
        await show_min_prob_menu(query.message, user_manager, chat_id, edit=True)
        return

    elif data.startswith("set_prob_"):
        # Format: set_prob_d:50 or set_prob_a:60
        parts = data.split(":")
        if len(parts) != 2:
            return
            
        action_type = parts[0] # set_prob_d or set_prob_a
        val = float(parts[1])
        prob = val / 100.0
        
        target = "Discovery" if "d" in action_type else "Alpha"
        key = "min_prob_discovery" if "d" in action_type else "min_prob_alpha"
        
        user_manager.update_user_prefs(chat_id, {key: prob})
        await query.answer(f"{target} Prob Set to {val:.0f}%")
        await show_min_prob_menu(query.message, user_manager, chat_id, edit=True)
        return
    
    # ========================================================================
    # TRADING MENU
    # ========================================================================
    elif data == "menu_trading":
        await query.answer()
        await show_trading_menu(query.message, user_manager, portfolio_manager, chat_id, edit=True)
        return
    
    elif data == "enable_trading":
        await query.answer()
        await show_enable_trading_menu(query.message, edit=True)
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
        await query.answer()
        from telegram import Update as TgUpdate
        new_update = TgUpdate(
            update_id=update.update_id,
            message=query.message
        )
        await portfolio_cmd(new_update, context, user_manager, portfolio_manager)
        return
    
    elif data == "pnl_direct":
        await query.answer()
        from telegram import Update as TgUpdate
        new_update = TgUpdate(
            update_id=update.update_id,
            message=query.message
        )
        await pnl_cmd(new_update, context, user_manager, portfolio_manager)
        return
    
    elif data == "history_direct":
        await query.answer()
        from telegram import Update as TgUpdate
        new_update = TgUpdate(
            update_id=update.update_id,
            message=query.message
        )
        await history_cmd(new_update, context, user_manager, portfolio_manager)
        return
    
    elif data == "performance_direct":
        await query.answer()
        from telegram import Update as TgUpdate
        new_update = TgUpdate(
            update_id=update.update_id,
            message=query.message
        )
        await performance_cmd(new_update, context, user_manager, portfolio_manager)
        return
    
    elif data == "watchlist_direct":
        await query.answer()
        from telegram import Update as TgUpdate
        from types import SimpleNamespace
        new_update = TgUpdate(
            update_id=update.update_id,
            message=query.message
        )
        temp_context = SimpleNamespace(args=[])
        temp_context.__dict__.update(context.__dict__)
        await watchlist_cmd(new_update, temp_context, user_manager, portfolio_manager)
        return
    
    elif data == "resetcapital_menu":
        await query.answer()
        await show_reset_capital_menu(query.message, edit=True)
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
        await query.answer()
        await show_ml_menu(query.message, edit=True)
        return
    
    elif data == "predict_single":
        await query.answer()
        await query.message.reply_html(
            "🎯 <b>ML Prediction - Single Token</b>\n\n"
            "Send a token mint address:\n"
            "Example: <code>So11111111111111111111111111111111111111112</code>"
        )
        context.user_data['awaiting_predict'] = True
        return
    
    elif data == "predict_batch_menu":
        await query.answer()
        await query.message.reply_html(
            "📊 <b>ML Prediction - Batch</b>\n\n"
            "Send comma-separated token mint addresses:\n"
            "Example: <code>So111..., EPjFW...</code>"
        )
        context.user_data['awaiting_predict_batch'] = True
        return
    
    # ========================================================================
    # DASHBOARD MENU (New)
    # ========================================================================
    elif data == "menu_dashboard":
        await query.answer()
        await show_dashboard_menu(query.message, user_manager, portfolio_manager, chat_id, edit=True)
        return
    
    # ========================================================================
    # MAIN SETTINGS MENU
    # ========================================================================
    elif data == "menu_settings":
        await query.answer()
        await show_settings_menu(query.message, user_manager, portfolio_manager, chat_id, edit=True)
        return
    
    elif data == "settings_mode":
        await query.answer()
        await show_mode_selection_menu(query.message, edit=True)
        return
    
    # ========================================================================
    # PAPER TRADING SETTINGS SUBMENU (New)
    # ========================================================================
    elif data == "settings_trading":
        await query.answer()
        await show_trading_settings_menu(query.message, user_manager, portfolio_manager, chat_id, edit=True)
        return
    
    # ========================================================================
    # STOP LOSS SETTINGS MENU (New)
    # ========================================================================
    elif data == "settings_sl_menu":
        await query.answer()
        await show_sl_settings_menu(query.message, user_manager, chat_id, edit=True)
        return
    
    elif data.startswith("set_default_sl:"):
        _, sl_value = data.split(":")
        if sl_value.lower() == "none":
            user_manager.update_user_prefs(chat_id, {"default_sl": None})
            await query.message.reply_html("✅ <b>Stop Loss Setting Updated</b>\n\nNo default SL will be applied.\nUsers can set SL on individual trades.")
        else:
            sl_percent = float(sl_value)
            user_manager.update_user_prefs(chat_id, {"default_sl": -sl_percent})
            await query.message.reply_html(f"✅ <b>Stop Loss Setting Updated</b>\n\nDefault SL set to {sl_percent}%")
        return
    
    elif data == "set_default_sl_custom":
        await query.message.reply_html(
            "🛑 <b>Set Custom Stop Loss</b>\n\n"
            "Send a number for stop loss percentage:\n"
            "Example: <code>25</code> (for 25%)\n\n"
            "Or send <code>none</code> for no default SL."
        )
        context.user_data['awaiting_default_sl'] = True
        return
    
    # ========================================================================
    # TRADE SIZE MODE SETTINGS MENU (New)
    # ========================================================================
    elif data == "settings_trade_size_menu":
        await query.answer()
        await show_trade_size_mode_menu(query.message, user_manager, chat_id, edit=True)
        return
    
    elif data.startswith("set_trade_size_mode_select:"):
        _, mode = data.split(":")
        if mode in ["percent", "fixed"]:
            # Save the mode and prompt for custom value
            user_manager.update_user_prefs(chat_id, {"trade_size_mode": mode})
            context.user_data['awaiting_trade_size_value'] = mode
            
            if mode == "percent":
                await query.message.reply_html(
                    f"📊 <b>Enter Trade Size Percentage</b>\n\n"
                    f"Send a number for the percentage of your portfolio:\n\n"
                    f"<b>Examples:</b>\n"
                    f"<code>25</code> - Trade 25% of portfolio per trade\n"
                    f"<code>50</code> - Trade 50% of portfolio per trade\n"
                    f"<code>10</code> - Trade 10% of portfolio per trade\n\n"
                    f"<b>Note:</b> Valid range is 1-100%"
                )
            else:  # fixed
                await query.message.reply_html(
                    f"💵 <b>Enter Fixed Trade Amount</b>\n\n"
                    f"Send a dollar amount for each trade:\n\n"
                    f"<b>Examples:</b>\n"
                    f"<code>10</code> - $10 per trade\n"
                    f"<code>50</code> - $50 per trade\n"
                    f"<code>100</code> - $100 per trade\n\n"
                    f"<b>Note:</b> Will be capped by available capital"
                )
        return
    
    # ========================================================================
    # ALERT SETTINGS SUBMENU (New)
    # ========================================================================
    elif data == "settings_alerts_submenu" or data == "settings_tp":
        # Relaxed: All users can access the TP submenu to set Global TP
        await query.answer()
        await show_alert_settings_menu(query.message, edit=True)
        return
    
    # ========================================================================
    # LEGACY SETTINGS MENU (For backward compat)
    # ========================================================================
    # settings_tp handled above
    
    
    elif data == "menu_trading":
        await show_trading_menu(query.message, user_manager, portfolio_manager, chat_id, edit=True)
        return
    
    elif data == "tp_global_menu":
        await query.answer()
        await query.message.reply_html(
            "🎯 <b>Global Take Profit</b>\n\n"
            "This applies to all trades (Paper Trading & Manual) unless overridden by signal-specific settings.\n\n"
            "Send a number for take profit percentage:\n"
            "Example: <code>50</code> (for 50%)\n\n"
            "Or use special values:\n"
            "• <code>median</code> - Use median historical ATH\n"
            "• <code>mean</code> - Use average historical ATH\n"
            "• <code>mode</code> - Use most frequent profit level\n"
            "• <code>smart</code> - Use TP targets statistically reached 75% of the time"
        )
        context.user_data['awaiting_tp'] = True
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
    
    elif data == "tp_discovery_menu":
        if not user_manager.is_subscribed(chat_id):
            if user_manager.is_subscription_expired(chat_id):
                await query.answer("⚠️ Subscription expired.", show_alert=True)
            else:
                await query.answer("⚠️ Active subscription required.", show_alert=True)
            return
        await query.answer()
        await query.message.reply_html(
            "🔍 <b>Discovery Take Profit</b>\n\n"
            "Send a number for take profit percentage:\n"
            "Example: <code>50</code> (for 50%)\n\n"
            "Or use special values:\n"
            "• <code>median</code> - Use median historical ATH\n"
            "• <code>mean</code> - Use average historical ATH\n"
            "• <code>mode</code> - Use most frequent profit level\n"
            "• <code>smart</code> - Use TP targets statistically reached 75% of the time"
        )
        context.user_data['awaiting_tp_discovery'] = True
        return
    
    elif data == "tp_alpha_menu":
        if not user_manager.is_subscribed(chat_id):
            if user_manager.is_subscription_expired(chat_id):
                await query.answer("⚠️ Subscription expired.", show_alert=True)
            else:
                await query.answer("⚠️ Active subscription required.", show_alert=True)
            return
        await query.answer()
        await query.message.reply_html(
            "⭐ <b>Alpha Take Profit</b>\n\n"
            "Send a number for take profit percentage:\n"
            "Example: <code>50</code> (for 50%)\n\n"
            "Or use special values:\n"
            "• <code>median</code> - Use median historical ATH\n"
            "• <code>mean</code> - Use average historical ATH\n"
            "• <code>mode</code> - Use most frequent profit level\n"
            "• <code>smart</code> - Use TP targets statistically reached 75% of the time"
        )
        context.user_data['awaiting_tp_alpha'] = True
        return
    
    elif data == "tp_view":
        if not user_manager.is_subscribed(chat_id):
            if user_manager.is_subscription_expired(chat_id):
                await query.answer("⚠️ Subscription expired.", show_alert=True)
            else:
                await query.answer("⚠️ Active subscription required.", show_alert=True)
            return
        await query.answer()
        from telegram import Update as TgUpdate
        # Import the new command
        from alerts.commands import view_tp_settings_cmd
        
        new_update = TgUpdate(
            update_id=update.update_id,
            message=query.message
        )
        await view_tp_settings_cmd(new_update, context, user_manager)
        return
    
    elif data == "mysettings_direct":
        await query.answer()
        from telegram import Update as TgUpdate
        new_update = TgUpdate(
            update_id=update.update_id,
            message=query.message
        )
        await myalerts_cmd(new_update, context, user_manager)
        return
    
    # ========================================================================
    # AUTO-TRADE FILTER HANDLERS (Decoupled)
    # ========================================================================
    elif data == "settings_trade_filters":
        await query.answer()
        await show_trade_filters_menu(query.message, edit=True)
        return

    elif data == "set_trade_grades_menu":
        await query.answer()
        await show_trade_grades_menu(query.message, user_manager, chat_id, edit=True)
        return

    elif data == "trade_alpha_menu":
        await query.answer()
        await show_trade_alpha_menu(query.message, user_manager, chat_id, edit=True)
        return

    elif data.startswith("trade_grade_"):
        grade = data.replace("trade_grade_", "").upper()
        user_prefs = user_manager.get_user_prefs(chat_id)
        trade_grades = user_prefs.get("trade_grades", [])
        
        if grade in trade_grades:
            trade_grades.remove(grade)
        else:
            trade_grades.append(grade)
        
        user_manager.update_user_prefs(chat_id, {"trade_grades": trade_grades})
        await show_trade_grades_menu(query.message, user_manager, chat_id, edit=True)
        await query.answer()
        return

    elif data == "trade_alpha_toggle":
        user_prefs = user_manager.get_user_prefs(chat_id)
        current = user_prefs.get("trade_alpha_alerts", False)
        new_val = not current
        user_manager.update_user_prefs(chat_id, {"trade_alpha_alerts": new_val})
        await show_trade_alpha_menu(query.message, user_manager, chat_id, edit=True)
        await query.answer(f"Alpha Trading {'Enabled' if new_val else 'Disabled'}")
        return

    # ========================================================================
    # HELP MENU
    # ========================================================================
        # Capital Management
    elif data == "set_reserve_menu":
        await show_reserve_balance_menu(query.message, edit=True)
        await query.answer()
        return
    
    elif data == "set_mintrade_menu":
        await show_min_trade_size_menu(query.message, edit=True)
        await query.answer()
        return
    
    elif data.startswith("set_reserve:"):
        _, amount_str = data.split(":")
        amount = float(amount_str)
        user_manager.update_user_prefs(chat_id, {"reserve_balance": amount})
        msg = f"✅ <b>Reserve Balance Set!</b>\n\n<b>Amount:</b> ${amount:,.2f}\n\nBot will keep this amount untouched."
        await query.edit_message_text(msg, parse_mode="HTML")
        return
    
    elif data == "set_reserve_custom":
        msg = "💵 <b>Enter Custom Reserve Balance</b>\n\nSend the amount in USD:\nExample: <code>150</code>"
        await query.message.reply_text(msg, parse_mode="HTML")
        context.user_data["awaiting_reserve_custom"] = True
        await query.answer()
        return
    
    elif data.startswith("set_mintrade:"):
        _, amount_str = data.split(":")
        amount = float(amount_str)
        user_manager.update_user_prefs(chat_id, {"min_trade_size": amount})
        msg = f"✅ <b>Min Trade Size Set!</b>\n\n<b>Amount:</b> ${amount:,.2f}\n\nBot will skip trades smaller than this."
        await query.edit_message_text(msg, parse_mode="HTML")
        return
    
    elif data == "set_mintrade_custom":
        msg = "📏 <b>Enter Custom Min Trade Size</b>\n\nSend the amount in USD:\nExample: <code>25</code>"
        await query.message.reply_text(msg, parse_mode="HTML")
        context.user_data["awaiting_mintrade_custom"] = True
        await query.answer()
        return
    
    elif data == "menu_help":
        await query.answer()
        await show_help_menu(query.message, edit=True)
        return
    
    elif data == "help_getting_started":
        await query.answer()
        await show_help_topic(query.message, "getting_started")
        return
    
    elif data == "help_alerts":
        await query.answer()
        await show_help_topic(query.message, "alerts")
        return
    
    elif data == "help_trading":
        await query.answer()
        await show_help_topic(query.message, "trading")
        return
    
    elif data == "help_ml":
        await query.answer()
        await show_help_topic(query.message, "ml")
        return
    
    # ========================================================================
    # GRADE SELECTION
    # ========================================================================
    elif data.startswith("grade_"):
        if not user_manager.is_subscribed(chat_id):
            if user_manager.is_subscription_expired(chat_id):
                await query.answer("⚠️ Subscription expired.", show_alert=True)
            else:
                await query.answer("⚠️ Active subscription required.", show_alert=True)
            return
        grade = data.replace("grade_", "").upper()
        user_prefs = user_manager.get_user_prefs(chat_id)
        grades = user_prefs.get("grades", [])
        
        if grade in grades:
            grades.remove(grade)
        else:
            grades.append(grade)
        
        user_manager.update_user_prefs(chat_id, {"grades": grades})
        
        # Show updated menu
        await show_alert_grades_menu(query.message, user_manager, chat_id, edit=True)
        return
    
    elif data == "grades_done":
        if not user_manager.is_subscribed(chat_id):
            if user_manager.is_subscription_expired(chat_id):
                await query.answer("⚠️ Subscription expired.", show_alert=True)
            else:
                await query.answer("⚠️ Active subscription required.", show_alert=True)
            return
        
        # Ensure 'alerts' mode is enabled when grades are configured
        user_prefs = user_manager.get_user_prefs(chat_id)
        modes = user_prefs.get("modes", [])
        if "alerts" not in modes:
            modes.append("alerts")
            user_manager.update_user_prefs(chat_id, {"modes": modes})
            mode_notice = "\n\n🔔 <b>Alert Notifications enabled automatically.</b>"
        else:
            mode_notice = ""

        grades = user_prefs.get("grades", [])
        grades_text = ", ".join(grades) if grades else "None selected"
        await query.message.reply_html(
            f"✅ <b>Notifications Configured!</b>\n\n"
            f"<b>Selected Grades:</b>\n"
            f"{grades_text}\n\n"
            f"You'll now receive notifications for these grades.{mode_notice}"
        )
        return
    
    else:
        logger.warning(f"Unknown callback: {data}")
        await query.answer("❌ Unknown command", show_alert=True)
