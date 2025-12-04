#!/usr/bin/env python3
"""
alerts/message_handler.py - Handle text message inputs from users

Processes user input for custom amounts, take profit settings, and predictions
initiated through menu buttons.
"""

import logging
from telegram import Update
from telegram.ext import ContextTypes
from alerts.user_manager import UserManager
from trade_manager import PortfolioManager

logger = logging.getLogger(__name__)


async def handle_text_message(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    user_manager: UserManager,
    portfolio_manager: PortfolioManager
):
    """Handle various text inputs from menu interactions."""
    chat_id = str(update.effective_user.id)
    text = update.message.text.strip()
    
    try:
        # ====================================================================
        # CUSTOM CAPITAL INPUT
        # ====================================================================
        if context.user_data.get('awaiting_capital'):
            context.user_data['awaiting_capital'] = False
            try:
                amount = float(text)
                if amount <= 0:
                    await update.message.reply_text("❌ Amount must be positive.")
                    return
                
                # Check if initializing or resetting
                if context.user_data.get('resetting_capital'):
                    context.user_data['resetting_capital'] = False
                    portfolio_manager.reset_portfolio(chat_id, amount)
                    await update.message.reply_html(
                        f"✅ <b>Capital Reset!</b>\n\n"
                        f"<b>New Capital:</b> ${amount:,.2f}\n"
                        f"All positions have been closed."
                    )
                else:
                    user_manager.update_user_prefs(chat_id, {"modes": ["papertrade"]})
                    portfolio_manager.init_portfolio(chat_id, amount)
                    await update.message.reply_html(
                        f"✅ <b>Paper Trading Enabled!</b>\n\n"
                        f"<b>Initial Capital:</b> ${amount:,.2f}\n"
                        f"You can now start trading.\n\n"
                        f"Use the Trading menu to manage your positions."
                    )
            except ValueError:
                await update.message.reply_text(
                    "❌ Invalid amount. Please send a valid number.\n"
                    "Example: 2500"
                )
            return
        
        # ====================================================================
        # TAKE PROFIT - DISCOVERY
        # ====================================================================
        if context.user_data.get('awaiting_tp_discovery'):
            context.user_data['awaiting_tp_discovery'] = False
            
            val = text.lower()
            if val not in ["median", "mean"]:
                try:
                    float(val)
                except ValueError:
                    await update.message.reply_text("❌ Invalid value. Use 'median', 'mean', or a number.")
                    return
            
            user_manager.update_user_prefs(chat_id, {"tp_discovery": val})
            await update.message.reply_html(
                f"✅ <b>Discovery TP Set!</b>\n\n"
                f"<b>Value:</b> {val}"
            )
            return
        
        # ====================================================================
        # TAKE PROFIT - ALPHA
        # ====================================================================
        if context.user_data.get('awaiting_tp_alpha'):
            context.user_data['awaiting_tp_alpha'] = False
            
            val = text.lower()
            if val not in ["median", "mean"]:
                try:
                    float(val)
                except ValueError:
                    await update.message.reply_text("❌ Invalid value. Use 'median', 'mean', or a number.")
                    return
            
            user_manager.update_user_prefs(chat_id, {"tp_alpha": val})
            await update.message.reply_html(
                f"✅ <b>Alpha TP Set!</b>\n\n"
                f"<b>Value:</b> {val}"
            )
            return
        
        # ====================================================================
        # ML PREDICTION - SINGLE TOKEN
        # ====================================================================
        if context.user_data.get('awaiting_predict'):
            context.user_data['awaiting_predict'] = False
            
            # Import predict command here to avoid circular imports
            from alerts.commands import predict_cmd
            
            # Create fake context with args
            from types import SimpleNamespace
            temp_context = SimpleNamespace(args=[text])
            temp_context.__dict__.update(context.__dict__)
            
            await predict_cmd(update, temp_context, user_manager)
            return
        
        # ====================================================================
        # ML PREDICTION - BATCH
        # ====================================================================
        if context.user_data.get('awaiting_predict_batch'):
            context.user_data['awaiting_predict_batch'] = False
            
            from alerts.commands import predict_batch_cmd
            
            # Create fake context with args
            from types import SimpleNamespace
            tokens = text.split(',')
            temp_context = SimpleNamespace(args=tokens)
            temp_context.__dict__.update(context.__dict__)
            
            await predict_batch_cmd(update, temp_context, user_manager)
            return

        # ====================================================================
        # MANUAL BUY - CUSTOM AMOUNT
        # ====================================================================
        if context.user_data.get('awaiting_buy_custom'):
            mint = context.user_data['awaiting_buy_custom']
            
            try:
                amount = float(text)
                if amount <= 0:
                    await update.message.reply_text("❌ Amount must be positive.")
                    return

                # Clear flag
                del context.user_data['awaiting_buy_custom']
                
                from alerts.commands import ask_buy_tp
                await ask_buy_tp(update, context, mint, str(amount), portfolio_manager, user_manager)
                
            except ValueError:
                await update.message.reply_text("❌ Invalid amount. Please enter a number (e.g. 100).")
            return

        # Custom TP for Buy
        if context.user_data.get("awaiting_tp_custom"):
            data = context.user_data.pop("awaiting_tp_custom")
            try:
                tp = float(text)
                if tp <= 0:
                    await update.message.reply_text("❌ TP must be positive.")
                    return
                # Proceed to SL
                from alerts.commands import ask_buy_sl
                await ask_buy_sl(update, context, data["mint"], data["amount"], tp)
            except ValueError:
                await update.message.reply_text("❌ Invalid number.")
            return

        # Custom SL for Buy
        if context.user_data.get("awaiting_sl_custom"):
            data = context.user_data.pop("awaiting_sl_custom")
            try:
                sl = float(text)
                if sl <= 0:
                    await update.message.reply_text("❌ SL must be positive.")
                    return
                # Execute
                from alerts.commands import _execute_manual_buy
                await _execute_manual_buy(update, context, user_manager, portfolio_manager, data["mint"], float(data["amount"]), float(data["tp"]), sl)
            except ValueError:
                await update.message.reply_text("❌ Invalid number.")
            return
                
            try:
                amount = float(text)
                if amount <= 0:
                    await update.message.reply_text("❌ Amount must be positive.")
                    return
                
                # Execute Buy
                from alerts.commands import _execute_manual_buy
                await _execute_manual_buy(update, context, user_manager, portfolio_manager, mint, amount)
                
            except ValueError:
                await update.message.reply_text("❌ Invalid amount. Please send a number.")
            return

        # ====================================================================
        # CAPITAL MANAGEMENT - CUSTOM RESERVE
        # ====================================================================
        if context.user_data.get("awaiting_reserve_custom"):
            context.user_data["awaiting_reserve_custom"] = False
            try:
                amount = float(text)
                if amount < 0:
                    await update.message.reply_text("❌ Amount must be positive or zero.")
                    return
                
                user_manager.update_user_prefs(chat_id, {"reserve_balance": amount})
                msg = f"✅ <b>Reserve Balance Set!</b>\n\n<b>Amount:</b> ${amount:,.2f}\n\nBot will keep this amount untouched."
                await update.message.reply_html(msg)
            except ValueError:
                await update.message.reply_text("❌ Invalid amount. Please send a number.")
            return
        
        # ====================================================================
        # CAPITAL MANAGEMENT - CUSTOM MIN TRADE
        # ====================================================================
        if context.user_data.get("awaiting_mintrade_custom"):
            context.user_data["awaiting_mintrade_custom"] = False
            try:
                amount = float(text)
                if amount <= 0:
                    await update.message.reply_text("❌ Amount must be positive.")
                    return
                
                user_manager.update_user_prefs(chat_id, {"min_trade_size": amount})
                msg = f"✅ <b>Min Trade Size Set!</b>\n\n<b>Amount:</b> ${amount:,.2f}\n\nBot will skip trades smaller than this."
                await update.message.reply_html(msg)
            except ValueError:
                await update.message.reply_text("❌ Invalid amount. Please send a number.")
            return
        
        # ====================================================================
        # TRADE SIZE SETTINGS - CUSTOM VALUE
        # ====================================================================
        if context.user_data.get("awaiting_trade_size_value"):
            mode = context.user_data.pop("awaiting_trade_size_value")
            try:
                value = float(text)
                
                if mode == "percent":
                    if value <= 0 or value > 100:
                        await update.message.reply_text("❌ Percentage must be between 1 and 100.")
                        return
                    user_manager.update_user_prefs(chat_id, {
                        "trade_size_mode": "percent",
                        "trade_size_value": value
                    })
                    msg = f"✅ <b>Trade Size Set!</b>\n\n<b>Mode:</b> 📊 Percentage-Based\n<b>Value:</b> {value}% of portfolio per trade"
                else:  # fixed
                    if value <= 0:
                        await update.message.reply_text("❌ Amount must be positive.")
                        return
                    user_manager.update_user_prefs(chat_id, {
                        "trade_size_mode": "fixed",
                        "trade_size_value": value
                    })
                    msg = f"✅ <b>Trade Size Set!</b>\n\n<b>Mode:</b> 💵 Fixed Amount\n<b>Value:</b> ${value:,.2f} per trade"
                
                await update.message.reply_html(msg)
            except ValueError:
                await update.message.reply_text("❌ Invalid number. Please send a valid number.")
            return
        
                # ====================================================================
        # IMPLICIT COMMANDS (Mint Address Detection)
        # ====================================================================
        import re
        # Solana Mint Address Regex (Base58, 32-44 chars)
        mint_pattern = r'^[1-9A-HJ-NP-Za-km-z]{32,44}$'
        
        if re.match(mint_pattern, text):
            # Allow buying regardless of mode - users can trade anytime
            from alerts.commands import buy_token_process
            await buy_token_process(update, context, user_manager, portfolio_manager, text)
            return
        
    except Exception as e:
        logger.error(f"Error handling text message from {chat_id}: {e}")
        await update.message.reply_text(
            f"❌ An error occurred: {str(e)[:50]}\n"
            f"Please try again or use /help"
        )
