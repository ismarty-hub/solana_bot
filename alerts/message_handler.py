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
        
    except Exception as e:
        logger.error(f"Error handling text message from {chat_id}: {e}")
        await update.message.reply_text(
            f"❌ An error occurred: {str(e)[:50]}\n"
            f"Please try again or use /help"
        )
