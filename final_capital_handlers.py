#!/usr/bin/env python3
"""Simple direct text insertion for capital management handlers"""

# Part 1: Add menu callback handlers to menu_handler.py
file_path = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\menu_handler.py"

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# Find a good insertion point - before help menu callbacks
marker = 'elif data == "menu_help":'

handler_code = '''    # Capital Management
    elif data == "set_reserve_menu":
        await show_reserve_balance_menu(query.message)
        await query.answer()
        return
    
    elif data == "set_mintrade_menu":
        await show_min_trade_size_menu(query.message)
        await query.answer()
        return
    
    elif data.startswith("set_reserve:"):
        _, amount_str = data.split(":")
        amount = float(amount_str)
        user_manager.update_user_prefs(chat_id, {"reserve_balance": amount})
        msg = f"✅ <b>Reserve Balance Set!</b>\\n\\n<b>Amount:</b> ${amount:,.2f}\\n\\nBot will keep this amount untouched."
        await query.edit_message_text(msg, parse_mode="HTML")
        return
    
    elif data == "set_reserve_custom":
        msg = "💵 <b>Enter Custom Reserve Balance</b>\\n\\nSend the amount in USD:\\nExample: <code>150</code>"
        await query.message.reply_text(msg, parse_mode="HTML")
        context.user_data["awaiting_reserve_custom"] = True
        await query.answer()
        return
    
    elif data.startswith("set_mintrade:"):
        _, amount_str = data.split(":")
        amount = float(amount_str)
        user_manager.update_user_prefs(chat_id, {"min_trade_size": amount})
        msg = f"✅ <b>Min Trade Size Set!</b>\\n\\n<b>Amount:</b> ${amount:,.2f}\\n\\nBot will skip trades smaller than this."
        await query.edit_message_text(msg, parse_mode="HTML")
        return
    
    elif data == "set_mintrade_custom":
        msg = "📏 <b>Enter Custom Min Trade Size</b>\\n\\nSend the amount in USD:\\nExample: <code>25</code>"
        await query.message.reply_text(msg, parse_mode="HTML")
        context.user_data["awaiting_mintrade_custom"] = True
        await query.answer()
        return
    
    '''

if marker in content:
    content = content.replace(marker, handler_code + marker)
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    print("SUCCESS: Added capital management handlers to menu_handler.py")
else:
    print("ERROR: Could not find insertion marker")

# Part 2: Add message handlers to message_handler.py
file_path = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\message_handler.py"

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# Insert before the implicit mint detection section
marker = '        # ===================================================================='
marker_full = marker + '\n        # IMPLICIT COMMANDS (Mint Address Detection)'

message_handlers = '''        # ====================================================================
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
                msg = f"✅ <b>Reserve Balance Set!</b>\\n\\n<b>Amount:</b> ${amount:,.2f}\\n\\nBot will keep this amount untouched."
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
                msg = f"✅ <b>Min Trade Size Set!</b>\\n\\n<b>Amount:</b> ${amount:,.2f}\\n\\nBot will skip trades smaller than this."
                await update.message.reply_html(msg)
            except ValueError:
                await update.message.reply_text("❌ Invalid amount. Please send a number.")
            return
        
        '''

if marker_full in content:
    content = content.replace(marker_full, message_handlers + marker_full)
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    print("SUCCESS: Added capital management message handlers")
else:
    print("ERROR: Could not find message handler insertion point")

print("\n✅ Capital Management UI Complete!")
