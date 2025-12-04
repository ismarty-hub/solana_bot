#!/usr/bin/env python3
"""
Comprehensive script to implement Manual Buy TP/SL Configuration
Updates trade_manager.py, commands.py, and message_handler.py
"""

import os

print("Step 1: Updating trade_manager.py...")
file_path = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\trade_manager.py"

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# Update add_manual_position signature
old_sig = 'def add_manual_position(self, chat_id: str, mint: str, symbol: str, price: float, amount_usd: float) -> bool:'
new_sig = 'def add_manual_position(self, chat_id: str, mint: str, symbol: str, price: float, amount_usd: float, tp_percent: float = 50.0, sl_percent: float = 20.0) -> bool:'

if old_sig in content:
    content = content.replace(old_sig, new_sig)
    
    # Update defaults in dictionary
    old_defaults = '''                "tp_used": 50.0, # Default TP
                "sl_used": -50.0, # Default SL'''
    new_defaults = '''                "tp_used": float(tp_percent),
                "sl_used": -abs(float(sl_percent)), # Ensure negative'''
    
    content = content.replace(old_defaults, new_defaults)
    print("SUCCESS: Updated add_manual_position signature and logic")
else:
    print("WARNING: Could not find add_manual_position signature")

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)


print("\nStep 2: Updating alerts/commands.py...")
file_path = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\commands.py"

with open(file_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# We need to insert helper functions and update handlers
# 1. Insert ask_buy_tp and ask_buy_sl before buy_token_callback_handler

insert_idx = -1
for i, line in enumerate(lines):
    if 'async def buy_token_callback_handler' in line:
        insert_idx = i
        break

if insert_idx != -1:
    helpers = [
        '\n',
        'async def ask_buy_tp(update, context, mint, amount):\n',
        '    """Step 2: Ask for Take Profit percentage."""\n',
        '    keyboard = [\n',
        '        [\n',
        '            InlineKeyboardButton("25%", callback_data=f"set_buy_tp:{mint}:{amount}:25"),\n',
        '            InlineKeyboardButton("50%", callback_data=f"set_buy_tp:{mint}:{amount}:50"),\n',
        '            InlineKeyboardButton("100%", callback_data=f"set_buy_tp:{mint}:{amount}:100")\n',
        '        ],\n',
        '        [\n',
        '            InlineKeyboardButton("Custom", callback_data=f"set_buy_tp_custom:{mint}:{amount}"),\n',
        '            InlineKeyboardButton("Skip (Default 50%)", callback_data=f"set_buy_tp:{mint}:{amount}:50")\n',
        '        ],\n',
        '        [InlineKeyboardButton("❌ Cancel", callback_data="delete_msg")]\n',
        '    ]\n',
        '    reply_markup = InlineKeyboardMarkup(keyboard)\n',
        '    \n',
        '    msg = (\n',
        '        f"💰 <b>Amount Set:</b> ${float(amount):.2f}\\n\\n"\n',
        '        f"🎯 <b>Select Take Profit (TP)</b>\\n"\n',
        '        f"At what percentage gain should the bot sell?"\n',
        '    )\n',
        '    \n',
        '    if update.callback_query:\n',
        '        await update.callback_query.edit_message_text(msg, reply_markup=reply_markup, parse_mode="HTML")\n',
        '    else:\n',
        '        await update.message.reply_html(msg, reply_markup=reply_markup)\n',
        '\n',
        'async def ask_buy_sl(update, context, mint, amount, tp):\n',
        '    """Step 3: Ask for Stop Loss percentage."""\n',
        '    keyboard = [\n',
        '        [\n',
        '            InlineKeyboardButton("10%", callback_data=f"set_buy_sl:{mint}:{amount}:{tp}:10"),\n',
        '            InlineKeyboardButton("20%", callback_data=f"set_buy_sl:{mint}:{amount}:{tp}:20"),\n',
        '            InlineKeyboardButton("30%", callback_data=f"set_buy_sl:{mint}:{amount}:{tp}:30")\n',
        '        ],\n',
        '        [\n',
        '            InlineKeyboardButton("Custom", callback_data=f"set_buy_sl_custom:{mint}:{amount}:{tp}"),\n',
        '            InlineKeyboardButton("Skip (Default 20%)", callback_data=f"set_buy_sl:{mint}:{amount}:{tp}:20")\n',
        '        ],\n',
        '        [InlineKeyboardButton("❌ Cancel", callback_data="delete_msg")]\n',
        '    ]\n',
        '    reply_markup = InlineKeyboardMarkup(keyboard)\n',
        '    \n',
        '    msg = (\n',
        '        f"💰 <b>Amount:</b> ${float(amount):.2f}\\n"\n',
        '        f"🎯 <b>TP:</b> {tp}%\\n\\n"\n',
        '        f"🛑 <b>Select Stop Loss (SL)</b>\\n"\n',
        '        f"At what percentage loss should the bot sell?"\n',
        '    )\n',
        '    \n',
        '    if update.callback_query:\n',
        '        await update.callback_query.edit_message_text(msg, reply_markup=reply_markup, parse_mode="HTML")\n',
        '    else:\n',
        '        await update.message.reply_html(msg, reply_markup=reply_markup)\n',
        '\n'
    ]
    
    for idx, line in enumerate(helpers):
        lines.insert(insert_idx + idx, line)
    print("SUCCESS: Inserted helper functions")
    
    # 2. Update buy_token_callback_handler logic
    # We'll replace the existing function body with new logic
    
    # Find start and end of function
    func_start = -1
    for i, line in enumerate(lines):
        if 'async def buy_token_callback_handler' in line:
            func_start = i
            break
            
    if func_start != -1:
        # Construct new handler body
        new_handler = [
            '    """Handle buy amount and TP/SL selection callbacks."""\n',
            '    query = update.callback_query\n',
            '    data = query.data\n',
            '    chat_id = str(query.from_user.id)\n',
            '    \n',
            '    # Step 1: Amount Selected -> Ask TP\n',
            '    if data.startswith("buy_amount:"):\n',
            '        _, mint, amount_str = data.split(":")\n',
            '        await ask_buy_tp(update, context, mint, amount_str)\n',
            '        \n',
            '    # Step 2: TP Selected -> Ask SL\n',
            '    elif data.startswith("set_buy_tp:"):\n',
            '        parts = data.split(":")\n',
            '        # Format: set_buy_tp:mint:amount:tp_val\n',
            '        if len(parts) == 4:\n',
            '            _, mint, amount, tp = parts\n',
            '            await ask_buy_sl(update, context, mint, amount, tp)\n',
            '            \n',
            '    # Step 3: SL Selected -> Execute\n',
            '    elif data.startswith("set_buy_sl:"):\n',
            '        parts = data.split(":")\n',
            '        # Format: set_buy_sl:mint:amount:tp:sl_val\n',
            '        if len(parts) == 5:\n',
            '            _, mint, amount, tp, sl = parts\n',
            '            await _execute_manual_buy(update, context, user_manager, portfolio_manager, mint, float(amount), float(tp), float(sl))\n',
            '            \n',
            '    # Custom Inputs\n',
            '    elif data.startswith("buy_custom:"):\n',
            '        _, mint = data.split(":")\n',
            '        await query.message.reply_text(\n',
            '            "💰 <b>Enter Custom Amount</b>\\n\\n"\n',
            '            f"Send the amount in USD to buy {mint}\\n"\n',
            '            "Example: <code>250</code>",\n',
            '            parse_mode="HTML"\n',
            '        )\n',
            '        context.user_data["awaiting_buy_custom"] = mint\n',
            '        await query.answer()\n',
            '        \n',
            '    elif data.startswith("set_buy_tp_custom:"):\n',
            '        _, mint, amount = data.split(":")\n',
            '        await query.message.reply_text(\n',
            '            "🎯 <b>Enter Custom Take Profit</b>\\n\\n"\n',
            '            "Send the percentage (e.g., 150):",\n',
            '            parse_mode="HTML"\n',
            '        )\n',
            '        context.user_data["awaiting_tp_custom"] = {"mint": mint, "amount": amount}\n',
            '        await query.answer()\n',
            '        \n',
            '    elif data.startswith("set_buy_sl_custom:"):\n',
            '        _, mint, amount, tp = data.split(":")\n',
            '        await query.message.reply_text(\n',
            '            "🛑 <b>Enter Custom Stop Loss</b>\\n\\n"\n',
            '            "Send the percentage (e.g., 25):",\n',
            '            parse_mode="HTML"\n',
            '        )\n',
            '        context.user_data["awaiting_sl_custom"] = {"mint": mint, "amount": amount, "tp": tp}\n',
            '        await query.answer()\n'
        ]
        
        # Replace existing body
        # Find where next function starts to know where to stop replacing
        func_end = -1
        for i in range(func_start + 1, len(lines)):
            if lines[i].startswith('async def '):
                func_end = i
                break
        
        if func_end == -1: func_end = len(lines)
        
        # Replace
        lines[func_start+1:func_end] = new_handler
        print("SUCCESS: Updated buy_token_callback_handler")
        
        # 3. Update _execute_manual_buy signature and call
        # Find definition
        exec_start = -1
        for i, line in enumerate(lines):
            if 'async def _execute_manual_buy' in line:
                exec_start = i
                break
        
        if exec_start != -1:
            lines[exec_start] = 'async def _execute_manual_buy(update, context, user_manager, portfolio_manager, mint, amount, tp=50.0, sl=20.0):\n'
            
            # Update the add_manual_position call inside
            for i in range(exec_start, min(exec_start + 50, len(lines))):
                if 'portfolio_manager.add_manual_position' in lines[i]:
                    lines[i] = '    success = portfolio_manager.add_manual_position(chat_id, mint, symbol, price, amount, tp, sl)\n'
                    break
            
            # Update success message to show TP/SL
            for i in range(exec_start, min(exec_start + 80, len(lines))):
                if 'msg = (' in lines[i] and '✅ <b>Buy Order Executed!</b>' in lines[i+1]:
                    # Insert TP/SL info into message
                    lines.insert(i+4, '        f"🎯 <b>TP:</b> {tp}% | 🛑 <b>SL:</b> {sl}%\\n"\n')
                    break
            
            print("SUCCESS: Updated _execute_manual_buy")

with open(file_path, 'w', encoding='utf-8') as f:
    f.writelines(lines)


print("\nStep 3: Updating alerts/message_handler.py...")
file_path = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\message_handler.py"

with open(file_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Insert custom TP/SL handlers
# Find a good spot (e.g., after awaiting_buy_custom)

insert_idx = -1
for i, line in enumerate(lines):
    if 'awaiting_buy_custom' in line:
        # Find end of this block
        for j in range(i, min(i+50, len(lines))):
            if 'return' in lines[j] and lines[j].strip() == 'return':
                insert_idx = j + 1
                break
        break

if insert_idx != -1:
    indent = '        '
    handlers = [
        '\n',
        f'{indent}# Custom TP for Buy\n',
        f'{indent}if context.user_data.get("awaiting_tp_custom"):\n',
        f'{indent}    data = context.user_data.pop("awaiting_tp_custom")\n',
        f'{indent}    try:\n',
        f'{indent}        tp = float(text)\n',
        f'{indent}        if tp <= 0:\n',
        f'{indent}            await update.message.reply_text("❌ TP must be positive.")\n',
        f'{indent}            return\n',
        f'{indent}        # Proceed to SL\n',
        f'{indent}        from alerts.commands import ask_buy_sl\n',
        f'{indent}        await ask_buy_sl(update, context, data["mint"], data["amount"], tp)\n',
        f'{indent}    except ValueError:\n',
        f'{indent}        await update.message.reply_text("❌ Invalid number.")\n',
        f'{indent}    return\n',
        '\n',
        f'{indent}# Custom SL for Buy\n',
        f'{indent}if context.user_data.get("awaiting_sl_custom"):\n',
        f'{indent}    data = context.user_data.pop("awaiting_sl_custom")\n',
        f'{indent}    try:\n',
        f'{indent}        sl = float(text)\n',
        f'{indent}        if sl <= 0:\n',
        f'{indent}            await update.message.reply_text("❌ SL must be positive.")\n',
        f'{indent}            return\n',
        f'{indent}        # Execute\n',
        f'{indent}        from alerts.commands import _execute_manual_buy\n',
        f'{indent}        await _execute_manual_buy(update, context, user_manager, portfolio_manager, data["mint"], float(data["amount"]), float(data["tp"]), sl)\n',
        f'{indent}    except ValueError:\n',
        f'{indent}        await update.message.reply_text("❌ Invalid number.")\n',
        f'{indent}    return\n'
    ]
    
    lines[insert_idx:insert_idx] = handlers
    print("SUCCESS: Added custom TP/SL message handlers")

with open(file_path, 'w', encoding='utf-8') as f:
    f.writelines(lines)

print("\nAll updates complete!")
