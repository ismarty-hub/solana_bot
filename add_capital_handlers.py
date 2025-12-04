#!/usr/bin/env python3
"""Add handlers for capital management callbacks"""

file_path = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\menu_handler.py"

with open(file_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Find where to insert handlers - look for handle_menu_callback function
# Add handlers before the final else/return

insert_idx = -1
for i, line in enumerate(lines):
    if 'elif data == "menu_help":' in line:
        # Insert before help menu callback
        insert_idx = i
        break

if insert_idx > 0:
    indent = '    '
    handlers = [
        '\n',
        f'{indent}# Capital Management Menus\n',
        f'{indent}elif data == "set_reserve_menu":\n',
        f'{indent}    await show_reserve_balance_menu(query.message)\n',
        f'{indent}    await query.answer()\n',
        f'{indent}    return\n',
        '\n',
        f'{indent}elif data == "set_mintrade_menu":\n',
        f'{indent}    await show_min_trade_size_menu(query.message)\n',
        f'{indent}    await query.answer()\n',
        f'{indent}    return\n',
        '\n',
        f'{indent}# Set Reserve Balance\n',
        f'{indent}elif data.startswith("set_reserve:"):\n',
        f'{indent}    _, amount_str = data.split(":")\n',
        f'{indent}    amount = float(amount_str)\n',
        f'{indent}    user_manager.update_user_prefs(chat_id, {{"reserve_balance": amount}})\n',
        f'{indent}    await query.edit_message_text(\n',
        f'{indent}        f"✅ <b>Reserve Balance Set!</b>\\\\n\\\\n"\n',
        f'{indent}        f"<b>Amount:</b> ${amount:,.2f}\\\\n\\\\n"\n',
        f'{indent}        f"Bot will keep this amount untouched.",\n',
        f'{indent}        parse_mode="HTML"\n',
        f'{indent}    )\n',
        f'{indent}    return\n',
        '\n',
        f'{indent}elif data == "set_reserve_custom":\n',
        f'{indent}    await query.message.reply_text(\n',
        f'{indent}        "💵 <b>Enter Custom Reserve Balance</b>\\\\n\\\\n"\n',
        f'{indent}        "Send the amount in USD:\\\\n"\n',
        f'{indent}        "Example: <code>150</code>",\n',
        f'{indent}        parse_mode="HTML"\n',
        f'{indent}    )\n',
        f'{indent}    context.user_data["awaiting_reserve_custom"] = True\n',
        f'{indent}    await query.answer()\n',
        f'{indent}    return\n',
        '\n',
        f'{indent}# Set Min Trade Size\n',
        f'{indent}elif data.startswith("set_mintrade:"):\n',
        f'{indent}    _, amount_str = data.split(":")\n',
        f'{indent}    amount = float(amount_str)\n',
        f'{indent}    user_manager.update_user_prefs(chat_id, {{"min_trade_size": amount}})\n',
        f'{indent}    await query.edit_message_text(\n',
        f'{indent}        f"✅ <b>Min Trade Size Set!</b>\\\\n\\\\n"\n',
        f'{indent}        f"<b>Amount:</b> ${amount:,.2f}\\\\n\\\\n"\n',
        f'{indent}        f"Bot will skip trades smaller than this.",\n',
        f'{indent}        parse_mode="HTML"\n',
        f'{indent}    )\n',
        f'{indent}    return\n',
        '\n',
        f'{indent}elif data == "set_mintrade_custom":\n',
        f'{indent}    await query.message.reply_text(\n',
        f'{indent}        "📏 <b>Enter Custom Min Trade Size</b>\\\\n\\\\n"\n',
        f'{indent}        "Send the amount in USD:\\\\n"\n',
        f'{indent}        "Example: <code>25</code>",\n',
        f'{indent}        parse_mode="HTML"\n',
        f'{indent}    )\n',
        f'{indent}    context.user_data["awaiting_mintrade_custom"] = True\n',
        f'{indent}    await query.answer()\n',
        f'{indent}    return\n',
        '\n',
    ]
    
    for idx, line in enumerate(handlers):
        lines.insert(insert_idx + idx, line)
    
    # Add imports at top if missing
    import_line = 'from alerts.menu_navigation import show_reserve_balance_menu, show_min_trade_size_menu\n'
    
    # Find imports section
    for i, line in enumerate(lines):
        if 'from alerts.menu_navigation import' in line and 'show_reserve' not in line:
            # Append to existing import
            lines[i] = lines[i].rstrip('\n') + ', show_reserve_balance_menu, show_min_trade_size_menu\n'
            break
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.writelines(lines)
    
    print("SUCCESS: Added capital management handlers to menu_handler.py")
else:
    print("ERROR: Could not find insertion point")

# Now add message handlers
print("\nAdding message handlers for custom input...")
file_path = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\message_handler.py"

with open(file_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Find end of awaiting_buy_custom handler and add new handlers
insert_idx = -1
for i, line in enumerate(lines):
    if 'awaiting_buy_custom' in line and 'context.user_data.get' in line:
        # Find the end of this if block
        for j in range(i, min(i + 50, len(lines))):
            if lines[j].strip() == 'return' and j > i + 5:
                insert_idx = j + 1
                break
        break

if insert_idx > 0:
    indent = '        '  # 8 spaces (inside try block)
    handlers = [
        '\n',
        f'{indent}# ====================================================================\n',
        f'{indent}# CAPITAL MANAGEMENT - CUSTOM RESERVE\n',
        f'{indent}# ====================================================================\n',
        f'{indent}if context.user_data.get("awaiting_reserve_custom"):\n',
        f'{indent}    context.user_data["awaiting_reserve_custom"] = False\n',
        f'{indent}    try:\n',
        f'{indent}        amount = float(text)\n',
        f'{indent}        if amount < 0:\n',
        f'{indent}            await update.message.reply_text("❌ Amount must be positive or zero.")\n',
        f'{indent}            return\n',
        f'{indent}        \n',
        f'{indent}        user_manager.update_user_prefs(chat_id, {{"reserve_balance": amount}})\n',
        f'{indent}        await update.message.reply_html(\n',
        f'{indent}            f"✅ <b>Reserve Balance Set!</b>\\\\n\\\\n"\n',
        f'{indent}            f"<b>Amount:</b> ${amount:,.2f}\\\\n\\\\n"\n',
        f'{indent}            f"Bot will keep this amount untouched."\n',
        f'{indent}        )\n',
        f'{indent}    except ValueError:\n',
        f'{indent}        await update.message.reply_text("❌ Invalid amount. Please send a number.")\n',
        f'{indent}    return\n',
        '\n',
        f'{indent}# ====================================================================\n',
        f'{indent}# CAPITAL MANAGEMENT - CUSTOM MIN TRADE\n',
        f'{indent}# ====================================================================\n',
        f'{indent}if context.user_data.get("awaiting_mintrade_custom"):\n',
        f'{indent}    context.user_data["awaiting_mintrade_custom"] = False\n',
        f'{indent}    try:\n',
        f'{indent}        amount = float(text)\n',
        f'{indent}        if amount <= 0:\n',
        f'{indent}            await update.message.reply_text("❌ Amount must be positive.")\n',
        f'{indent}            return\n',
        f'{indent}        \n',
        f'{indent}        user_manager.update_user_prefs(chat_id, {{"min_trade_size": amount}})\n',
        f'{indent}        await update.message.reply_html(\n',
        f'{indent}            f"✅ <b>Min Trade Size Set!</b>\\\\n\\\\n"\n',
        f'{indent}            f"<b>Amount:</b> ${amount:,.2f}\\\\n\\\\n"\n',
        f'{indent}            f"Bot will skip trades smaller than this."\n',
        f'{indent}        )\n',
        f'{indent}    except ValueError:\n',
        f'{indent}        await update.message.reply_text("❌ Invalid amount. Please send a number.")\n',
        f'{indent}    return\n',
        '\n',
    ]
    
    for idx, line in enumerate(handlers):
        lines.insert(insert_idx + idx, line)
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.writelines(lines)
    
    print("SUCCESS: Added message handlers for custom capital management input")
else:
    print("ERROR: Could not find insertion point in message_handler.py")

print("\n✅ Capital Management UI Complete!")
