#!/usr/bin/env python3
"""
Update commands.py to support "No TP" and "No SL" via Skip buttons.
Uses sentinel values: TP=99999, SL=-999
"""

file_path = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\commands.py"

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Update ask_buy_tp button
old_tp_btn = 'InlineKeyboardButton("Skip (Default 50%)", callback_data=f"set_buy_tp:{mint}:{amount}:50")'
new_tp_btn = 'InlineKeyboardButton("Skip (No TP)", callback_data=f"set_buy_tp:{mint}:{amount}:99999")'

content = content.replace(old_tp_btn, new_tp_btn)

# 2. Update ask_buy_sl button
old_sl_btn = 'InlineKeyboardButton("Skip (Default 20%)", callback_data=f"set_buy_sl:{mint}:{amount}:{tp}:20")'
new_sl_btn = 'InlineKeyboardButton("Skip (No SL)", callback_data=f"set_buy_sl:{mint}:{amount}:{tp}:-999")'

content = content.replace(old_sl_btn, new_sl_btn)

# 3. Update ask_buy_sl display logic for TP
# We need to find the msg construction in ask_buy_sl
# It looks like: f"🎯 <b>TP:</b> {tp}%\n\n"
# We want to replace it with logic to show "None" if 99999

# Since we can't easily inject logic inside the f-string in the existing code structure without parsing,
# we'll replace the whole function body or use a clever replace.

# Let's replace the msg definition block in ask_buy_sl
old_msg_block = '''    msg = (
        f"💰 <b>Amount:</b> ${float(amount):.2f}\\n"
        f"🎯 <b>TP:</b> {tp}%\\n\\n"
        f"🛑 <b>Select Stop Loss (SL)</b>\\n"
        f"At what percentage loss should the bot sell?"
    )'''

new_msg_block = '''    tp_display = "None" if float(tp) >= 99999 else f"{tp}%"
    msg = (
        f"💰 <b>Amount:</b> ${float(amount):.2f}\\n"
        f"🎯 <b>TP:</b> {tp_display}\\n\\n"
        f"🛑 <b>Select Stop Loss (SL)</b>\\n"
        f"At what percentage loss should the bot sell?"
    )'''

content = content.replace(old_msg_block, new_msg_block)


# 4. Update _execute_manual_buy display logic
# We need to handle both TP and SL display
# Look for: f"🎯 <b>TP:</b> {tp}% | 🛑 <b>SL:</b> {sl}%\n"

old_exec_msg = '        f"🎯 <b>TP:</b> {tp}% | 🛑 <b>SL:</b> {sl}%\\n"'

# We need to calculate display values before this.
# This is tricky with simple replace.
# Let's replace the whole message construction block in _execute_manual_buy

# Find the block
start_marker = '    msg = ('
end_marker = '        f"✅ <b>Buy Order Executed!</b>\\n"'

# This is risky if there are multiple msg = (.
# Let's look for the specific context in _execute_manual_buy

# Alternative: We can just replace the line with a function call or variable if we insert the variable calculation before.
# But inserting before is hard with replace.

# Let's try to replace the line with an f-string that has inline logic?
# f"TP: {'None' if tp > 90000 else tp}%"
# Python f-strings support expressions!

new_exec_msg = '        f"🎯 <b>TP:</b> {\'None\' if float(tp) >= 99999 else f\'{tp}%\'} | 🛑 <b>SL:</b> {\'None\' if float(sl) <= -999 else f\'{sl}%\'}\\n"'

content = content.replace(old_exec_msg, new_exec_msg)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

print("SUCCESS: Updated commands.py for No TP/SL support")
