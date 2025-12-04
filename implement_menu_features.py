#!/usr/bin/env python3
"""
1. Add current capital settings display to trading menu
2. Add edit parameter to all menu functions
"""

import re

print("Part 1: Adding capital settings display to trading menu...")
file_path = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\menu_navigation.py"

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# Find the enabled trading menu text section and update it
old_menu_text = '''        menu_text = (
            f"📈 <b>Paper Trading Menu</b>\\n\\n"
            f"<b>Status:</b> ✅ Enabled\\n"
            f"<b>Capital:</b> ${capital:,.2f}\\n"
            f"<b>Open Positions:</b> {positions}\\n\\n"
            f"Manage your paper trading portfolio below."
        )'''

new_menu_text = '''        # Get capital management settings
        user_prefs = user_manager.get_user_prefs(chat_id)
        reserve = user_prefs.get("reserve_balance", 0.0)
        min_trade = user_prefs.get("min_trade_size", 10.0)
        available = capital - reserve
        
        menu_text = (
            f"📈 <b>Paper Trading Menu</b>\\n\\n"
            f"<b>Status:</b> ✅ Enabled\\n"
            f"<b>Capital:</b> ${capital:,.2f}\\n"
            f"<b>Reserve:</b> ${reserve:,.2f}\\n"
            f"<b>Available:</b> ${available:,.2f}\\n"
            f"<b>Min Trade:</b> ${min_trade:,.2f}\\n"
            f"<b>Open Positions:</b> {positions}\\n\\n"
            f"Manage your paper trading portfolio below."
        )'''

if old_menu_text in content:
    content = content.replace(old_menu_text, new_menu_text)
    print("SUCCESS: Added capital settings display")
else:
    print("WARNING: Could not find exact trading menu text")

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

print("\nPart 2: Adding edit parameter to all menu functions...")

# Add edit=False parameter to all async def show_ functions
menu_functions = [
    'show_main_menu', 'show_alerts_menu', 'show_alert_grades_menu', 
    'show_alpha_alerts_menu', 'show_trading_menu', 'show_enable_trading_menu',
    'show_reset_capital_menu', 'show_ml_menu', 'show_settings_menu',
    'show_mode_selection_menu', 'show_tp_settings_menu', 'show_help_menu',
    'show_reserve_balance_menu', 'show_min_trade_size_menu'
]

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

for func in menu_functions:
    # Find function definition
    pattern = f'async def {func}\\(([^)]+)\\):'
    match = re.search(pattern, content)
    if match:
        params = match.group(1)
        if 'edit' not in params:
            new_params = params + ', edit=False'
            content = content.replace(match.group(0), f'async def {func}({new_params}):')
            print(f"  Added edit to {func}")

# Now update reply_html calls to use conditional logic
old_reply = 'await message.reply_html(menu_text, reply_markup=reply_markup)'
new_reply = '''if edit:
        await message.edit_text(menu_text, reply_markup=reply_markup, parse_mode="HTML")
    else:
        await message.reply_html(menu_text, reply_markup=reply_markup)'''

content = content.replace(old_reply, new_reply)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

print("SUCCESS: Added edit parameter to all menu functions")

print("\nPart 3: Updating menu_handler to pass edit=True...")
file_path = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\menu_handler.py"

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# Replace menu function calls to add edit=True
menu_calls = [
    ('show_main_menu(query.message, user_manager, chat_id)', 'show_main_menu(query.message, user_manager, chat_id, edit=True)'),
    ('show_alerts_menu(query.message, user_manager, chat_id)', 'show_alerts_menu(query.message, user_manager, chat_id, edit=True)'),
    ('show_alert_grades_menu(query.message)', 'show_alert_grades_menu(query.message, edit=True)'),
    ('show_alpha_alerts_menu(query.message, user_manager, chat_id)', 'show_alpha_alerts_menu(query.message, user_manager, chat_id, edit=True)'),
    ('show_trading_menu(query.message, user_manager, portfolio_manager, chat_id)', 'show_trading_menu(query.message, user_manager, portfolio_manager, chat_id, edit=True)'),
    ('show_enable_trading_menu(query.message)', 'show_enable_trading_menu(query.message, edit=True)'),
    ('show_reset_capital_menu(query.message)', 'show_reset_capital_menu(query.message, edit=True)'),
    ('show_ml_menu(query.message)', 'show_ml_menu(query.message, edit=True)'),
    ('show_settings_menu(query.message, user_manager, chat_id)', 'show_settings_menu(query.message, user_manager, chat_id, edit=True)'),
    ('show_mode_selection_menu(query.message)', 'show_mode_selection_menu(query.message, edit=True)'),
    ('show_tp_settings_menu(query.message)', 'show_tp_settings_menu(query.message, edit=True)'),
    ('show_help_menu(query.message)', 'show_help_menu(query.message, edit=True)'),
    ('show_reserve_balance_menu(query.message)', 'show_reserve_balance_menu(query.message, edit=True)'),
    ('show_min_trade_size_menu(query.message)', 'show_min_trade_size_menu(query.message, edit=True)'),
]

for old_call, new_call in menu_calls:
    content = content.replace(old_call, new_call)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

print("SUCCESS: Updated menu_handler to pass edit=True")

print("\nAll features implemented!")
print("\n Summary:")
print("1. Trading menu now shows Reserve Balance, Available Capital, and Min Trade Size")
print("2. All menu functions support edit parameter")
print("3. Menu navigation now edits messages instead of sending new ones")
