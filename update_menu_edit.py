#!/usr/bin/env python3
"""Update all menu functions to support editing messages"""

import re

file_path = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\menu_navigation.py"

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# List of functions to update
functions_to_update = [
    'show_alerts_menu',
    'show_alert_grades_menu',
    'show_alpha_alerts_menu',
    'show_trading_menu',
    'show_enable_trading_menu',
    'show_reset_capital_menu',
    'show_ml_menu',
    'show_settings_menu',
    'show_mode_selection_menu',
    'show_tp_settings_menu',
    'show_help_menu',
    'show_help_topic'
]

# Pattern to match function signatures and add edit parameter
for func_name in functions_to_update:
   # Add edit=False parameter to function signature
    pattern = f'async def {func_name}\\(message(.*?)\\):'
    replacement = f'async def {func_name}(message\\1, edit=False):'
    content = re.sub(pattern, replacement, content)
    
# Replace all reply_html calls with conditional edit/reply logic
# Match: await message.reply_html(text, reply_markup=reply_markup)
pattern = r'await message\.reply_html\((.*?), reply_markup=(.*?)\)'
replacement = (
    r'if edit:\n'
    r'        await message.edit_text(\1, reply_markup=\2, parse_mode="HTML")\n'
    r'    else:\n'
    r'        await message.reply_html(\1, reply_markup=\2)'
)

# This won't work with simple regex, need a more targeted approach
# Let's just replace the specific await message.reply_html patterns

original_patterns = [
    'await message.reply_html(menu_text, reply_markup=reply_markup)',
    'await message.reply_html(text, reply_markup=reply_markup)',
]

for pattern in original_patterns:
    new_code = (
        'if edit:\n'
        '        await message.edit_text(menu_text if "menu_text" in locals() else text, reply_markup=reply_markup, parse_mode="HTML")\n'
        '    else:\n'
        '        ' + pattern
    )
    content = content.replace('    ' + pattern, '    ' + new_code)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

print("SUCCESS: Updated all menu functions to support edit parameter!")
print(f"Updated functions: {', '.join(functions_to_update)}")
