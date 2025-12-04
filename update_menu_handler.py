#!/usr/bin/env python3
"""Update menu_handler.py to pass edit=True for all menu navigation calls"""

file_path = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\menu_handler.py"

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# Replace all menu function calls to add edit=True when called from callbacks
# The pattern is: await show_*_menu(query.message, ...)

replacements = [
    ('await show_main_menu(query.message, user_manager, chat_id)',
     'await show_main_menu(query.message, user_manager, chat_id, edit=True)'),
    
    ('await show_alerts_menu(query.message, user_manager, chat_id)',
     'await show_alerts_menu(query.message, user_manager, chat_id, edit=True)'),
    
    ('await show_alert_grades_menu(query.message)',
     'await show_alert_grades_menu(query.message, edit=True)'),
    
    ('await show_alpha_alerts_menu(query.message, user_manager, chat_id)',
     'await show_alpha_alerts_menu(query.message, user_manager, chat_id, edit=True)'),
    
    ('await show_trading_menu(query.message, user_manager, portfolio_manager, chat_id)',
     'await show_trading_menu(query.message, user_manager, portfolio_manager, chat_id, edit=True)'),
    
    ('await show_enable_trading_menu(query.message)',
     'await show_enable_trading_menu(query.message, edit=True)'),
    
    ('await show_reset_capital_menu(query.message)',
     'await show_reset_capital_menu(query.message, edit=True)'),
    
    ('await show_ml_menu(query.message)',
     'await show_ml_menu(query.message, edit=True)'),
    
    ('await show_settings_menu(query.message, user_manager, chat_id)',
     'await show_settings_menu(query.message, user_manager, chat_id, edit=True)'),
    
    ('await show_mode_selection_menu(query.message)',
     'await show_mode_selection_menu(query.message, edit=True)'),
    
    ('await show_tp_settings_menu(query.message)',
     'await show_tp_settings_menu(query.message, edit=True)'),
    
    ('await show_help_menu(query.message)',
     'await show_help_menu(query.message, edit=True)'),
    
    ('await show_help_topic(query.message, "getting_started")',
     'await show_help_topic(query.message, "getting_started", edit=True)'),
    
    ('await show_help_topic(query.message, "alerts")',
     'await show_help_topic(query.message, "alerts", edit=True)'),
    
    ('await show_help_topic(query.message, "trading")',
     'await show_help_topic(query.message, "trading", edit=True)'),
    
    ('await show_help_topic(query.message, "ml")',
     'await show_help_topic(query.message, "ml", edit=True)'),
]

count = 0
for old, new in replacements:
    if old in content:
        content = content.replace(old, new)
        count += 1

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

print(f"SUCCESS: Updated {count} menu function calls with edit=True!")
