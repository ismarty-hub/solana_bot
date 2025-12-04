#!/usr/bin/env python3
"""
1. Add edit parameter to all menu functions in menu_navigation.py
2. Integrate RugCheck display into buy_token_process in commands.py
"""

import re

print("Step 1: Adding edit parameter to menu functions...")
file_path = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\menu_navigation.py"

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# Add edit=False parameter to all menu function definitions
menu_functions = [
    'show_main_menu',
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
    'show_help_menu'
]

for func_name in menu_functions:
    # Find function definition
    pattern = f'async def {func_name}\\(([^)]+)\\):'
    match = re.search(pattern, content)
    if match:
        params = match.group(1)
        if 'edit' not in params:
            new_params = params + ', edit=False'
            content = content.replace(match.group(0), f'async def {func_name}({new_params}):')

# Now add conditional edit/reply logic to each function
# Find where each function does reply_html and add conditional
old_pattern = r'await message\.reply_html\(menu_text, reply_markup=reply_markup\)'
new_pattern = '''if edit:
        await message.edit_text(menu_text, reply_markup=reply_markup, parse_mode="HTML")
    else:
        await message.reply_html(menu_text, reply_markup=reply_markup)'''

content = content.replace(
    'await message.reply_html(menu_text, reply_markup=reply_markup)',
    new_pattern
)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

print("SUCCESS: Updated menu_navigation.py with edit parameter")

print("\nStep 2: Integrating RugCheck into buy_token_process...")
file_path = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\commands.py"

with open(file_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Find buy_token_process function
found_idx = -1
for i, line in enumerate(lines):
    if 'async def buy_token_process' in line:
        found_idx = i
        break

if found_idx > 0:
    # Find where we call PriceFetcher.get_token_info
    token_info_idx = -1
    for i in range(found_idx, min(found_idx + 50, len(lines))):
        if 'token_info = await PriceFetcher.get_token_info(mint)' in lines[i]:
            token_info_idx = i
            break
    
    if token_info_idx > 0:
        # Add RugCheck call after token_info fetch
        indent = '    '
        rugcheck_lines = [
            '\n',
            f'{indent}# Fetch RugCheck security analysis\n',
            f'{indent}rugcheck_data = await PriceFetcher.get_rugcheck_analysis(mint)\n',
            '\n'
        ]
        
        for idx, line in enumerate(rugcheck_lines):
            lines.insert(token_info_idx + 1 + idx, line)
        
        # Now find where we build the message and add security section
        # Look for "msg += f\"\\n<b>Source:</b>"
        msg_idx = -1
        for i in range(token_info_idx + len(rugcheck_lines), min(token_info_idx + len(rugcheck_lines) + 80, len(lines))):
            if 'msg += f"\\n<b>Source:</b>' in lines[i] or 'msg += f"<b>Source:</b>' in lines[i]:
                msg_idx = i
                break
        
        if msg_idx > 0:
            # Insert security analysis after source line
            security_lines = [
                '\n',
                f'{indent}# Add RugCheck Security Analysis\n',
                f'{indent}if rugcheck_data:\n',
                f'{indent}    score = rugcheck_data.get("score", 0)\n',
                f'{indent}    if score >= 80:\n',
                f'{indent}        score_emoji, score_text = "🟢", "GOOD"\n',
                f'{indent}    elif score >= 60:\n',
                f'{indent}        score_emoji, score_text = "🟡", "FAIR"\n',
                f'{indent}    elif score >= 40:\n',
                f'{indent}        score_emoji, score_text = "🟠", "POOR"\n',
                f'{indent}    else:\n',
                f'{indent}        score_emoji, score_text = "🔴", "DANGER"\n',
                f'{indent}    \n',
                f'{indent}    msg += f"\\n\\n<b>🔒 SECURITY (RugCheck)</b>\\n"\n',
                f'{indent}    msg += f"Score: {{score_emoji}} {{score_text}} ({{score}}/100)\\n"\n',
                f'{indent}    \n',
                f'{indent}    # Key metrics\n',
                f'{indent}    insider_count = rugcheck_data.get("insider_wallets_count", 0)\n',
                f'{indent}    insider_pct = rugcheck_data.get("insider_supply_pct", 0)\n',
                f'{indent}    dev_pct = rugcheck_data.get("dev_supply_pct", 0)\n',
                f'{indent}    top_holders = rugcheck_data.get("top_holders_pct", 0)\n',
                f'{indent}    liq_locked = rugcheck_data.get("liquidity_locked_pct", 0)\n',
                f'{indent}    dev_sold = rugcheck_data.get("dev_sold", False)\n',
                f'{indent}    \n',
                f'{indent}    if insider_count > 0:\n',
                f'{indent}        msg += f"Insider Wallets: {{insider_count}}\\n"\n',
                f'{indent}    if insider_pct > 0:\n',
                f'{indent}        msg += f"Insider Supply: {{insider_pct:.1f}}%\\n"\n',
                f'{indent}    if dev_pct > 0:\n',
                f'{indent}        msg += f"Dev Supply: {{dev_pct:.1f}}%\\n"\n',
                f'{indent}    msg += f"Top 10 Holders: {{top_holders:.1f}}%\\n"\n',
                f'{indent}    msg += f"Liquidity Locked: {{liq_locked:.1f}}%\\n"\n',
                f'{indent}    if dev_sold:\n',
                f'{indent}        msg += "⚠️ Dev/Creator Sold\\n"\n',
                f'{indent}    \n',
                f'{indent}    # Top risks\n',
                f'{indent}    risks = rugcheck_data.get("risks", [])\n',
                f'{indent}    if risks:\n',
                f'{indent}        msg += "\\n⚠️ Risks:\\n"\n',
                f'{indent}        for risk in risks[:3]:\n',
                f'{indent}            msg += f"• {{risk.get(\\'name\\', \\'Unknown\\')}}\\n"\n',
                '\n'
            ]
            
            for idx, line in enumerate(security_lines):
                lines.insert(msg_idx + 1 + idx, line)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                f.writelines(lines)
            
            print("SUCCESS: Integrated RugCheck into buy_token_process")
        else:
            print("ERROR: Could not find message source line")
    else:
        print("ERROR: Could not find token_info line")
else:
    print("ERROR: Could not find buy_token_process function")

print("\nAll updates complete!")
