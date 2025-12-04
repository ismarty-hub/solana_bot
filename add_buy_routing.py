#!/usr/bin/env python3
"""Add buy callback routing to commands.py"""

file_path = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\commands.py"

with open(file_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Find the line "    # --- Handle Trading Button Callbacks ---"
target_idx = -1
for i, line in enumerate(lines):
    if "    # --- Handle Trading Button Callbacks ---" in line:
        target_idx = i
        break

if target_idx != -1:
    # Insert routing logic
    new_lines = [
        '    if data.startswith("buy_amount:") or data.startswith("buy_custom:"):\n',
        '        if portfolio_manager:\n',
        '            await buy_token_callback_handler(update, context, user_manager, portfolio_manager)\n',
        '        return\n',
        '    \n'
    ]
    
    # Insert after the comment
    for j, new_line in enumerate(new_lines):
        lines.insert(target_idx + 1 + j, new_line)
        
    with open(file_path, 'w', encoding='utf-8') as f:
        f.writelines(lines)
        
    print("SUCCESS: Added buy callback routing!")
else:
    print("ERROR: Could not find target line")
