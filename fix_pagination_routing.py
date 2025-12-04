#!/usr/bin/env python3
"""Fix pagination callback routing in commands.py"""

file_path = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\commands.py"

with open(file_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Find and fix line 1047 (index 1046)
if 1046 < len(lines):
    old_line = lines[1046]
    print(f"Original line 1047: {repr(old_line)}")
    
    # Replace the problematic startswith checks with exact matches
    new_line = old_line.replace(
        'data.startswith("watchlist_") or data.startswith("portfolio_") or data.startswith("pnl_")',
        'data == "watchlist_direct" or data == "portfolio_direct" or data == "pnl_direct"'
    )
    
    if new_line != old_line:
        lines[1046] = new_line
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.writelines(lines)
        
        print(f"New line 1047: {repr(new_line)}")
        print("SUCCESS: Fixed pagination routing!")
    else:
        print("ERROR: Could not find the text to replace")
else:
    print("ERROR: File doesn't have enough lines")
