#!/usr/bin/env python3
"""Add mint and signal_type to positions_detail"""

file_path = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\trade_manager.py"

with open(file_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Find line 200 (index 199) and insert new lines
if len(lines) > 200 and '"symbol": pos["symbol"]' in lines[199]:
    # Insert mint and signal_type after symbol line
    lines.insert(200, '                "mint": pos["mint"],\n')
    lines.insert(201, '                "signal_type": pos["signal_type"],\n')
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.writelines(lines)
    
    print("SUCCESS: Added mint and signal_type fields!")
    print(f"Line 200 was: {repr(lines[199])}")
    print(f"Inserted at 201: {repr(lines[200])}")
    print(f"Inserted at 202: {repr(lines[201])}")
else:
    print("ERROR: Could not find the right line")
    if len(lines) > 200:
        print(f"Line 200 is: {repr(lines[199])}")
