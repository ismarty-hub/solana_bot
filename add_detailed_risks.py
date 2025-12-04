#!/usr/bin/env python3
"""
Script to add detailed risks list to the security insights display
"""

file_path = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\commands.py"

with open(file_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Find the location where we handle warnings
start_idx = -1
end_idx = -1

# We look for the block where we check for warnings and then append to msg
for i, line in enumerate(lines):
    if 'if warnings:' in line:
        start_idx = i
    if 'msg += f"\\n💰 <b>Select Amount to Buy:</b>"' in line:
        end_idx = i
        break

if start_idx != -1 and end_idx != -1:
    # We want to insert the new risks section BEFORE the "Select Amount to Buy" line
    # But AFTER the warnings loop.
    
    # Find the end of the warnings loop
    insert_idx = end_idx
    
    # New code to insert
    new_code = [
        '        \n',
        '        # 5. Detailed Risks (from API)\n',
        '        if risks:\n',
        '            msg += "\\n<b>⚠️ Potential Risks:</b>\\n"\n',
        '            for risk in risks:\n',
        '                r_name = risk.get("name", "Unknown")\n',
        '                r_desc = risk.get("description", "")\n',
        '                if r_desc:\n',
        '                    msg += f"• {r_name}: {r_desc}\\n"\n',
        '                else:\n',
        '                    msg += f"• {r_name}\\n"\n'
    ]
    
    # Insert the code
    for idx, line in enumerate(new_code):
        lines.insert(insert_idx + idx, line)
        
    with open(file_path, 'w', encoding='utf-8') as f:
        f.writelines(lines)
        
    print("SUCCESS: Added detailed risks section to display.")

else:
    print("ERROR: Could not find insertion point for risks.")
