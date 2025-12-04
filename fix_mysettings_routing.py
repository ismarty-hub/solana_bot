#!/usr/bin/env python3
"""Fix callback routing to include mysettings_direct"""

file_path = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\commands.py"

with open(file_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Find the line ending with enable_"):
target_line_idx = -1
for i, line in enumerate(lines):
    if 'data.startswith("enable_"):' in line:
        target_line_idx = i
        break

if target_line_idx != -1:
    old_line = lines[target_line_idx]
    print(f"Found line {target_line_idx+1}: {repr(old_line)}")
    
    # Replace the end of the line
    new_line = old_line.replace('data.startswith("enable_"):', 'data.startswith("enable_") or data == "mysettings_direct":')
    
    lines[target_line_idx] = new_line
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.writelines(lines)
    
    print("SUCCESS: Added mysettings_direct to routing!")
    print(f"New line: {repr(new_line)}")
else:
    print("ERROR: Could not find the target line")
