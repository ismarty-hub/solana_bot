#!/usr/bin/env python3
"""
Robust fix for datetime parsing in trade_manager.py
Strips all timezone suffixes and re-appends single UTC to avoid double suffix errors.
"""

file_path = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\trade_manager.py"

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# The block to replace
old_block = '''        # Handle both Z and +00:00 timezone formats
        entry_time_str = pos["entry_time"]
        if entry_time_str.endswith("Z"):
            entry_time_str = entry_time_str.replace("Z", "+00:00")
        elif "+00:00" in entry_time_str and entry_time_str.count("+00:00") > 1:
            # Fix double timezone suffix
            entry_time_str = entry_time_str.replace("+00:00+00:00", "+00:00")
        entry_time = datetime.fromisoformat(entry_time_str)'''

# The new robust block
new_block = '''        # Robust timezone handling: strip all potential suffixes and add single UTC
        entry_time_str = pos["entry_time"]
        # Remove Z and +00:00 (handle multiple occurrences)
        entry_time_str = entry_time_str.replace("Z", "").replace("+00:00", "")
        # Re-append single UTC timezone
        entry_time_str = entry_time_str + "+00:00"
        entry_time = datetime.fromisoformat(entry_time_str)'''

if old_block in content:
    content = content.replace(old_block, new_block)
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    print("SUCCESS: Applied robust datetime fix.")
else:
    print("ERROR: Could not find the target code block.")
    # Debug: print what we found around the area
    start_marker = 'stats["worst_trade"] = min(stats["worst_trade"], exit_roi)'
    end_marker = 'exit_time = datetime.now(timezone.utc)'
    
    start_idx = content.find(start_marker)
    end_idx = content.find(end_marker)
    
    if start_idx != -1 and end_idx != -1:
        print("\nFound content between markers:")
        print(content[start_idx:end_idx+len(end_marker)])
    else:
        print("\nCould not locate surrounding markers either.")
