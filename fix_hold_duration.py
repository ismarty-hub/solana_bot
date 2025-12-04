#!/usr/bin/env python3
"""Fix trade_manager.py to add hold_duration_minutes to trade history"""

import re

file_path = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\trade_manager.py"

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# Find and replace the history_item creation
old_code = '''        # History
        history_item = {
            "symbol": pos["symbol"],
            "entry_price": pos["entry_price"],
            "exit_reason": reason,
            "pnl_usd": pnl_usd,
            "pnl_percent": exit_roi,
            "exit_time": datetime.now(timezone.utc).isoformat() + "Z",
            "signal_type": pos["signal_type"]
        }'''

new_code = '''        # History - Calculate hold duration
        entry_time = datetime.fromisoformat(pos["entry_time"].replace("Z", "+00:00"))
        exit_time = datetime.now(timezone.utc)
        hold_duration = exit_time - entry_time
        hold_duration_minutes = int(hold_duration.total_seconds() / 60)
        
        history_item = {
            "symbol": pos["symbol"],
            "entry_price": pos["entry_price"],
            "exit_reason": reason,
            "pnl_usd": pnl_usd,
            "pnl_percent": exit_roi,
            "exit_time": exit_time.isoformat() + "Z",
            "signal_type": pos["signal_type"],
            "hold_duration_minutes": hold_duration_minutes
        }'''

if old_code in content:
    content = content.replace(old_code, new_code)
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    print("SUCCESS: Added hold_duration_minutes to trade history!")
else:
    print("ERROR: Could not find the code to replace")
    print("Searching for similar patterns...")
    if "# History" in content and '"signal_type": pos["signal_type"]' in content:
        print("Found partial match - file might have different whitespace")
