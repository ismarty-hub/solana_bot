#!/usr/bin/env python3
"""Fix positions_detail to include mint and signal_type for sell buttons"""

file_path = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\trade_manager.py"

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

old_code = '''        positions_detail.append({
            "symbol": pos["symbol"],
            "current_price": current_price,
            "unrealized_pnl_usd": unrealized_pnl,
            "unrealized_pnl_pct": unrealized_pct,
            "token_balance": token_balance
        })'''

new_code = '''        positions_detail.append({
            "symbol": pos["symbol"],
            "mint": pos["mint"],
            "signal_type": pos["signal_type"],
            "current_price": current_price,
            "unrealized_pnl_usd": unrealized_pnl,
            "unrealized_pnl_pct": unrealized_pct,
            "token_balance": token_balance
        })'''

if old_code in content:
    content = content.replace(old_code, new_code)
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    print("SUCCESS: Added mint and signal_type to positions_detail!")
else:
    print("ERROR: Could not find the code to replace")
    # Try to find if it exists with different whitespace
    if '"symbol": pos["symbol"]' in content and '"token_balance": token_balance' in content:
        print("Found partial match - checking for similar pattern")
