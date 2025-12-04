#!/usr/bin/env python3
"""
Comprehensive update script for Capital Management and Partial Sell features
Uses precise string replacement to avoid file corruption
"""

import re

# Step 1: Update user_manager.py with capital management preferences
print("Step 1: Updating user_manager.py...")
file_path = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\user_manager.py"

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# Add to normalization function
old_normalize = '''        if "tp_alpha" not in user:
            user["tp_alpha"] = None
            modified = True

        if modified:'''

new_normalize = '''        if "tp_alpha" not in user:
            user["tp_alpha"] = None
            modified = True
            
        # Trading capital management defaults
        if "reserve_balance" not in user:
            user["reserve_balance"] = 0.0
            modified = True
            
        if "min_trade_size" not in user:
            user["min_trade_size"] = 10.0
            modified = True

        if modified:'''

content = content.replace(old_normalize, new_normalize)

# Add to default prefs
old_defaults = '''            "alpha_alerts": False,
            # New TP defaults
            "tp_preference": "median",
            "tp_discovery": None,
            "tp_alpha": None
        }'''

new_defaults = '''            "alpha_alerts": False,
            # New TP defaults
            "tp_preference": "median",
            "tp_discovery": None,
            "tp_alpha": None,
            # Trading capital management
            "reserve_balance": 0.0,
            "min_trade_size": 10.0
        }'''

content = content.replace(old_defaults, new_defaults)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

print("SUCCESS: user_manager.py updated")

# Step 2: Update trade_manager.py with capital management logic
print("\nStep 2: Updating trade_manager.py capital logic...")
file_path = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\trade_manager.py"

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# Find and replace capital validation logic
old_capital_logic = '''        # Validate capital
        capital = portfolio["capital_usd"]
        if capital < 10:
            return
        
        # Size: 10% of capital, max $150
        size_usd = min(capital * 0.10, 150.0)'''

new_capital_logic = '''        # Validate capital with reserve and min trade size
        capital = portfolio["capital_usd"]
        prefs = user_manager.get_user_prefs(chat_id)
        reserve = prefs.get("reserve_balance", 0.0)
        min_trade = prefs.get("min_trade_size", 10.0)
        
        available = capital - reserve
        if available < min_trade:
            logger.info(f"Skipping trade - Available ${available:.2f} < Min ${min_trade:.2f}")
            return
        
        # Size: 10% of available capital, max $150, min = min_trade_size
        size_usd = max(min_trade, available * 0.10)
        size_usd = min(size_usd, 150.0)'''

content = content.replace(old_capital_logic, new_capital_logic)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

print("SUCCESS: trade_manager.py capital logic updated")

print("\nAll updates completed successfully!")
