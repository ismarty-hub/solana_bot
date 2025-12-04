#!/usr/bin/env python3
"""
Update trade_manager.py to support custom TP/SL and enforce Stop Loss
"""

file_path = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\trade_manager.py"

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Update add_manual_position signature and logic
old_add_sig = 'def add_manual_position(self, chat_id, mint, amount_usd, symbol, price):'
new_add_sig = 'def add_manual_position(self, chat_id, mint, amount_usd, symbol, price, tp_percent=50.0, sl_percent=20.0):'

if old_add_sig in content:
    content = content.replace(old_add_sig, new_add_sig)
    
    # Update the dictionary creation to use the passed values
    # We look for "tp_used": 50.0, and "sl_used": -50.0,
    # Note: sl_percent passed is usually positive (e.g. 20), but stored as negative (-20)
    
    # We'll replace the fixed values with the variables
    # Be careful with indentation
    
    old_defaults = '''                "tp_used": 50.0, # Default TP
                "sl_used": -50.0, # Default SL'''
    
    new_defaults = '''                "tp_used": float(tp_percent),
                "sl_used": -abs(float(sl_percent)), # Ensure negative'''
    
    content = content.replace(old_defaults, new_defaults)
    print("Updated add_manual_position")
else:
    print("Could not find add_manual_position signature")

# 2. Update check_and_exit_positions to enforce SL
# Find the TP CHECK section
tp_check_marker = '# --- 2. TP CHECK ---'
sl_check_code = '''            # --- 2. TP/SL CHECK ---
            # Check actual ATH from analytics against user TP
            ath_roi = float(pos.get("ath_roi", 0))
            current_roi = float(pos.get("current_roi", 0))
            
            # TP Check
            if ath_roi >= user_tp:
                # Exit at the actual peak recorded
                await self.exit_position(chat_id, key, "TP Hit 🎯", app, exit_roi=ath_roi)
                continue
                
            # SL Check
            sl_threshold = pos.get("sl_used", -50.0)
            if current_roi <= sl_threshold:
                # Exit at current ROI
                await self.exit_position(chat_id, key, "SL Hit 🛑", app, exit_roi=current_roi)
                continue'''

# We need to replace the existing TP check block
# It starts with # --- 2. TP CHECK --- and ends before # --- 3. EXPIRY CHECK ---

start_marker = '# --- 2. TP CHECK ---'
end_marker = '# --- 3. EXPIRY CHECK ---'

start_idx = content.find(start_marker)
end_idx = content.find(end_marker)

if start_idx != -1 and end_idx != -1:
    # Extract the block to replace to ensure we match correctly
    # But simpler to just construct the new content
    pre_content = content[:start_idx]
    post_content = content[end_idx:]
    
    content = pre_content + sl_check_code + "\n            \n            " + post_content
    print("Updated check_and_exit_positions with SL check")
else:
    print("Could not find TP CHECK block")

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)
