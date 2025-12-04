#!/usr/bin/env python3
"""Update trade_manager.py for manual positions"""

file_path = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\trade_manager.py"

with open(file_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# 1. Update check_and_exit_positions
# Find: # --- 1. UPDATE LIVE DATA ---
target_idx = -1
for i, line in enumerate(lines):
    if "# --- 1. UPDATE LIVE DATA ---" in line:
        target_idx = i
        break

if target_idx != -1:
    # Insert manual logic before the existing logic
    new_code = [
        '            if signal_type == "manual":\n',
        '                # Fetch live price for manual position\n',
        '                from alerts.price_fetcher import PriceFetcher\n',
        '                token_info = await PriceFetcher.get_token_info(mint)\n',
        '                if token_info:\n',
        '                    pos["current_price"] = token_info["price"]\n',
        '                    pos["last_updated"] = datetime.now(timezone.utc).isoformat() + "Z"\n',
        '                    \n',
        '                    # Calculate ROI\n',
        '                    entry_price = pos["entry_price"]\n',
        '                    if entry_price > 0:\n',
        '                        current_roi = ((pos["current_price"] - entry_price) / entry_price) * 100\n',
        '                        pos["current_roi"] = current_roi\n',
        '                        \n',
        '                        # Update ATH\n',
        '                        if pos["current_price"] > pos.get("ath_price", 0):\n',
        '                            pos["ath_price"] = pos["current_price"]\n',
        '                            pos["ath_roi"] = current_roi\n',
        '            elif data:\n'
    ]
    
    # Replace "            if data:" with the new block
    if "if data:" in lines[target_idx + 1]:
        lines.pop(target_idx + 1) # Remove "if data:"
        for j, line in enumerate(new_code):
            lines.insert(target_idx + 1 + j, line)
        print("SUCCESS: Updated check_and_exit_positions")
    else:
        print("ERROR: Unexpected line after UPDATE LIVE DATA")
else:
    print("ERROR: Could not find UPDATE LIVE DATA")

# 2. Add add_manual_position method
# Find end of class or insert before check_and_exit_positions
# Let's insert before check_and_exit_positions
insert_idx = -1
for i, line in enumerate(lines):
    if "async def check_and_exit_positions" in line:
        insert_idx = i
        break

if insert_idx != -1:
    new_method = [
        '    def add_manual_position(self, chat_id: str, mint: str, symbol: str, price: float, amount_usd: float) -> bool:\n',
        '        """Add a manually purchased position."""\n',
        '        portfolio = self.get_portfolio(chat_id)\n',
        '        \n',
        '        # Deduct capital\n',
        '        if portfolio["capital_usd"] < amount_usd:\n',
        '            return False\n',
        '            \n',
        '        portfolio["capital_usd"] -= amount_usd\n',
        '        \n',
        '        token_amount = amount_usd / price if price > 0 else 0\n',
        '        \n',
        '        position_key = f"{mint}_manual"\n',
        '        \n',
        '        # If position exists, average down/up\n',
        '        if position_key in portfolio["positions"]:\n',
        '            pos = portfolio["positions"][position_key]\n',
        '            total_tokens = pos["token_amount"] + token_amount\n',
        '            total_cost = (pos["token_amount"] * pos["entry_price"]) + amount_usd\n',
        '            avg_price = total_cost / total_tokens\n',
        '            \n',
        '            pos["token_amount"] = total_tokens\n',
        '            pos["entry_price"] = avg_price\n',
        '            pos["avg_buy_price"] = avg_price\n',
        '            pos["status"] = "active"\n',
        '        else:\n',
        '            # Create new position\n',
        '            portfolio["positions"][position_key] = {\n',
        '                "mint": mint,\n',
        '                "symbol": symbol,\n',
        '                "signal_type": "manual",\n',
        '                "entry_price": price,\n',
        '                "avg_buy_price": price,\n',
        '                "token_amount": token_amount,\n',
        '                "entry_time": datetime.now(timezone.utc).isoformat() + "Z",\n',
        '                "tracking_end_time": (datetime.now(timezone.utc) + timedelta(days=365)).isoformat() + "Z",\n',
        '                "status": "active",\n',
        '                "tp_used": 50.0, # Default TP\n',
        '                "sl_used": -50.0, # Default SL\n',
        '                "current_price": price,\n',
        '                "current_roi": 0.0,\n',
        '                "ath_price": price,\n',
        '                "ath_roi": 0.0,\n',
        '                "last_updated": datetime.now(timezone.utc).isoformat() + "Z"\n',
        '            }\n',
        '            \n',
        '        self.save_portfolio(chat_id, portfolio)\n',
        '        return True\n',
        '    \n'
    ]
    
    for j, line in enumerate(new_method):
        lines.insert(insert_idx + j, line)
    print("SUCCESS: Added add_manual_position method")
else:
    print("ERROR: Could not find check_and_exit_positions")

with open(file_path, 'w', encoding='utf-8') as f:
    f.writelines(lines)
