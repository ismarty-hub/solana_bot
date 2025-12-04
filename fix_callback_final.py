"""Direct line-by-line fix for callback_data length issue."""

with open(r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\commands.py", "r", encoding="utf-8") as f:
    lines = f.readlines()

# Function to replace ask_buy_tp
in_ask_buy_tp = False
tp_replaced = False
for i in range(len(lines)):
    line = lines[i]
    
    # Find the start of ask_buy_tp function
    if "async def ask_buy_tp(update, context, mint, amount):" in line and not tp_replaced:
        in_ask_buy_tp = True
        # Add context storage after the docstring
        if i+1 < len(lines) and '"""' in lines[i+1]:
            # Insert after docstring
            lines.insert(i+2, "    # Store mint and amount in context to avoid callback_data length limit (64 bytes)\n")
            lines.insert(i+3, "    context.user_data[\"buy_mint\"] = mint\n")
            lines.insert(i+4, "    context.user_data[\"buy_amount\"] = amount\n")
            lines.insert(i+5, "    \n")
            tp_replaced = True
    
    # Replace the callback_data lines within ask_buy_tp
    if in_ask_buy_tp and "callback_data=f\"set_buy_tp:" in line:
        if ":25\")" in line:
            lines[i] = line.replace('callback_data=f"set_buy_tp:{mint}:{amount}:25"', 'callback_data="buy_tp:25"')
        elif ":50\")" in line:
            lines[i] = line.replace('callback_data=f"set_buy_tp:{mint}:{amount}:50"', 'callback_data="buy_tp:50"')
        elif ":100\")" in line:
            lines[i] = line.replace('callback_data=f"set_buy_tp:{mint}:{amount}:100"', 'callback_data="buy_tp:100"')
        elif ":99999\")" in line:
            lines[i] = line.replace('callback_data=f"set_buy_tp:{mint}:{amount}:99999"', 'callback_data="buy_tp:99999"')
    
    if in_ask_buy_tp and "callback_data=f\"set_buy_tp_custom:" in line:
        lines[i] = line.replace('callback_data=f"set_buy_tp_custom:{mint}:{amount}"', 'callback_data="buy_tp_custom"')
    
    # End of ask_buy_tp function
    if in_ask_buy_tp and "async def ask_buy_sl" in line:
        in_ask_buy_tp = False

# Function to replace ask_buy_sl
in_ask_buy_sl = False
sl_replaced = False
for i in range(len(lines)):
    line = lines[i]
    
    # Find the start of ask_buy_sl function
    if "async def ask_buy_sl(update, context, mint, amount, tp):" in line and not sl_replaced:
        in_ask_buy_sl = True
        # Add context storage after the docstring
        if i+1 < len(lines) and '"""' in lines[i+1]:
            # Insert after docstring
            lines.insert(i+2, "    # Store tp in context (mint and amount already stored)\n")
            lines.insert(i+3, "    context.user_data[\"buy_tp\"] = tp\n")
            lines.insert(i+4, "    \n")
            sl_replaced = True
    
    # Replace the callback_data lines within ask_buy_sl
    if in_ask_buy_sl and "callback_data=f\"set_buy_sl:" in line:
        if ":10\")" in line:
            lines[i] = line.replace('callback_data=f"set_buy_sl:{mint}:{amount}:{tp}:10"', 'callback_data="buy_sl:10"')
        elif ":20\")" in line:
            lines[i] = line.replace('callback_data=f"set_buy_sl:{mint}:{amount}:{tp}:20"', 'callback_data="buy_sl:20"')
        elif ":30\")" in line:
            lines[i] = line.replace('callback_data=f"set_buy_sl:{mint}:{amount}:{tp}:30"', 'callback_data="buy_sl:30"')
        elif ":-999\")" in line:
            lines[i] = line.replace('callback_data=f"set_buy_sl:{mint}:{amount}:{tp}:-999"', 'callback_data="buy_sl:-999"')
    
    if in_ask_buy_sl and "callback_data=f\"set_buy_sl_custom:" in line:
        lines[i] = line.replace('callback_data=f"set_buy_sl_custom:{mint}:{amount}:{tp}"', 'callback_data="buy_sl_custom"')
    
    # End of ask_buy_sl function
    if in_ask_buy_sl and "async def buy_token_callback_handler" in line:
        in_ask_buy_sl = False

# Now fix the handler to use short callback_data
for i in range(len(lines)):
    line = lines[i]
    
    # Fix TP handler
    if 'elif data.startswith("set_buy_tp:")' in line:
        lines[i] = line.replace('data.startswith("set_buy_tp:")', 'data.startswith("buy_tp:")')
        # Fix the logic in following lines
        if i+1 < len(lines) and "parts = data.split" in lines[i+1]:
            lines[i+1] = "        tp = data.split(':')[1]\n"
        if i+2 < len(lines) and "# Format:" in lines[i+2]:
            lines[i+2] = "        mint = context.user_data.get('buy_mint')\n"
        if i+3 < len(lines) and "if len(parts)" in lines[i+3]:
            lines[i+3] = "        amount = context.user_data.get('buy_amount')\n"
        if i+4 < len(lines) and "_, mint, amount, tp = parts" in lines[i+4]:
            lines[i+4] = "        if mint and amount:\n"
        if i+5 < len(lines) and "await ask_buy_sl" in lines[i+5]:
            lines[i+5] = "            await ask_buy_sl(update, context, mint, amount, tp)\n"
    
    # Fix SL handler  
    if 'elif data.startswith("set_buy_sl:")' in line:
        lines[i] = line.replace('data.startswith("set_buy_sl:")', 'data.startswith("buy_sl:")')
        # Fix the logic in following lines
        if i+1 < len(lines) and "parts = data.split" in lines[i+1]:
            lines[i+1] = "        sl = data.split(':')[1]\n"
        if i+2 < len(lines) and "# Format:" in lines[i+2]:
            lines[i+2] = "        mint = context.user_data.get('buy_mint')\n"
        if i+3 < len(lines) and "if len(parts)" in lines[i+3]:
            lines[i+3] = "        amount = context.user_data.get('buy_amount')\n"
        if i+4 < len(lines) and "_, mint, amount, tp, sl = parts" in lines[i+4]:
            lines[i+4] = "        tp = context.user_data.get('buy_tp')\n"
        if i+5 < len(lines) and "await _execute_manual_buy" in lines[i+5]:
            lines[i+5] = "        if mint and amount and tp:\n"
        if i+6 < len(lines):
            lines.insert(i+6, "            await _execute_manual_buy(update, context, user_manager, portfolio_manager, mint, float(amount), float(tp), float(sl))\n")
    
    # Fix custom TP
    if 'elif data.startswith("set_buy_tp_custom:")' in line:
        lines[i] = line.replace('data.startswith("set_buy_tp_custom:")', 'data == "buy_tp_custom"')
        # Remove the split line
        if i+1 < len(lines) and "_, mint, amount = data.split" in lines[i+1]:
            lines[i+1] = "        mint = context.user_data.get('buy_mint')\n"
            lines.insert(i+2, "        amount = context.user_data.get('buy_amount')\n")
    
    # Fix custom SL
    if 'elif data.startswith("set_buy_sl_custom:")' in line:
        lines[i] = line.replace('data.startswith("set_buy_sl_custom:")', 'data == "buy_sl_custom"')
        # Remove the split line
        if i+1 < len(lines) and "_, mint, amount, tp = data.split" in lines[i+1]:
            lines[i+1] = "        mint = context.user_data.get('buy_mint')\n"
            lines.insert(i+2, "        amount = context.user_data.get('buy_amount')\n")
            lines.insert(i+3, "        tp = context.user_data.get('buy_tp')\n")

# Write back
with open(r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\commands.py", "w", encoding="utf-8") as f:
    f.writelines(lines)

print("Successfully fixed callback_data length issue!")
print(f"- Total lines processed: {len(lines)}")
print(f"- TP function context storage added: {tp_replaced}")
print(f"- SL function context storage added: {sl_replaced}")
