"""Fix callback_data length issue by storing mint/amount in context.user_data."""

with open(r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\commands.py", "r", encoding="utf-8") as f:
    content = f.read()

# Fix ask_buy_tp to use shorter callback_data and store data in context
old_ask_buy_tp = '''async def ask_buy_tp(update, context, mint, amount):
    """Step 2: Ask for Take Profit percentage."""
    keyboard = [
        [
            InlineKeyboardButton("25%", callback_data=f"set_buy_tp:{mint}:{amount}:25"),
            InlineKeyboardButton("50%", callback_data=f"set_buy_tp:{mint}:{amount}:50"),
            InlineKeyboardButton("100%", callback_data=f"set_buy_tp:{mint}:{amount}:100")
        ],
        [
            InlineKeyboardButton("Custom", callback_data=f"set_buy_tp_custom:{mint}:{amount}"),
            InlineKeyboardButton("Skip (No TP)", callback_data=f"set_buy_tp:{mint}:{amount}:99999")
        ],
        [InlineKeyboardButton("❌ Cancel", callback_data="delete_msg")]
    ]'''

new_ask_buy_tp = '''async def ask_buy_tp(update, context, mint, amount):
    """Step 2: Ask for Take Profit percentage."""
    # Store mint and amount in context to avoid callback_data length limit (64 bytes)
    context.user_data["buy_mint"] = mint
    context.user_data["buy_amount"] = amount
    
    keyboard = [
        [
            InlineKeyboardButton("25%", callback_data="buy_tp:25"),
            InlineKeyboardButton("50%", callback_data="buy_tp:50"),
            InlineKeyboardButton("100%", callback_data="buy_tp:100")
        ],
        [
            InlineKeyboardButton("Custom", callback_data="buy_tp_custom"),
            InlineKeyboardButton("Skip (No TP)", callback_data="buy_tp:99999")
        ],
        [InlineKeyboardButton("❌ Cancel", callback_data="delete_msg")]
    ]'''

content = content.replace(old_ask_buy_tp, new_ask_buy_tp)

# Fix ask_buy_sl similarly
old_ask_buy_sl = '''async def ask_buy_sl(update, context, mint, amount, tp):
    """Step 3: Ask for Stop Loss percentage."""
    keyboard = [
        [
            InlineKeyboardButton("10%", callback_data=f"set_buy_sl:{mint}:{amount}:{tp}:10"),
            InlineKeyboardButton("20%", callback_data=f"set_buy_sl:{mint}:{amount}:{tp}:20"),
            InlineKeyboardButton("30%", callback_data=f"set_buy_sl:{mint}:{amount}:{tp}:30")
        ],
        [
            InlineKeyboardButton("Custom", callback_data=f"set_buy_sl_custom:{mint}:{amount}:{tp}"),
            InlineKeyboardButton("Skip (No SL)", callback_data=f"set_buy_sl:{mint}:{amount}:{tp}:-999")
        ],
        [InlineKeyboardButton("❌ Cancel", callback_data="delete_msg")]
    ]'''

new_ask_buy_sl = '''async def ask_buy_sl(update, context, mint, amount, tp):
    """Step 3: Ask for Stop Loss percentage."""
    # Store tp in context (mint and amount already stored)
    context.user_data["buy_tp"] = tp
    
    keyboard = [
        [
            InlineKeyboardButton("10%", callback_data="buy_sl:10"),
            InlineKeyboardButton("20%", callback_data="buy_sl:20"),
            InlineKeyboardButton("30%", callback_data="buy_sl:30")
        ],
        [
            InlineKeyboardButton("Custom", callback_data="buy_sl_custom"),
            InlineKeyboardButton("Skip (No SL)", callback_data="buy_sl:-999")
        ],
        [InlineKeyboardButton("❌ Cancel", callback_data="delete_msg")]
    ]'''

content = content.replace(old_ask_buy_sl, new_ask_buy_sl)

# Fix buy_token_callback_handler to use the new callback_data format
old_handler = '''    # Step 2: TP Selected -> Ask SL
    elif data.startswith("set_buy_tp:"):
        parts = data.split(":")
        # Format: set_buy_tp:mint:amount:tp_val
        if len(parts) == 4:
            _, mint, amount, tp = parts
            await ask_buy_sl(update, context, mint, amount, tp)

    # Step 3: SL Selected -> Execute
    elif data.startswith("set_buy_sl:"):
        parts = data.split(":")
        # Format: set_buy_sl:mint:amount:tp:sl_val
        if len(parts) == 5:
            _, mint, amount, tp, sl = parts
            await _execute_manual_buy(update, context, user_manager, portfolio_manager, mint, float(amount), float(tp), float(sl))'''

new_handler = '''    # Step 2: TP Selected -> Ask SL
    elif data.startswith("buy_tp:"):
        tp = data.split(":")[1]
        mint = context.user_data.get("buy_mint")
        amount = context.user_data.get("buy_amount")
        if mint and amount:
            await ask_buy_sl(update, context, mint, amount, tp)

    # Step 3: SL Selected -> Execute
    elif data.startswith("buy_sl:"):
        sl = data.split(":")[1]
        mint = context.user_data.get("buy_mint")
        amount = context.user_data.get("buy_amount")
        tp = context.user_data.get("buy_tp")
        if mint and amount and tp:
            await _execute_manual_buy(update, context, user_manager, portfolio_manager, mint, float(amount), float(tp), float(sl))'''

content = content.replace(old_handler, new_handler)

# Fix custom TP/SL handlers
old_custom_tp = '''    elif data.startswith("set_buy_tp_custom:"):
        _, mint, amount = data.split(":")
        await query.message.reply_text(
            "🎯 <b>Enter Custom Take Profit</b>\\n\\n"
            "Send the percentage (e.g., 150):",
            parse_mode="HTML"
        )
        context.user_data["awaiting_tp_custom"] = {"mint": mint, "amount": amount}
        await query.answer()'''

new_custom_tp = '''    elif data == "buy_tp_custom":
        mint = context.user_data.get("buy_mint")
        amount = context.user_data.get("buy_amount")
        await query.message.reply_text(
            "🎯 <b>Enter Custom Take Profit</b>\\n\\n"
            "Send the percentage (e.g., 150):",
            parse_mode="HTML"
        )
        context.user_data["awaiting_tp_custom"] = {"mint": mint, "amount": amount}
        await query.answer()'''

content = content.replace(old_custom_tp, new_custom_tp)

old_custom_sl = '''    elif data.startswith("set_buy_sl_custom:"):
        _, mint, amount, tp = data.split(":")
        await query.message.reply_text(
            "🛑 <b>Enter Custom Stop Loss</b>\\n\\n"
            "Send the percentage (e.g., 25):",
            parse_mode="HTML"
        )
        context.user_data["awaiting_sl_custom"] = {"mint": mint, "amount": amount, "tp": tp}
        await query.answer()'''

new_custom_sl = '''    elif data == "buy_sl_custom":
        mint = context.user_data.get("buy_mint")
        amount = context.user_data.get("buy_amount")
        tp = context.user_data.get("buy_tp")
        await query.message.reply_text(
            "🛑 <b>Enter Custom Stop Loss</b>\\n\\n"
            "Send the percentage (e.g., 25):",
            parse_mode="HTML"
        )
        context.user_data["awaiting_sl_custom"] = {"mint": mint, "amount": amount, "tp": tp}
        await query.answer()'''

content = content.replace(old_custom_sl, new_custom_sl)

# Write back
with open(r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\commands.py", "w", encoding="utf-8") as f:
    f.write(content)

print("Successfully fixed callback_data length issue!")
print("- Storing mint/amount in context.user_data instead of callback_data")
print("- Shortened callback_data to under 64 bytes")
