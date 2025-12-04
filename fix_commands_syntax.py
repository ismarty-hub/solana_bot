"""Script to fix syntax errors in commands.py by replacing corrupted functions."""

# Read the file
with open(r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\commands.py", "r", encoding="utf-8") as f:
    content = f.read()

# Define the replacement code for the three missing functions
replacement = '''        ],
        [
            InlineKeyboardButton("Custom", callback_data=f"set_buy_tp_custom:{mint}:{amount}"),
            InlineKeyboardButton("Skip (No TP)", callback_data=f"set_buy_tp:{mint}:{amount}:99999")
        ],
        [InlineKeyboardButton("❌ Cancel", callback_data="delete_msg")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    msg = (
        f"💰 <b>Amount Set:</b> ${float(amount):.2f}\\n\\n"
        f"🎯 <b>Select Take Profit (TP)</b>\\n"
        f"At what percentage gain should the bot sell?"
    )
    
    if update.callback_query:
        await update.callback_query.edit_message_text(msg, reply_markup=reply_markup, parse_mode="HTML")
    else:
        await update.message.reply_html(msg, reply_markup=reply_markup)

async def ask_buy_sl(update, context, mint, amount, tp):
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
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    tp_display = "None" if float(tp) >= 99999 else f"{tp}%"
    msg = (
        f"💰 <b>Amount:</b> ${float(amount):.2f}\\n"
        f"🎯 <b>TP:</b> {tp_display}\\n\\n"
        f"🛑 <b>Select Stop Loss (SL)</b>\\n"
        f"At what percentage loss should the bot sell?"
    )
    
    if update.callback_query:
        await update.callback_query.edit_message_text(msg, reply_markup=reply_markup, parse_mode="HTML")
    else:
        await update.message.reply_html(msg, reply_markup=reply_markup)

async def buy_token_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE,
                                   user_manager: UserManager, portfolio_manager: PortfolioManager):
    """Handle buy amount and TP/SL selection callbacks."""
    query = update.callback_query
    data = query.data
    chat_id = str(query.from_user.id)
    
    # Step 1: Amount Selected -> Ask TP
    if data.startswith("buy_amount:"):
        _, mint, amount_str = data.split(":")
        await ask_buy_tp(update, context, mint, amount_str)
        
    # Step 2: TP Selected -> Ask SL
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
            await _execute_manual_buy(update, context, user_manager, portfolio_manager, mint, float(amount), float(tp), float(sl))

    # Custom Inputs
    elif data.startswith("buy_custom:"):
        _, mint = data.split(":")
        await query.message.reply_text(
            "💰 <b>Enter Custom Amount</b>\\n\\n"
            f"Send the amount in USD to buy {mint}\\n"
            "Example: <code>250</code>",
            parse_mode="HTML"
        )
        context.user_data["awaiting_buy_custom"] = mint
        await query.answer()
        
    elif data.startswith("set_buy_tp_custom:"):
        _, mint, amount = data.split(":")
        await query.message.reply_text(
            "🎯 <b>Enter Custom Take Profit</b>\\n\\n"
            "Send the percentage (e.g., 150):",
            parse_mode="HTML"
        )
        context.user_data["awaiting_tp_custom"] = {"mint": mint, "amount": amount}
        await query.answer()
        
    elif data.startswith("set_buy_sl_custom:"):
        _, mint, amount, tp = data.split(":")
        await query.message.reply_text(
            "🛑 <b>Enter Custom Stop Loss</b>\\n\\n"
            "Send the percentage (e.g., 25):",
            parse_mode="HTML"
        )
        context.user_data["awaiting_sl_custom"] = {"mint": mint, "amount": amount, "tp": tp}
        await query.answer()

async def _execute_manual_buy(update, context, user_manager, portfolio_manager, mint, amount, tp=50.0, sl=20.0):
    """Execute the trade and confirm."""
    from alerts.price_fetcher import PriceFetcher
    
    # Re-fetch price to be accurate at execution time
    token_info = await PriceFetcher.get_token_info(mint)
    if not token_info:
        if update.callback_query:
            await update.callback_query.message.edit_text("❌ Failed to fetch latest price. Try again.")
        else:
            await update.message.reply_text("❌ Failed to fetch latest price. Try again.")
        return
        
    price = token_info.get("price", 0.0)
    symbol = token_info.get("symbol", "UNKNOWN")
    
    # Add position
    chat_id = str(update.effective_chat.id)
    
    success = portfolio_manager.add_manual_position(chat_id, mint, symbol, price, amount, tp, sl)
    
    msg = (
        f"✅ <b>Buy Successful!</b>\\n\\n"
        f"💎 <b>Token:</b> {symbol}\\n"
        f"💵 <b>Amount:</b> ${amount:,.2f}\\n"
        f"💲 <b>Entry Price:</b> ${price:.6f}\\n\\n"
        f"Position added to portfolio."
    )
    
    if update.callback_query:
        await update.callback_query.message.edit_text(msg, parse_mode="HTML")
    else:
        await update.message.reply_html(msg)
'''

# Find the line numbers to replace
lines = content.split('\n')
start_line = -1
end_line = -1

for i, line in enumerate(lines):
    if i >= 1497 and '            InlineKeyboardButton("Custom",' in line:
        start_line = i
    if i > 1500 and start_line != -1 and 'async def closeposition_cmd' in line:
        end_line = i - 2  # Before the blank lines
        break

if start_line != -1 and end_line != -1:
    # Replace the corrupted section
    new_lines = lines[:start_line] + replacement.split('\n') + lines[end_line+1:]
    new_content = '\n'.join(new_lines)
    
    # Write back
    with open(r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\commands.py", "w", encoding="utf-8") as f:
        f.write(new_content)
    
    print(f"Successfully replaced lines {start_line+1} to {end_line+1}")
    print(f"Added {len(replacement.split(chr(10)))} lines of replacement code")
else:
    print(f"Could not find boundaries. start_line={start_line}, end_line={end_line}")
