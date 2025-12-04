"""Fix message editing issues by adding try-except fallback to send new message."""

with open(r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\commands.py", "r", encoding="utf-8") as f:
    content = f.read()

# Replace ask_buy_tp function to add try-except
old_ask_buy_tp = '''    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(msg, reply_markup=reply_markup, parse_mode="HTML")
    else:
        await update.message.reply_html(msg, reply_markup=reply_markup)'''

new_ask_buy_tp = '''    if update.callback_query:
        await update.callback_query.answer()
        try:
            await update.callback_query.edit_message_text(msg, reply_markup=reply_markup, parse_mode="HTML")
        except Exception as e:
            # If edit fails, send new message
            await update.callback_query.message.reply_html(msg, reply_markup=reply_markup)
    else:
        await update.message.reply_html(msg, reply_markup=reply_markup)'''

content = content.replace(old_ask_buy_tp, new_ask_buy_tp, 1)

# Replace ask_buy_sl function similarly
old_ask_buy_sl = '''    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(msg, reply_markup=reply_markup, parse_mode="HTML")
    else:
        await update.message.reply_html(msg, reply_markup=reply_markup)

async def buy_token_callback_handler'''

new_ask_buy_sl = '''    if update.callback_query:
        await update.callback_query.answer()
        try:
            await update.callback_query.edit_message_text(msg, reply_markup=reply_markup, parse_mode="HTML")
        except Exception as e:
            # If edit fails, send new message
            await update.callback_query.message.reply_html(msg, reply_markup=reply_markup)
    else:
        await update.message.reply_html(msg, reply_markup=reply_markup)

async def buy_token_callback_handler'''

content = content.replace(old_ask_buy_sl, new_ask_buy_sl, 1)

# Write back
with open(r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\commands.py", "w", encoding="utf-8") as f:
    f.write(content)

print("Successfully added error handling for message editing")
