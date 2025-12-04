#!/usr/bin/env python3
"""Simple fix for corrupted keyboard array in buy_token_process"""

file_path = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\commands.py"

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# Find and replace the exact corrupted section
corrupted_section = """        ],
        [
    query = update.callback_query
    data = query.data
    chat_id = str(query.from_user.id)
    
    if data.startswith("buy_amount:"):
        _, mint, amount_str = data.split(":")
        amount = float(amount_str)
        
        # Execute Buy
        await _execute_manual_buy(update, context, user_manager, portfolio_manager, mint, amount)
        
    elif data.startswith("buy_custom:"):
        _, mint = data.split(":")
        await query.message.reply_text(
            "💰 <b>Enter Custom Amount</b>\\n\\n"
            f"Send the amount in USD to buy {mint}\\n"
            "Example: <code>250</code>",
            parse_mode="HTML"
        )
        context.user_data['awaiting_buy_custom'] = True
        context.user_data['buy_mint'] = mint
        await query.answer()


async def _execute_manual_buy"""

# Replace with proper ending of keyboard and buy_token_process function
proper_section = """        ],
        [
            InlineKeyboardButton("Custom Amount", callback_data=f"buy_custom:{mint}")
        ],
        [InlineKeyboardButton("❌ Cancel", callback_data="delete_msg")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Format detailed metrics (if available from DexScreener)
    fdv = token_info.get("fdv", 0)
    volume24h = token_info.get("volume24h", 0)
    liquidity = token_info.get("liquidity", 0)
    price_change_24h = token_info.get("price_change_24h", 0)
    
    msg = (
        f"💎 <b>Found {name} ({symbol})</b>\\n"
        f"<code>{mint}</code>\\n\\n"
        f"<b>Price:</b> ${price:.6f}\\n"
    )
    
    # Add detailed metrics if available
    if fdv > 0:
        msg += f"<b>Market Cap (FDV):</b> ${fdv:,.0f}\\n"
    if volume24h > 0:
        msg += f"<b>24h Volume:</b> ${volume24h:,.0f}\\n"
    if liquidity > 0:
        msg += f"<b>Liquidity:</b> ${liquidity:,.0f}\\n"
    if price_change_24h != 0:
        change_emoji = "📈" if price_change_24h > 0 else "📉"
        msg += f"<b>24h Change:</b> {change_emoji} {price_change_24h:+.2f}%\\n"
    
    msg += f"\\n<b>Source:</b> {source.title()}\\n\\n💰 <b>Select Amount to Buy:</b>"
    
    await status_msg.edit_text(msg, reply_markup=reply_markup, parse_mode="HTML")


async def buy_token_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE,
                                   user_manager: UserManager, portfolio_manager: PortfolioManager):
    """Handle buy amount selection callbacks."""
    query = update.callback_query
    data = query.data
    chat_id = str(query.from_user.id)
    
    if data.startswith("buy_amount:"):
        _, mint, amount_str = data.split(":")
        amount = float(amount_str)
        
        # Execute Buy
        await _execute_manual_buy(update, context, user_manager, portfolio_manager, mint, amount)
        
    elif data.startswith("buy_custom:"):
        _, mint = data.split(":")
        await query.message.reply_text(
            "💰 <b>Enter Custom Amount</b>\\n\\n"
            f"Send the amount in USD to buy {mint}\\n"
            "Example: <code>250</code>",
            parse_mode="HTML"
        )
        context.user_data['awaiting_buy_custom'] = True
        context.user_data['buy_mint'] = mint
        await query.answer()


async def _execute_manual_buy"""

if corrupted_section in content:
    content = content.replace(corrupted_section, proper_section)
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    print("SUCCESS: Fixed corrupted buy_token_process and added enhanced metrics!")
else:
    print("ERROR: Corrupted section not found exactly as expected")
