#!/usr/bin/env python3
"""Fix corrupted buy_token_process in commands.py"""

file_path = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\commands.py"

with open(file_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Find and fix the corrupted section
# Looking for line 1376: `    keyboard = [` in buy_token_process
target_idx = -1
for i, line in enumerate(lines):
    if i > 1370 and 'InlineKeyboardButton("Custom Amount"' in line:
        target_idx = i
        break

if target_idx != -1:
    # The line after should be a closing bracket ], not "query = update.callback_query"
    # Let's find where the corruption starts and ends
    corruption_start = -1
    for i in range(target_idx, min(target_idx + 50, len(lines))):
        if "query = update.callback_query" in lines[i]:
            corruption_start = i
            break
    
    if corruption_start != -1:
        # Find where _execute_manual_buy starts
        exec_start = -1
        for i in range(corruption_start, min(corruption_start + 100, len(lines))):
            if "async def _execute_manual_buy" in lines[i]:
                exec_start = i
                break
        
        if exec_start != -1:
            # The corrupted section is from corruption_start to exec_start-2
            # It should be replaced with the proper end of buy_token_process
            
            proper_end = [
                '            InlineKeyboardButton("Custom Amount", callback_data=f"buy_custom:{mint}")\n',
                '        ],\n',
                '        [InlineKeyboardButton("❌ Cancel", callback_data="delete_msg")]\n',
                '    ]\n',
                '    \n',
                '    reply_markup = InlineKeyboardMarkup(keyboard)\n',
                '    \n',
                '    # Format detailed metrics (if available from DexScreener)\n',
                '    fdv = token_info.get("fdv", 0)\n',
                '    volume24h = token_info.get("volume24h", 0)\n',
                '    liquidity = token_info.get("liquidity", 0)\n',
                '    price_change_24h = token_info.get("price_change_24h", 0)\n',
                '    \n',
                '    msg = (\n',
                '        f"💎 <b>Found {name} ({symbol})</b>\\n"\n',
                '        f"<code>{mint}</code>\\n\\n"\n',
                '        f"<b>Price:</b> ${price:.6f}\\n"\n',
                '    )\n',
                '    \n',
                '    # Add detailed metrics if available\n',
                '    if fdv > 0:\n',
                '        msg += f"<b>Market Cap (FDV):</b> ${fdv:,.0f}\\n"\n',
                '    if volume24h > 0:\n',
                '        msg += f"<b>24h Volume:</b> ${volume24h:,.0f}\\n"\n',
                '    if liquidity > 0:\n',
                '        msg += f"<b>Liquidity:</b> ${liquidity:,.0f}\\n"\n',
                '    if price_change_24h != 0:\n',
                '        change_emoji = "📈" if price_change_24h > 0 else "📉"\n',
                '        msg += f"<b>24h Change:</b> {change_emoji} {price_change_24h:+.2f}%\\n"\n',
                '    \n',
                '    msg += f"\\n<b>Source:</b> {source.title()}\\n\\n💰 <b>Select Amount to Buy:</b>"\n',
                '    \n',
                '    await status_msg.edit_text(msg, reply_markup=reply_markup, parse_mode="HTML")\n',
                '\n',
                '\n',
                'async def buy_token_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE,\n',
                '                                   user_manager: UserManager, portfolio_manager: PortfolioManager):\n',
                '    """Handle buy amount selection callbacks."""\n',
            ]
            
            # Remove old line with partial Custom Amount button
            del lines[target_idx]
            
            # Insert proper ending
            for j, new_line in enumerate(proper_end):
                lines.insert(target_idx + j, new_line)
            
            # Find and remove the corrupted section (query = ... to before async def _execute)
            # which is now at a different index
            # Let's recalculate
            new_corruption_start = -1
            for i in range(target_idx + len(proper_end), min(target_idx + len(proper_end) + 50, len(lines))):
                if "query = update.callback_query" in lines[i] and "async def buy_token_callback_handler" not in lines[i-1]:
                    new_corruption_start = i
                    break
            
            if new_corruption_start != -1:
                new_exec_start = -1
                for i in range(new_corruption_start, min(new_corruption_start + 50, len(lines))):
                    if "async def _execute_manual_buy" in lines[i]:
                        new_exec_start = i
                        break
                
                if new_exec_start != -1:
                    # Delete from new_corruption_start to new_exec_start-2
                    for _ in range(new_exec_start - new_corruption_start - 2):
                        del lines[new_corruption_start]
            
            with open(file_path, 'w', encoding='utf-8') as f:
                f.writelines(lines)
            
            print("SUCCESS: Fixed corrupted buy_token_process and added detailed metrics!")
        else:
            print("ERROR: Could not find _execute_manual_buy")
    else:
        print("ERROR: Corruption not found where expected")
else:
    print("ERROR: Could not find target line")
