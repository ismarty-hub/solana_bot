#!/usr/bin/env python3
"""Add Capital Management menu options to Trading menu"""

file_path = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\menu_navigation.py"

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# Find show_trading_menu and add buttons for capital management
# Look for the section with enabled trading buttons
old_buttons = '''        keyboard = [
            [InlineKeyboardButton("💼 View Portfolio", callback_data="portfolio_direct")],
            [InlineKeyboardButton("📊 View P&L", callback_data="pnl_direct")],
            [InlineKeyboardButton("📜 Trade History", callback_data="history_direct")],
            [InlineKeyboardButton("📈 Performance Stats", callback_data="performance_direct")],
            [InlineKeyboardButton("👀 Watchlist", callback_data="watchlist_direct")],
            [InlineKeyboardButton("💰 Reset Capital", callback_data="resetcapital_menu")],
            [InlineKeyboardButton("◀️ Back", callback_data="menu_main")]
        ]'''

new_buttons = '''        keyboard = [
            [InlineKeyboardButton("💼 View Portfolio", callback_data="portfolio_direct")],
            [InlineKeyboardButton("📊 View P&L", callback_data="pnl_direct")],
            [InlineKeyboardButton("📜 Trade History", callback_data="history_direct")],
            [InlineKeyboardButton("📈 Performance Stats", callback_data="performance_direct")],
            [InlineKeyboardButton("👀 Watchlist", callback_data="watchlist_direct")],
            [InlineKeyboardButton("💰 Reset Capital", callback_data="resetcapital_menu")],
            [InlineKeyboardButton("💵 Set Reserve Balance", callback_data="set_reserve_menu")],
            [InlineKeyboardButton("📏 Set Min Trade Size", callback_data="set_mintrade_menu")],
            [InlineKeyboardButton("◀️ Back", callback_data="menu_main")]
        ]'''

if old_buttons in content:
    content = content.replace(old_buttons, new_buttons)
    print("SUCCESS: Added capital management buttons to trading menu")
else:
    print("WARNING: Could not find exact trading menu buttons - may need manual update")

# Add menu functions for reserve and min trade at the end before help menu
insert_marker = '# ============================================================================\n# HELP MENU\n# ============================================================================'

reserve_menu = '''
# ============================================================================
# CAPITAL MANAGEMENT MENUS
# ============================================================================

async def show_reserve_balance_menu(message):
    """Display menu for setting reserve balance."""
    keyboard = [
        [
            InlineKeyboardButton("$0", callback_data="set_reserve:0"),
            InlineKeyboardButton("$50", callback_data="set_reserve:50")
        ],
        [
            InlineKeyboardButton("$100", callback_data="set_reserve:100"),
            InlineKeyboardButton("$200", callback_data="set_reserve:200")
        ],
        [InlineKeyboardButton("💵 Custom Amount", callback_data="set_reserve_custom")],
        [InlineKeyboardButton("◀️ Back", callback_data="menu_trading")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    menu_text = (
        f"💵 <b>Set Reserve Balance</b>\\n\\n"
        f"Reserve balance is the minimum capital that the bot will NOT use for trading.\\n\\n"
        f"<b>Current:</b> Check /portfolio\\n\\n"
        f"Select a preset or enter custom amount:"
    )
    
    await message.reply_html(menu_text, reply_markup=reply_markup)


async def show_min_trade_size_menu(message):
    """Display menu for setting minimum trade size."""
    keyboard = [
        [
            InlineKeyboardButton("$10", callback_data="set_mintrade:10"),
            InlineKeyboardButton("$20", callback_data="set_mintrade:20")
        ],
        [
            InlineKeyboardButton("$50", callback_data="set_mintrade:50"),
            InlineKeyboardButton("$100", callback_data="set_mintrade:100")
        ],
        [InlineKeyboardButton("💵 Custom Amount", callback_data="set_mintrade_custom")],
        [InlineKeyboardButton("◀️ Back", callback_data="menu_trading")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    menu_text = (
        f"📏 <b>Set Minimum Trade Size</b>\\n\\n"
        f"Minimum USD amount per trade. Bot will skip trades smaller than this.\\n\\n"
        f"<b>Current:</b> Check settings\\n\\n"
        f"Select a preset or enter custom amount:"
    )
    
    await message.reply_html(menu_text, reply_markup=reply_markup)


''' + insert_marker

content = content.replace(insert_marker, reserve_menu)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

print("SUCCESS: Added capital management menu functions")

print("\nCapital Management Menus Ready!")
