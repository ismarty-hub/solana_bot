"""
Simple script to update the /pnl command to fetch live prices.
Run this file to update commands.py automatically.
"""

import re

# Read the current commands.py file
with open('alerts/commands.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Define the new pnl_cmd function
new_pnl_cmd = '''async def pnl_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_manager: UserManager,portfolio_manager: PortfolioManager):
    """Get current unrealized P/L for all open positions with live prices."""
    chat_id = str(update.effective_chat.id)
    prefs = user_manager.get_user_prefs(chat_id)
    
    if "papertrade" not in prefs.get("modes", []):
        await update.message.reply_html("❌ Paper trading is not enabled.")
        return
    
    portfolio = portfolio_manager.get_portfolio(chat_id)
    positions = portfolio.get('positions', {})
    
    if not positions:
        await update.message.reply_html("📊 No open positions to calculate P/L.")
        return
    
    # Fetch live prices for all positions
    try:
        live_prices = await portfolio_manager.update_positions_with_live_prices(chat_id)
        pnl_data = portfolio_manager.calculate_unrealized_pnl(chat_id, live_prices)
    except Exception as e:
        logger.exception(f"Error calculating PnL for {chat_id}: {e}")
        await update.message.reply_html("❌ Error fetching live prices. Please try again.")
        return
    
    total_unrealized_usd = pnl_data["total_unrealized_usd"]
    total_unrealized_pct = pnl_data["total_unrealized_pct"]
    total_cost_basis = pnl_data["total_cost_basis"]
    position_count = pnl_data["position_count"]
    positions_detail = pnl_data["positions_detail"]
    
    capital = portfolio.get('capital_usd', 0)
    total_value = capital + total_cost_basis + total_unrealized_usd
    
    # Overall PnL message
    pnl_emoji = "🟢" if total_unrealized_usd > 0 else "🔴" if total_unrealized_usd < 0 else "⚪"
    
    msg = (
        f"📊 <b>Unrealized P/L Report</b>\\n\\n"
        f"<b>💰 Portfolio Value:</b>\\n"
        f"• Available Capital: <b>${capital:,.2f}</b>\\n"
        f"• Invested (Cost Basis): <b>${total_cost_basis:,.2f}</b>\\n"
        f"• Total Value: <b>${total_value:,.2f}</b>\\n\\n"
        f"<b>{pnl_emoji} Unrealized P/L:</b>\\n"
        f"• USD: <b>${total_unrealized_usd:+,.2f}</b>\\n"
        f"• Percentage: <b>{total_unrealized_pct:+.2f}%</b>\\n\\n"
        f"<b>📈 Open Positions ({position_count}):</b>\\n"
    )
    
    # Show individual positions (up to 10)
    for i, pos in enumerate(positions_detail[:10], 1):
        pnl_emoji = "🟢" if pos["unrealized_pnl_usd"] > 0 else "🔴"
        msg += (
            f"{i}. {pnl_emoji} <b>{pos['symbol']}</b>\\n"
            f"   Price: ${pos['current_price']:.8f}\\n"
            f"   P/L: ${pos['unrealized_pnl_usd']:+,.2f} ({pos['unrealized_pnl_pct']:+.2f}%)\\n"
        )
    
    if len(positions_detail) > 10:
        msg += f"\\n<i>...and {len(positions_detail) - 10} more positions</i>\\n"
    
    msg += "\\n<i>💡 Use /portfolio to see full position details</i>"
    
    await update.message.reply_html(msg)
'''

# Find and replace the old pnl_cmd function
# Pattern to match the function from start to end
pattern = r'async def pnl_cmd\(.*?\):\s*""".*?""".*?await update\.message\.reply_html\(msg\)'

# Use re.DOTALL to match across multiple lines
new_content = re.sub(pattern, new_pnl_cmd, content, flags=re.DOTALL)

# Write back to file
with open('alerts/commands.py', 'w', encoding='utf-8') as f:
    f.write(new_content)

print("✅ Successfully updated /pnl command to fetch live prices!")
print("Restart the bot to see the changes.")
