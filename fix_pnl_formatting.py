"""
Script to fix the /pnl command formatting by adding newlines and f-prefix back.
"""

# Read the file
with open('alerts/commands.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Fix lines 510-520 (the msg =  section)
# Line 511 should become: f"📊 <b>Unrealized P/L Report</b>\n\n"
lines[510] = '        f"📊 <b>Unrealized P/L Report</b>\\n\\n"\r\n'
lines[511] = '        f"<b>💰 Portfolio Value:</b>\\n"\r\n'
lines[512] = '        f"• Available Capital: <b>${capital:,.2f}</b>\\n"\r\n'
lines[513] = '        f"• Invested (Cost Basis): <b>${total_cost_basis:,.2f}</b>\\n"\r\n'
lines[514] = '        f"• Total Value: <b>${total_value:,.2f}</b>\\n\\n"\r\n'
lines[515] = '        f"<b>{pnl_emoji} Unrealized P/L:</b>\\n"\r\n'
lines[516] = '        f"• USD: <b>${total_unrealized_usd:+,.2f}</b>\\n"\r\n'
lines[517] = '        f"• Percentage: <b>{total_unrealized_pct:+.2f}%</b>\\n\\n"\r\n'
lines[518] = '        f"<b>📈 Open Positions ({position_count}):</b>\\n"\r\n'

# Fix lines 525-529 (position loop)
lines[525] = '            f"{i}. {pnl_emoji} <b>{pos[\'symbol\']}</b>\\n"\r\n'
lines[526] = '            f"   Price: ${pos[\'current_price\']:.8f}\\n"\r\n'
lines[527] = '            f"   P/L: ${pos[\'unrealized_pnl_usd\']:+,.2f} ({pos[\'unrealized_pnl_pct\']:+.2f}%)\\n"\r\n'

# Fix line 531 (more positions message)
lines[531] = '        msg += f"\\n<i>...and {len(positions_detail) - 10} more positions</i>\\n"\r\n'

# Fix line 533 (footer)
lines[533] = '    msg += "\\n<i>💡 Use /portfolio to see full position details</i>"\r\n'

# Write back
with open('alerts/commands.py', 'w', encoding='utf-8') as f:
    f.writelines(lines)

print("Successfully fixed /pnl command formatting!")
print("Restart the bot to see the changes.")
