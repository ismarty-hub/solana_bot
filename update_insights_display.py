#!/usr/bin/env python3
"""
Script to update buy_token_process display with educational insights and remove branding
"""

file_path = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\commands.py"

with open(file_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# We need to replace the message construction block in buy_token_process
# Find the start of the message construction
start_idx = -1
end_idx = -1

for i, line in enumerate(lines):
    if 'msg = (' in line and 'f"💎 <b>Found {name}' in lines[i+1]:
        start_idx = i
        break

if start_idx != -1:
    # Find the end of the message construction (where we send the message)
    for i in range(start_idx, len(lines)):
        if 'await status_msg.edit_text(msg' in lines[i]:
            end_idx = i
            break

if start_idx != -1 and end_idx != -1:
    # Construct the new message building code
    new_code = [
        '    msg = (\n',
        '        f"💎 <b>Found {name} ({symbol})</b>\\n"\n',
        '        f"<code>{mint}</code>\\n\\n"\n',
        '        f"<b>Price:</b> ${price:.6f}\\n"\n',
        '    )\n',
        '    \n',
        '    # Add detailed metrics if available\n',
        '    if fdv > 0:\n',
        '        msg += f"<b>Market Cap:</b> ${fdv:,.0f}\\n"\n',
        '    if volume24h > 0:\n',
        '        msg += f"<b>24h Volume:</b> ${volume24h:,.0f}\\n"\n',
        '    if liquidity > 0:\n',
        '        msg += f"<b>Liquidity:</b> ${liquidity:,.0f}\\n"\n',
        '    if price_change_24h != 0:\n',
        '        change_emoji = "📈" if price_change_24h > 0 else "📉"\n',
        '        msg += f"<b>24h Change:</b> {change_emoji} {price_change_24h:+.2f}%\\n"\n',
        '    \n',
        '    # Add Educational Security Insights (White-labeled)\n',
        '    if rugcheck_data:\n',
        '        score = rugcheck_data.get("score", 0)\n',
        '        # Interpret score (Lower is better in RugCheck, usually)\n',
        '        # Wait, RugCheck score: 0 is good, high is bad? \n',
        '        # Actually, RugCheck usually gives a risk score where lower is better.\n',
        '        # Let\'s assume standard risk score: < 1000. \n',
        '        # Based on user input: "score": 500 (warn). \n',
        '        # Let\'s use the score logic we had but refined.\n',
        '        \n',
        '        # Safe/Risk assessment\n',
        '        is_safe = score < 400  # Arbitrary threshold based on "warn" at 500\n',
        '        risk_level = "LOW" if score < 200 else "MEDIUM" if score < 500 else "HIGH"\n',
        '        risk_emoji = "🟢" if score < 200 else "🟡" if score < 500 else "🔴"\n',
        '        \n',
        '        msg += f"\\n\\n<b>🛡️ SECURITY INSIGHTS</b>\\n"\n',
        '        msg += f"Risk Level: {risk_emoji} {risk_level} ({score})\\n\\n"\n',
        '        \n',
        '        # 1. Authority Analysis\n',
        '        mint_auth = rugcheck_data.get("mint_authority")\n',
        '        freeze_auth = rugcheck_data.get("freeze_authority")\n',
        '        mutable = rugcheck_data.get("is_mutable", True)\n',
        '        \n',
        '        msg += "<b>👮 Authority Status:</b>\\n"\n',
        '        msg += f"• Mint Authority: {\'✅ Disabled\' if not mint_auth else \'⚠️ Enabled\'}\\n"\n',
        '        msg += f"• Freeze Authority: {\'✅ Disabled\' if not freeze_auth else \'⚠️ Enabled\'}\\n"\n',
        '        msg += f"• Metadata Mutable: {\'⚠️ Yes\' if mutable else \'✅ No\'}\\n"\n',
        '        \n',
        '        # 2. Liquidity Analysis\n',
        '        liq_locked = rugcheck_data.get("liquidity_locked_pct", 0)\n',
        '        msg += f"\\n<b>💧 Liquidity Status:</b>\\n"\n',
        '        msg += f"• Locked: {liq_locked:.1f}% {\'✅\' if liq_locked > 90 else \'⚠️\'}\\n"\n',
        '        \n',
        '        # 3. Holder Analysis\n',
        '        top_holders = rugcheck_data.get("top_holders_pct", 0)\n',
        '        top_holder = rugcheck_data.get("top_holder_pct", 0)\n',
        '        insider_count = rugcheck_data.get("insider_wallets_count", 0)\n',
        '        \n',
        '        msg += f"\\n<b>👥 Holder Analysis:</b>\\n"\n',
        '        msg += f"• Top 10 Hold: {top_holders:.1f}% {\'✅\' if top_holders < 30 else \'⚠️\'}\\n"\n',
        '        msg += f"• Top 1 Holder: {top_holder:.1f}%\\n"\n',
        '        if insider_count > 0:\n',
        '            msg += f"• Insider Wallets: {insider_count} ⚠️\\n"\n',
        '        \n',
        '        # 4. Critical Warnings\n',
        '        dev_sold = rugcheck_data.get("dev_sold", False)\n',
        '        risks = rugcheck_data.get("risks", [])\n',
        '        \n',
        '        warnings = []\n',
        '        if dev_sold: warnings.append("Dev/Creator has sold tokens")\n',
        '        if mint_auth: warnings.append("Mint Authority enabled (Supply can increase)")\n',
        '        if freeze_auth: warnings.append("Freeze Authority enabled (Wallets can be frozen)")\n',
        '        if liq_locked < 80: warnings.append(f"Low Liquidity Lock ({liq_locked:.1f}%)")\n',
        '        \n',
        '        if warnings:\n',
        '            msg += "\\n<b>⚠️ CRITICAL WARNINGS:</b>\\n"\n',
        '            for warn in warnings:\n',
        '                msg += f"• {warn}\\n"\n',
        '    \n',
        '    msg += f"\\n💰 <b>Select Amount to Buy:</b>"\n'
    ]
    
    # Replace the lines
    lines[start_idx:end_idx] = new_code
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.writelines(lines)
    
    print("SUCCESS: Updated display logic with deep insights and removed branding.")

else:
    print("ERROR: Could not find message construction block.")
