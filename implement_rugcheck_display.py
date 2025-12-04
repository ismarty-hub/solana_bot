#!/usr/bin/env python3
"""
Script to inject RugCheck security analysis into buy_token_process in alerts/commands.py
"""

import os

file_path = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\commands.py"

with open(file_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Define the code blocks to insert

# Block 1: API Call
# Insert after: token_info = await PriceFetcher.get_token_info(mint)
api_call_code = [
    "    \n",
    "    # Fetch RugCheck security analysis\n",
    "    rugcheck_data = await PriceFetcher.get_rugcheck_analysis(mint)\n"
]

# Block 2: Display Logic
# Insert before: msg += f"\n<b>Source:</b> {source.title()}\n\n💰 <b>Select Amount to Buy:</b>"
display_logic_code = [
    "    \n",
    "    # Add RugCheck Security Analysis\n",
    "    if rugcheck_data:\n",
    "        score = rugcheck_data.get('score', 0)\n",
    "        if score <= 20:\n",
    "            score_emoji, score_text = '🟢', 'GOOD'\n",
    "        elif score <= 50:\n",
    "            score_emoji, score_text = '🟡', 'FAIR'\n",
    "        else:\n",
    "            score_emoji, score_text = '🔴', 'DANGER'\n",
    "        \n",
    "        msg += f\"\\n\\n<b>🔒 SECURITY (RugCheck)</b>\\n\"\n",
    "        msg += f\"Risk Score: {score_emoji} {score_text} ({score})\\n\"\n",
    "        \n",
    "        # Key metrics\n",
    "        insider_count = rugcheck_data.get('insider_wallets_count', 0)\n",
    "        insider_pct = rugcheck_data.get('insider_supply_pct', 0)\n",
    "        dev_pct = rugcheck_data.get('dev_supply_pct', 0)\n",
    "        top_holders = rugcheck_data.get('top_holders_pct', 0)\n",
    "        liq_locked = rugcheck_data.get('liquidity_locked_pct', 0)\n",
    "        dev_sold = rugcheck_data.get('dev_sold', False)\n",
    "        \n",
    "        if insider_count > 0:\n",
    "            msg += f\"Insider Wallets: {insider_count}\\n\"\n",
    "        if insider_pct > 0:\n",
    "            msg += f\"Insider Supply: {insider_pct:.1f}%\\n\"\n",
    "        if dev_pct > 0:\n",
    "            msg += f\"Dev Supply: {dev_pct:.1f}%\\n\"\n",
    "        msg += f\"Top 10 Holders: {top_holders:.1f}%\\n\"\n",
    "        msg += f\"Liquidity Locked: {liq_locked:.1f}%\\n\"\n",
    "        if dev_sold:\n",
    "            msg += \"⚠️ Dev/Creator Sold\\n\"\n",
    "        \n",
    "        # Top risks\n",
    "        risks = rugcheck_data.get('risks', [])\n",
    "        if risks:\n",
    "            msg += \"\\n⚠️ Risks:\\n\"\n",
    "            for risk in risks[:3]:\n",
    "                risk_name = risk.get('name', 'Unknown')\n",
    "                msg += f\"• {risk_name}\\n\"\n"
]

# Perform insertion
new_lines = []
inserted_api = False
inserted_display = False

for line in lines:
    new_lines.append(line)
    
    # Insert API call
    if not inserted_api and "token_info = await PriceFetcher.get_token_info(mint)" in line:
        new_lines.extend(api_call_code)
        inserted_api = True
        print("Inserted API call code.")

    # Insert Display logic
    # Look for the line that adds the source, which is near the end of the message construction
    if not inserted_display and "msg += f\"\\n<b>Source:</b> {source.title()}" in line:
        # We want to insert BEFORE this line, so we pop the last line (which is the current line),
        # add our code, then add the current line back.
        current_line = new_lines.pop() 
        new_lines.extend(display_logic_code)
        new_lines.append(current_line)
        inserted_display = True
        print("Inserted display logic code.")

if inserted_api and inserted_display:
    with open(file_path, 'w', encoding='utf-8') as f:
        f.writelines(new_lines)
    print("Successfully updated alerts/commands.py")
else:
    print("Failed to find insertion points.")
    if not inserted_api: print("- Could not find API call insertion point")
    if not inserted_display: print("- Could not find Display logic insertion point")

