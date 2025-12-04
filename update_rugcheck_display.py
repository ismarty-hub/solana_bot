#!/usr/bin/env python3
"""
Update buy_token_process in commands.py to include RugCheck security analysis
"""

file_path = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\commands.py"

with open(file_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Find the buy_token_process function and locate where we display token info
# Look for the lines where we construct the message

found_function = False
insert_index = -1

for i, line in enumerate(lines):
    if 'async def buy_token_process' in line:
        found_function = True
    
    if found_function and 'await status_msg.edit_text(msg, reply_markup=reply_markup' in line:
        insert_index = i
        break

if insert_index > 0:
    # Find where we start building the message (look backwards from insert_index)
    # Find "msg = ("
    msg_start_index = -1
    for i in range(insert_index, max(0, insert_index - 50), -1):
        if 'msg = (' in lines[i] or 'msg += f' in lines[i]:
            msg_start_index = i
            break
    
    if msg_start_index > 0:
        # Insert RugCheck API call before message construction
        # Find where we define token_info variables (after status message fetch)
        rugcheck_insert = -1
        for i in range(msg_start_index, max(0, msg_start_index - 30), -1):
            if 'token_info = await PriceFetcher.get_token_info(mint)' in lines[i]:
                rugcheck_insert = i + 1
                while lines[rugcheck_insert].strip() == '':
                    rugcheck_insert += 1
                break
        
        if rugcheck_insert > 0:
            # Add RugCheck fetch before message building
            indent = '    '  # 4 spaces for function body indent
            rugcheck_code = [
                f'{indent}# Fetch RugCheck security analysis\n',
                f'{indent}rugcheck_data = await PriceFetcher.get_rugcheck_analysis(mint)\n',
                '\n'
            ]
            
            for idx, code_line in enumerate(rugcheck_code):
                lines.insert(rugcheck_insert + idx, code_line)
            
            # Now find where we build the msg and add security section
            # Find the line after we add price change or source
            new_msg_start = msg_start_index + len(rugcheck_code)
            
            # Find line with 'msg += f"\\n<b>Source:</b>' or similar
            security_insert = -1
            for i in range(new_msg_start, min(len(lines), new_msg_start + 50)):
                if 'msg += f"\\n<b>Source:</b>' in lines[i]:
                    security_insert = i + 1
                    break
            
            if security_insert > 0:
                # Insert security analysis section
                security_code = [
                    '\n',
                    f'{indent}# Add RugCheck Security Analysis if available\n',
                    f'{indent}if rugcheck_data:\n',
                    f'{indent}    score = rugcheck_data.get("score", 0)\n',
                    f'{indent}    risks = rugcheck_data.get("risks", [])\n',
                    f'{indent}    \n',
                    f'{indent}    # Score interpretation\n',
                    f'{indent}    if score >= 80:\n',
                    f'{indent}        score_emoji = "\\U0001F7E2"  # Green circle\n',
                    f'{indent}        score_text = "GOOD"\n',
                    f'{indent}    elif score >= 60:\n',
                    f'{indent}        score_emoji = "\\U0001F7E1"  # Yellow circle\n',
                    f'{indent}        score_text = "FAIR"\n',
                    f'{indent}    elif score >= 40:\n',
                    f'{indent}        score_emoji = "\\U0001F7E0"  # Orange circle\n',
                    f'{indent}        score_text = "POOR"\n',
                    f'{indent}    else:\n',
                    f'{indent}        score_emoji = "\\U0001F534"  # Red circle\n',
                    f'{indent}        score_text = "DANGER"\n',
                    f'{indent}    \n',
                    f'{indent}    msg += f"\\n\\n<b>\\U0001F512 SECURITY ANALYSIS (RugCheck)</b>\\n"\n',
                    f'{indent}    msg += f"Score: {{score_emoji}} {{score_text}} ({{score}}/100)\\n"\n',
                    f'{indent}    \n',
                    f'{indent}    # Display key metrics\n',
                    f'{indent}    insider_pct = rugcheck_data.get("insider_supply_pct", 0)\n',
                    f'{indent}    dev_pct = rugcheck_data.get("dev_supply_pct", 0)\n',
                    f'{indent}    top_holders = rugcheck_data.get("top_holders_pct", 0)\n',
                    f'{indent}    liquidity_locked = rugcheck_data.get("liquidity_locked_pct", 0)\n',
                    f'{indent}    insider_wallets = rugcheck_data.get("insider_wallets_count", 0)\n',
                    f'{indent}    dev_sold = rugcheck_data.get("dev_sold", False)\n',
                    f'{indent}    \n',
                    f'{indent}    msg += f"\\nInsider Wallets: {{insider_wallets}}\\n"\n',
                    f'{indent}    msg += f"Insider Supply: {{insider_pct:.1f}}%\\n"\n',
                    f'{indent}    if dev_pct > 0:\n',
                    f'{indent}        msg += f"Dev/Creator Supply: {{dev_pct:.1f}}%\\n"\n',
                    f'{indent}    msg += f"Top 10 Holders: {{top_holders:.1f}}%\\n"\n',
                    f'{indent}    msg += f"Liquidity Locked: {{liquidity_locked:.1f}}%\\n"\n',
                    f'{indent}    if dev_sold:\n',
                    f'{indent}        msg += "\\U000026A0 Dev/Creator Sold\\n"\n',
                    f'{indent}    \n',
                    f'{indent}    # Display top 3 risks\n',
                    f'{indent}    if risks:\n',
                    f'{indent}        msg += "\\n\\U000026A0 Top Risks:\\n"\n',
                    f'{indent}        for risk in risks[:3]:\n',
                    f'{indent}            risk_name = risk.get("name", "Unknown Risk")\n',
                    f'{indent}            msg += f"\\U00002022 {{risk_name}}\\n"\n',
                    '\n',
                ]
                
                for idx, code_line in enumerate(security_code):
                    lines.insert(security_insert + idx, code_line)
            
            # Write back to file
            with open(file_path, 'w', encoding='utf-8') as f:
                f.writelines(lines)
            
            print("SUCCESS: Updated buy_token_process with RugCheck security analysis")
        else:
            print("ERROR: Could not find token_info fetch location")
    else:
        print("ERROR: Could not find message construction location")
else:
    print("ERROR: Could not find buy_token_process function")
