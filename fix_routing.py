"""Add routing for buy_tp and buy_sl callbacks to button_handler."""

with open(r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\commands.py", "r", encoding="utf-8") as f:
    lines = f.readlines()

# Find and replace the line with buy_amount routing
for i in range(len(lines)):
    if 'if data.startswith("buy_amount:") or data.startswith("buy_custom:")' in lines[i]:
        # Replace this line with expanded routing
        indent = "    "
        lines[i] = indent + "# Handle all buy-related callbacks (amount, TP, SL, custom)\n"
        lines.insert(i+1, indent + "if (data.startswith(\"buy_amount:\") or data.startswith(\"buy_custom:\") or \n")
        lines.insert(i+2, indent + "    data.startswith(\"buy_tp:\") or data.startswith(\"buy_sl:\") or\n")
        lines.insert(i+3, indent + "    data == \"buy_tp_custom\" or data == \"buy_sl_custom\"):\n")
        print(f"Found and updated routing at line {i+1}")
        break

# Write back
with open(r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\commands.py", "w", encoding="utf-8") as f:
    f.writelines(lines)

print("Successfully added routing for buy_tp and buy_sl callbacks!")
