"""Fix callback query answer issue in ask_buy_tp and ask_buy_sl functions."""

with open(r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\commands.py", "r", encoding="utf-8") as f:
    lines = f.readlines()

# Fix ask_buy_tp function (around line 1515)
for i in range(len(lines)):
    if i >= 1514 and i <= 1518:
        if "if update.callback_query:" in lines[i]:
            # Insert answer() call before edit_message_text
            indent = "        "
            lines[i] = lines[i]  # Keep the if statement
            if i+1 < len(lines) and "edit_message_text" in lines[i+1]:
                lines[i+1] = indent + "await update.callback_query.answer()\n" + lines[i+1]
                break

# Fix ask_buy_sl function (around line 1544)
for i in range(len(lines)):
    if i >= 1543 and i <= 1548:
        if "if update.callback_query:" in lines[i] and i > 1520:  # Make sure it's the second occurrence
            indent = "        "
            lines[i] = lines[i]  # Keep the if statement
            if i+1 < len(lines) and "edit_message_text" in lines[i+1]:
                lines[i+1] = indent + "await update.callback_query.answer()\n" + lines[i+1]
                break

# Write back
with open(r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\commands.py", "w", encoding="utf-8") as f:
    f.writelines(lines)

print("Successfully added callback query answers")
