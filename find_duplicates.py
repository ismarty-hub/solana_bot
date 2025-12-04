"""Find all occurrences of async def button_handler in alerts/commands.py."""

import os

file_path = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\commands.py"

with open(file_path, "r", encoding="utf-8") as f:
    lines = f.readlines()

found = []
for i, line in enumerate(lines):
    if "async def button_handler" in line:
        found.append(i + 1)

print(f"Found {len(found)} occurrences of 'async def button_handler':")
for line_num in found:
    print(f"Line {line_num}")
