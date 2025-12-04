#!/usr/bin/env python3
"""Fix save_portfolio typo in trade_manager.py"""

file_path = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\trade_manager.py"

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# Fix the typo
content = content.replace('self.save_portfolio(chat_id, portfolio)', 'self.save()')

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

print("SUCCESS: Fixed save_portfolio typo!")
