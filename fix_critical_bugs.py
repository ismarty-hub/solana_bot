#!/usr/bin/env python3
"""Fix critical bugs: menu edit parameter and datetime parsing"""

# Fix 1: Remove edit=True from menu_handler.py calls
print("Fixing menu_handler.py...")
file_path = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\alerts\menu_handler.py"

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# Remove all edit=True from menu function calls
content = content.replace(', edit=True)', ')')

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

print("SUCCESS: Fixed menu_handler.py")

# Fix 2: Fix datetime parsing in trade_manager.py
print("\nFixing trade_manager.py datetime parsing...")
file_path = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\trade_manager.py"

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# Fix the datetime parsing to handle both Z and +00:00 endings
old_code = 'entry_time = datetime.fromisoformat(pos["entry_time"].replace("Z", "+00:00"))'
new_code = '''# Handle both Z and +00:00 timezone formats
        entry_time_str = pos["entry_time"]
        if entry_time_str.endswith("Z"):
            entry_time_str = entry_time_str.replace("Z", "+00:00")
        elif "+00:00" in entry_time_str and entry_time_str.count("+00:00") > 1:
            # Fix double timezone suffix
            entry_time_str = entry_time_str.replace("+00:00+00:00", "+00:00")
        entry_time = datetime.fromisoformat(entry_time_str)'''

content = content.replace(old_code, new_code)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

print("SUCCESS: Fixed trade_manager.py datetime parsing")
print("\nAll critical bugs fixed!")
