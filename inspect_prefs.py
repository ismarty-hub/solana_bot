import pickle
import os
from pathlib import Path

# Path to the data directory
data_dir = Path(r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\data")
prefs_file = data_dir / "bot_user_prefs.pkl"

# Identify the user (assuming the user is the one who took the screenshot)
# We can find all keys in the prefs and see what they look like
# If the user provides their chat_id, that would be better, but we can dump a summary.

if not prefs_file.exists():
    print(f"File not found: {prefs_file}")
else:
    try:
        with open(prefs_file, 'rb') as f:
            prefs = pickle.load(f)
        
        print(f"Total users in prefs: {len(prefs)}")
        for chat_id, user_data in prefs.items():
            print(f"User {chat_id}:")
            print(f"  Subscribed: {user_data.get('subscribed')}")
            print(f"  Active: {user_data.get('active')}")
            print(f"  Expires: {user_data.get('expires_at')}")
            print(f"  Modes: {user_data.get('modes')}")
            print("-" * 20)
            
    except Exception as e:
        print(f"Error reading prefs: {e}")
