import pickle
from pathlib import Path

file_path = "c:/Users/HP USER/Documents/Data Analyst/solana_bot/data/bot_user_prefs.pkl"
try:
    with open(file_path, "rb") as f:
        data = pickle.load(f)
    print(f"Loaded {len(data)} user prefs")
    for chat_id, p in data.items():
        print(f"Chat ID: {chat_id}")
        print(f"  Subscribed: {p.get('subscribed')}")
        print(f"  Modes: {p.get('modes')}")
        print(f"  Grades: {p.get('grades')}")
except Exception as e:
    print(f"Error: {e}")
