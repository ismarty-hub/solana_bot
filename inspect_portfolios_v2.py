import pickle
from pathlib import Path

file_path = "c:/Users/HP USER/Documents/Data Analyst/solana_bot/data/bot_portfolios.pkl"
try:
    with open(file_path, "rb") as f:
        data = pickle.load(f)
    print(f"Loaded {len(data)} profiles")
    for chat_id, p in data.items():
        positions = p.get("positions", {})
        active = [k for k, v in positions.items() if v.get("status") == "active"]
        history = p.get("trade_history", [])
        capital = p.get("capital_usd", "N/A")
        print(f"Chat ID: {chat_id}")
        print(f"  Capital: {capital}")
        print(f"  Active Positions: {len(active)}")
        print(f"  History Size: {len(history)}")
        if active:
            print(f"  Active Mints: {active[:3]}...")
        if history:
            print(f"  Latest History Reason: {history[-1].get('exit_reason')}")
except Exception as e:
    print(f"Critical error: {e}")
