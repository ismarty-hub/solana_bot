import pickle
from pathlib import Path

file = Path("c:/Users/HP USER/Documents/Data Analyst/solana_bot/data/bot_portfolios.pkl")
if file.exists():
    try:
        with open(file, 'rb') as f:
            data = pickle.load(f)
        print(f"Total users in portfolio: {len(data)}")
        for chat_id, p in data.items():
            active = [k for k, v in p.get("positions", {}).items() if v.get("status") == "active"]
            closed = [k for k, v in p.get("positions", {}).items() if v.get("status") != "active"]
            history = p.get("trade_history", [])
            history_len = len(history)
            print(f"User {chat_id}: Capital=${p.get('capital_usd')}, Active={len(active)}, Closed={len(closed)}, History={history_len}")
            if active:
                print(f"  Active: {active}")
            if history_len > 0:
                print(f"  Latest History Exit Reason: {history[-1].get('exit_reason')}")
                # Print last 3 exits
                for h in history[-3:]:
                    print(f"    - {h.get('symbol')}: {h.get('exit_reason')} @ {h.get('exit_time')}")
    except Exception as e:
        print(f"Error loading with pickle: {e}")
else:
    print("File not found")
