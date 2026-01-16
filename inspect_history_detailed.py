import pickle
from pathlib import Path

file_path = "c:/Users/HP USER/Documents/Data Analyst/solana_bot/data/bot_portfolios.pkl"
try:
    with open(file_path, "rb") as f:
        data = pickle.load(f)
    user_id = "1220056877"
    if user_id in data:
        p = data[user_id]
        print(f"User {user_id}:")
        print(f"  Capital: {p.get('capital_usd')}")
        print(f"  Starting Capital: {p.get('starting_capital')}")
        print(f"  Active Positions: {len(p.get('positions', {}))}")
        history = p.get('trade_history', [])
        print(f"  History Total Size: {len(history)}")
        print("  Latest 10 Trades:")
        for h in history[-10:]:
            roi = h.get('exit_roi')
            symbol = h.get('symbol', 'UNK')
            reason = h.get('exit_reason', 'UNK')
            time = h.get('exit_time', 'UNK')
            print(f"    - {symbol}: ROI={roi} | Reason={reason} | At={time}")
    else:
        print(f"User {user_id} not found")
except Exception as e:
    print(f"Error: {e}")
