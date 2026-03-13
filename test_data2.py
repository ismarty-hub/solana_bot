import pickle
import os

degen_smart_dir = r"c:\Users\HP USER\Documents\Data Analyst\degen smart\data"
solana_bot_dir = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\data"

with open("stats.txt", "w") as out:
    for env, d in [("DEGEN_SMART", degen_smart_dir), ("SOLANA_BOT", solana_bot_dir)]:
        out.write(f"--- {env} ---\n")
        prefs = os.path.join(d, "bot_user_prefs.pkl")
        if os.path.exists(prefs):
            with open(prefs, "rb") as f:
                data = pickle.load(f)
                out.write(f"Users: {len(data)}\n")
        else:
            out.write("Users: FILE MISSING\n")
            
        ports = os.path.join(d, "bot_portfolios.pkl")
        if os.path.exists(ports):
            with open(ports, "rb") as f:
                data = pickle.load(f)
                out.write(f"Portfolios: {len(data)}\n")
        else:
            out.write("Portfolios: FILE MISSING\n")
