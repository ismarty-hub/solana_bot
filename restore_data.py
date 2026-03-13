import pickle
import os
import shutil

degen_smart_prefs = r"c:\Users\HP USER\Documents\Data Analyst\degen smart\data\bot_user_prefs.pkl"
solana_bot_prefs = r"c:\Users\HP USER\Documents\Data Analyst\solana_bot\data\bot_user_prefs.pkl"

with open(degen_smart_prefs, "rb") as f:
    degen_users = pickle.load(f)
print(f"DEGEN SMART USERS: {len(degen_users)}")

with open(solana_bot_prefs, "rb") as f:
    solana_users = pickle.load(f)
print(f"SOLANA BOT USERS: {len(solana_users)}")

if len(degen_users) > len(solana_users):
    print("Restoring from degen smart backup to solana bot...")
    shutil.copy2(degen_smart_prefs, solana_bot_prefs)
    
    # Push back to supabase
    import sys
    sys.path.append(r"c:\Users\HP USER\Documents\Data Analyst\solana_bot")
    try:
        from alerts.monitoring import upload_all_bot_data_to_supabase
        upload_all_bot_data_to_supabase()
        print("Successfully pushed restored data to Supabase!")
    except Exception as e:
        import traceback
        traceback.print_exc()
else:
    print("Solana bot has more or equal users. Skipping restore to be safe.")
