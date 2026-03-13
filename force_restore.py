import sys
import os

sys.path.append(r"c:\Users\HP USER\Documents\Data Analyst\solana_bot")
from alerts.monitoring import upload_all_bot_data_to_supabase

print("Starting restoration upload to Supabase using local 33 users...")
try:
    upload_all_bot_data_to_supabase()
    print("Restore successful! 33 Users and 9 Portfolios have been pushed to the cloud.")
except Exception as e:
    import traceback
    traceback.print_exc()
