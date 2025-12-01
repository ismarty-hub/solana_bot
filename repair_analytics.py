import os
import json
import asyncio
import aiohttp
from datetime import datetime
from supabase import create_client, Client
from dateutil import parser
from dotenv import load_dotenv
import copy

# --- Configuration ---
load_dotenv()
BUCKET_NAME = "monitor-data"
TEMP_DIR = "repair_temp"

# --- Supabase Setup ---
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
if not url or not key:
    raise ValueError("Missing SUPABASE_URL or SUPABASE_KEY in .env")
supabase = create_client(url, key)

async def download_json(remote_path):
    """Downloads a JSON file from Supabase."""
    local_path = os.path.join(TEMP_DIR, remote_path)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    
    try:
        # Create signed URL
        res = supabase.storage.from_(BUCKET_NAME).create_signed_url(remote_path, 60)
        signed_url = res.get('signedURL')
        if not signed_url: return None

        async with aiohttp.ClientSession() as session:
            async with session.get(signed_url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data
                elif resp.status == 404:
                    return None # File doesn't exist yet, which is fine
                else:
                    print(f"Error downloading {remote_path}: {resp.status}")
                    return None
    except Exception as e:
        print(f"Exception downloading {remote_path}: {e}")
        return None

async def upload_json(data, remote_path):
    """Uploads a JSON file to Supabase."""
    try:
        # Convert to JSON string
        json_str = json.dumps(data, indent=2, default=str)
        
        # Try update first
        try:
            supabase.storage.from_(BUCKET_NAME).update(
                remote_path, 
                json_str.encode(), 
                {"content-type": "application/json"}
            )
        except Exception:
            # Fallback to upload if update fails (file new)
            supabase.storage.from_(BUCKET_NAME).upload(
                remote_path, 
                json_str.encode(), 
                {"content-type": "application/json"}
            )
        print(f"SUCCESS: Updated {remote_path}")
        return True
    except Exception as e:
        print(f"FAILED to upload {remote_path}: {e}")
        return False

def calculate_daily_summary(tokens):
    """Recalculates summary stats for a daily file."""
    wins = [t for t in tokens if t.get("status") == "win"]
    losses = [t for t in tokens if t.get("status") == "loss"]
    total_valid = len(tokens)
    
    ath_rois = [t.get("ath_roi", 0) for t in tokens]
    win_aths = [t.get("ath_roi", 0) for t in wins]
    
    return {
        "total_tokens": total_valid,
        "wins": len(wins),
        "losses": len(losses),
        "success_rate": (len(wins) / total_valid * 100) if total_valid > 0 else 0,
        "average_ath_all": sum(ath_rois) / total_valid if total_valid > 0 else 0,
        "average_ath_wins": sum(win_aths) / len(wins) if len(wins) > 0 else 0,
        "average_final_roi": sum((t.get("final_roi") or 0) for t in tokens) / total_valid if total_valid > 0 else 0,
        "max_roi": max(ath_rois, default=0),
    }

async def repair_zombie_wins():
    print("--- Starting Repair Process ---")
    
    # 1. Download Active Tracking
    active_path = "analytics/active_tracking.json"
    active_data = await download_json(active_path)
    
    if not active_data:
        print("CRITICAL: Could not download active_tracking.json")
        return

    # 2. Iterate through active tokens
    fixes_needed = {} # Map: date_str -> {signal_type -> [tokens]}

    for key, token in active_data.items():
        # Check if it is a winner
        if token.get("hit_50_percent") or token.get("status") == "win":
            
            # Determine the date it SHOULD be in
            win_time_str = token.get("hit_50_percent_time")
            if not win_time_str:
                print(f"Skipping {token['symbol']} (Win but no timestamp)")
                continue
                
            try:
                win_date = parser.isoparse(win_time_str)
                date_str = win_date.strftime('%Y-%m-%d')
            except:
                print(f"Error parsing date for {token['symbol']}")
                continue

            signal_type = token.get("signal_type", "alpha")
            
            # Organize by File Key
            file_key = f"analytics/{signal_type}/daily/{date_str}.json"
            
            if file_key not in fixes_needed:
                fixes_needed[file_key] = []
            
            fixes_needed[file_key].append(token)

    # 3. Process each Daily File
    for file_path, active_winners in fixes_needed.items():
        print(f"\nChecking file: {file_path}")
        
        # Download the daily file
        daily_data = await download_json(file_path)
        
        # If file doesn't exist, create structure
        if not daily_data:
            print(f"  -> File missing. Creating new.")
            # Extract date and type from path
            parts = file_path.split('/')
            daily_data = {
                "date": parts[3].replace(".json", ""), 
                "signal_type": parts[1], 
                "tokens": [], 
                "daily_summary": {}
            }

        daily_tokens = daily_data.get("tokens", [])
        modified = False
        
        # Check if active winners are missing from this file
        existing_mints = {t["mint"] for t in daily_tokens}
        
        for winner in active_winners:
            if winner["mint"] not in existing_mints:
                print(f"  -> REPAIRING: Adding missing token {winner['symbol']} (ROI: {winner.get('current_roi')}%)")
                # Deep copy to ensure we don't mess up references
                entry_to_add = copy.deepcopy(winner)
                # Ensure is_final is set correctly (False because it's still active)
                entry_to_add["is_final"] = False 
                daily_tokens.append(entry_to_add)
                modified = True
            else:
                # OPTIONAL: Update the existing entry if the active one has better stats (higher ATH)
                # This fixes cases where the daily file exists but is stale
                for i, t in enumerate(daily_tokens):
                    if t["mint"] == winner["mint"]:
                        active_ath = winner.get("ath_roi", 0)
                        daily_ath = t.get("ath_roi", 0)
                        if active_ath > daily_ath:
                             print(f"  -> UPDATING: {winner['symbol']} stats (Active ATH {active_ath:.1f}% > Daily {daily_ath:.1f}%)")
                             daily_tokens[i] = copy.deepcopy(winner)
                             daily_tokens[i]["is_final"] = False
                             modified = True

        if modified:
            # Recalculate Summary
            daily_data["tokens"] = daily_tokens
            daily_data["daily_summary"] = calculate_daily_summary(daily_tokens)
            
            # Upload Fixed File
            await upload_json(daily_data, file_path)
        else:
            print("  -> No missing tokens found in this file.")

    print("\n--- Repair Complete ---")
    print("Please verify the dashboard/stats in a few minutes.")

if __name__ == "__main__":
    asyncio.run(repair_zombie_wins())