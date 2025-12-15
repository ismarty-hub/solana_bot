#!/usr/bin/env python3
"""
cleanup_tokens.py

Utility script to remove specific tokens from the analytics system.
It performs the following:
1. Removes tokens from 'active_tracking.json'.
2. Scans ALL daily files (discovery & alpha), removes the tokens, and recalculates daily stats.
3. Regenerates the 'summary_stats.json' files for both signals.
4. Regenerates the 'overall/summary_stats.json'.
"""

import os
import json
import asyncio
import aiohttp
import logging
from datetime import datetime, timedelta, timezone
try:
    from supabase import create_client, Client
    SUPABASE_AVAILABLE = True
except ImportError:
    logger = logging.getLogger(__name__)
    logger.warning("⚠️ Supabase module not installed. Will only clean local files.")
    SUPABASE_AVAILABLE = False
    Client = None
from dateutil import parser
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not required
import requests

# --- Configuration ---

# LIST THE TOKENS TO DELETE HERE
TOKENS_TO_DELETE = [
    "nPdW6TsL7FdLVLyVSQXLwDru5379KhXgQyZF4rm1649"
]

BUCKET_NAME = "monitor-data"
TEMP_DIR = "/tmp/analytics_cleanup"

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- Supabase & File Helpers (Reused from Tracker) ---

supabase = None  # Type: Client | None (when available)

def get_supabase_client() -> Client:
    global supabase
    if supabase is None:
        SUPABASE_URL = os.getenv("SUPABASE_URL")
        SUPABASE_KEY = os.getenv("SUPABASE_KEY")
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise ValueError("Missing SUPABASE_URL or SUPABASE_KEY")
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    return supabase

def list_files_in_supabase_folder(folder_path: str) -> list[str]:
    try:
        client = get_supabase_client()
        result = client.storage.from_(BUCKET_NAME).list(folder_path)
        if not result:
            return []
        return [item['name'] for item in result if item.get('name')]
    except Exception as e:
        logger.error(f"Error listing files in {folder_path}: {e}")
        return []

async def download_file_from_supabase(remote_path: str, local_path: str) -> bool:
    client = get_supabase_client()
    try:
        # Simple download without caching logic for cleanup script
        data = await asyncio.to_thread(client.storage.from_(BUCKET_NAME).download, remote_path)
        os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
        with open(local_path, "wb") as f:
            f.write(data)
        return True
    except Exception as e:
        # Only log if it's not a "file not found" which is common for new days
        logger.warning(f"Could not download {remote_path} (might not exist): {e}")
        return False

async def upload_file_to_supabase(local_path: str, remote_path: str) -> bool:
    if not os.path.exists(local_path):
        return False
    try:
        client = get_supabase_client()
        # Always overwrite in cleanup
        try:
            await asyncio.to_thread(client.storage.from_(BUCKET_NAME).remove, [remote_path])
        except Exception:
            pass

        with open(local_path, "rb") as f:
            data = f.read()
        
        await asyncio.to_thread(
            client.storage.from_(BUCKET_NAME).upload,
            remote_path, data, {"content-type": "application/json", "cache-control": "3600"}
        )
        return True
    except Exception as e:
        logger.error(f"Upload failed for {remote_path}: {e}")
        return False

def load_json(file_path: str) -> dict | list | None:
    if not os.path.exists(file_path):
        return None
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading {file_path}: {e}")
        return None

def save_json(data: dict | list, file_path: str) -> str | None:
    try:
        os.makedirs(os.path.dirname(file_path) or ".", exist_ok=True)
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        return file_path
    except Exception as e:
        logger.error(f"Error saving {file_path}: {e}")
        return None

def get_now() -> datetime:
    return datetime.now(timezone.utc)

def to_iso(dt: datetime) -> str:
    return dt.isoformat().replace('+00:00', 'Z')

def parse_ts(ts_str: str) -> datetime:
    try:
        return parser.isoparse(ts_str).astimezone(timezone.utc)
    except Exception:
        return get_now()

# --- Cleanup Logic ---

async def clean_active_tracking():
    """Removes tokens from active_tracking.json (both Supabase and local)."""
    logger.info("Cleaning active_tracking.json...")
    removed_from_supabase = 0
    removed_from_local = 0
    
    # 1. Clean Supabase version (if available)
    if SUPABASE_AVAILABLE:
        remote_path = "analytics/active_tracking.json"
        local_path = os.path.join(TEMP_DIR, remote_path)
        
        if await download_file_from_supabase(remote_path, local_path):
            data = load_json(local_path)
            if data:
                initial_count = len(data)
                
                # Filter dictionary keys
                keys_to_remove = []
                for key, token_data in data.items():
                    if token_data.get("mint") in TOKENS_TO_DELETE:
                        keys_to_remove.append(key)
                
                for k in keys_to_remove:
                    del data[k]
                
                if len(keys_to_remove) > 0:
                    removed_from_supabase = len(keys_to_remove)
                    logger.info(f"✅ Removed {removed_from_supabase} tokens from Supabase active_tracking.json")
                    save_json(data, local_path)
                    await upload_file_to_supabase(local_path, remote_path)
                else:
                    logger.info("No target tokens found in Supabase active_tracking.json")
        else:
            logger.warning("Could not download active_tracking.json from Supabase")
    else:
        logger.info("Skipping Supabase active_tracking (module not available)")
    
    # 2. Clean local version (if exists)
    local_data_path = "data/active_tracking.json"
    if os.path.exists(local_data_path):
        logger.info(f"Found local file at {local_data_path}")
        data = load_json(local_data_path)
        if data:
            initial_count = len(data)
            
            keys_to_remove = []
            for key, token_data in data.items():
                if token_data.get("mint") in TOKENS_TO_DELETE:
                    keys_to_remove.append(key)
            
            for k in keys_to_remove:
                del data[k]
            
            if len(keys_to_remove) > 0:
                removed_from_local = len(keys_to_remove)
                logger.info(f"✅ Removed {removed_from_local} tokens from LOCAL active_tracking.json")
                save_json(data, local_data_path)
            else:
                logger.info("No target tokens found in local active_tracking.json")
    else:
        logger.info(f"Local file {local_data_path} not found")
    
    total_removed = removed_from_supabase + removed_from_local
    logger.info(f"📊 Total removed from active_tracking: {total_removed} tokens")

def recalculate_daily_summary(daily_data: dict):
    tokens = daily_data.get("tokens", [])
    wins = [t for t in tokens if t.get("status") == "win"]
    losses = [t for t in tokens if t.get("status") == "loss"]
    total_valid = len(tokens)
    
    # Use 'or 0' to handle None values
    # For active tokens without final_roi, use ath_roi as fallback
    total_ath_roi_all = sum((t.get("ath_roi") or 0) for t in tokens)
    total_final_roi_all = sum((t.get("final_roi") or t.get("ath_roi") or 0) for t in tokens)
    total_ath_roi_wins = sum((t.get("ath_roi") or 0) for t in wins)
    
    daily_data["daily_summary"] = {
        "total_tokens": total_valid,
        "wins": len(wins),
        "losses": len(losses),
        "success_rate": (len(wins) / total_valid * 100) if total_valid > 0 else 0,
        "average_ath_all": total_ath_roi_all / total_valid if total_valid > 0 else 0,
        "average_ath_wins": total_ath_roi_wins / len(wins) if len(wins) > 0 else 0,
        "average_final_roi": total_final_roi_all / total_valid if total_valid > 0 else 0,
        "max_roi": max(((t.get("ath_roi") or 0) for t in tokens), default=0),
    }
    return daily_data

async def clean_daily_files_for_signal(signal_type: str):
    """Scans all daily files for a signal type, deletes tokens, updates summaries."""
    logger.info(f"Scanning daily files for {signal_type}...")
    folder = f"analytics/{signal_type}/daily"
    files = await asyncio.to_thread(list_files_in_supabase_folder, folder)
    
    updated_files_count = 0
    
    for filename in files:
        if not filename.endswith(".json"): continue
        
        remote_path = f"{folder}/{filename}"
        local_path = os.path.join(TEMP_DIR, remote_path)
        
        if await download_file_from_supabase(remote_path, local_path):
            data = load_json(local_path)
            if not data or "tokens" not in data: continue
            
            original_len = len(data["tokens"])
            
            # Filter tokens
            data["tokens"] = [t for t in data["tokens"] if t.get("mint") not in TOKENS_TO_DELETE]
            
            if len(data["tokens"]) < original_len:
                removed_count = original_len - len(data["tokens"])
                logger.info(f"Removing {removed_count} tokens from {filename}")
                
                # Recalculate stats for this day
                # data = recalculate_daily_summary(data)
                
                save_json(data, local_path)
                await upload_file_to_supabase(local_path, remote_path)
                updated_files_count += 1
                
    logger.info(f"Finished {signal_type}. Updated {updated_files_count} daily files.")

# --- Summary Regeneration Logic (Duplicate from Tracker) ---

def calculate_timeframe_stats(tokens: list[dict]) -> dict:
    wins = [t for t in tokens if t.get("status") == "win"]
    losses = [t for t in tokens if t.get("status") == "loss"]
    total_valid = len(wins) + len(losses)
    
    # Use 'or 0' to handle None values
    # For active tokens without final_roi, use ath_roi as fallback
    total_ath_roi_all = sum((t.get("ath_roi") or 0) for t in tokens)
    total_final_roi_all = sum((t.get("final_roi") or t.get("ath_roi") or 0) for t in tokens)
    total_ath_roi_wins = sum((t.get("ath_roi") or 0) for t in wins)

    top_tokens = sorted(tokens, key=lambda x: x.get("ath_roi") or 0, reverse=True)

    return {
        "total_tokens": total_valid,
        "wins": len(wins),
        "losses": len(losses),
        "success_rate": (len(wins) / total_valid * 100) if total_valid > 0 else 0,
        "average_ath_all": total_ath_roi_all / total_valid if total_valid > 0 else 0,
        "average_ath_wins": total_ath_roi_wins / len(wins) if len(wins) > 0 else 0,
        "average_final_roi": total_final_roi_all / total_valid if total_valid > 0 else 0,
        "max_roi": max(((t.get("ath_roi") or 0) for t in tokens), default=0),
        "top_tokens": top_tokens[:10]
    }

async def regenerate_summary_stats(signal_type: str):
    """Regenerates the main summary_stats.json based on the NOW CLEANED daily files."""
    logger.info(f"Regenerating summary stats for {signal_type}...")
    
    folder = f"analytics/{signal_type}/daily"
    files = await asyncio.to_thread(list_files_in_supabase_folder, folder)
    all_tokens = []
    
    # Load all tokens from cleaned daily files
    for filename in files:
        if not filename.endswith(".json"): continue
        remote_path = f"{folder}/{filename}"
        local_path = os.path.join(TEMP_DIR, remote_path)
        
        # Files should already be local from the previous step, but safe to check/redownload
        data = load_json(local_path)
        if not data:
            if await download_file_from_supabase(remote_path, local_path):
                data = load_json(local_path)
        
        if data and "tokens" in data:
            all_tokens.extend(data["tokens"])
            
    now = get_now()
    available_dates = sorted([f.replace(".json","") for f in files if f.endswith(".json")])
    if not available_dates:
        start_time = now
    else:
        start_time = parse_ts(f"{available_dates[0]}T00:00:00Z")

    timeframes = {
        "1_day": now - timedelta(days=1),
        "7_days": now - timedelta(days=7),
        "1_month": now - timedelta(days=30),
        "all_time": start_time
    }
    
    summary_data = {
        "signal_type": signal_type, "last_updated": to_iso(now), "timeframes": {}
    }
    
    for period, start_date in timeframes.items():
        # Standard inclusion logic matching tracker
        filtered = [t for t in all_tokens if parse_ts(t.get("tracking_completed_at", to_iso(now))) >= start_date]
        summary_data["timeframes"][period] = calculate_timeframe_stats(filtered)

    remote_path = f"analytics/{signal_type}/summary_stats.json"
    local_path = os.path.join(TEMP_DIR, remote_path)
    save_json(summary_data, local_path)
    await upload_file_to_supabase(local_path, remote_path)
    logger.info(f"Regenerated summary stats for {signal_type}.")

async def regenerate_overall_stats():
    """Regenerates the overall combined stats."""
    logger.info("Regenerating overall stats...")
    
    # Download the just-regenerated summaries
    d_path = "analytics/discovery/summary_stats.json"
    a_path = "analytics/alpha/summary_stats.json"
    
    # Ensure we have the latest
    await download_file_from_supabase(d_path, os.path.join(TEMP_DIR, d_path))
    await download_file_from_supabase(a_path, os.path.join(TEMP_DIR, a_path))
    
    disc = load_json(os.path.join(TEMP_DIR, d_path))
    alph = load_json(os.path.join(TEMP_DIR, a_path))
    
    if not disc or not alph:
        logger.error("Missing summary stats, cannot generate overall.")
        return

    overall = {"signal_type": "overall", "last_updated": to_iso(get_now()), "timeframes": {}}
    
    for period in ["1_day", "7_days", "1_month", "all_time"]:
        d = disc["timeframes"].get(period)
        a = alph["timeframes"].get(period)
        if not d or not a: continue
        
        total = d["total_tokens"] + a["total_tokens"]
        wins = d["wins"] + a["wins"]
        
        avg_ath = 0
        if total > 0:
            avg_ath = ((d["average_ath_all"] * d["total_tokens"]) + (a["average_ath_all"] * a["total_tokens"])) / total
            
        overall["timeframes"][period] = {
            "total_tokens": total,
            "wins": wins,
            "losses": d["losses"] + a["losses"],
            "success_rate": (wins / total * 100) if total > 0 else 0,
            "average_ath_all": avg_ath,
            "max_roi": max(d["max_roi"], a["max_roi"]),
            "top_tokens": sorted(d["top_tokens"] + a["top_tokens"], key=lambda x: x["ath_roi"], reverse=True)[:10]
        }

    remote_path = "analytics/overall/summary_stats.json"
    local_path = os.path.join(TEMP_DIR, remote_path)
    save_json(overall, local_path)
    await upload_file_to_supabase(local_path, remote_path)
    logger.info("Overall stats regenerated.")

# --- Main ---

async def main():
    logger.info("="*60)
    logger.info("Starting cleanup process...")
    logger.info(f"Tokens to delete: {TOKENS_TO_DELETE}")
    logger.info(f"Supabase available: {SUPABASE_AVAILABLE}")
    logger.info("="*60)
    
    # 1. Active Tracking (both Supabase and local)
    await clean_active_tracking()
    
    if SUPABASE_AVAILABLE:
        # 2. Discovery
        await clean_daily_files_for_signal("discovery")
        # await regenerate_summary_stats("discovery")
        
        # 3. Alpha
        await clean_daily_files_for_signal("alpha")
        # await regenerate_summary_stats("alpha")
        
        # 4. Overall
        # await regenerate_overall_stats()
    else:
        logger.warning("⚠️ Skipping Supabase daily files and stats (module not available)")
        logger.info("💡 To clean Supabase files, install: pip install supabase")
    
    logger.info("="*60)
    logger.info("✅ Cleanup complete!")
    logger.info("="*60)

if __name__ == "__main__":
    asyncio.run(main())