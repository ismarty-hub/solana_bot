#!/usr/bin/env python3
"""
analytics_tracker.py

Standalone Python script to track and analyze the performance of Solana
memecoin trading signals from Supabase.

This script:
1.  Downloads 'discovery' (overlap_results.json) and 'alpha'
    (overlap_results_alpha.json) signal files every 3 minutes.
2.  (CORRECTED) Deduplicates tokens PER SIGNAL TYPE, tracking each
    mint-signal pair (e.g., 'XYZ_discovery' and 'XYZ_alpha') distinctly.
3.  (CORRECTED) Fetches token age from the JSON data to determine tracking duration.
4.  (CORRECTED) Gets entry price with priority: JSON data (multiple fields) -> Jupiter -> Dexscreener.
5.  Tracks prices asynchronously (Jupiter -> Dexscreener) at intervals
    based on token age (5s for new, 4min for old).
6.  Calculates ROI, ATH, and win/loss status (win >= 50% ROI).
7.  Handles API failures with retry. Network errors are retried until 
    tracking duration ends. Tokens are ONLY finalized when tracking 
    duration completes.
8.  Generates and uploads daily, summary (1d, 7d, 30d, all-time),
    and overall analytics to Supabase.
9.  Resumes tracking from 'active_tracking.json' on restart.
"""

import os
import json
import time
import asyncio
import aiohttp
import logging
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client
from dateutil import parser
import copy
from dotenv import load_dotenv

load_dotenv()


# --- Configuration Variables ---

# Polling & Tracking
SIGNAL_DOWNLOAD_INTERVAL = 60  # Download signals every 1 minutes (in seconds)
PRICE_CHECK_INTERVAL_NEW = 5    # 5 seconds for tokens <= 12 hours old
PRICE_CHECK_INTERVAL_OLD = 240  # 4 minutes for tokens > 12 hours old
TRACKING_DURATION_NEW = 24      # Track new tokens for 24 hours
TRACKING_DURATION_OLD = 168     # Track old tokens for 7 days (168 hours) 

# Retry Logic
RETRY_INTERVAL = 5              # Check every 5 seconds during retry

# Supabase
BUCKET_NAME = "monitor-data"
TEMP_DIR = "/tmp/analytics_tracker"  # For temporary file storage

# Analytics
STATS_UPDATE_INTERVAL = 3600    # Update summary stats every 1 hour (in seconds)
ACTIVE_UPLOAD_INTERVAL = 300    # Upload active_tracking.json every 5 mins (in seconds)
WIN_ROI_THRESHOLD = 45.0        # ROI percentage to mark as a "win"

# API Timeouts
JUPITER_TIMEOUT = 5
DEXSCREENER_TIMEOUT = 10

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('analytics_tracker.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Global Variables ---
supabase: Client | None = None
active_tracking: dict = {}  # In-memory store for active tokens
http_session: aiohttp.ClientSession | None = None

# --- Supabase Client & Helpers ---

def get_supabase_client() -> Client:
    """Create and return a Supabase client."""
    global supabase
    if supabase is None:
        SUPABASE_URL = os.getenv("SUPABASE_URL")
        SUPABASE_KEY = os.getenv("SUPABASE_KEY")
        if not SUPABASE_URL or not SUPABASE_KEY:
            logger.critical("Missing SUPABASE_URL or SUPABASE_KEY env variables.")
            raise ValueError("Missing required environment variables")
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    return supabase

def list_files_in_supabase_folder(folder_path: str) -> list[str]:
    """List all files in a Supabase Storage folder."""
    try:
        client = get_supabase_client()
        result = client.storage.from_(BUCKET_NAME).list(folder_path)
        
        if not result:
            logger.info(f"No files found in {folder_path}")
            return []
        
        # Extract file names
        files = [item['name'] for item in result if item.get('name')]
        logger.info(f"Found {len(files)} files in {folder_path}")
        return files
    except Exception as e:
        logger.error(f"Error listing files in {folder_path}: {e}")
        return []

def download_file_from_supabase(remote_path: str, local_path: str) -> bool:
    """Download file from Supabase Storage."""
    try:
        client = get_supabase_client()
        data = client.storage.from_(BUCKET_NAME).download(remote_path)
        os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
        with open(local_path, "wb") as f:
            f.write(data)
        logger.info(f"Downloaded {remote_path}")
        return True
    except Exception as e:
        if "404" in str(e) or "not found" in str(e):
            logger.debug(f"File not found: {remote_path}")
        else:
            logger.error(f"Download failed for {remote_path}: {e}")
        return False

def upload_file_to_supabase(local_path: str, remote_path: str, content_type: str = "application/json") -> bool:
    """Upload file to Supabase Storage, overwriting if exists."""
    if not os.path.exists(local_path):
        logger.error(f"Cannot upload, local file missing: {local_path}")
        return False
        
    try:
        client = get_supabase_client()
        with open(local_path, "rb") as f:
            data = f.read()

        # Supabase Python client's upload handles upsert logic
        client.storage.from_(BUCKET_NAME).upload(
            remote_path,
            data,
            {"content-type": content_type, "cache-control": "3600", "upsert": "true"}
        )
        logger.info(f"Uploaded {remote_path} ({len(data)/1024:.2f} KB)")
        return True
    except Exception as e:
        logger.error(f"Upload failed for {remote_path}: {e}")
        return False

# --- JSON File Helpers ---

def load_json(file_path: str) -> dict | list | None:
    """Safely load a JSON file from the local temp directory."""
    local_file = os.path.join(TEMP_DIR, file_path)
    if not os.path.exists(local_file):
        return None
    try:
        with open(local_file, 'r') as f:
            return json.load(f)
    except json.JSONDecodeError:
        logger.error(f"Failed to decode JSON from {local_file}")
        return None
    except Exception as e:
        logger.error(f"Error loading {local_file}: {e}")
        return None

def save_json(data: dict | list, file_path: str) -> str | None:
    """Safely save data to a JSON file in the local temp directory."""
    local_file = os.path.join(TEMP_DIR, file_path)
    try:
        os.makedirs(os.path.dirname(local_file) or ".", exist_ok=True)
        with open(local_file, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        return local_file
    except Exception as e:
        logger.error(f"Error saving {local_file}: {e}")
        return None

# --- Utility Functions ---

def get_composite_key(mint: str, signal_type: str) -> str:
    """
    (*** NEW ***)
    Create a unique key for tracking a token per signal type.
    """
    return f"{mint}_{signal_type}"

def safe_get_timestamp(entry: dict) -> str | None:
    """Extract timestamp from a history entry using priority list."""
    if not isinstance(entry, dict):
        return None

    # Priority 1: Top-level fields
    for field in ["ts", "timestamp", "checked_at", "created_at", "updated_at"]:
        ts = entry.get(field)
        if isinstance(ts, str):
            return ts

    # Priority 2: Nested in 'result'
    result = entry.get("result", {})
    if isinstance(result, dict):
        for field in ["discovered_at", "checked_at", "timestamp"]:
            ts = result.get(field)
            if isinstance(ts, str):
                return ts
    
    return None

def calculate_roi(entry_price: float, current_price: float) -> float:
    """Calculate ROI percentage."""
    if entry_price == 0:
        return 0.0
    return ((current_price - entry_price) / entry_price) * 100

def get_now() -> datetime:
    """Return the current time in UTC."""
    return datetime.now(timezone.utc)

def to_iso(dt: datetime) -> str:
    """Convert datetime to UTC ISO 8601 string."""
    return dt.isoformat().replace('+00:00', 'Z')

def parse_ts(ts_str: str) -> datetime:
    """Parse a timestamp string into a UTC datetime object."""
    try:
        return parser.isoparse(ts_str).astimezone(timezone.utc)
    except Exception:
        logger.warning(f"Could not parse timestamp: {ts_str}. Using 'now'.")
        return get_now()

# --- Price Fetching Functions ---

async def fetch_price_jupiter(mints: list[str]) -> dict[str, float]:
    """Fetch prices from Jupiter API in a batch."""
    global http_session
    if not http_session or http_session.closed:
        http_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=JUPITER_TIMEOUT))

    url = "https://lite-api.jup.ag/price/v3"
    params = {"ids": ",".join(mints)}
    prices = {}
    
    try:
        async with http_session.get(url, params=params) as response:
            if response.status != 200:
                logger.warning(f"Jupiter API failed (Status: {response.status}) for {len(mints)} tokens.")
                return {}
            
            data = await response.json()
            for mint, info in data.items():
                if info and info.get("usdPrice"):
                    prices[mint] = float(info["usdPrice"])
            return prices
            
    except Exception as e:
        logger.error(f"Error fetching Jupiter prices: {e}")
        return {}

async def fetch_price_dexscreener(mint: str) -> float | None:
    """Fetch price for a single token from Dexscreener API."""
    global http_session
    if not http_session or http_session.closed:
        http_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=DEXSCREENER_TIMEOUT))

    url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
    try:
        async with http_session.get(url) as response:
            if response.status != 200:
                logger.warning(f"Dexscreener price check failed for {mint} (Status: {response.status})")
                return None
            
            data = await response.json()
            pairs = data.get("pairs")
            if not pairs:
                logger.warning(f"No pairs found for price on Dexscreener for {mint}")
                return None
            
            # Use the first pair's price
            price_str = pairs[0].get("priceUsd")
            return float(price_str) if price_str else None

    except Exception as e:
        logger.error(f"Error fetching Dexscreener price for {mint}: {e}")
        return None

async def get_entry_price(mint: str, signal_data: dict) -> float | None:
    """Get entry price using priority: JSON (dexscreener) -> JSON (rugcheck) -> Jupiter -> Dexscreener."""
    
    price = None
    
    # 1. Try to get from signal data (dexscreener field)
    if "result" in signal_data and isinstance(signal_data.get("result"), dict):
        price = signal_data["result"].get("dexscreener", {}).get("current_price_usd")
    else:
        price = signal_data.get("dexscreener", {}).get("current_price_usd")

    if price is not None:
        try:
            price_float = float(price)
            if price_float > 0:
                logger.info(f"Entry price for {mint} from JSON (dexscreener): ${price_float}")
                return price_float
        except (ValueError, TypeError):
            pass # Price was null or invalid

    # 2. Try to get from signal data (rugcheck_raw.price field)
    try:
        price = signal_data.get("result", {}).get("security", {}).get("rugcheck_raw", {}).get("raw", {}).get("price")
        if price is not None:
            price_float = float(price)
            if price_float > 0:
                logger.info(f"Entry price for {mint} from JSON (rugcheck.raw.price): ${price_float}")
                return price_float
    except (ValueError, TypeError, AttributeError):
        pass # Field didn't exist or was invalid

    # 3. Try Jupiter (live)
    prices = await fetch_price_jupiter([mint])
    if mint in prices and prices[mint] > 0:
        logger.info(f"Entry price for {mint} from Jupiter (live): ${prices[mint]}")
        return prices[mint]
    
    # 4. Try Dexscreener (live)
    price = await fetch_price_dexscreener(mint)
    if price is not None and price > 0:
        logger.info(f"Entry price for {mint} from Dexscreener (live): ${price}")
        return price

    logger.error(f"Failed to get any entry price for {mint}")
    return None

async def fetch_current_price(mint: str) -> float | None:
    """
    Fetch current price with priority: Jupiter -> Dexscreener.
    Returns price or None. All errors are treated as retryable.
    """
    
    # 1. Try Jupiter
    try:
        prices = await fetch_price_jupiter([mint])
        if mint in prices and prices[mint] > 0:
            return prices[mint]
    except Exception as e:
        logger.warning(f"Jupiter fetch error for {mint}: {e}. Trying Dexscreener.")

    # 2. Try Dexscreener
    try:
        price = await fetch_price_dexscreener(mint)
        if price is not None and price > 0:
            return price
    except Exception as e:
        logger.warning(f"Dexscreener fetch error for {mint}: {e}")

    # All errors are retryable - we'll keep trying until tracking duration ends
    return None


# --- Core Tracking Logic ---

def handle_price_failure(token_data: dict):
    """
    Handle a failed price fetch attempt.
    ALL errors are retryable - we only finalize when tracking duration ends.
    """
    now = get_now()
    mint = token_data["mint"]
    
    token_data["consecutive_failures"] += 1
    
    if token_data["retry_start_time"] is None:
        # Start the retry "state"
        token_data["retry_start_time"] = to_iso(now)
        logger.warning(f"Price fetch failed for {mint} ({token_data['signal_type']}). Entering retry state. Will retry every {RETRY_INTERVAL}s.")
    
    # Update last_price_check to align with RETRY_INTERVAL
    token_data["last_price_check"] = to_iso(now)
    
    logger.warning(f"Retryable error for {mint} ({token_data['signal_type']}). Fail count: {token_data['consecutive_failures']}. Will continue retrying until tracking duration ends.")


def update_token_price(token_data: dict, price: float):
    """Update a token's data with a new price."""
    now = get_now()
    entry_price = token_data["entry_price"]
    current_roi = calculate_roi(entry_price, price)
    
    token_data["current_price"] = price
    token_data["current_roi"] = current_roi
    token_data["last_price_check"] = to_iso(now)
    token_data["last_successful_price"] = price
    token_data["consecutive_failures"] = 0
    token_data["retry_start_time"] = None  # Reset retry state

    # Update ATH if needed
    if current_roi > token_data["ath_roi"]:
        token_data["ath_price"] = price
        token_data["ath_roi"] = current_roi
        token_data["ath_time"] = to_iso(now)
        time_to_ath = (now - parse_ts(token_data["entry_time"])).total_seconds() / 60
        token_data["time_to_ath_minutes"] = round(time_to_ath, 2)
    
    # Check win condition
    if current_roi >= WIN_ROI_THRESHOLD and not token_data["hit_50_percent"]:
        token_data["hit_50_percent"] = True
        token_data["hit_50_percent_time"] = to_iso(now)
        time_to_50 = (now - parse_ts(token_data["entry_time"])).total_seconds() / 60
        token_data["time_to_50_percent_minutes"] = round(time_to_50, 2)
        token_data["status"] = "win"
        logger.info(f"WIN: {token_data['symbol']} ({token_data['mint'][:6]}... | {token_data['signal_type']}) hit {current_roi:.2f}% ROI!")

def finalize_token_tracking(composite_key: str, token_data: dict):
    """
    (*** MODIFIED ***)
    Move a token from active_tracking to its daily file.
    Uses composite_key to remove from active_tracking.
    """
    mint = token_data["mint"]
    logger.info(f"Tracking complete for {composite_key}. Final status: {token_data['status']}")
    
    # Set final status if still active
    if token_data["status"] == "active":
        token_data["status"] = "loss" # Never hit 50%
    
    token_data["tracking_completed_at"] = to_iso(get_now())
    
    # Use last_successful_price as final_price
    final_price = token_data.get("last_successful_price", token_data["entry_price"])
    if final_price is None:
        final_price = 0.0
        
    token_data["final_price"] = final_price
    token_data["final_roi"] = calculate_roi(token_data["entry_price"], final_price)
    
    # 1. Add to daily file
    entry_date_str = parse_ts(token_data["entry_time"]).strftime('%Y-%m-%d')
    signal_type = token_data["signal_type"]
    
    generate_daily_file(entry_date_str, signal_type, completed_token=token_data)
    
    # 2. Remove from active tracking using the composite key
    if composite_key in active_tracking:
        del active_tracking[composite_key]
        logger.info(f"Removed {composite_key} from active tracking.")

async def add_new_token_to_tracking(mint: str, signal_type: str, signal_data: dict):
    """
    (*** MODIFIED ***)
    Use JSON data for entry price and age, not live APIs.
    Saves to active_tracking using a composite key.
    """
    
    # 1. Get Entry Price (using our updated function)
    entry_price = await get_entry_price(mint, signal_data)
    if entry_price is None:
        logger.warning(f"Excluding {mint} ({signal_type}): No entry price found.")
        return

    # 2. Get Token Age (from JSON, not live API)
    detected_at_str = signal_data.get("result", {}).get("security", {}).get("rugcheck_raw", {}).get("raw", {}).get("detectedAt")
    entry_ts_str = safe_get_timestamp(signal_data) # Fallback to entry timestamp
    
    creation_ts_str = detected_at_str or entry_ts_str
    
    if creation_ts_str is None:
        logger.warning(f"Excluding {mint} ({signal_type}): Could not determine creation timestamp from JSON.")
        return

    creation_dt = parse_ts(creation_ts_str)
    pair_created_at_seconds = int(creation_dt.timestamp())
    age_seconds = (get_now() - creation_dt).total_seconds()
    age_hours = age_seconds / 3600
    
    if age_hours < 0:
        logger.warning(f"Token {mint} ({signal_type}) has a future creation date: {creation_ts_str}. Using 0 for age.")
        age_hours = 0
    
    # 3. Determine tracking intervals
    is_new = age_hours <= 12
    interval_sec = PRICE_CHECK_INTERVAL_NEW if is_new else PRICE_CHECK_INTERVAL_OLD
    duration_hours = TRACKING_DURATION_NEW if is_new else TRACKING_DURATION_OLD

    # 4. Get metadata from signal (using new, more reliable paths)
    entry_time = get_now() # This is the time *we* start tracking
    symbol = "N/A"
    name = "N/A"

    try:
        # Priority 1: From rugcheck_raw.tokenMeta (most reliable)
        meta = signal_data.get("result", {}).get("security", {}).get("rugcheck_raw", {}).get("raw", {}).get("tokenMeta", {})
        symbol = meta.get("symbol", "N/A")
        name = meta.get("name", "N/A")
        
        if symbol == "N/A" or name == "N/A":
            # Priority 2: From result.token_metadata (alpha structure)
            meta_alpha = signal_data.get("result", {}).get("token_metadata", {})
            if meta_alpha:
                if symbol == "N/A":
                    symbol = meta_alpha.get("symbol", "N/A")
                if name == "N/A":
                    name = meta_alpha.get("name", "N/A")
        
        if symbol == "N/A" or name == "N/A":
             # Priority 3: From top-level token_metadata (discovery structure)
            meta_disc = signal_data.get("token_metadata", {})
            if not meta_disc:
                meta_disc = signal_data.get("token", {}) # even older fallback
            if meta_disc:
                if symbol == "N/A":
                    symbol = meta_disc.get("symbol", "N/A")
                if name == "N/A":
                    name = meta_disc.get("name", "N/A")
                 
    except Exception as e:
        logger.error(f"Error extracting metadata for {mint}: {e}")


    # 5. Create active_tracking entry
    tracking_end_time = entry_time + timedelta(hours=duration_hours)
    
    token_data = {
        "mint": mint,
        "signal_type": signal_type,
        "symbol": symbol,
        "name": name,
        "entry_price": entry_price,
        "entry_time": to_iso(entry_time),
        "token_age_hours": round(age_hours, 2),
        "pair_created_at": pair_created_at_seconds, # Storing the timestamp
        "tracking_interval_seconds": interval_sec,
        "tracking_duration_hours": duration_hours,
        "tracking_end_time": to_iso(tracking_end_time),
        
        "current_price": entry_price,
        "current_roi": 0.0,
        "ath_price": entry_price,
        "ath_roi": 0.0,
        "ath_time": to_iso(entry_time),
        "status": "active",
        
        "hit_50_percent": False,
        "hit_50_percent_time": None,
        "time_to_ath_minutes": 0.0,
        "time_to_50_percent_minutes": None,
        
        "last_price_check": to_iso(entry_time),
        "last_successful_price": entry_price,
        "consecutive_failures": 0,
        "retry_start_time": None,
        "price_history": [
            {"time": to_iso(entry_time), "price": entry_price, "roi": 0}
        ],
        "final_price": None,
        "final_roi": None,
        "tracking_completed_at": None
    }
    
    # (*** MODIFIED ***) Use composite key to store in active_tracking
    composite_key = get_composite_key(mint, signal_type)
    active_tracking[composite_key] = token_data
    
    logger.info(f"New token: {symbol} ({mint[:6]}...) | Key: {composite_key} | Age: {age_hours:.2f}h | Entry: ${entry_price}")


async def process_signals(signal_data: dict, signal_type: str):
    """
    (*** MODIFIED ***)
    Process downloaded signal file, find new tokens (per signal type),
    and add to tracking using a composite key.
    """
    if not isinstance(signal_data, dict):
        logger.warning(f"Signal data for {signal_type} is not a dict. Skipping.")
        return

    logger.info(f"Processing {len(signal_data)} tokens for {signal_type} signals...")
    
    # Sort tokens by timestamp to process earliest first
    sorted_tokens = []
    for mint, history in signal_data.items():
        if isinstance(history, list) and history:
            first_entry = history[0]
            ts_str = safe_get_timestamp(first_entry)
            if ts_str:
                sorted_tokens.append((mint, first_entry, parse_ts(ts_str)))
    
    # Sort by timestamp, ascending
    sorted_tokens.sort(key=lambda x: x[2])
    
    new_tokens_found = 0
    for mint, first_entry, ts in sorted_tokens:
        
        # (*** MODIFIED ***) Check active_tracking using the composite key
        composite_key = get_composite_key(mint, signal_type)
        
        if composite_key not in active_tracking:
            new_tokens_found += 1
            await add_new_token_to_tracking(mint, signal_type, first_entry)

    logger.info(f"Found {new_tokens_found} new tokens for {signal_type}.")


async def update_active_token_prices():
    """
    (*** MODIFIED ***)
    Check all active tokens, batch fetch prices for those needing
    an update, and update their state.
    Iterates by composite_key and batches API calls by mint.
    """
    now = get_now()
    
    # (*** MODIFIED ***) These dicts are now keyed by composite_key
    tokens_to_check = {}
    tokens_to_retry = {}
    
    # --- 1. Identify tokens to update ---
    # Iterate by composite_key
    for composite_key, token_data in list(active_tracking.items()):
        
        # Check if tracking period ended - ONLY reason to finalize
        tracking_end_time = parse_ts(token_data["tracking_end_time"])
        if now >= tracking_end_time:
            logger.info(f"Tracking duration ended for {composite_key}. Finalizing...")
            finalize_token_tracking(composite_key, token_data) # (*** MODIFIED ***) Pass composite_key
            continue

        # Check if in retry state
        if token_data["retry_start_time"] is not None:
            # Token is in a retry state - check on RETRY_INTERVAL
            last_check = parse_ts(token_data["last_price_check"])
            if (now - last_check).total_seconds() >= RETRY_INTERVAL:
                tokens_to_retry[composite_key] = token_data # (*** MODIFIED ***)
            continue

        # Check for normal price update
        last_check = parse_ts(token_data["last_price_check"])
        interval = token_data["tracking_interval_seconds"]
        if (now - last_check).total_seconds() >= interval:
            tokens_to_check[composite_key] = token_data # (*** MODIFIED ***)

    # --- 2. Batch fetch prices for normal checks ---
    if tokens_to_check:
        logger.info(f"Fetching prices for {len(tokens_to_check)} token-signals...")
        
        # (*** MODIFIED ***) Create a map of {mint: [list of composite_keys]}
        # This allows batching API calls by mint, even if we track mint_alpha and mint_discovery
        mint_to_keys_map = {}
        for composite_key, token_data in tokens_to_check.items():
            mint = token_data["mint"]
            if mint not in mint_to_keys_map:
                mint_to_keys_map[mint] = []
            mint_to_keys_map[mint].append(composite_key)
            
        mints_list = list(mint_to_keys_map.keys()) # Unique mints to fetch
        
        try:
            prices = await fetch_price_jupiter(mints_list)
        except Exception as e:
            logger.error(f"Jupiter batch fetch failed: {e}. Will retry individuals.")
            prices = {}
        
        # (*** MODIFIED ***) Store failures as (composite_key, token_data)
        failed_mints_with_keys = []
        
        # (*** MODIFIED ***) Iterate over the map to apply updates
        for mint, composite_keys in mint_to_keys_map.items():
            price = prices.get(mint)
            for composite_key in composite_keys:
                token_data = tokens_to_check[composite_key]
                if price is not None:
                    update_token_price(token_data, price)
                else:
                    failed_mints_with_keys.append((composite_key, token_data))
        
        # For tokens Jupiter failed on, try Dexscreener one-by-one
        if failed_mints_with_keys:
            logger.info(f"Jupiter failed for {len(failed_mints_with_keys)} token-signals. Trying Dexscreener...")
            for composite_key, token_data in failed_mints_with_keys:
                mint = token_data["mint"] # Get mint from token_data
                price = await fetch_current_price(mint)
                if price is not None:
                    logger.info(f"Price recovery successful for {mint} ({composite_key})!")
                    update_token_price(token_data, price)
                else:
                    handle_price_failure(token_data)

    # --- 3. Handle tokens in retry mode ---
    if tokens_to_retry:
        logger.info(f"Retrying {len(tokens_to_retry)} tokens in retry state...")
        # (*** MODIFIED ***) Iterate by composite_key
        for composite_key, token_data in tokens_to_retry.items():
            mint = token_data["mint"] # Get mint from token_data
            price = await fetch_current_price(mint)
            if price is not None:
                logger.info(f"Retry successful for {mint} ({composite_key})!")
                update_token_price(token_data, price)
            else:
                handle_price_failure(token_data)


# --- Analytics Generation ---

def generate_daily_file(date_str: str, signal_type: str, completed_token: dict = None):
    """
    (*** MODIFIED ***)
    Load, update, and save the daily file for a given date and signal type.
    Duplicate check now uses both mint and signal_type.
    """
    remote_path = f"analytics/{signal_type}/daily/{date_str}.json"
    local_path = os.path.join(TEMP_DIR, remote_path)
    
    # 1. Load existing daily file
    daily_data = load_json(remote_path)
    if daily_data is None:
        daily_data = {
            "date": date_str,
            "signal_type": signal_type,
            "tokens": [],
            "daily_summary": {}
        }
    
    # 2. Add completed token if provided
    if completed_token:
        # (*** MODIFIED ***) Avoid duplicates: check for mint AND signal_type
        token_key = get_composite_key(completed_token["mint"], completed_token["signal_type"])
        
        is_duplicate = False
        for t in daily_data["tokens"]:
            # Check if token in file has both mint and signal_type
            if "mint" in t and "signal_type" in t:
                existing_key = get_composite_key(t["mint"], t["signal_type"])
                if existing_key == token_key:
                    is_duplicate = True
                    break
            # Fallback for old data: just check mint
            elif t["mint"] == completed_token["mint"]:
                 is_duplicate = True
                 break

        if not is_duplicate:
            # Prune price history before saving to daily file
            pruned_token = copy.deepcopy(completed_token)
            pruned_token.pop("price_history", None)
            daily_data["tokens"].append(pruned_token)
            logger.info(f"Added {token_key} to daily file {remote_path}")
        else:
            logger.warning(f"Token {token_key} already in daily file {remote_path}")

    # 3. Recalculate daily summary
    tokens = daily_data.get("tokens", [])
    wins = [t for t in tokens if t.get("status") == "win"]
    losses = [t for t in tokens if t.get("status") == "loss"]
    
    total_valid = len(tokens) 
    
    total_ath_roi_all = sum(t.get("ath_roi", 0) for t in tokens)
    total_final_roi_all = sum(t.get("final_roi", 0) for t in tokens)
    total_ath_roi_wins = sum(t.get("ath_roi", 0) for t in wins)
    
    daily_data["daily_summary"] = {
        "total_tokens": total_valid,
        "wins": len(wins),
        "losses": len(losses),
        "excluded": 0,
        "success_rate": (len(wins) / total_valid * 100) if total_valid > 0 else 0,
        
        "total_ath_all": total_ath_roi_all,
        "average_ath_all": total_ath_roi_all / total_valid if total_valid > 0 else 0,
        
        "total_ath_wins": total_ath_roi_wins,
        "average_ath_wins": total_ath_roi_wins / len(wins) if len(wins) > 0 else 0,
        
        "max_roi": max((t.get("ath_roi", 0) for t in tokens), default=0),
        
        "average_final_roi": total_final_roi_all / total_valid if total_valid > 0 else 0,
        
        "win_loss_ratio": len(wins) / len(losses) if len(losses) > 0 else (len(wins) if len(wins) > 0 else 0),
        "average_time_to_ath_minutes": sum(t.get("time_to_ath_minutes", 0) for t in tokens) / total_valid if total_valid else 0,
        "average_time_to_50_percent_minutes": sum(t.get("time_to_50_percent_minutes", 0) for t in wins) / len(wins) if wins else 0,
    }

    # 4. Save and upload
    saved_path = save_json(daily_data, remote_path)
    if saved_path:
        upload_file_to_supabase(saved_path, remote_path)

def get_available_daily_files(signal_type: str) -> list[str]:
    """
    Get list of available daily JSON files from Supabase for a signal type.
    Returns list of date strings (YYYY-MM-DD).
    """
    folder_path = f"analytics/{signal_type}/daily"
    files = list_files_in_supabase_folder(folder_path)
    
    # Extract dates from filenames (e.g., "2025-11-03.json" -> "2025-11-03")
    dates = []
    for filename in files:
        if filename.endswith('.json'):
            date_str = filename.replace('.json', '')
            # Validate date format
            try:
                datetime.strptime(date_str, '%Y-%m-%d')
                dates.append(date_str)
            except ValueError:
                logger.warning(f"Invalid date filename: {filename}")
                continue
    
    return sorted(dates)

def load_tokens_from_daily_files(signal_type: str, date_list: list[str]) -> list[dict]:
    """
    Load all completed tokens from specified daily files.
    """
    all_tokens = []
    
    for date_str in date_list:
        remote_path = f"analytics/{signal_type}/daily/{date_str}.json"
        
        # Try loading from local cache first
        daily_data = load_json(remote_path)
        
        # If not in cache, try downloading
        if daily_data is None:
            local_path = os.path.join(TEMP_DIR, remote_path)
            if download_file_from_supabase(remote_path, local_path):
                daily_data = load_json(remote_path)
        
        if daily_data and isinstance(daily_data.get("tokens"), list):
            all_tokens.extend(daily_data["tokens"])
            logger.debug(f"Loaded {len(daily_data['tokens'])} tokens from {date_str}")
    
    logger.info(f"Loaded {len(all_tokens)} total tokens from {len(date_list)} daily files for {signal_type}")
    return all_tokens

def filter_tokens_by_completion_time(tokens: list[dict], start_date: datetime, end_date: datetime = None) -> list[dict]:
    """
    Filter tokens by their tracking_completed_at timestamp.
    """
    if end_date is None:
        end_date = get_now()
    
    filtered = []
    for token in tokens:
        completed_at = token.get("tracking_completed_at")
        if not completed_at:
            continue
        
        completed_dt = parse_ts(completed_at)
        if start_date <= completed_dt <= end_date:
            filtered.append(token)
    
    return filtered

def calculate_timeframe_stats(tokens: list[dict]) -> dict:
    """Calculate the summary stats block for a list of tokens."""
    wins = [t for t in tokens if t.get("status") == "win"]
    losses = [t for t in tokens if t.get("status") == "loss"]
    
    valid_tokens = wins + losses
    total_valid = len(valid_tokens)
    
    excluded = [t for t in tokens if t.get("status") not in ["win", "loss"]] 

    total_ath_roi_all = sum(t.get("ath_roi", 0) for t in valid_tokens)
    total_final_roi_all = sum(t.get("final_roi", 0) for t in valid_tokens)
    total_ath_roi_wins = sum(t.get("ath_roi", 0) for t in wins)

    top_tokens = sorted(
        valid_tokens,
        key=lambda x: x.get("ath_roi", 0), 
        reverse=True
    )

    return {
        "total_tokens": total_valid,
        "wins": len(wins),
        "losses": len(losses),
        "excluded": len(excluded),
        "success_rate": (len(wins) / total_valid * 100) if total_valid > 0 else 0,
        
        "total_ath_all": total_ath_roi_all,
        "average_ath_all": total_ath_roi_all / total_valid if total_valid > 0 else 0,
        
        "total_ath_wins": total_ath_roi_wins,
        "average_ath_wins": total_ath_roi_wins / len(wins) if len(wins) > 0 else 0,

        "max_roi": max((t.get("ath_roi", 0) for t in valid_tokens), default=0),
        
        "average_final_roi": total_final_roi_all / total_valid if total_valid > 0 else 0,
        
        "win_loss_ratio": len(wins) / len(losses) if len(losses) > 0 else (len(wins) if len(wins) > 0 else 0),
        
        "average_time_to_ath_minutes": sum(t.get("time_to_ath_minutes", 0) for t in valid_tokens) / total_valid if total_valid else 0,
        "average_time_to_50_percent_minutes": sum(t.get("time_to_50_percent_minutes", 0) for t in wins) / len(wins) if wins else 0,
        
        "top_tokens": top_tokens[:10]
    }

def generate_summary_stats(signal_type: str):
    """Generate and upload summary stats for a single signal type."""
    logger.info(f"Generating summary stats for {signal_type}...")
    now = get_now()
    
    # 1. Get list of all available daily files from Supabase
    available_dates = get_available_daily_files(signal_type)
    
    if not available_dates:
        logger.warning(f"No daily files found for {signal_type}. Skipping summary stats.")
        return
    
    logger.info(f"Found {len(available_dates)} daily files for {signal_type}: {available_dates[0]} to {available_dates[-1]}")
    
    # 2. Load ALL tokens from available daily files
    all_tokens = load_tokens_from_daily_files(signal_type, available_dates)
    
    if not all_tokens:
        logger.warning(f"No tokens loaded for {signal_type}. Skipping summary stats.")
        return
    
    # 3. Define timeframes
    timeframes = {
        "1_day": now - timedelta(days=1),
        "7_days": now - timedelta(days=7),
        "1_month": now - timedelta(days=30),
        "all_time": parse_ts(f"{available_dates[0]}T00:00:00Z")  # Earliest available date
    }
    
    summary_data = {
        "signal_type": signal_type,
        "last_updated": to_iso(now),
        "available_date_range": {
            "start": available_dates[0],
            "end": available_dates[-1]
        },
        "timeframes": {}
    }
    
    for period, start_date in timeframes.items():
        if period == "all_time":
            # Use all tokens for all_time
            tokens_for_period = all_tokens
        else:
            # Filter by completion time
            tokens_for_period = filter_tokens_by_completion_time(all_tokens, start_date, now)
            
        summary_data["timeframes"][period] = calculate_timeframe_stats(tokens_for_period)
        logger.info(f"{signal_type} - {period}: {len(tokens_for_period)} tokens")
        
    # Save and upload
    remote_path = f"analytics/{signal_type}/summary_stats.json"
    local_path = save_json(summary_data, remote_path)
    if local_path:
        upload_file_to_supabase(local_path, remote_path)

def generate_overall_analytics():
    """Combine discovery and alpha stats for overall summary."""
    logger.info("Generating overall system analytics...")
    
    # Load the summary stats we just generated
    disc_stats = load_json("analytics/discovery/summary_stats.json")
    alpha_stats = load_json("analytics/alpha/summary_stats.json")
    
    if not disc_stats or not alpha_stats:
        logger.error("Cannot generate overall stats: Missing discovery or alpha summary files.")
        return

    now = get_now()
    overall = {
        "signal_type": "overall",
        "last_updated": to_iso(now),
        "timeframes": {}
    }

    for period in ["1_day", "7_days", "1_month", "all_time"]:
        disc = disc_stats["timeframes"].get(period)
        alph = alpha_stats["timeframes"].get(period)
        
        if not disc or not alph:
            logger.warning(f"Missing period {period} in summary stats. Skipping.")
            continue
            
        total_tokens = disc["total_tokens"] + alph["total_tokens"]
        total_wins = disc["wins"] + alph["wins"]
        total_losses = disc["losses"] + alph["losses"]
        total_valid = total_tokens
        
        # Weighted average for average_final_roi
        average_final_roi = 0
        if total_valid > 0:
            average_final_roi = ((disc["average_final_roi"] * disc["total_tokens"]) + 
                               (alph["average_final_roi"] * alph["total_tokens"])) / total_valid
                           
        # Weighted average for average_ath_all
        average_ath_all = 0
        if total_valid > 0:
            average_ath_all = ((disc["average_ath_all"] * disc["total_tokens"]) +
                               (alph["average_ath_all"] * alph["total_tokens"])) / total_valid

        # Weighted average for average_ath_wins
        average_ath_wins = 0
        if total_wins > 0:
            average_ath_wins = (disc["total_ath_wins"] + alph["total_ath_wins"]) / total_wins

        overall["timeframes"][period] = {
            "total_tokens": total_valid,
            "wins": total_wins,
            "losses": total_losses,
            "excluded": disc["excluded"] + alph["excluded"],
            "success_rate": (total_wins / total_valid * 100) if total_valid > 0 else 0,
            
            "total_ath_all": disc["total_ath_all"] + alph["total_ath_all"],
            "average_ath_all": average_ath_all,
            
            "total_ath_wins": disc["total_ath_wins"] + alph["total_ath_wins"],
            "average_ath_wins": average_ath_wins,
            
            "max_roi": max(disc["max_roi"], alph["max_roi"]),
            "average_final_roi": average_final_roi,
            
            "win_loss_ratio": total_wins / total_losses if total_losses > 0 else (total_wins if total_wins > 0 else 0),
            "top_tokens": sorted(
                disc["top_tokens"] + alph["top_tokens"], 
                key=lambda x: x.get("ath_roi", 0), 
                reverse=True
            )[:10]
        }

    # Save and upload
    remote_path = "analytics/overall/summary_stats.json"
    local_path = save_json(overall, remote_path)
    if local_path:
        upload_file_to_supabase(local_path, remote_path)

async def update_all_summary_stats():
    """Run the complete analytics generation pipeline."""
    try:
        generate_summary_stats("discovery")
        generate_summary_stats("alpha")
        generate_overall_analytics()
        logger.info("All summary stats updated.")
    except Exception as e:
        logger.exception(f"Error during summary stats generation: {e}")

# --- Main Event Loop ---

async def initialize():
    """Initialize the tracker on startup."""
    global active_tracking, http_session
    logger.info("Initializing Analytics Tracker...")
    
    # Create temp directory
    os.makedirs(TEMP_DIR, exist_ok=True)
    logger.info(f"Using temp directory: {TEMP_DIR}")
    
    # Initialize Supabase client
    get_supabase_client()
    
    # Initialize HTTP session
    http_session = aiohttp.ClientSession()
    
    # Download active tracking file
    remote_path = "analytics/active_tracking.json"
    local_path = os.path.join(TEMP_DIR, remote_path)
    
    if download_file_from_supabase(remote_path, local_path):
        active_data = load_json(remote_path)
        if isinstance(active_data, dict):
            active_tracking = active_data
            logger.info(f"Loaded {len(active_tracking)} active token-signals to resume tracking.")
        else:
            logger.warning("active_tracking.json is invalid, starting fresh.")
            active_tracking = {}
    else:
        logger.info("No existing active_tracking.json, starting fresh.")
        active_tracking = {}
        
    logger.info("Initialization complete. Stats will be generated in the first loop.")

async def download_and_process_signals():
    """Download both signal files and process them."""
    logger.info("Downloading signals...")
    
    # Define file paths
    disc_remote = "overlap_results.json"
    alpha_remote = "overlap_results_alpha.json"
    disc_local = os.path.join(TEMP_DIR, disc_remote)
    alpha_local = os.path.join(TEMP_DIR, alpha_remote)
    
    # Download
    disc_success = download_file_from_supabase(disc_remote, disc_local)
    alpha_success = download_file_from_supabase(alpha_remote, alpha_local)
    
    # Process
    if disc_success:
        disc_data = load_json(disc_remote)
        if disc_data:
            await process_signals(disc_data, "discovery")
    else:
        logger.error("Failed to download discovery signals.")

    if alpha_success:
        alpha_data = load_json(alpha_remote)
        if alpha_data:
            await process_signals(alpha_data, "alpha")
    else:
        logger.error("Failed to download alpha signals.")

async def upload_active_tracking():
    """Save and upload the current active_tracking.json."""
    logger.info(f"Uploading active_tracking.json with {len(active_tracking)} items...")
    remote_path = "analytics/active_tracking.json"
    local_path = save_json(active_tracking, remote_path)
    if local_path:
        upload_file_to_supabase(local_path, remote_path)

async def main_loop():
    """The main event loop for the tracker."""
    await initialize()
    
    last_signal_download = datetime.min.replace(tzinfo=timezone.utc)
    last_stats_update = datetime.min.replace(tzinfo=timezone.utc)
    last_active_upload = datetime.min.replace(tzinfo=timezone.utc)
    
    while True:
        try:
            now = get_now()
            
            # 1. Download signals every 1 minute
            if (now - last_signal_download).total_seconds() >= SIGNAL_DOWNLOAD_INTERVAL:
                await download_and_process_signals()
                last_signal_download = now
            
            # 2. Update prices for all active tokens (runs every loop)
            await update_active_token_prices()
            
            # 3. Upload active_tracking every 5 minutes
            if (now - last_active_upload).total_seconds() >= ACTIVE_UPLOAD_INTERVAL:
                await upload_active_tracking()
                last_active_upload = now
                
            # 4. Update summary stats every hour
            if (now - last_stats_update).total_seconds() >= STATS_UPDATE_INTERVAL:
                await update_all_summary_stats()
                last_stats_update = now

            # Sleep for 1 second before next iteration
            await asyncio.sleep(1)
            
        except Exception as e:
            logger.exception(f"CRITICAL ERROR in main loop: {e}")
            logger.info("Restarting loop after 10 seconds...")
            await asyncio.sleep(10)

if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger.info("Shutting down tracker.")
    finally:
        # Cleanup HTTP session
        if http_session and not http_session.closed:
            asyncio.run(http_session.close())
        logger.info("Tracker stopped.")