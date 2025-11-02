#!/usr/bin/env python3
"""
analytics_tracker.py

Standalone Python script to track and analyze the performance of Solana
memecoin trading signals from Supabase.

This script:
1.  Downloads 'discovery' (overlap_results.json) and 'alpha'
    (overlap_results_alpha.json) signal files every 3 minutes.
2.  Deduplicates tokens, tracking each mint only once per signal type.
3.  Fetches token age from Dexscreener to determine tracking duration.
4.  Gets entry price with priority: JSON data -> Jupiter -> Dexscreener.
5.  Tracks prices asynchronously (Jupiter -> Dexscreener) at intervals
    based on token age (5s for new, 4min for old).
6.  Calculates ROI, ATH, and win/loss status (win >= 50% ROI).
7.  Handles API failures with a 1-minute retry window.
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

# --- Configuration Variables ---

# Polling & Tracking
SIGNAL_DOWNLOAD_INTERVAL = 60  # Download signals every 1 minutes (in seconds)
PRICE_CHECK_INTERVAL_NEW = 5    # 5 seconds for tokens <= 12 hours old
PRICE_CHECK_INTERVAL_OLD = 240  # 4 minutes for tokens > 12 hours old
TRACKING_DURATION_NEW = 24      # Track new tokens for 24 hours
TRACKING_DURATION_OLD = 168     # Track old tokens for 7 days (168 hours) 

# Retry Logic
RETRY_TIMEOUT = 60              # Retry price fetching for 1 minute (in seconds)
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
            logger.warning(f"File not found on Supabase: {remote_path} (This is normal if starting fresh)")
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

async def fetch_token_age(mint: str) -> tuple[float | None, int | None]:
    """
    Fetch token age from Dexscreener API.
    Returns (age_in_hours, pair_created_at_timestamp_seconds).
    """
    global http_session
    if not http_session or http_session.closed:
        http_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=DEXSCREENER_TIMEOUT))

    url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
    try:
        async with http_session.get(url) as response:
            if response.status != 200:
                logger.warning(f"Dexscreener age check failed for {mint} (Status: {response.status})")
                return None, None
            
            data = await response.json()
            pairs = data.get("pairs")
            if not pairs:
                logger.warning(f"No pairs found on Dexscreener for {mint}")
                return None, None
            
            # --- NEW ROBUST LOGIC ---
            # Collect all found creation timestamps (in milliseconds)
            timestamps_ms = []
            for pair in pairs:
                if not isinstance(pair, dict):
                    continue
                    
                # Check Path 1: Top-level of pair object
                ts1 = pair.get("pairCreatedAt")
                if isinstance(ts1, (int, float, str)) and str(ts1).isdigit():
                    timestamps_ms.append(int(ts1))
                    
                # Check Path 2: Inside liquidity object
                liquidity = pair.get("liquidity")
                if isinstance(liquidity, dict):
                    ts2 = liquidity.get("pairCreatedAt")
                    if isinstance(ts2, (int, float, str)) and str(ts2).isdigit():
                        timestamps_ms.append(int(ts2))

            if not timestamps_ms:
                logger.warning(f"No 'pairCreatedAt' key found in any pair for {mint}")
                return None, None
            
            # Find the earliest timestamp (in milliseconds)
            earliest_ms = min(timestamps_ms)
            
            # Convert to seconds for calculation
            earliest_seconds = earliest_ms / 1000.0
            # --- END NEW LOGIC ---
            
            age_seconds = time.time() - earliest_seconds
            age_hours = age_seconds / 3600
            
            return age_hours, int(earliest_seconds) # Return timestamp in seconds

    except Exception as e:
        logger.error(f"Error fetching token age for {mint}: {e}")
        return None, None

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
    """Get entry price using priority: JSON -> Jupiter -> Dexscreener."""
    
    # 1. Try to get from signal data
    # Handle both 'alpha' and 'discovery' structures
    price = None
    if "result" in signal_data and isinstance(signal_data.get("result"), dict):
        price = signal_data["result"].get("dexscreener", {}).get("current_price_usd")
    else:
        price = signal_data.get("dexscreener", {}).get("current_price_usd")

    if price is not None and float(price) > 0:
        logger.info(f"Entry price for {mint} from JSON: ${price}")
        return float(price)

    # 2. Try Jupiter
    prices = await fetch_price_jupiter([mint])
    if mint in prices and prices[mint] > 0:
        logger.info(f"Entry price for {mint} from Jupiter: ${prices[mint]}")
        return prices[mint]
    
    # 3. Try Dexscreener
    price = await fetch_price_dexscreener(mint)
    if price is not None and price > 0:
        logger.info(f"Entry price for {mint} from Dexscreener: ${price}")
        return price

    logger.error(f"Failed to get any entry price for {mint}")
    return None

async def fetch_current_price(mint: str) -> tuple[float | None, str]:
    """
    Fetch current price with priority: Jupiter -> Dexscreener.
    Returns (price, error_type). error_type is 'retryable' or 'fatal'.
    """
    
    # 1. Try Jupiter
    try:
        prices = await fetch_price_jupiter([mint])
        if mint in prices and prices[mint] > 0:
            return prices[mint], "none"
    except Exception as e:
        logger.warning(f"Jupiter fetch error for {mint}: {e}. Trying Dexscreener.")
        # Treat Jupiter error as retryable for now
        pass

    # 2. Try Dexscreener
    try:
        price = await fetch_price_dexscreener(mint)
        if price is not None and price > 0:
            return price, "none"
        
        # If price is 0 or None, it's a fatal error (token likely rugged)
        if price == 0:
            logger.warning(f"Fatal price error for {mint}: Price is 0.")
            return None, "fatal"
        
        # If price is None (e.g., 404, no pairs), it's also fatal
        logger.warning(f"Fatal price error for {mint}: No price data on Dexscreener.")
        return None, "fatal"
        
    except aiohttp.ClientError as e:
        # Network errors are retryable
        logger.warning(f"Retryable network error for {mint} on Dexscreener: {e}")
        return None, "retryable"
    except Exception as e:
        # Other errors (like JSON decode) could be retryable
        logger.warning(f"Retryable error for {mint} on Dexscreener: {e}")
        return None, "retryable"

    # Fallback, should be unreachable
    return None, "fatal"


# --- Core Tracking Logic ---

def handle_price_failure(token_data: dict, error_type: str):
    """Handle a failed price fetch attempt."""
    now = get_now()
    mint = token_data["mint"]
    
    if error_type == "fatal":
        logger.error(f"Fatal error for {mint}. Stopping tracking.")
        token_data["status"] = "loss" if not token_data["hit_50_percent"] else "win"
        finalize_token_tracking(mint, token_data)
        return

    # --- Handle Retryable Error ---
    token_data["consecutive_failures"] += 1
    
    if token_data["retry_start_time"] is None:
        # Start the 1-minute retry window
        token_data["retry_start_time"] = to_iso(now)
        logger.warning(f"Price fetch failed for {mint}. Starting 1-min retry window.")
    
    retry_start = parse_ts(token_data["retry_start_time"])
    
    if (now - retry_start).total_seconds() > RETRY_TIMEOUT:
        # Retry window exhausted. Mark as loss and stop.
        logger.error(f"Retry timeout for {mint}. Stopping tracking.")
        token_data["status"] = "loss" if not token_data["hit_50_percent"] else "win"
        finalize_token_tracking(mint, token_data)
    else:
        # Still within retry window. Will try again in RETRY_INTERVAL.
        pass

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
        logger.info(f"WIN: {token_data['symbol']} ({token_data['mint'][:6]}...) hit {current_roi:.2f}% ROI!")

    # Append to price history (optional, can be memory-intensive)
    # token_data["price_history"].append({
    #     "time": to_iso(now),
    #     "price": price,
    #     "roi": current_roi
    # })

def finalize_token_tracking(mint: str, token_data: dict):
    """Move a token from active_tracking to its daily file."""
    logger.info(f"Tracking complete for {mint}. Final status: {token_data['status']}")
    
    # Set final status if still active
    if token_data["status"] == "active":
        token_data["status"] = "loss" # Never hit 50%
    
    token_data["tracking_completed_at"] = to_iso(get_now())
    token_data["final_price"] = token_data["last_successful_price"]
    token_data["final_roi"] = calculate_roi(token_data["entry_price"], token_data["final_price"])
    
    # 1. Add to daily file
    entry_date_str = parse_ts(token_data["entry_time"]).strftime('%Y-%m-%d')
    signal_type = token_data["signal_type"]
    
    generate_daily_file(entry_date_str, signal_type, completed_token=token_data)
    
    # 2. Remove from active tracking
    if mint in active_tracking:
        del active_tracking[mint]
        logger.info(f"Removed {mint} from active tracking.")

async def add_new_token_to_tracking(mint: str, signal_type: str, signal_data: dict):
    """Fetch all data for a new token and add to active_tracking."""
    
    # 1. Get Entry Price
    entry_price = await get_entry_price(mint, signal_data)
    if entry_price is None:
        logger.warning(f"Excluding {mint}: No entry price found.")
        # We don't add this to active_tracking, so it's auto-excluded
        return

    # 2. Get Token Age
    age_hours, pair_created_at = await fetch_token_age(mint)
    if age_hours is None:
        logger.warning(f"Excluding {mint}: Could not determine token age.")
        return

    # 3. Determine tracking intervals
    is_new = age_hours <= 12
    interval_sec = PRICE_CHECK_INTERVAL_NEW if is_new else PRICE_CHECK_INTERVAL_OLD
    duration_hours = TRACKING_DURATION_NEW if is_new else TRACKING_DURATION_OLD

    # 4. Get metadata from signal
    entry_time = get_now()
    symbol = "N/A"
    name = "N/A"
    
    if "result" in signal_data and isinstance(signal_data.get("result"), dict):
        # Alpha structure
        meta = signal_data["result"].get("token_metadata", {})
        symbol = meta.get("symbol", "N/A")
        name = meta.get("name", "N/A")
    else:
        # Discovery structure
        meta = signal_data.get("token_metadata") # This path might vary
        if not meta:
             meta = signal_data.get("token") # Another common path
        if isinstance(meta, dict):
            symbol = meta.get("symbol", "N/A")
            name = meta.get("name", "N/A")

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
        "pair_created_at": pair_created_at,
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
    
    active_tracking[mint] = token_data
    logger.info(f"New token: {symbol} ({mint[:6]}...) | Signal: {signal_type} | Age: {age_hours:.2f}h | Entry: ${entry_price}")


async def process_signals(signal_data: dict, signal_type: str):
    """Process downloaded signal file, find new tokens, and add to tracking."""
    if not isinstance(signal_data, dict):
        logger.warning(f"Signal data for {signal_type} is not a dict. Skipping.")
        return

    logger.info(f"Processing {len(signal_data)} tokens for {signal_type} signals...")
    
    # Sort tokens by timestamp to process earliest first
    sorted_tokens = []
    for mint, history in signal_data.items():
        if isinstance(history, list) and history:
            # Use the *first* (earliest) entry's timestamp
            first_entry = history[0]
            ts_str = safe_get_timestamp(first_entry)
            if ts_str:
                sorted_tokens.append((mint, first_entry, parse_ts(ts_str)))
    
    # Sort by timestamp, ascending
    sorted_tokens.sort(key=lambda x: x[2])
    
    new_tokens_found = 0
    for mint, first_entry, ts in sorted_tokens:
        # Deduplication: Check if already in active_tracking
        if mint not in active_tracking:
            # Check if this token is already in a daily file (completed)
            # This check is imperfect but prevents re-tracking
            # A more robust check would involve loading all daily files
            
            # Simple check: Is it in active tracking?
            # We assume if it's not active, it's new or completed
            # The prompt implies deduplication is against *active* tokens
            # and new signals
            
            # Re-check active_tracking, as a previous signal in *this*
            # batch might have added it.
            if mint not in active_tracking:
                new_tokens_found += 1
                await add_new_token_to_tracking(mint, signal_type, first_entry)

    logger.info(f"Found {new_tokens_found} new tokens for {signal_type}.")


async def update_active_token_prices():
    """
    Check all active tokens, batch fetch prices for those needing
    an update, and update their state.
    """
    now = get_now()
    tokens_to_check = {} # mint -> token_data
    tokens_to_retry = {} # mint -> token_data
    
    # --- 1. Identify tokens to update ---
    # Use list() to avoid issues modifying dict during iteration
    for mint, token_data in list(active_tracking.items()):
        
        # Check if tracking period ended
        tracking_end_time = parse_ts(token_data["tracking_end_time"])
        if now > tracking_end_time:
            finalize_token_tracking(mint, token_data)
            continue

        # Check if in retry-fail state
        if token_data["retry_start_time"] is not None:
            retry_start = parse_ts(token_data["retry_start_time"])
            if (now - retry_start).total_seconds() < RETRY_TIMEOUT:
                # Still in retry window. Check on RETRY_INTERVAL
                last_check = parse_ts(token_data["last_price_check"])
                if (now - last_check).total_seconds() >= RETRY_INTERVAL:
                    tokens_to_retry[mint] = token_data
                continue # Skip normal check
            else:
                # Retry window expired, finalize
                logger.error(f"Retry timeout for {mint} (detected in main loop). Stopping tracking.")
                token_data["status"] = "loss" if not token_data["hit_50_percent"] else "win"
                finalize_token_tracking(mint, token_data)
                continue

        # Check for normal price update
        last_check = parse_ts(token_data["last_price_check"])
        interval = token_data["tracking_interval_seconds"]
        if (now - last_check).total_seconds() >= interval:
            tokens_to_check[mint] = token_data

    # --- 2. Batch fetch prices for normal checks ---
    if tokens_to_check:
        logger.info(f"Fetching prices for {len(tokens_to_check)} tokens...")
        mints_list = list(tokens_to_check.keys())
        
        # Use Jupiter batch fetch first
        try:
            prices = await fetch_price_jupiter(mints_list)
        except Exception as e:
            logger.error(f"Jupiter batch fetch failed: {e}. Will retry individuals.")
            prices = {}
        
        failed_mints = []
        for mint, token_data in tokens_to_check.items():
            if mint in prices:
                update_token_price(token_data, prices[mint])
            else:
                failed_mints.append((mint, token_data))
        
        # For tokens Jupiter failed on, try Dexscreener one-by-one
        if failed_mints:
            logger.info(f"Jupiter failed for {len(failed_mints)} tokens. Trying Dexscreener...")
            for mint, token_data in failed_mints:
                price, error_type = await fetch_current_price(mint) # This already tries Jup -> Dex
                if price is not None:
                    logger.info(f"Price recovery successful for {mint}!")
                    update_token_price(token_data, price)
                else:
                    # This will increment failure count or finalize
                    handle_price_failure(token_data, error_type)

    # --- 3. Handle tokens in retry mode ---
    if tokens_to_retry:
        logger.info(f"Retrying {len(tokens_to_retry)} tokens in retry window...")
        for mint, token_data in tokens_to_retry.items():
            price, error_type = await fetch_current_price(mint)
            if price is not None:
                logger.info(f"Retry successful for {mint}!")
                update_token_price(token_data, price)
            else:
                handle_price_failure(token_data, error_type)

# --- Analytics Generation ---

def generate_daily_file(date_str: str, signal_type: str, completed_token: dict = None):
    """
    Load, update, and save the daily file for a given date and signal type.
    If completed_token is provided, add it to the file.
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
        # Avoid duplicates
        if not any(t["mint"] == completed_token["mint"] for t in daily_data["tokens"]):
            # Prune price history before saving to daily file
            pruned_token = copy.deepcopy(completed_token)
            pruned_token.pop("price_history", None)
            daily_data["tokens"].append(pruned_token)
            logger.info(f"Added {completed_token['mint']} to daily file {remote_path}")
        else:
            logger.warning(f"Token {completed_token['mint']} already in daily file {remote_path}")

    # 3. Recalculate daily summary
    tokens = daily_data.get("tokens", [])
    wins = [t for t in tokens if t.get("status") == "win"]
    losses = [t for t in tokens if t.get("status") == "loss"]
    
    total_valid = len(wins) + len(losses)
    total_ath_roi = sum(t.get("ath_roi", 0) for t in tokens)
    total_final_roi = sum(t.get("final_roi", 0) for t in tokens)
    
    daily_data["daily_summary"] = {
        "total_tokens": len(tokens), # This includes 'excluded' if they ever get here
        "wins": len(wins),
        "losses": len(losses),
        "excluded": 0, # We are not tracking excluded tokens
        "success_rate": (len(wins) / total_valid * 100) if total_valid > 0 else 0,
        "total_ath": total_ath_roi,
        "average_ath": total_ath_roi / len(tokens) if tokens else 0,
        "max_roi": max((t.get("ath_roi", 0) for t in tokens), default=0),
        "overall_roi": total_final_roi / len(tokens) if tokens else 0,
        "win_loss_ratio": len(wins) / len(losses) if len(losses) > 0 else (len(wins) if len(wins) > 0 else 0),
        "average_time_to_ath_minutes": sum(t.get("time_to_ath_minutes", 0) for t in tokens) / len(tokens) if tokens else 0,
        "average_time_to_50_percent_minutes": sum(t.get("time_to_50_percent_minutes", 0) for t in wins) / len(wins) if wins else 0,
    }

    # 4. Save and upload
    saved_path = save_json(daily_data, remote_path)
    if saved_path:
        upload_file_to_supabase(saved_path, remote_path)

def load_tokens_in_range(signal_type: str, start_date: datetime, end_date: datetime) -> list[dict]:
    """Load all completed tokens from daily files within a date range."""
    all_tokens = []
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        
        # Try loading from local cache first
        remote_path = f"analytics/{signal_type}/daily/{date_str}.json"
        daily_data = load_json(remote_path)
        
        # If not in cache, try downloading
        if daily_data is None:
            local_path = os.path.join(TEMP_DIR, remote_path)
            if download_file_from_supabase(remote_path, local_path):
                daily_data = load_json(remote_path)
        
        if daily_data and isinstance(daily_data.get("tokens"), list):
            all_tokens.extend(daily_data["tokens"])
            
        current_date += timedelta(days=1)
        
    return all_tokens

def calculate_timeframe_stats(tokens: list[dict]) -> dict:
    """Calculate the summary stats block for a list of tokens."""
    wins = [t for t in tokens if t.get("status") == "win"]
    losses = [t for t in tokens if t.get("status") == "loss"]
    # Excluded are tokens where entry price was not found, they are never tracked or saved.
    excluded = [t for t in tokens if t.get("status") == "excluded"] 

    total_valid = len(wins) + len(losses)
    total_ath_roi = sum(t.get("ath_roi", 0) for t in wins) # Only sum ATH from wins
    total_final_roi = sum(t.get("final_roi", 0) for t in tokens)

    top_tokens = sorted(
        wins, 
        key=lambda x: x.get("ath_roi", 0), 
        reverse=True
    )

    return {
        "total_tokens": total_valid, # Only count valid (non-excluded)
        "wins": len(wins),
        "losses": len(losses),
        "excluded": len(excluded),
        "success_rate": (len(wins) / total_valid * 100) if total_valid > 0 else 0,
        "total_ath": total_ath_roi,
        "average_ath": total_ath_roi / len(wins) if len(wins) > 0 else 0, # Avg ATH of *wins*
        "max_roi": max((t.get("ath_roi", 0) for t in wins), default=0),
        "overall_roi": total_final_roi / total_valid if total_valid > 0 else 0, # Avg *final* ROI of all
        "win_loss_ratio": len(wins) / len(losses) if len(losses) > 0 else (len(wins) if len(wins) > 0 else 0),
        "average_time_to_ath_minutes": sum(t.get("time_to_ath_minutes", 0) for t in tokens) / total_valid if total_valid else 0,
        "average_time_to_50_percent_minutes": sum(t.get("time_to_50_percent_minutes", 0) for t in wins) / len(wins) if wins else 0,
        "top_tokens": top_tokens
    }

# =================================================================
# ===                  CORRECTED FUNCTION BELOW                 ===
# =================================================================

def generate_summary_stats(signal_type: str):
    """Generate and upload summary stats for a single signal type."""
    logger.info(f"Generating summary stats for {signal_type}...")
    now = get_now()
    
    # 1. Define the "all_time" start date
    all_time_start_date = datetime(2025, 11, 1, tzinfo=timezone.utc)
    
    # 2. Load ALL completed tokens *before* the loop
    all_time_tokens = load_tokens_in_range(signal_type, all_time_start_date, now)
    
    # 3. Define all timeframes
    timeframes = {
        "1_day": now - timedelta(days=1),
        "7_days": now - timedelta(days=7),
        "1_month": now - timedelta(days=30),
        "all_time": all_time_start_date
    }
    
    summary_data = {
        "signal_type": signal_type,
        "last_updated": to_iso(now),
        "timeframes": {}
    }
    
    for period, start_date in timeframes.items():
        if period == "all_time":
            # For "all_time", just use the full list we already loaded
            tokens_for_period = all_time_tokens
        else:
            # For other periods, filter the *pre-loaded* all_time_tokens list
            # Filter based on COMPLETION TIME (tracking_completed_at)
            tokens_for_period = [
                t for t in all_time_tokens 
                if t.get("tracking_completed_at") and parse_ts(t["tracking_completed_at"]) >= start_date
            ]
            
        summary_data["timeframes"][period] = calculate_timeframe_stats(tokens_for_period)
        
    # Save and upload
    remote_path = f"analytics/{signal_type}/summary_stats.json"
    local_path = save_json(summary_data, remote_path)
    if local_path:
        upload_file_to_supabase(local_path, remote_path)

# =================================================================
# ===                  END OF CORRECTED FUNCTION                ===
# =================================================================

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
        total_valid = total_wins + total_losses
        
        # Weighted average for overall ROI
        overall_roi = 0
        if total_tokens > 0:
            overall_roi = ((disc["overall_roi"] * disc["total_tokens"]) + 
                           (alph["overall_roi"] * alph["total_tokens"])) / total_tokens
                           
        # Weighted average for average ATH
        avg_ath = 0
        if total_wins > 0:
            avg_ath = (disc["total_ath"] + alph["total_ath"]) / total_wins

        overall["timeframes"][period] = {
            "total_tokens": total_tokens,
            "wins": total_wins,
            "losses": total_losses,
            "excluded": disc["excluded"] + alph["excluded"],
            "success_rate": (total_wins / total_valid * 100) if total_valid > 0 else 0,
            "total_ath": disc["total_ath"] + alph["total_ath"],
            "average_ath": avg_ath,
            "max_roi": max(disc["max_roi"], alph["max_roi"]),
            "overall_roi": overall_roi,
            "win_loss_ratio": total_wins / total_losses if total_losses > 0 else (total_wins if total_wins > 0 else 0),
            "top_tokens": sorted(
                disc["top_tokens"] + alph["top_tokens"], 
                key=lambda x: x.get("ath_roi", 0), 
                reverse=True
            )
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
            logger.info(f"Loaded {len(active_tracking)} active tokens to resume tracking.")
        else:
            logger.warning("active_tracking.json is invalid, starting fresh.")
            active_tracking = {}
    else:
        logger.info("No existing active_tracking.json, starting fresh.")
        active_tracking = {}
        
    # Summary stats will be generated on the first run of the main loop.
    # No need to pre-load all daily files on startup, which causes log spam.
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
    logger.info("Uploading active_tracking.json...")
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
            
            # 1. Download signals every 3 minutes
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