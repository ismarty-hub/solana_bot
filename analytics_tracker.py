#!/usr/bin/env python3
"""
analytics_tracker.py

Standalone Python script to track and analyze the performance of Solana
memecoin trading signals from Supabase.

This script:
1.  Downloads 'discovery' (overlap_results.json) and 'alpha'
    (overlap_results_alpha.json) signal files every minute.
2.  Deduplicates tokens PER SIGNAL TYPE, tracking each mint-signal pair distinctly.
3.  Fetches token age, entry price, mcap, and liquidity from the JSON data.
4.  Tracks prices asynchronously (Jupiter -> Dexscreener verification).
5.  VALIDATES SUSPICIOUS PUMPS:
    - If Jupiter price implies Mcap/Liquidity ratio > 10x, triggers verification.
    - Verifies via Dexscreener with exponential backoff for 429s.
    - Validates against CURRENT liquidity and minimum 5m volume ($500).
6.  Calculates ROI, ATH, and win/loss status (win >= 45% ROI).
7.  Handles API failures with retry until tracking duration ends.
8.  Generates and uploads daily, summary, and overall analytics to Supabase.
9.  Resumes tracking from 'active_tracking.json' on restart.
"""

import os
import json
import time
import asyncio
import aiohttp
import logging
import requests
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client
from dateutil import parser
import copy
from dotenv import load_dotenv
from typing import Dict, Any, Optional

load_dotenv()

# --- Configuration Variables ---

# Polling & Tracking
SIGNAL_DOWNLOAD_INTERVAL = 60  # Download signals every 1 minute
PRICE_CHECK_INTERVAL_NEW = 5    # 5 seconds for tokens <= 12 hours old
PRICE_CHECK_INTERVAL_OLD = 240  # 4 minutes for tokens > 12 hours old
TRACKING_DURATION_NEW = 24      # Track new tokens for 24 hours
TRACKING_DURATION_OLD = 168     # Track old tokens for 7 days

# Validation Thresholds
MCAP_LIQUIDITY_RATIO_THRESHOLD = 10.0 # If Mcap / Liquidity > 10, verify
MIN_VOLUME_5M_USD = 500.0             # Minimum 5m volume to accept a suspicious pump

# Retry Logic
RETRY_INTERVAL = 5              # Check every 5 seconds during retry

# Supabase
BUCKET_NAME = "monitor-data"
TEMP_DIR = "/tmp/analytics_tracker"

# Analytics
STATS_UPDATE_INTERVAL = 3600    # Update summary stats every 1 hour
ACTIVE_UPLOAD_INTERVAL = 300    # Upload active_tracking.json every 5 mins
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
_file_cache_headers: Dict[str, Dict[str, str]] = {}

# --- Supabase Client & Helpers ---

def get_supabase_client() -> Client:
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
    global _file_cache_headers
    client = get_supabase_client()

    try:
        signed_url_response = await asyncio.to_thread(
            client.storage.from_(BUCKET_NAME).create_signed_url, remote_path, 60
        )
        signed_url = signed_url_response.get('signedURL')
        if not signed_url:
            return False

        headers = {}
        cached_headers = _file_cache_headers.get(remote_path, {})
        if cached_headers.get('Last-Modified'):
            headers['If-Modified-Since'] = cached_headers['Last-Modified']
        if cached_headers.get('ETag'):
            headers['If-None-Match'] = cached_headers['ETag']

        def _blocking_get():
            return requests.get(signed_url, headers=headers, timeout=15)
        
        response = await asyncio.to_thread(_blocking_get)

        if response.status_code == 304:
            if os.path.exists(local_path):
                return True
            else:
                headers.pop('If-Modified-Since', None)
                headers.pop('If-None-Match', None)
                response = await asyncio.to_thread(_blocking_get)

        if response.status_code == 200:
            data = response.content
            new_headers_to_cache = {}
            if response.headers.get('Last-Modified'):
                new_headers_to_cache['Last-Modified'] = response.headers.get('Last-Modified')
            if response.headers.get('ETag'):
                new_headers_to_cache['ETag'] = response.headers.get('ETag')
            
            if new_headers_to_cache:
                _file_cache_headers[remote_path] = new_headers_to_cache

            def _save_local():
                os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
                with open(local_path, "wb") as f:
                    f.write(data)
            
            await asyncio.to_thread(_save_local)
            return True
        
        if os.path.exists(local_path):
            return True
            
        return False

    except Exception as e:
        logger.error(f"File '{remote_path}': Download error. {e}")
        if os.path.exists(local_path):
            return True
        return False

async def upload_file_to_supabase(local_path: str, remote_path: str, content_type: str = "application/json") -> bool:
    if not os.path.exists(local_path):
        return False 
    try:
        client = get_supabase_client()
        try:
            await asyncio.to_thread(client.storage.from_(BUCKET_NAME).remove, [remote_path])
        except Exception:
            pass

        def _read_file():
            with open(local_path, "rb") as f:
                return f.read()
        
        data = await asyncio.to_thread(_read_file)
        await asyncio.to_thread(
            client.storage.from_(BUCKET_NAME).upload,
            remote_path, data, {"content-type": content_type, "cache-control": "3600"}
        )
        return True
    except Exception as e:
        logger.error(f"Upload failed for {remote_path}: {e}")
        return False

# --- JSON File Helpers ---

def load_json(file_path: str) -> dict | list | None:
    local_file = os.path.join(TEMP_DIR, file_path)
    if not os.path.exists(local_file):
        return None
    try:
        with open(local_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading {local_file}: {e}")
        return None

def save_json(data: dict | list, file_path: str) -> str | None:
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
    return f"{mint}_{signal_type}"

def safe_get_timestamp(entry: dict) -> str | None:
    if not isinstance(entry, dict): return None
    for field in ["ts", "timestamp", "checked_at", "created_at"]:
        if isinstance(entry.get(field), str): return entry[field]
    result = entry.get("result", {})
    if isinstance(result, dict):
        for field in ["discovered_at", "checked_at", "timestamp"]:
            if isinstance(result.get(field), str): return result[field]
    return None

def calculate_roi(entry_price: float, current_price: float) -> float:
    if entry_price == 0: return 0.0
    return ((current_price - entry_price) / entry_price) * 100

def get_now() -> datetime:
    return datetime.now(timezone.utc)

def to_iso(dt: datetime) -> str:
    return dt.isoformat().replace('+00:00', 'Z')

def parse_ts(ts_str: str) -> datetime:
    try:
        return parser.isoparse(ts_str).astimezone(timezone.utc)
    except Exception:
        return get_now()

# --- Price Fetching & Validation Functions ---

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
    """Simple Dexscreener fetch for fallback purposes (returns price only)."""
    data = await verify_suspicious_price_dexscreener(mint)
    return data.get("price") if data else None

async def verify_suspicious_price_dexscreener(mint: str) -> Dict[str, Any] | None:
    """
    (*** NEW ***)
    Fetch full market data from Dexscreener with exponential backoff for 429 errors.
    Returns a dict with {price, mcap, liquidity, volume_5m} or None on failure.
    """
    global http_session
    if not http_session or http_session.closed:
        http_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=DEXSCREENER_TIMEOUT))

    url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
    
    retries = 3
    base_delay = 2
    
    for attempt in range(retries):
        try:
            async with http_session.get(url) as response:
                if response.status == 429:
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"Dexscreener 429 (Rate Limit) for {mint}. Retrying in {delay}s...")
                    await asyncio.sleep(delay)
                    continue
                
                if response.status != 200:
                    logger.warning(f"Dexscreener failed for {mint} (Status: {response.status})")
                    return None
                
                data = await response.json()
                pairs = data.get("pairs")
                if not pairs:
                    return None
                
                # Use the first pair (usually highest liquidity)
                pair = pairs[0]
                
                price = float(pair.get("priceUsd") or 0)
                
                # Extract Market Cap (try marketCap, fallback to fdv)
                mcap = pair.get("marketCap")
                if mcap is None:
                    mcap = pair.get("fdv")
                mcap = float(mcap or 0)

                # Extract Liquidity (USD)
                liquidity_usd = float(pair.get("liquidity", {}).get("usd") or 0)

                # Extract 5m Volume (try m5, fallback to h1/12 if desperate, but m5 is standard)
                volume_5m = float(pair.get("volume", {}).get("m5") or 0)

                return {
                    "price": price,
                    "mcap": mcap,
                    "liquidity": liquidity_usd,
                    "volume_5m": volume_5m
                }

        except Exception as e:
            logger.error(f"Error fetching Dexscreener full data for {mint}: {e}")
            return None
            
    return None

async def get_entry_data_from_json(mint: str, signal_type: str, signal_data: dict) -> Dict[str, float | None]:
    """
    (*** NEW ***)
    Extract entry price, mcap, and liquidity from JSON based on signal type.
    """
    entry_price = None
    entry_mcap = None
    entry_liquidity = None
    
    result = signal_data.get("result", {})

    if signal_type == "discovery":
        # Discovery Paths
        # Price
        dex_data = result.get("dexscreener", {})
        entry_price = dex_data.get("current_price_usd")
        
        # Mcap
        entry_mcap = dex_data.get("market_cap_usd")
        
        # Liquidity
        entry_liquidity = result.get("rugcheck", {}).get("total_liquidity_usd")
        
    else:
        # Alpha Paths
        security = result.get("security", {})
        dex_raw = security.get("dexscreener", {}).get("raw", {})
        rug_raw = security.get("rugcheck_raw", {})
        
        # Price (try security.dexscreener first, then raw)
        entry_price = security.get("dexscreener", {}).get("current_price_usd")
        
        # Mcap (try marketCap, then fdv)
        entry_mcap = dex_raw.get("marketCap")
        if entry_mcap is None:
            entry_mcap = dex_raw.get("fdv")
            
        # Liquidity (try dexscreener raw first, then rugcheck)
        entry_liquidity = dex_raw.get("liquidity", {}).get("usd")
        if entry_liquidity is None:
            entry_liquidity = security.get("rugcheck", {}).get("total_liquidity_usd")

    # Fallbacks if live fetching needed (though prefer JSON for entry)
    if entry_price is None:
        # Try rugcheck raw price as last resort
        try:
             entry_price = result.get("security", {}).get("rugcheck_raw", {}).get("raw", {}).get("price")
        except Exception: pass

    # Clean conversion to float
    def to_float(val):
        try:
            f = float(val)
            return f if f > 0 else None
        except (ValueError, TypeError):
            return None

    return {
        "entry_price": to_float(entry_price),
        "entry_mcap": to_float(entry_mcap),
        "entry_liquidity": to_float(entry_liquidity)
    }

async def fetch_current_price_validated(mint: str, token_data: dict) -> float | None:
    """
    (*** NEW ***)
    Fetch price with validation logic:
    1. Try Jupiter.
    2. If Jupiter price implies > 50x ratio (Mcap/EntryLiq), verify with Dexscreener.
    3. If verifying, check Mcap/CurrentLiq <= 50x and Volume > $500.
    """
    entry_price = token_data.get("entry_price")
    entry_mcap = token_data.get("entry_mcap")
    entry_liquidity = token_data.get("entry_liquidity")
    
    # 1. Try Jupiter
    jupiter_price = None
    try:
        prices = await fetch_price_jupiter([mint])
        if mint in prices and prices[mint] > 0:
            jupiter_price = prices[mint]
    except Exception as e:
        logger.warning(f"Jupiter error for {mint}: {e}")

    if jupiter_price:
        # Check for suspicious pump if we have entry data
        if entry_price and entry_mcap and entry_liquidity and entry_liquidity > 0:
            # Calculate implied current market cap
            current_mcap_est = entry_mcap * (jupiter_price / entry_price)
            
            # Ratio of Current Mcap / Entry Liquidity
            ratio = current_mcap_est / entry_liquidity
            
            if ratio > MCAP_LIQUIDITY_RATIO_THRESHOLD:
                logger.warning(f"SUSPICIOUS PUMP {mint}: Ratio {ratio:.1f}x > {MCAP_LIQUIDITY_RATIO_THRESHOLD}x. Verifying with Dexscreener...")
                
                # --- Verification Step ---
                dex_data = await verify_suspicious_price_dexscreener(mint)
                
                if dex_data:
                    dex_price = dex_data["price"]
                    dex_mcap = dex_data["mcap"]
                    dex_liquidity = dex_data["liquidity"]
                    dex_vol_5m = dex_data["volume_5m"]
                    
                    # Check 1: Ratio against CURRENT liquidity
                    verified_ratio = dex_mcap / dex_liquidity if dex_liquidity > 0 else 9999
                    
                    # Check 2: Volume check
                    if verified_ratio <= MCAP_LIQUIDITY_RATIO_THRESHOLD and dex_vol_5m >= MIN_VOLUME_5M_USD:
                        logger.info(f"VERIFIED {mint}: Dexscreener confirms legitimate pump. Ratio: {verified_ratio:.1f}x, Vol: ${dex_vol_5m}.")
                        return dex_price # Accept verified price
                    else:
                        logger.warning(f"REJECTED {mint}: Failed verification. Ratio: {verified_ratio:.1f}x (Lim: {MCAP_LIQUIDITY_RATIO_THRESHOLD}), Vol: ${dex_vol_5m} (Lim: {MIN_VOLUME_5M_USD}).")
                        return None # Reject (treat as failure)
                else:
                     logger.warning(f"REJECTED {mint}: Dexscreener fetch failed during verification.")
                     return None # Reject
            
        # Not suspicious, accept Jupiter
        return jupiter_price

    # 2. Jupiter failed, fallback to Dexscreener (standard fetch)
    try:
        data = await verify_suspicious_price_dexscreener(mint)
        if data:
            return data["price"]
    except Exception as e:
        logger.warning(f"Dexscreener fallback error for {mint}: {e}")

    return None

# --- Core Tracking Logic ---

def handle_price_failure(token_data: dict):
    now = get_now()
    mint = token_data["mint"]
    
    token_data["consecutive_failures"] += 1
    
    if token_data["retry_start_time"] is None:
        token_data["retry_start_time"] = to_iso(now)
        logger.warning(f"Price failed for {mint} ({token_data['signal_type']}). Entering retry state.")
    
    token_data["last_price_check"] = to_iso(now)

def update_token_price(token_data: dict, price: float):
    now = get_now()
    entry_price = token_data["entry_price"]
    current_roi = calculate_roi(entry_price, price)
    
    token_data["current_price"] = price
    token_data["current_roi"] = current_roi
    token_data["last_price_check"] = to_iso(now)
    token_data["last_successful_price"] = price
    token_data["consecutive_failures"] = 0
    token_data["retry_start_time"] = None

    if current_roi > token_data["ath_roi"]:
        token_data["ath_price"] = price
        token_data["ath_roi"] = current_roi
        token_data["ath_time"] = to_iso(now)
        time_to_ath = (now - parse_ts(token_data["entry_time"])).total_seconds() / 60
        token_data["time_to_ath_minutes"] = round(time_to_ath, 2)
    
    if current_roi >= WIN_ROI_THRESHOLD and not token_data["hit_50_percent"]:
        token_data["hit_50_percent"] = True
        token_data["hit_50_percent_time"] = to_iso(now)
        time_to_50 = (now - parse_ts(token_data["entry_time"])).total_seconds() / 60
        token_data["time_to_50_percent_minutes"] = round(time_to_50, 2)
        token_data["status"] = "win"
        logger.info(f"WIN: {token_data['symbol']} hit {current_roi:.2f}% ROI!")

def finalize_token_tracking(composite_key: str, token_data: dict):
    mint = token_data["mint"]
    logger.info(f"Tracking complete for {composite_key}. Final status: {token_data['status']}")
    
    if token_data["status"] == "active":
        token_data["status"] = "loss"
    
    token_data["tracking_completed_at"] = to_iso(get_now())
    final_price = token_data.get("last_successful_price", token_data["entry_price"]) or 0.0
        
    token_data["final_price"] = final_price
    token_data["final_roi"] = calculate_roi(token_data["entry_price"], final_price)
    
    entry_date_str = parse_ts(token_data["entry_time"]).strftime('%Y-%m-%d')
    signal_type = token_data["signal_type"]
    
    generate_daily_file(entry_date_str, signal_type, completed_token=token_data)
    
    if composite_key in active_tracking:
        del active_tracking[composite_key]
        logger.info(f"Removed {composite_key} from active tracking.")

async def add_new_token_to_tracking(mint: str, signal_type: str, signal_data: dict):
    # 1. Get Entry Data (Price, Mcap, Liquidity)
    entry_data = await get_entry_data_from_json(mint, signal_type, signal_data)
    entry_price = entry_data["entry_price"]
    entry_mcap = entry_data["entry_mcap"]
    entry_liquidity = entry_data["entry_liquidity"]

    if entry_price is None:
        # Fallback: Try Dexscreener API if JSON failed
        try:
            dex_data = await verify_suspicious_price_dexscreener(mint)
            if dex_data:
                entry_price = dex_data["price"]
                # Only populate if missing
                if not entry_mcap: entry_mcap = dex_data["mcap"]
                if not entry_liquidity: entry_liquidity = dex_data["liquidity"]
        except Exception: pass

    if entry_price is None:
        logger.warning(f"Excluding {mint} ({signal_type}): No entry price found.")
        return

    # 2. Get Token Age
    detected_at_str = signal_data.get("result", {}).get("security", {}).get("rugcheck_raw", {}).get("raw", {}).get("detectedAt")
    entry_ts_str = safe_get_timestamp(signal_data)
    creation_ts_str = detected_at_str or entry_ts_str
    
    if creation_ts_str is None:
        logger.warning(f"Excluding {mint}: Could not determine creation timestamp.")
        return

    creation_dt = parse_ts(creation_ts_str)
    pair_created_at_seconds = int(creation_dt.timestamp())
    age_seconds = (get_now() - creation_dt).total_seconds()
    age_hours = age_seconds / 3600
    if age_hours < 0: age_hours = 0
    
    # 3. Intervals
    is_new = age_hours <= 12
    interval_sec = PRICE_CHECK_INTERVAL_NEW if is_new else PRICE_CHECK_INTERVAL_OLD
    duration_hours = TRACKING_DURATION_NEW if is_new else TRACKING_DURATION_OLD

    # 4. Metadata
    entry_time = get_now()
    symbol = "N/A"
    name = "N/A"
    try:
        meta = signal_data.get("result", {}).get("security", {}).get("rugcheck_raw", {}).get("raw", {}).get("tokenMeta", {})
        symbol = meta.get("symbol", "N/A")
        name = meta.get("name", "N/A")
        
        if symbol == "N/A":
            meta_alpha = signal_data.get("result", {}).get("token_metadata", {})
            if meta_alpha:
                symbol = meta_alpha.get("symbol", "N/A")
                name = meta_alpha.get("name", "N/A")
        
        if symbol == "N/A":
            meta_disc = signal_data.get("token_metadata", {}) or signal_data.get("token", {})
            if meta_disc:
                symbol = meta_disc.get("symbol", "N/A")
                name = meta_disc.get("name", "N/A")
    except Exception: pass

    tracking_end_time = entry_time + timedelta(hours=duration_hours)
    
    token_data = {
        "mint": mint,
        "signal_type": signal_type,
        "symbol": symbol,
        "name": name,
        "entry_price": entry_price,
        "entry_mcap": entry_mcap,          # Stored for validation
        "entry_liquidity": entry_liquidity,# Stored for validation
        "entry_time": to_iso(entry_time),
        "token_age_hours": round(age_hours, 2),
        "pair_created_at": pair_created_at_seconds,
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
    
    composite_key = get_composite_key(mint, signal_type)
    active_tracking[composite_key] = token_data
    
    logger.info(f"New token: {symbol} | Key: {composite_key} | Liq: ${entry_liquidity} | Mcap: ${entry_mcap}")

async def process_signals(signal_data: dict, signal_type: str):
    if not isinstance(signal_data, dict): return
    
    sorted_tokens = []
    for mint, history in signal_data.items():
        if isinstance(history, list) and history:
            first_entry = history[0]
            ts_str = safe_get_timestamp(first_entry)
            if ts_str:
                sorted_tokens.append((mint, first_entry, parse_ts(ts_str)))
    
    sorted_tokens.sort(key=lambda x: x[2])
    
    for mint, first_entry, ts in sorted_tokens:
        composite_key = get_composite_key(mint, signal_type)
        if composite_key not in active_tracking:
            await add_new_token_to_tracking(mint, signal_type, first_entry)

async def update_active_token_prices():
    """
    Updates prices for active tokens, handling batching and SUSPICIOUS price validation.
    """
    now = get_now()
    tokens_to_check = {}
    tokens_to_retry = {}
    
    # 1. Identify tokens to update
    for composite_key, token_data in list(active_tracking.items()):
        tracking_end_time = parse_ts(token_data["tracking_end_time"])
        if now >= tracking_end_time:
            finalize_token_tracking(composite_key, token_data)
            continue

        if token_data["retry_start_time"] is not None:
            last_check = parse_ts(token_data["last_price_check"])
            if (now - last_check).total_seconds() >= RETRY_INTERVAL:
                tokens_to_retry[composite_key] = token_data
            continue

        last_check = parse_ts(token_data["last_price_check"])
        if (now - last_check).total_seconds() >= token_data["tracking_interval_seconds"]:
            tokens_to_check[composite_key] = token_data

    # 2. Batch fetch & Validate Normal Checks
    if tokens_to_check:
        # Map mint -> [keys]
        mint_to_keys_map = {}
        for key, data in tokens_to_check.items():
            mint = data["mint"]
            if mint not in mint_to_keys_map: mint_to_keys_map[mint] = []
            mint_to_keys_map[mint].append(key)
            
        mints_list = list(mint_to_keys_map.keys())
        
        # Fetch Jupiter prices
        jupiter_prices = await fetch_price_jupiter(mints_list)
        
        # Process results
        for mint, keys in mint_to_keys_map.items():
            # We need to process validation for each signal type separately 
            # because they might have different entry prices/liquidity
            for composite_key in keys:
                token_data = tokens_to_check[composite_key]
                
                # Decide price: Jupiter (Validated) or Dexscreener fallback
                price = None
                
                if mint in jupiter_prices:
                    raw_jup_price = jupiter_prices[mint]
                    
                    # --- VALIDATION LOGIC ---
                    entry_price = token_data.get("entry_price")
                    entry_mcap = token_data.get("entry_mcap")
                    entry_liq = token_data.get("entry_liquidity")

                    is_suspicious = False
                    if entry_price and entry_mcap and entry_liq and entry_liq > 0:
                         current_mcap_est = entry_mcap * (raw_jup_price / entry_price)
                         ratio = current_mcap_est / entry_liq
                         if ratio > MCAP_LIQUIDITY_RATIO_THRESHOLD:
                             is_suspicious = True
                    
                    if is_suspicious:
                        logger.warning(f"Suspicious pump detected for {composite_key}. Validating...")
                        # Verify via Dexscreener
                        verified_data = await verify_suspicious_price_dexscreener(mint)
                        if verified_data:
                            dex_mcap = verified_data["mcap"]
                            dex_liq = verified_data["liquidity"]
                            dex_vol = verified_data["volume_5m"]
                            
                            # Check against CURRENT liquidity
                            verified_ratio = dex_mcap / dex_liq if dex_liq > 0 else 9999
                            
                            if verified_ratio <= MCAP_LIQUIDITY_RATIO_THRESHOLD and dex_vol >= MIN_VOLUME_5M_USD:
                                price = verified_data["price"] # Accepted
                                logger.info(f"Validation PASSED for {composite_key}. Using Dexscreener price.")
                            else:
                                price = None # Rejected
                                logger.warning(f"Validation REJECTED for {composite_key}. Ratio: {verified_ratio:.1f}, Vol: {dex_vol}")
                        else:
                            price = None # Rejected
                    else:
                        price = raw_jup_price # Accepted
                
                # If Jupiter failed or was rejected, price is None here.
                # Fallback logic is handled by handle_price_failure.
                # (We could double check dexscreener here if Jupiter returned nothing, 
                # but verify_suspicious handles logic better).
                
                if price is None and mint not in jupiter_prices:
                    # Jupiter didn't return a price, try Dexscreener fallback
                     try:
                        d_data = await verify_suspicious_price_dexscreener(mint)
                        if d_data: price = d_data["price"]
                     except Exception: pass

                if price is not None:
                    update_token_price(token_data, price)
                else:
                    handle_price_failure(token_data)

    # 3. Handle Retry Tokens (Individual Validated Check)
    for composite_key, token_data in tokens_to_retry.items():
        price = await fetch_current_price_validated(token_data["mint"], token_data)
        if price is not None:
            logger.info(f"Retry successful for {composite_key}")
            update_token_price(token_data, price)
        else:
            handle_price_failure(token_data)

# --- Analytics Generation ---

def generate_daily_file(date_str: str, signal_type: str, completed_token: dict = None):
    remote_path = f"analytics/{signal_type}/daily/{date_str}.json"
    local_path = os.path.join(TEMP_DIR, remote_path)
    
    daily_data = load_json(remote_path)
    if daily_data is None:
        daily_data = {"date": date_str, "signal_type": signal_type, "tokens": [], "daily_summary": {}}
    
    if completed_token:
        token_key = get_composite_key(completed_token["mint"], completed_token["signal_type"])
        is_duplicate = False
        for t in daily_data["tokens"]:
            existing_key = get_composite_key(t["mint"], t.get("signal_type", signal_type))
            if existing_key == token_key:
                is_duplicate = True
                break
        
        if not is_duplicate:
            pruned_token = copy.deepcopy(completed_token)
            pruned_token.pop("price_history", None)
            daily_data["tokens"].append(pruned_token)
            logger.info(f"Added {token_key} to daily file {remote_path}")

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
        "success_rate": (len(wins) / total_valid * 100) if total_valid > 0 else 0,
        "average_ath_all": total_ath_roi_all / total_valid if total_valid > 0 else 0,
        "average_ath_wins": total_ath_roi_wins / len(wins) if len(wins) > 0 else 0,
        "average_final_roi": total_final_roi_all / total_valid if total_valid > 0 else 0,
        "max_roi": max((t.get("ath_roi", 0) for t in tokens), default=0),
    }

    saved_path = save_json(daily_data, remote_path)
    if saved_path:
        asyncio.create_task(upload_file_to_supabase(saved_path, remote_path))

async def get_available_daily_files(signal_type: str) -> list[str]:
    folder_path = f"analytics/{signal_type}/daily"
    files = await asyncio.to_thread(list_files_in_supabase_folder, folder_path)
    dates = []
    for filename in files:
        if filename.endswith('.json'):
            try:
                dates.append(filename.replace('.json', ''))
            except: pass
    return sorted(dates)

async def load_tokens_from_daily_files(signal_type: str, date_list: list[str]) -> list[dict]:
    all_tokens = []
    for date_str in date_list:
        remote_path = f"analytics/{signal_type}/daily/{date_str}.json"
        daily_data = load_json(remote_path)
        if daily_data is None:
            local_path = os.path.join(TEMP_DIR, remote_path)
            if await download_file_from_supabase(remote_path, local_path):
                daily_data = load_json(remote_path)
        
        if daily_data and isinstance(daily_data.get("tokens"), list):
            all_tokens.extend(daily_data["tokens"])
    return all_tokens

def calculate_timeframe_stats(tokens: list[dict]) -> dict:
    wins = [t for t in tokens if t.get("status") == "win"]
    losses = [t for t in tokens if t.get("status") == "loss"]
    total_valid = len(wins) + len(losses)
    
    total_ath_roi_all = sum(t.get("ath_roi", 0) for t in tokens)
    total_final_roi_all = sum(t.get("final_roi", 0) for t in tokens)
    total_ath_roi_wins = sum(t.get("ath_roi", 0) for t in wins)

    top_tokens = sorted(tokens, key=lambda x: x.get("ath_roi", 0), reverse=True)

    return {
        "total_tokens": total_valid,
        "wins": len(wins),
        "losses": len(losses),
        "success_rate": (len(wins) / total_valid * 100) if total_valid > 0 else 0,
        "average_ath_all": total_ath_roi_all / total_valid if total_valid > 0 else 0,
        "average_ath_wins": total_ath_roi_wins / len(wins) if len(wins) > 0 else 0,
        "average_final_roi": total_final_roi_all / total_valid if total_valid > 0 else 0,
        "max_roi": max((t.get("ath_roi", 0) for t in tokens), default=0),
        "top_tokens": top_tokens[:10]
    }

async def generate_summary_stats(signal_type: str):
    logger.info(f"Generating summary stats for {signal_type}...")
    available_dates = await get_available_daily_files(signal_type)
    if not available_dates: return
    
    all_tokens = await load_tokens_from_daily_files(signal_type, available_dates)
    now = get_now()
    
    timeframes = {
        "1_day": now - timedelta(days=1),
        "7_days": now - timedelta(days=7),
        "1_month": now - timedelta(days=30),
        "all_time": parse_ts(f"{available_dates[0]}T00:00:00Z")
    }
    
    summary_data = {
        "signal_type": signal_type, "last_updated": to_iso(now), "timeframes": {}
    }
    
    for period, start_date in timeframes.items():
        filtered = [t for t in all_tokens if parse_ts(t.get("tracking_completed_at", to_iso(now))) >= start_date]
        summary_data["timeframes"][period] = calculate_timeframe_stats(filtered)

    remote_path = f"analytics/{signal_type}/summary_stats.json"
    local_path = save_json(summary_data, remote_path)
    if local_path: await upload_file_to_supabase(local_path, remote_path)

async def generate_overall_analytics():
    logger.info("Generating overall analytics...")
    disc = load_json("analytics/discovery/summary_stats.json")
    alph = load_json("analytics/alpha/summary_stats.json")
    if not disc or not alph: return

    overall = {"signal_type": "overall", "last_updated": to_iso(get_now()), "timeframes": {}}
    
    for period in ["1_day", "7_days", "1_month", "all_time"]:
        d = disc["timeframes"].get(period)
        a = alph["timeframes"].get(period)
        if not d or not a: continue
        
        total = d["total_tokens"] + a["total_tokens"]
        wins = d["wins"] + a["wins"]
        
        # Weighted Averages
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
    local_path = save_json(overall, remote_path)
    if local_path: await upload_file_to_supabase(local_path, remote_path)

async def update_all_summary_stats():
    await generate_summary_stats("discovery")
    await generate_summary_stats("alpha")
    await generate_overall_analytics()

# --- Main Event Loop ---

async def initialize():
    global active_tracking, http_session
    os.makedirs(TEMP_DIR, exist_ok=True)
    get_supabase_client()
    http_session = aiohttp.ClientSession()
    
    remote_path = "analytics/active_tracking.json"
    local_path = os.path.join(TEMP_DIR, remote_path)
    if await download_file_from_supabase(remote_path, local_path):
        active_tracking = load_json(remote_path) or {}
    else:
        active_tracking = {}
    logger.info(f"Initialized with {len(active_tracking)} active tokens.")

async def download_and_process_signals():
    logger.info("Downloading signals...")
    disc_path = "overlap_results.json"
    alpha_path = "overlap_results_alpha.json"
    
    if await download_file_from_supabase(disc_path, os.path.join(TEMP_DIR, disc_path)):
        await process_signals(load_json(disc_path), "discovery")
    
    if await download_file_from_supabase(alpha_path, os.path.join(TEMP_DIR, alpha_path)):
        await process_signals(load_json(alpha_path), "alpha")

async def upload_active_tracking():
    remote_path = "analytics/active_tracking.json"
    local_path = save_json(active_tracking, remote_path)
    if local_path: await upload_file_to_supabase(local_path, remote_path)

async def main_loop():
    await initialize()
    last_signal = datetime.min.replace(tzinfo=timezone.utc)
    last_stats = datetime.min.replace(tzinfo=timezone.utc)
    last_upload = datetime.min.replace(tzinfo=timezone.utc)
    
    while True:
        try:
            now = get_now()
            if (now - last_signal).total_seconds() >= SIGNAL_DOWNLOAD_INTERVAL:
                await download_and_process_signals()
                last_signal = now
            
            await update_active_token_prices()
            
            if (now - last_upload).total_seconds() >= ACTIVE_UPLOAD_INTERVAL:
                await upload_active_tracking()
                last_upload = now
                
            if (now - last_stats).total_seconds() >= STATS_UPDATE_INTERVAL:
                await update_all_summary_stats()
                last_stats = now

            await asyncio.sleep(1)
        except Exception as e:
            logger.exception(f"Main loop error: {e}")
            await asyncio.sleep(10)

if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        pass
    finally:
        if http_session: asyncio.run(http_session.close())