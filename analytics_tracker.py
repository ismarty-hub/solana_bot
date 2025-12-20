#!/usr/bin/env python3
"""
analytics_tracker.py

Standalone Python script to track and analyze the performance of Solana
memecoin trading signals from Supabase.

REVISION: ENHANCED PRICE VALIDATION & LIQUIDITY MANIPULATION DETECTION
- CRITICAL FIX: Multi-level validation system for extreme price movements
- CRITICAL FIX: Liquidity manipulation detection (rug pull prevention)
- CRITICAL FIX: Three-tier validation (Normal, Suspicious, Extreme)
- CRITICAL FIX: Final safety check prevents astronomical ROIs from persisting
- Enhanced logging for better debugging of edge cases
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

# Maximum realistic price increases
MAX_REALISTIC_PRICE_MULTIPLE = 100.0  # Price can't be >100x entry price
MAX_REALISTIC_ROI = 10000.0           # 10,000% (100x) maximum
EXTREME_PUMP_THRESHOLD = 1000.0       # 1000% ROI triggers extreme validation

# Minimum liquidity requirements
MIN_ABSOLUTE_LIQUIDITY_USD = 5000.0   # $5K minimum liquidity
MIN_LIQUIDITY_FOR_HIGH_ROI = 20000.0  # $20K minimum if ROI > 500%
MIN_LIQUIDITY_FOR_EXTREME_ROI = 50000.0  # $50K for ROIs > 1000%

# Liquidity manipulation detection
LIQUIDITY_DROP_THRESHOLD = 0.5        # Flag if liquidity drops below 50% of entry

# Consensus validation
SUSPICIOUS_PRICE_MULTIPLE = 5.0       # Trigger consensus check if price >5x entry
CONSENSUS_AGREEMENT_THRESHOLD = 20.0  # Sources must agree within 20%
EXTREME_CONSENSUS_THRESHOLD = 10.0    # Stricter threshold for extreme pumps

# Retry Logic
RETRY_INTERVAL = 5              # Check every 5 seconds during retry

# Supabase
BUCKET_NAME = "monitor-data"
TEMP_DIR = "/tmp/analytics_tracker"

# Analytics
STATS_UPDATE_INTERVAL = 3600    # Update summary stats every 1 hour (failsafe)
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
    
    client = get_supabase_client()
    
    def _read_file():
        with open(local_path, "rb") as f:
            return f.read()
    
    try:
        data = await asyncio.to_thread(_read_file)
    except Exception as e:
        logger.error(f"Error reading local file {local_path}: {e}")
        return False
        
    file_options = {"content-type": content_type, "cache-control": "3600"}

    try:
        await asyncio.to_thread(
            client.storage.from_(BUCKET_NAME).update,
            remote_path, data, file_options
        )
        return True
    except Exception as update_e:
        is_not_found = "404" in str(update_e) or "not found" in str(update_e).lower()
        
        if is_not_found:
            logger.warning(f"Update failed for {remote_path} (likely new file). Trying upload instead.")
        else:
            logger.warning(f"Update failed for {remote_path}: {update_e}. Trying upload as fallback.")
            
        try:
            await asyncio.to_thread(
                client.storage.from_(BUCKET_NAME).upload,
                remote_path, data, file_options
            )
            return True
        except Exception as upload_e:
            logger.error(f"Upload failed for {remote_path} on second attempt: {upload_e}")
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
    """Fetch prices from Jupiter API in a batch with rate limit handling."""
    global http_session
    if not http_session or http_session.closed:
        http_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=JUPITER_TIMEOUT))

    url = "https://lite-api.jup.ag/price/v3"
    params = {"ids": ",".join(mints)}
    prices = {}
    
    retries = 3
    base_delay = 1
    
    for attempt in range(retries):
        try:
            async with http_session.get(url, params=params) as response:
                if response.status == 429:
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"Jupiter 429 (Rate Limit). Retrying in {delay}s...")
                    await asyncio.sleep(delay)
                    continue
                
                if response.status != 200:
                    logger.warning(f"Jupiter API error: {response.status}")
                    return {}
                    
                data = await response.json()
                for mint, info in data.items():
                    if info and info.get("usdPrice"):
                        prices[mint] = float(info["usdPrice"])
                return prices
        except Exception as e:
            logger.error(f"Error fetching Jupiter prices (attempt {attempt + 1}): {e}")
            if attempt < retries - 1:
                await asyncio.sleep(base_delay)
            
    return {}

async def verify_suspicious_price_dexscreener(mint: str) -> Dict[str, Any] | None:
    """
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
                
                pair = pairs[0]
                
                price = float(pair.get("priceUsd") or 0)
                
                mcap = pair.get("marketCap")
                if mcap is None:
                    mcap = pair.get("fdv")
                mcap = float(mcap or 0)

                liquidity_usd = float(pair.get("liquidity", {}).get("usd") or 0)
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
    Extract entry price, mcap, and liquidity from JSON based on signal type.
    """
    entry_price = None
    entry_mcap = None
    entry_liquidity = None
    
    result = signal_data.get("result", {})

    if signal_type == "discovery":
        dex_data = result.get("dexscreener", {})
        entry_price = dex_data.get("current_price_usd")
        entry_mcap = dex_data.get("market_cap_usd")
        entry_liquidity = result.get("rugcheck", {}).get("total_liquidity_usd")
        
    else:
        security = result.get("security", {})
        dex_raw = security.get("dexscreener", {}).get("raw", {})
        
        entry_price = security.get("dexscreener", {}).get("current_price_usd")
        
        entry_mcap = dex_raw.get("marketCap")
        if entry_mcap is None:
            entry_mcap = dex_raw.get("fdv")
            
        entry_liquidity = dex_raw.get("liquidity", {}).get("usd")
        if entry_liquidity is None:
            entry_liquidity = security.get("rugcheck", {}).get("total_liquidity_usd")

    if entry_price is None:
        try:
             entry_price = result.get("security", {}).get("rugcheck_raw", {}).get("raw", {}).get("price")
        except Exception: 
            pass

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

def detect_liquidity_manipulation(token_data: dict, current_liquidity: float) -> bool:
    """
    Detect if liquidity has been manipulated (rug pull indicator).
    Returns True if manipulation is suspected.
    """
    entry_liquidity = token_data.get("entry_liquidity")
    
    if entry_liquidity and entry_liquidity > 0:
        liquidity_ratio = current_liquidity / entry_liquidity
        
        if liquidity_ratio < LIQUIDITY_DROP_THRESHOLD:
            logger.warning(
                f"LIQUIDITY DROP DETECTED for {token_data['mint']}: "
                f"${current_liquidity:.0f} vs entry ${entry_liquidity:.0f} "
                f"({liquidity_ratio*100:.1f}% remaining)"
            )
            return True
    
    return False

async def validate_price_with_multi_source(mint: str, token_data: dict, jupiter_price: float) -> float | None:
    """
    Multi-source validation with cross-referencing and historical comparison.
    Returns validated price or None if validation fails.
    """
    entry_price = token_data.get("entry_price")
    entry_liquidity = token_data.get("entry_liquidity")
    baseline_price = token_data.get("consensus_baseline_price") or entry_price
    
    if not entry_price or entry_price <= 0:
        return None
    
    potential_roi = calculate_roi(entry_price, jupiter_price)
    
    # LEVEL 1: Extreme ROI Check (>1000%)
    if potential_roi > EXTREME_PUMP_THRESHOLD:
        logger.warning(
            f"EXTREME ROI DETECTED {mint}: {potential_roi:.0f}%. "
            f"Triggering strict validation..."
        )
        
        dex_data = await verify_suspicious_price_dexscreener(mint)
        
        if not dex_data:
            logger.error(f"REJECTED {mint}: No Dexscreener data for extreme pump")
            return None
        
        dex_price = dex_data["price"]
        dex_liquidity = dex_data["liquidity"]
        dex_volume_5m = dex_data["volume_5m"]
        
        if detect_liquidity_manipulation(token_data, dex_liquidity):
            logger.error(f"REJECTED {mint}: Liquidity manipulation detected")
            return None
        
        if dex_liquidity < MIN_LIQUIDITY_FOR_EXTREME_ROI:
            logger.error(
                f"REJECTED {mint}: Extreme ROI needs ${MIN_LIQUIDITY_FOR_EXTREME_ROI} liq, "
                f"got ${dex_liquidity:.0f}"
            )
            return None
        
        price_diff_pct = abs(jupiter_price - dex_price) / min(jupiter_price, dex_price) * 100
        
        if price_diff_pct > EXTREME_CONSENSUS_THRESHOLD:
            logger.error(
                f"REJECTED {mint}: Extreme pump consensus fail. "
                f"Jup=${jupiter_price:.8f} vs Dex=${dex_price:.8f} ({price_diff_pct:.1f}%)"
            )
            return None
        
        if dex_volume_5m < MIN_VOLUME_5M_USD * 5:
            logger.error(
                f"REJECTED {mint}: Extreme ROI needs ${MIN_VOLUME_5M_USD * 5} volume, "
                f"got ${dex_volume_5m:.0f}"
            )
            return None
        
        validated_price = min(jupiter_price, dex_price)
        logger.info(
            f"EXTREME PUMP VALIDATED {mint}: Using conservative price ${validated_price:.8f}"
        )
        token_data["consensus_baseline_price"] = validated_price
        token_data["last_validated_liquidity"] = dex_liquidity
        return validated_price
    
    # LEVEL 2: Regular suspicious price check (>500% or >5x baseline)
    elif potential_roi > 500 or (baseline_price and (jupiter_price / baseline_price) > SUSPICIOUS_PRICE_MULTIPLE):
        dex_data = await verify_suspicious_price_dexscreener(mint)
        
        if not dex_data:
            logger.warning(f"REJECTED {mint}: Dexscreener validation failed")
            return None
        
        dex_price = dex_data["price"]
        dex_liquidity = dex_data["liquidity"]
        dex_volume_5m = dex_data["volume_5m"]
        
        if detect_liquidity_manipulation(token_data, dex_liquidity):
            logger.warning(f"Liquidity drop detected for {mint}, applying strict validation")
        
        if dex_liquidity < MIN_ABSOLUTE_LIQUIDITY_USD:
            logger.warning(f"REJECTED {mint}: Liquidity ${dex_liquidity:.0f} < ${MIN_ABSOLUTE_LIQUIDITY_USD}")
            return None
        
        if potential_roi > 500 and dex_liquidity < MIN_LIQUIDITY_FOR_HIGH_ROI:
            logger.warning(
                f"REJECTED {mint}: ROI {potential_roi:.0f}% needs ${MIN_LIQUIDITY_FOR_HIGH_ROI} liq, "
                f"got ${dex_liquidity:.0f}"
            )
            return None
        
        price_diff_pct = abs(jupiter_price - dex_price) / min(jupiter_price, dex_price) * 100
        
        if price_diff_pct > CONSENSUS_AGREEMENT_THRESHOLD:
            logger.warning(
                f"Consensus disagreement for {mint}: {price_diff_pct:.1f}%. "
                f"Using conservative price."
            )
            validated_price = min(jupiter_price, dex_price)
        else:
            validated_price = (jupiter_price + dex_price) / 2
        
        if potential_roi > 200 and dex_volume_5m < MIN_VOLUME_5M_USD:
            logger.warning(
                f"REJECTED {mint}: ROI {potential_roi:.0f}% needs ${MIN_VOLUME_5M_USD} volume, "
                f"got ${dex_volume_5m:.0f}"
            )
            return None
        
        token_data["consensus_baseline_price"] = validated_price
        token_data["last_validated_liquidity"] = dex_liquidity
        
        return validated_price
    
    # LEVEL 3: Price looks normal, return Jupiter price
    return jupiter_price

async def fetch_current_price_validated(mint: str, token_data: dict) -> float | None:
    """
    Fetch price with validation logic for retry scenarios.
    """
    jupiter_price = None
    try:
        prices = await fetch_price_jupiter([mint])
        if mint in prices and prices[mint] > 0:
            jupiter_price = prices[mint]
    except Exception as e:
        logger.warning(f"Jupiter error for {mint}: {e}")

    if jupiter_price:
        return await validate_price_with_multi_source(mint, token_data, jupiter_price)
    
    try:
        dex_data = await verify_suspicious_price_dexscreener(mint)
        if dex_data and dex_data["price"]:
            return await validate_price_with_multi_source(mint, token_data, dex_data["price"])
    except Exception as e:
        logger.warning(f"Dexscreener fallback error for {mint}: {e}")

    return None

# --- Analytics Generation (Daily File Handler) ---

async def update_daily_file_entry(date_str: str, signal_type: str, token_data: dict, is_final: bool) -> bool:
    """
    Handles creating or updating entries in daily files.
    """
    remote_path = f"analytics/{signal_type}/daily/{date_str}.json"
    local_path = os.path.join(TEMP_DIR, remote_path)
    
    daily_data = load_json(remote_path)
    
    if daily_data is None:
        downloaded = await download_file_from_supabase(remote_path, local_path)
        if downloaded:
            daily_data = load_json(remote_path)
    
    if daily_data is None:
        daily_data = {"date": date_str, "signal_type": signal_type, "tokens": [], "daily_summary": {}}
    
    token_key = get_composite_key(token_data["mint"], signal_type)
    
    entry_to_write = copy.deepcopy(token_data)
    entry_to_write["is_final"] = is_final
    
    tokens = daily_data.get("tokens", [])
    found_index = -1
    
    for i, t in enumerate(tokens):
        existing_key = get_composite_key(t["mint"], t.get("signal_type", signal_type))
        if existing_key == token_key:
            found_index = i
            break
            
    if found_index != -1:
        tokens[found_index] = entry_to_write
        logger.info(f"Updated entry for {token_key} in {remote_path} (is_final={is_final})")
    else:
        tokens.append(entry_to_write)
        logger.info(f"Added new entry for {token_key} to {remote_path} (is_final={is_final})")
    
    daily_data["tokens"] = tokens

    # Filter for stats calculation - ONLY tokens that passed ML check
    # We use .get("ML_PASSED") safely. Defaults to False if not present (legacy data).
    ml_passed_tokens = [t for t in tokens if t.get("ML_PASSED") is True]

    wins = [t for t in ml_passed_tokens if t.get("status") == "win"]
    losses = [t for t in ml_passed_tokens if t.get("status") == "loss"]
    total_valid = len(ml_passed_tokens)
    
    total_ath_roi_all = sum(t.get("ath_roi", 0) for t in ml_passed_tokens)
    total_final_roi_all = sum((t.get("final_roi") or 0) for t in ml_passed_tokens)
    total_ath_roi_wins = sum(t.get("ath_roi", 0) for t in wins)
    
    daily_data["daily_summary"] = {
        "total_tokens": total_valid,
        "wins": len(wins),
        "losses": len(losses),
        "success_rate": (len(wins) / total_valid * 100) if total_valid > 0 else 0,
        "average_ath_all": total_ath_roi_all / total_valid if total_valid > 0 else 0,
        "average_ath_wins": total_ath_roi_wins / len(wins) if len(wins) > 0 else 0,
        "average_final_roi": total_final_roi_all / total_valid if total_valid > 0 else 0,
        "max_roi": max((t.get("ath_roi", 0) for t in ml_passed_tokens), default=0),
    }

    saved_path = save_json(daily_data, remote_path)
    if saved_path:
        success = await upload_file_to_supabase(saved_path, remote_path)
        if success:
            logger.info(f"Successfully uploaded {remote_path}")
            return True
        else:
            logger.error(f"Failed to upload {remote_path}")
            return False
    else:
        logger.error(f"Failed to save local file {remote_path}")
        return False

# --- Core Tracking Logic ---

def handle_price_failure(token_data: dict):
    now = get_now()
    mint = token_data["mint"]
    
    token_data["consecutive_failures"] += 1
    
    if token_data["retry_start_time"] is None:
        token_data["retry_start_time"] = to_iso(now)
        logger.warning(f"Price failed for {mint} ({token_data['signal_type']}). Entering retry state.")
    
    token_data["last_price_check"] = to_iso(now)

async def update_token_price(token_data: dict, price: float):
    """
    Enhanced update with final sanity checks.
    """
    entry_price = token_data["entry_price"]
    
    # Final safety check: Reject anything beyond MAX_REALISTIC_PRICE_MULTIPLE
    price_multiple = price / entry_price
    if price_multiple > MAX_REALISTIC_PRICE_MULTIPLE:
        logger.error(
            f"REJECTED FINAL UPDATE {token_data['mint']}: "
            f"Price multiple {price_multiple:.0f}x exceeds maximum {MAX_REALISTIC_PRICE_MULTIPLE}x. "
            f"Likely data error."
        )
        handle_price_failure(token_data)
        return
    
    now = get_now()
    current_roi = calculate_roi(entry_price, price)
    
    token_data["current_price"] = price
    token_data["current_roi"] = current_roi
    token_data["last_price_check"] = to_iso(now)
    token_data["last_successful_price"] = price
    token_data["consecutive_failures"] = 0
    token_data["retry_start_time"] = None

    is_new_ath = False
    if current_roi > token_data["ath_roi"]:
        token_data["ath_price"] = price
        token_data["ath_roi"] = current_roi
        token_data["ath_time"] = to_iso(now)
        time_to_ath = (now - parse_ts(token_data["entry_time"])).total_seconds() / 60
        token_data["time_to_ath_minutes"] = round(time_to_ath, 2)
        is_new_ath = True
    
    if current_roi >= WIN_ROI_THRESHOLD:
        
        if not token_data["hit_50_percent"]:
            logger.info(f"WIN CANDIDATE: {token_data.get('symbol', 'Unknown')} hit {current_roi:.2f}% ROI! Attempting to write.")
            
            original_status = token_data["status"]
            token_data["hit_50_percent"] = True
            token_data["hit_50_percent_time"] = to_iso(now)
            time_to_50 = (now - parse_ts(token_data["entry_time"])).total_seconds() / 60
            token_data["time_to_50_percent_minutes"] = round(time_to_50, 2)
            token_data["status"] = "win"
            
            win_date_str = now.strftime('%Y-%m-%d')
            success = await update_daily_file_entry(win_date_str, token_data["signal_type"], token_data, is_final=False)
            
            if success:
                logger.info(f"WIN CONFIRMED: {token_data.get('symbol')} stats saved to daily file.")
                asyncio.create_task(update_all_summary_stats())
            else:
                logger.error(f"WIN WRITE FAILED: {token_data.get('symbol')}. Reverting state to retry next loop.")
                token_data["hit_50_percent"] = False
                token_data["hit_50_percent_time"] = None
                token_data["time_to_50_percent_minutes"] = None
                token_data["status"] = original_status

        elif is_new_ath:
            if token_data.get("hit_50_percent_time"):
                win_date = parse_ts(token_data["hit_50_percent_time"])
                date_str = win_date.strftime('%Y-%m-%d')
                await update_daily_file_entry(date_str, token_data["signal_type"], token_data, is_final=False)

async def finalize_token_tracking(composite_key: str, token_data: dict):
    """
    Finalizes a token.
    """
    mint = token_data["mint"]
    logger.info(f"Tracking complete for {composite_key}. Finalizing...")
    
    if token_data["status"] == "active":
        token_data["status"] = "loss"
        if "hit_50_percent" not in token_data:
            token_data["hit_50_percent"] = False
    
    token_data["tracking_completed_at"] = to_iso(get_now())
    final_price = token_data.get("last_successful_price", token_data["entry_price"]) or 0.0
        
    token_data["final_price"] = final_price
    token_data["final_roi"] = calculate_roi(token_data["entry_price"], final_price)
    
    signal_type = token_data["signal_type"]
    date_str = ""
    
    if token_data["status"] == "win":
        if token_data.get("hit_50_percent_time"):
            win_date = parse_ts(token_data["hit_50_percent_time"])
            date_str = win_date.strftime('%Y-%m-%d')
        else:
            date_str = get_now().strftime('%Y-%m-%d')
    else:
        date_str = get_now().strftime('%Y-%m-%d')
            
    success = await update_daily_file_entry(date_str, signal_type, token_data, is_final=True)
    
    if success:
        if composite_key in active_tracking:
            del active_tracking[composite_key]
            logger.info(f"SUCCESS: Archived and removed {composite_key} from active tracking.")
    else:
        logger.error(f"FAILURE: Could not archive {composite_key}. Retaining in active tracking for retry.")

async def add_new_token_to_tracking(mint: str, signal_type: str, signal_data: dict):
    entry_data = await get_entry_data_from_json(mint, signal_type, signal_data)
    entry_price = entry_data["entry_price"]
    entry_mcap = entry_data["entry_mcap"]
    entry_liquidity = entry_data["entry_liquidity"]

    if entry_price is None:
        try:
            dex_data = await verify_suspicious_price_dexscreener(mint)
            if dex_data:
                entry_price = dex_data["price"]
                if not entry_mcap: entry_mcap = dex_data["mcap"]
                if not entry_liquidity: entry_liquidity = dex_data["liquidity"]
        except Exception: 
            pass

    if entry_price is None:
        logger.warning(f"Excluding {mint} ({signal_type}): No entry price found.")
        return

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
    
    is_new = age_hours <= 12
    interval_sec = PRICE_CHECK_INTERVAL_NEW if is_new else PRICE_CHECK_INTERVAL_OLD
    duration_hours = TRACKING_DURATION_NEW if is_new else TRACKING_DURATION_OLD

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
    except Exception: 
        pass

    tracking_end_time = entry_time + timedelta(hours=duration_hours)
    
    token_data = {
        "mint": mint,
        "signal_type": signal_type,
        "symbol": symbol,
        "name": name,
        "entry_price": entry_price,
        "entry_mcap": entry_mcap,          
        "entry_liquidity": entry_liquidity,
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
        
        "consensus_baseline_price": None,
        "last_validated_liquidity": None,
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
    Enhanced batch update with multi-source validation.
    """
    now = get_now()
    tokens_to_check = {}
    tokens_to_retry = {}
    
    for composite_key, token_data in list(active_tracking.items()):
        tracking_end_time = parse_ts(token_data["tracking_end_time"])
        if now >= tracking_end_time:
            await finalize_token_tracking(composite_key, token_data)
            continue

        if token_data["retry_start_time"] is not None:
            last_check = parse_ts(token_data["last_price_check"])
            if (now - last_check).total_seconds() >= RETRY_INTERVAL:
                tokens_to_retry[composite_key] = token_data
            continue

        last_check = parse_ts(token_data["last_price_check"])
        if (now - last_check).total_seconds() >= token_data["tracking_interval_seconds"]:
            tokens_to_check[composite_key] = token_data

    if tokens_to_check:
        mint_to_keys_map = {}
        for key, data in tokens_to_check.items():
            mint = data["mint"]
            if mint not in mint_to_keys_map: 
                mint_to_keys_map[mint] = []
            mint_to_keys_map[mint].append(key)
            
        mints_list = list(mint_to_keys_map.keys())
        jupiter_prices = await fetch_price_jupiter(mints_list)
        
        for mint, keys in mint_to_keys_map.items():
            for composite_key in keys:
                token_data = tokens_to_check[composite_key]
                
                raw_jupiter_price = jupiter_prices.get(mint)
                
                if raw_jupiter_price:
                    validated_price = await validate_price_with_multi_source(
                        mint, token_data, raw_jupiter_price
                    )
                    
                    if validated_price:
                        await update_token_price(token_data, validated_price)
                    else:
                        handle_price_failure(token_data)
                else:
                    dex_data = await verify_suspicious_price_dexscreener(mint)
                    if dex_data and dex_data["price"]:
                        validated_price = await validate_price_with_multi_source(
                            mint, token_data, dex_data["price"]
                        )
                        if validated_price:
                            await update_token_price(token_data, validated_price)
                        else:
                            handle_price_failure(token_data)
                    else:
                        handle_price_failure(token_data)

    for composite_key, token_data in tokens_to_retry.items():
        price = await fetch_current_price_validated(token_data["mint"], token_data)
        if price is not None:
            await update_token_price(token_data, price)
        else:
            handle_price_failure(token_data)

async def get_available_daily_files(signal_type: str) -> list[str]:
    folder_path = f"analytics/{signal_type}/daily"
    files = await asyncio.to_thread(list_files_in_supabase_folder, folder_path)
    dates = []
    for filename in files:
        if filename.endswith('.json'):
            try:
                dates.append(filename.replace('.json', ''))
            except: 
                pass
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
    """
    Calculate comprehensive statistics for a given set of tokens.
    """
    # Filter for stats calculation - ONLY tokens that passed ML check
    tokens = [t for t in tokens if t.get("ML_PASSED") is True]

    wins = [t for t in tokens if t.get("status") == "win"]
    losses = [t for t in tokens if t.get("status") == "loss"]
    total_valid = len(wins) + len(losses)
    
    total_ath_roi_all = sum(t.get("ath_roi", 0) for t in tokens)
    total_final_roi_all = sum((t.get("final_roi") or 0) for t in tokens)
    total_ath_roi_wins = sum(t.get("ath_roi", 0) for t in wins)

    negative_tokens = [
        t for t in tokens 
        if t.get("status") == "loss" 
        and t.get("final_roi") is not None 
        and t.get("final_roi") < 0
    ]
    negative_count = len(negative_tokens)
    loss_rate = (negative_count / total_valid * 100) if total_valid > 0 else 0.0
    negative_rois = [t["final_roi"] for t in negative_tokens]
    average_loss = sum(negative_rois) / len(negative_rois) if negative_rois else 0.0

    times_to_ath = [float(t["time_to_ath_minutes"]) for t in tokens if t.get("time_to_ath_minutes") is not None]
    times_to_50 = [float(t["time_to_50_percent_minutes"]) for t in wins if t.get("time_to_50_percent_minutes") is not None]

    avg_time_to_ath = (sum(times_to_ath) / len(times_to_ath)) if times_to_ath else 0.0
    avg_time_to_50 = (sum(times_to_50) / len(times_to_50)) if times_to_50 else 0.0

    top_tokens = sorted(tokens, key=lambda x: x.get("ath_roi", 0), reverse=True)
    total_ath_roi = sum(float(t.get('ath_roi', 0.0)) for t in tokens)

    return {
        "total_tokens": total_valid,
        "wins": len(wins),
        "losses": len(losses),
        "success_rate": (len(wins) / total_valid * 100) if total_valid > 0 else 0,
        "negative_returns": negative_count,
        "loss_rate": round(loss_rate, 2),
        "average_loss": round(average_loss, 2),
        "average_ath_all": total_ath_roi_all / total_valid if total_valid > 0 else 0,
        "average_ath_wins": total_ath_roi_wins / len(wins) if len(wins) > 0 else 0,
        "average_final_roi": total_final_roi_all / total_valid if total_valid > 0 else 0,
        "max_roi": max((t.get("ath_roi", 0) for t in tokens), default=0),
        "avg_time_to_ath_minutes": round(avg_time_to_ath, 2),
        "avg_time_to_50_percent_minutes": round(avg_time_to_50, 2),
        "total_aths_recorded": round(total_ath_roi, 2),
        "top_tokens": top_tokens[:10]
    }

async def generate_summary_stats(signal_type: str):
    """
    Generates summary stats with strict Event-Based Attribution.
    """
    logger.info(f"Generating summary stats for {signal_type}...")
    available_dates = await get_available_daily_files(signal_type)
    if not available_dates: return
    
    all_tokens = await load_tokens_from_daily_files(signal_type, available_dates)
    now = get_now()
    
    timeframes = {
        "1_day": now - timedelta(days=1),
        "7_days": now - timedelta(days=7),
        "1_month": now - timedelta(days=30),
        "all_time": parse_ts(f"{available_dates[0]}T00:00:00Z") if available_dates else now - timedelta(days=365)
    }
    
    summary_data = {
        "signal_type": signal_type, "last_updated": to_iso(now), "timeframes": {}
    }
    
    for period, start_date in timeframes.items():
        filtered = []
        for t in all_tokens:
            status = t.get("status")
            attribution_date = None

            if status == "active": 
                continue

            if status == "win":
                ts = t.get("hit_50_percent_time")
                if not ts:
                    ts = t.get("tracking_completed_at") or t.get("ath_time")
                attribution_date = parse_ts(ts) if ts else None
            
            elif status == "loss":
                ts = t.get("tracking_completed_at")
                attribution_date = parse_ts(ts) if ts else None
            
            if attribution_date and attribution_date >= start_date:
                filtered.append(t)
        
        summary_data["timeframes"][period] = calculate_timeframe_stats(filtered)

    remote_path = f"analytics/{signal_type}/summary_stats.json"
    local_path = save_json(summary_data, remote_path)
    if local_path: await upload_file_to_supabase(local_path, remote_path)

async def generate_overall_analytics():
    """
    Generate overall analytics combining discovery and alpha signals.
    """
    logger.info("Generating overall analytics...")
    disc = load_json("analytics/discovery/summary_stats.json")
    alph = load_json("analytics/alpha/summary_stats.json")
    if not disc or not alph: 
        return

    overall = {"signal_type": "overall", "last_updated": to_iso(get_now()), "timeframes": {}}
    
    for period in ["1_day", "7_days", "1_month", "all_time"]:
        d = disc["timeframes"].get(period)
        a = alph["timeframes"].get(period)
        if not d or not a: 
            continue
        
        total = d["total_tokens"] + a["total_tokens"]
        wins = d["wins"] + a["wins"]
        losses = d["losses"] + a["losses"]
        
        negative_returns_d = d.get("negative_returns", 0)
        negative_returns_a = a.get("negative_returns", 0)
        total_negative = negative_returns_d + negative_returns_a
        
        combined_loss_rate = (total_negative / total * 100) if total > 0 else 0.0
        
        avg_loss_d = d.get("average_loss", 0)
        avg_loss_a = a.get("average_loss", 0)
        
        combined_avg_loss = 0.0
        if total_negative > 0:
            combined_avg_loss = (
                (avg_loss_d * negative_returns_d) + (avg_loss_a * negative_returns_a)
            ) / total_negative
        
        avg_ath = 0
        if total > 0:
            avg_ath = ((d["average_ath_all"] * d["total_tokens"]) + (a["average_ath_all"] * a["total_tokens"])) / total

        total_aths_d = d.get("total_aths_recorded", 0)
        total_aths_a = a.get("total_aths_recorded", 0)
        total_aths_combined = total_aths_d + total_aths_a

        avg_time_ath_d = d.get("avg_time_to_ath_minutes", 0)
        avg_time_ath_a = a.get("avg_time_to_ath_minutes", 0)
        
        avg_time_ath_combined = 0
        if total_aths_combined > 0:
            avg_time_ath_combined = (
                (avg_time_ath_d * total_aths_d) + (avg_time_ath_a * total_aths_a)
            ) / total_aths_combined

        avg_time_50_d = d.get("avg_time_to_50_percent_minutes", 0)
        avg_time_50_a = a.get("avg_time_to_50_percent_minutes", 0)
        
        avg_time_50_combined = 0
        if wins > 0:
            d_wins = d.get("wins", 0)
            a_wins = a.get("wins", 0)
            avg_time_50_combined = (
                (avg_time_50_d * d_wins) + (avg_time_50_a * a_wins)
            ) / wins

        overall["timeframes"][period] = {
            "total_tokens": total,
            "wins": wins,
            "losses": losses,
            "success_rate": (wins / total * 100) if total > 0 else 0,
            "negative_returns": total_negative,
            "loss_rate": round(combined_loss_rate, 2),
            "average_loss": round(combined_avg_loss, 2),
            "average_ath_all": avg_ath,
            "max_roi": max(d["max_roi"], a["max_roi"]),
            "avg_time_to_ath_minutes": round(avg_time_ath_combined, 2),
            "avg_time_to_50_percent_minutes": round(avg_time_50_combined, 2),
            "total_aths_recorded": total_aths_combined,
            "top_tokens": sorted(d["top_tokens"] + a["top_tokens"], key=lambda x: x["ath_roi"], reverse=True)[:10]
        }

    remote_path = "analytics/overall/summary_stats.json"
    local_path = save_json(overall, remote_path)
    if local_path: 
        await upload_file_to_supabase(local_path, remote_path)

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