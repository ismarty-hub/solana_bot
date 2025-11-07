#!/usr/bin/env python3
"""
snapshot_collector.py

Production-ready data-collector service with label aggregation.
Watches new token signals from Supabase (overlap_results.json, 
overlap_results_alpha.json), enriches each signal with live market
data (Dexscreener), security data (RugCheck), and holiday/time context,
computes derived features, and persists a canonical snapshot per signal
as both .json and .pkl locally and to Supabase.

CORRECTED: Now properly processes BOTH discovery and alpha signals,
creating datasets for both pipelines.

Key Fixes:
1. Ensures signal_type from file processing matches analytics files
2. Proper composite key usage throughout
3. Validates both discovery and alpha are being processed
4. Better logging for debugging signal flow
"""

import os
import json
import pickle
import asyncio
import aiohttp
import logging
import argparse
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple, Callable
from dataclasses import dataclass, field
from dateutil import parser
from functools import wraps
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---

@dataclass
class Config:
    """Configuration class, populated from environment variables."""
    SUPABASE_URL: str = field(default_factory=lambda: os.getenv("SUPABASE_URL", ""))
    SUPABASE_KEY: str = field(default_factory=lambda: os.getenv("SUPABASE_KEY", ""))
    SUPABASE_BUCKET: str = field(default_factory=lambda: os.getenv("SUPABASE_BUCKET", "monitor-data"))
    
    SIGNAL_FILE_DISCOVERY: str = field(default_factory=lambda: os.getenv("SIGNAL_FILE_DISCOVERY", "overlap_results.json"))
    SIGNAL_FILE_ALPHA: str = field(default_factory=lambda: os.getenv("SIGNAL_FILE_ALPHA", "overlap_results_alpha.json"))
    
    SNAPSHOT_DIR_LOCAL: str = field(default_factory=lambda: os.getenv("SNAPSHOT_DIR_LOCAL", "./data/snapshots"))
    SNAPSHOT_DIR_REMOTE: str = field(default_factory=lambda: os.getenv("SNAPSHOT_DIR_REMOTE", "analytics/snapshots"))
    
    DATASET_DIR_LOCAL: str = field(default_factory=lambda: os.getenv("DATASET_DIR_LOCAL", "./data/datasets"))
    DATASET_DIR_REMOTE: str = field(default_factory=lambda: os.getenv("DATASET_DIR_REMOTE", "datasets"))
    
    POLL_INTERVAL: int = field(default_factory=lambda: int(os.getenv("POLL_INTERVAL", "180"))) # 3 minutes
    AGGREGATOR_INTERVAL: int = field(default_factory=lambda: int(os.getenv("AGGREGATOR_INTERVAL", "60"))) # 1 minute
    
    HOLIDAY_COUNTRY_CODES: List[str] = field(default_factory=lambda: os.getenv("HOLIDAY_COUNTRY_CODES", "US,GB,DE,JP,SG,KR,CN,CA,AU").split(','))
    
    API_TIMEOUT_DEX: int = field(default_factory=lambda: int(os.getenv("API_TIMEOUT_DEX", "10")))
    API_TIMEOUT_RUG: int = field(default_factory=lambda: int(os.getenv("API_TIMEOUT_RUG", "10")))
    API_TIMEOUT_HOLIDAY: int = field(default_factory=lambda: int(os.getenv("API_TIMEOUT_HOLIDAY", "5")))
    API_MAX_RETRIES: int = field(default_factory=lambda: int(os.getenv("API_MAX_RETRIES", "3")))
    
    CACHE_TTL_SECONDS: int = field(default_factory=lambda: int(os.getenv("CACHE_TTL_SECONDS", "120")))
    
    LOG_LEVEL: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO").upper())
    
    UPLOAD_TO_SUPABASE: bool = field(default_factory=lambda: os.getenv("UPLOAD_TO_SUPABASE", "true").lower() == "true")
    CLEANUP_LOCAL_FILES: bool = field(default_factory=lambda: os.getenv("CLEANUP_LOCAL_FILES", "true").lower() == "true")
    
    PROCESSOR_CONCURRENCY: int = field(default_factory=lambda: int(os.getenv("PROCESSOR_CONCURRENCY", "1")))
    
    # --- Label aggregation settings ---
    CHECK_INTERVAL_MINUTES: int = field(default_factory=lambda: int(os.getenv("CHECK_INTERVAL_MINUTES", "30")))
    TOKEN_AGE_THRESHOLD_HOURS: float = field(default_factory=lambda: float(os.getenv("TOKEN_AGE_THRESHOLD_HOURS", "12.0")))
    SHORT_FINALIZE_HOURS: int = field(default_factory=lambda: int(os.getenv("SHORT_FINALIZE_HOURS", "24")))
    LONG_FINALIZE_HOURS: int = field(default_factory=lambda: int(os.getenv("LONG_FINALIZE_HOURS", "168")))
    ANALYTICS_INDEX_TTL: int = field(default_factory=lambda: int(os.getenv("ANALYTICS_INDEX_TTL", "600")))
    AGGREGATOR_BATCH_SIZE: int = field(default_factory=lambda: int(os.getenv("AGGREGATOR_BATCH_SIZE", "10")))

    def __post_init__(self):
        if not self.SUPABASE_URL or not self.SUPABASE_KEY:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in environment.")
        os.makedirs(self.SNAPSHOT_DIR_LOCAL, exist_ok=True)
        os.makedirs(self.DATASET_DIR_LOCAL, exist_ok=True)

# --- Logging Setup ---

def setup_logging(level: str) -> logging.Logger:
    """Configures the root logger."""
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('snapshot_collector.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("supabase").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    return logging.getLogger("CollectorService")

# --- Keying Utility Function ---

def get_composite_key(mint: str, signal_type: str) -> str:
    """
    Create a unique key for tracking a token per signal type.
    Must match the key used by analytics_tracker.py.
    """
    return f"{mint}_{signal_type}"

# --- Async TTL Cache Utility ---

def async_ttl_cache(ttl_seconds: int):
    """Decorator for async in-memory TTL caching."""
    cache: Dict[Any, Tuple[Any, float]] = {}
    lock = asyncio.Lock()

    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            key = (args, tuple(sorted(kwargs.items())))
            now = time.monotonic()

            async with lock:
                if key in cache:
                    result, expiry = cache[key]
                    if now < expiry:
                        log.debug(f"[Cache HIT] for {func.__name__} key: {key[0]}")
                        return result
                    else:
                        log.debug(f"[Cache EXPIRED] for {func.__name__} key: {key[0]}")
                        del cache[key]

            log.debug(f"[Cache MISS] for {func.__name__} key: {key[0]}")
            new_result = await func(*args, **kwargs)

            async with lock:
                cache[key] = (new_result, now + ttl_seconds)
            
            return new_result
        return wrapper
    return decorator 

# --- Base API Client with Retries ---

class BaseAPIClient:
    """Base class for API clients with retries and backoff."""
    def __init__(self, session: aiohttp.ClientSession, max_retries: int, name: str):
        self.session = session
        self.max_retries = max_retries
        self.name = name

    async def async_get(self, url: str, timeout: int) -> Optional[Dict]:
        """Performs an async GET request with exponential backoff."""
        for attempt in range(self.max_retries):
            try:
                async with self.session.get(url, timeout=timeout) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    
                    log.warning(f"[{self.name}] API Error: Status {resp.status} for {url}")
                    
                    if resp.status == 429:
                        retry_after = int(resp.headers.get("Retry-After", "10"))
                        log.warning(f"[{self.name}] Rate limited. Retrying after {retry_after}s.")
                        await asyncio.sleep(retry_after)
                    elif resp.status >= 500:
                        delay = (2 ** attempt) + (0.1 * (attempt + 1))
                        log.warning(f"[{self.name}] Server error. Retrying in {delay:.2f}s.")
                        await asyncio.sleep(delay)
                    elif resp.status == 404:
                        log.error(f"[{self.name}] Not Found (404) for {url}. Giving up.")
                        return None
                    else:
                        return None
            
            except asyncio.TimeoutError:
                log.warning(f"[{self.name}] Timeout for {url} (attempt {attempt+1})")
            except aiohttp.ClientError as e:
                log.error(f"[{self.name}] ClientError for {url}: {e}")
            
            if attempt < self.max_retries - 1:
                delay = (2 ** attempt) + (0.1 * (attempt + 1))
                await asyncio.sleep(delay)

        log.error(f"[{self.name}] Failed to fetch {url} after {self.max_retries} attempts.")
        return None

# --- Specific API Clients ---

class DexscreenerClient(BaseAPIClient):
    BASE_URL = "https://api.dexscreener.com/latest/dex/tokens"

    def __init__(self, session: aiohttp.ClientSession, config: Config):
        super().__init__(session, config.API_MAX_RETRIES, "Dexscreener")
        self.timeout = config.API_TIMEOUT_DEX
        self.cache_ttl = config.CACHE_TTL_SECONDS

    @async_ttl_cache(ttl_seconds=120)
    async def get_token_data(self, mint: str) -> Optional[Dict]:
        url = f"{self.BASE_URL}/{mint}"
        return await self.async_get(url, self.timeout)

class RugCheckClient(BaseAPIClient):
    BASE_URL = "https://api.rugcheck.xyz/v1/tokens"

    def __init__(self, session: aiohttp.ClientSession, config: Config):
        super().__init__(session, config.API_MAX_RETRIES, "RugCheck")
        self.timeout = config.API_TIMEOUT_RUG
        self.cache_ttl = config.CACHE_TTL_SECONDS

    @async_ttl_cache(ttl_seconds=120)
    async def get_token_report(self, mint: str) -> Optional[Dict]:
        url = f"{self.BASE_URL}/{mint}/report"
        return await self.async_get(url, self.timeout)

class HolidayClient(BaseAPIClient):
    BASE_URL = "https://date.nager.at/api/v3"

    def __init__(self, session: aiohttp.ClientSession, config: Config):
        super().__init__(session, config.API_MAX_RETRIES, "HolidayAPI")
        self.timeout = config.API_TIMEOUT_HOLIDAY

    @async_ttl_cache(ttl_seconds=3600 * 24)
    async def _fetch_holidays_for_year(self, year: int, country_code: str) -> Set[str]:
        """Fetches all holidays for a year/country and returns a set of 'YYYY-MM-DD' strings."""
        url = f"{self.BASE_URL}/PublicHolidays/{year}/{country_code}"
        data = await self.async_get(url, self.timeout)
        if data and isinstance(data, list):
            return {item['date'] for item in data if 'date' in item and 'types' in item and 'Public' in item['types']}
        log.warning(f"Failed to fetch or parse holiday data for {country_code} {year}")
        return set()

    async def is_holiday(self, dt: datetime, country_codes: List[str]) -> bool:
        """Checks if the given date is a holiday in any of the specified countries."""
        date_str = dt.strftime('%Y-%m-%d')
        year = dt.year

        tasks = []
        for code in country_codes:
            tasks.append(self._fetch_holidays_for_year(year, code))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for res in results:
            if isinstance(res, Exception):
                log.warning(f"Holiday check sub-task failed: {res}")
            elif isinstance(res, set) and date_str in res:
                return True
        return False

# --- Supabase Manager (Async-safe) ---

class SupabaseManager:
    """Manages Supabase interactions using asyncio.to_thread for blocking calls."""
    def __init__(self, config: Config):
        self.config = config
        try:
            self.client: Client = create_client(config.SUPABASE_URL, config.SUPABASE_KEY)
            log.info("Supabase client initialized.")
        except Exception as e:
            log.critical(f"Failed to initialize Supabase client: {e}")
            raise
    
    async def download_json_file(self, remote_path: str) -> Optional[Dict]:
        """Downloads and parses a JSON file from Supabase storage."""
        try:
            log.debug(f"Downloading file: {remote_path}")
            file_bytes = await asyncio.to_thread(
                self.client.storage.from_(self.config.SUPABASE_BUCKET).download,
                remote_path
            )
            return json.loads(file_bytes)
        except Exception as e:
            if "404" in str(e) or "not_found" in str(e).lower():
                log.debug(f"File not found: {remote_path}")
            else:
                log.error(f"Failed to download {remote_path}: {e}")
            return None

    async def check_file_exists(self, remote_path: str) -> bool:
        """Checks if a file already exists in Supabase storage."""
        try:
            folder = os.path.dirname(remote_path)
            filename = os.path.basename(remote_path)
            
            file_list = await asyncio.to_thread(
                self.client.storage.from_(self.config.SUPABASE_BUCKET).list,
                folder,
                {"search": filename}
            )
            
            return any(f['name'] == filename for f in file_list)
        except Exception as e:
            log.error(f"Failed to check existence of {remote_path}: {e}")
            raise
    
    async def upload_file(self, local_path: str, remote_path: str):
        """Uploads a local file to Supabase storage."""
        try:
            await asyncio.to_thread(
                self.client.storage.from_(self.config.SUPABASE_BUCKET).upload,
                remote_path,
                local_path,
                {"content-type": "application/octet-stream", "upsert": "true"}
            )
            log.debug(f"Uploaded {local_path} to {remote_path}")
        except Exception as e:
            log.error(f"Failed to upload {local_path}: {e}")
            raise
    
    async def delete_file(self, remote_path: str):
        """Deletes a file from Supabase storage."""
        try:
            await asyncio.to_thread(
                self.client.storage.from_(self.config.SUPABASE_BUCKET).remove,
                [remote_path]
            )
            log.debug(f"Deleted {remote_path}")
        except Exception as e:
            log.error(f"Failed to delete {remote_path}: {e}")
            raise
    
    async def list_files(self, folder: str, limit: int = 1000) -> List[Dict]:
        """Lists files in a Supabase storage folder."""
        try:
            files = await asyncio.to_thread(
                self.client.storage.from_(self.config.SUPABASE_BUCKET).list,
                folder,
                {"limit": limit, "sortBy": {"column": "created_at", "order": "asc"}}
            )
            return files if files else []
        except Exception as e:
            log.error(f"Failed to list files in {folder}: {e}")
            return []

# --- Persistence Manager ---

class PersistenceManager:
    """Handles saving snapshots and datasets locally and uploading them."""
    def __init__(self, config: Config, supabase_manager: SupabaseManager):
        self.config = config
        self.supabase = supabase_manager

    async def save_snapshot(self, snapshot_data: Dict, filename_base: str):
        """Saves snapshot as .json and .pkl locally, then uploads."""
        json_path = os.path.join(self.config.SNAPSHOT_DIR_LOCAL, f"{filename_base}.json")
        pkl_path = os.path.join(self.config.SNAPSHOT_DIR_LOCAL, f"{filename_base}.pkl")

        try:
            def _save_files():
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(snapshot_data, f, indent=2, default=str)
                with open(pkl_path, 'wb') as f:
                    pickle.dump(snapshot_data, f)
            
            await asyncio.to_thread(_save_files)
            log.debug(f"Saved snapshot locally: {filename_base}")

        except Exception as e:
            log.error(f"Failed to save local snapshot {filename_base}: {e}")
            return

        if self.config.UPLOAD_TO_SUPABASE:
            remote_json_path = f"{self.config.SNAPSHOT_DIR_REMOTE}/{filename_base}.json"
            remote_pkl_path = f"{self.config.SNAPSHOT_DIR_REMOTE}/{filename_base}.pkl"
            
            try:
                await self.supabase.upload_file(json_path, remote_json_path)
                await self.supabase.upload_file(pkl_path, remote_pkl_path)
                log.info(f"Uploaded snapshot to Supabase: {filename_base}")
            except Exception as e:
                log.error(f"Failed to upload snapshot {filename_base}: {e}")

        if self.config.CLEANUP_LOCAL_FILES:
            try:
                os.remove(json_path)
                os.remove(pkl_path)
                log.debug(f"Cleaned up local snapshot: {filename_base}")
            except Exception as e:
                log.warning(f"Failed to clean up local file {filename_base}: {e}")
    
    async def save_dataset(self, dataset_data: Dict, pipeline: str, date_str: str, mint: str, is_expired: bool = False):
        """Saves labeled dataset as .json and .pkl, uploads, returns success status."""
        subfolder_name = "expired_no_label" if is_expired else date_str
        dataset_dir_local = os.path.join(self.config.DATASET_DIR_LOCAL, pipeline, subfolder_name)
        os.makedirs(dataset_dir_local, exist_ok=True)
        
        safe_timestamp = dataset_data['features']['checked_at_utc'].replace(':', '-').replace('+', '_')
        filename_base = f"{mint}_{safe_timestamp}"
        
        json_path = os.path.join(dataset_dir_local, f"{filename_base}.json")
        pkl_path = os.path.join(dataset_dir_local, f"{filename_base}.pkl")
        
        try:
            def _save_files():
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(dataset_data, f, indent=2, default=str)
                with open(pkl_path, 'wb') as f:
                    pickle.dump(dataset_data, f)
            
            await asyncio.to_thread(_save_files)
            log.debug(f"Saved dataset locally: {filename_base}")
        except Exception as e:
            log.error(f"Failed to save local dataset {filename_base}: {e}")
            return False
        
        if self.config.UPLOAD_TO_SUPABASE:
            remote_json_path = f"{self.config.DATASET_DIR_REMOTE}/{pipeline}/{subfolder_name}/{filename_base}.json"
            remote_pkl_path = f"{self.config.DATASET_DIR_REMOTE}/{pipeline}/{subfolder_name}/{filename_base}.pkl"
            
            try:
                await self.supabase.upload_file(json_path, remote_json_path)
                await self.supabase.upload_file(pkl_path, remote_pkl_path)
                log.info(f"Uploaded dataset to Supabase: {pipeline}/{subfolder_name}/{filename_base}")
            except Exception as e:
                log.error(f"Failed to upload dataset {filename_base}: {e}")
                return False
        
        if self.config.CLEANUP_LOCAL_FILES:
            try:
                os.remove(json_path)
                os.remove(pkl_path)
                log.debug(f"Cleaned up local dataset: {filename_base}")
            except Exception as e:
                log.warning(f"Failed to clean up local dataset {filename_base}: {e}")
        
        return True

# --- Feature Computation ---

class FeatureComputer:
    """Computes derived features from raw signal and API data."""

    def _safe_get_timestamp(self, entry: dict) -> Optional[str]:
        """Extract timestamp from a history entry using priority list."""
        if not isinstance(entry, dict): return None
        for field in ["ts", "timestamp", "checked_at", "created_at", "updated_at"]:
            ts = entry.get(field)
            if isinstance(ts, str): return ts
        result = entry.get("result", {})
        if isinstance(result, dict):
            for field in ["discovered_at", "checked_at", "timestamp"]:
                ts = result.get(field)
                if isinstance(ts, str): return ts
        return None

    def _safe_get_grade(self, entry: dict) -> str:
        """Safely extract grade from a history entry with multiple fallback paths."""
        if not isinstance(entry, dict): return "UNKNOWN"
        if isinstance(entry.get("result"), dict):
            grade = entry["result"].get("grade")
            if isinstance(grade, str): return grade
        if isinstance(entry.get("grade"), str): return entry["grade"]
        for path in [["overlap_result", "grade"], ["data", "grade"], ["analysis", "grade"]]:
            obj = entry
            for key in path:
                obj = obj.get(key) if isinstance(obj, dict) else None
            if isinstance(obj, str): return obj
        return "UNKNOWN"

    def _safe_get_dex_data(self, entry: dict) -> Optional[Dict]:
        """Safely extract Dexscreener data from a history entry."""
        if not isinstance(entry, dict): return None
        
        # Path 1: Top-level key (discovery-style)
        dex_data = entry.get("dexscreener")
        if isinstance(dex_data, dict) and dex_data:
            log.debug("Found 'dexscreener' data at top level.")
            return dex_data
        
        # Path 2: Nested in result (alpha-style)
        if isinstance(entry.get("result"), dict):
            dex_data = entry["result"].get("dexscreener")
            if isinstance(dex_data, dict) and dex_data:
                log.debug("Found 'dexscreener' data in result.")
                return dex_data
        
        log.debug("No pre-fetched 'dexscreener' block found in history entry.")
        return None

    def _safe_get_rug_data(self, entry: dict) -> Optional[Dict]:
        """Safely extract RugCheck API response from a history entry."""
        if not isinstance(entry, dict): return None
        
        # Path 1: Alpha-style (result.security.rugcheck_raw)
        try:
            raw_data = entry.get("result", {}).get("security", {}).get("rugcheck_raw")
            if isinstance(raw_data, dict) and "ok" in raw_data:
                log.debug("Found 'rugcheck_raw' data in result.security.")
                return raw_data
        except Exception:
            pass
        
        # Path 2: Discovery-style (top-level 'rugcheck_raw')
        raw_data = entry.get("rugcheck_raw")
        if isinstance(raw_data, dict) and "ok" in raw_data:
            log.debug("Found 'rugcheck_raw' data at top level.")
            return raw_data
            
        # Path 3: Fallback to 'rugcheck' key
        raw_data = entry.get("rugcheck")
        if isinstance(raw_data, dict) and "ok" in raw_data:
            log.debug("Found 'rugcheck' data at top level (as fallback).")
            return raw_data
        
        log.debug("No pre-fetched 'rugcheck_raw' or 'rugcheck' block found.")
        return None

    def compute_features(self, signal_type: str, history_entry: Dict, 
                         dex_data: Optional[Dict], rug_data: Optional[Dict], 
                         is_holiday: bool) -> Tuple[Optional[Dict], Optional[datetime]]:
        """Computes all derived features and returns a feature dict and parsed timestamp."""
        
        checked_at_str = self._safe_get_timestamp(history_entry)
        if not checked_at_str:
            log.warning(f"Could not parse timestamp from signal: {json.dumps(history_entry, default=str)[:200]}")
            return None, None
        
        try:
            checked_at_dt = parser.isoparse(checked_at_str).astimezone(timezone.utc)
            checked_at_timestamp = int(checked_at_dt.timestamp())
        except Exception as e:
            log.warning(f"Failed to parse timestamp '{checked_at_str}': {e}")
            return None, None

        time_features = {
            "checked_at_utc": checked_at_dt.isoformat(),
            "checked_at_timestamp": checked_at_timestamp,
            "time_of_day_utc": checked_at_dt.hour,
            "day_of_week_utc": checked_at_dt.weekday(),
            "is_weekend_utc": checked_at_dt.weekday() >= 5,
            "is_public_holiday_any": is_holiday,
        }

        signal_features = {
            "signal_source": signal_type,
            "grade": self._safe_get_grade(history_entry)
        }

        market_features = {}
        pair_created_at_timestamp = None
        if dex_data and dex_data.get("pairs") and isinstance(dex_data["pairs"], list) and len(dex_data["pairs"]) > 0:
            best_pair = max(
                dex_data["pairs"], 
                key=lambda p: float(p.get("liquidity", {}).get("usd", 0.0) or 0.0),
                default=None
            )
            
            if best_pair:
                pair_created_at_str = best_pair.get("pairCreatedAt")
                if pair_created_at_str:
                    try:
                        created_at_val = int(pair_created_at_str)
                        if created_at_val > 999999999999:
                             pair_created_at_timestamp = created_at_val // 1000
                        else:
                             pair_created_at_timestamp = created_at_val
                    except Exception as e:
                        log.warning(f"Could not parse pairCreatedAt '{pair_created_at_str}': {e}")
                
                market_features = {
                    "price_usd": float(best_pair.get("priceUsd", 0.0) or 0.0),
                    "fdv_usd": float(best_pair.get("fdv", 0.0) or 0.0),
                    "liquidity_usd": float(best_pair.get("liquidity", {}).get("usd", 0.0) or 0.0),
                    "volume_h24_usd": float(best_pair.get("volume", {}).get("h24", 0.0) or 0.0),
                    "price_change_h24_pct": float(best_pair.get("priceChange", {}).get("h24", 0.0) or 0.0),
                    "pair_created_at_timestamp": pair_created_at_timestamp,
                }
        
        if market_features.get("price_usd", 0.0) == 0.0 and rug_data and rug_data.get("ok"):
            try:
                rug_price = float(rug_data.get("raw", {}).get("price", 0.0) or 0.0)
                if rug_price > 0:
                    market_features["price_usd"] = rug_price
                    log.debug(f"Using fallback price from rug_data: {rug_price}")
            except Exception:
                pass

        security_features = {}
        if rug_data and rug_data.get("ok"):
            data = rug_data.get("data", rug_data.get("raw", {}))
            if not isinstance(data, dict):
                data = {}

            top_holders = data.get("topHolders", []) or []
            markets = data.get("markets", []) or []

            total_lp_locked_usd = sum(float(m.get("lp", {}).get("lpLockedUSD", 0.0) or 0.0) for m in markets)
            
            security_features = {
                "rugcheck_risk_level": (data.get('risk', {}) or {}).get('level', 'unknown'),
                "is_rugged": data.get('rugged', False),
                "has_mint_authority": bool(data.get('mintAuthority')),
                "has_freeze_authority": bool(data.get('freezeAuthority')),
                "creator_balance_pct": float((data.get('creatorBalance', {}) or {}).get('pct', 0.0) or 0.0),
                "top_10_holders_pct": sum(float(h.get('pct', 0.0) or 0.0) for h in top_holders[:10]),
                "is_lp_locked_95_plus": any(float(m.get("lp", {}).get("lockedPct", 0.0) or 0.0) >= 95.0 for m in markets),
                "total_lp_locked_usd": total_lp_locked_usd,
            }
        
        derived_features = {}
        if pair_created_at_timestamp:
            derived_features["token_age_at_signal_seconds"] = max(0, checked_at_timestamp - pair_created_at_timestamp)

        all_features = {
            **time_features,
            **signal_features,
            **market_features,
            **security_features,
            **derived_features,
        }
        
        return all_features, checked_at_dt

# --- Snapshot Aggregator ---

class SnapshotAggregator:
    """
    Handles efficient aggregation of snapshots with labels from analytics tracker.
    Uses a targeted, per-snapshot-deadline approach.
    """
    
    def __init__(self, config: Config, supabase: SupabaseManager, persistence: 'PersistenceManager'):
        self.config = config
        self.supabase = supabase
        self.persistence = persistence
        self.claimed_snapshots: Set[str] = set()
        
        self._file_cache: Dict[str, Tuple[Optional[Dict], float]] = {}
        self._cache_lock = asyncio.Lock()
        self._label_index: Dict[str, Dict] = {}

    def _clear_caches(self):
        """Clears caches at the start of a scan."""
        log.debug("Clearing aggregator pass caches (files, labels).")
        self._file_cache.clear()
        self._label_index.clear()

    async def _fetch_analytics_file(self, remote_path: str) -> Optional[Dict]:
        """
        Cached fetch for a single analytics file.
        Cache lives for self.config.ANALYTICS_INDEX_TTL seconds.
        """
        now = time.monotonic()
        ttl = self.config.ANALYTICS_INDEX_TTL
        
        async with self._cache_lock:
            if remote_path in self._file_cache:
                data, expiry = self._file_cache[remote_path]
                if now < expiry:
                    log.debug(f"[Cache HIT] for {remote_path}")
                    return data
                log.debug(f"[Cache EXPIRED] for {remote_path}")
        
        log.debug(f"[Cache MISS] for {remote_path}")
        data = await self.supabase.download_json_file(remote_path)
        
        async with self._cache_lock:
            self._file_cache[remote_path] = (data, now + ttl)
        return data

    async def scan_and_aggregate(self):
        """Main aggregation loop: scan snapshots, check labels, aggregate or reschedule."""
        log.info("Starting snapshot aggregation scan...")
        
        self._clear_caches()
        
        # NOTE: Reduced limit from 1000 to 50 as a workaround for Egress limits
        snapshot_files = await self.supabase.list_files(self.config.SNAPSHOT_DIR_REMOTE, limit=50)
        
        if not snapshot_files:
            log.info("No snapshot files found.")
            return
        
        now = datetime.now(timezone.utc)
        due_snapshots = []
        
        for file_info in snapshot_files:
            filename = file_info.get('name', '')
            if not filename.endswith('.json'):
                continue
            
            if filename in self.claimed_snapshots:
                continue
            
            remote_path = f"{self.config.SNAPSHOT_DIR_REMOTE}/{filename}"
            snapshot = await self.supabase.download_json_file(remote_path)
            
            if not snapshot:
                log.warning(f"Failed to download snapshot {remote_path}, possibly due to Egress. Skipping.")
                continue
            
            finalization = snapshot.get('finalization', {})
            finalization_status = finalization.get("finalization_status", "pending")
            next_check_str = finalization.get('next_check_at')

            if finalization_status not in ("pending", "awaiting_label"):
                continue
            
            if not next_check_str:
                log.warning(f"Snapshot {filename} missing next_check_at, skipping")
                continue
            
            try:
                next_check_dt = parser.isoparse(next_check_str).astimezone(timezone.utc)
            except Exception as e:
                log.error(f"Failed to parse next_check_at for {filename}: {e}")
                continue
            
            if now >= next_check_dt:
                due_snapshots.append((filename, snapshot, remote_path))
        
        
        if not due_snapshots:
            log.info("No snapshots are due for checking.")
            return

        log.info(f"Found {len(due_snapshots)} snapshots due for checking")
        
        # Build the set of required analytics files to download
        dates_to_scan: Set[str] = set()
        pipelines_to_scan: Set[str] = set()

        for _, snapshot, _ in due_snapshots:
            try:
                # 'signal_source' is the key for 'discovery' or 'alpha'
                pipeline = snapshot['features']['signal_source']
                pipelines_to_scan.add(pipeline)
                
                checked_at = parser.isoparse(snapshot['features']['checked_at_utc']).astimezone(timezone.utc)
                finalize_deadline = parser.isoparse(snapshot['finalization']['finalize_deadline']).astimezone(timezone.utc)
                
                start_date = checked_at.date()
                end_date = min(now, finalize_deadline).date()
                
                current_date = start_date
                while current_date <= end_date:
                    dates_to_scan.add(current_date.strftime('%Y-%m-%d'))
                    current_date += timedelta(days=1)
            except Exception as e:
                log.error(f"Error calculating date range for snapshot {snapshot.get('snapshot_id')}: {e}")
        
        if not dates_to_scan or not pipelines_to_scan:
            log.warning("No valid dates or pipelines to scan. Aborting aggregation pass.")
            return

        log.info(f"Scanning {len(dates_to_scan)} unique dates ({min(dates_to_scan)} to {max(dates_to_scan)}) and {len(pipelines_to_scan)} pipelines: {list(pipelines_to_scan)}")
        
        # Download required files and build targeted label index
        fetch_tasks = []
        for pipeline in pipelines_to_scan:
            for date_str in dates_to_scan:
                remote_path = f"analytics/{pipeline}/daily/{date_str}.json"
                fetch_tasks.append(self._fetch_analytics_file(remote_path))
        
        file_contents = await asyncio.gather(*fetch_tasks, return_exceptions=True)
        
        for content in file_contents:
            if isinstance(content, Exception) or not content or not isinstance(content.get("tokens"), list):
                continue
            
            for token in content["tokens"]:
                if not isinstance(token, dict): continue
                mint = token.get("mint")
                signal_type = token.get("signal_type")
                
                if not mint or not signal_type or token.get("status") not in ("win", "loss"):
                    continue
                
                composite_key = get_composite_key(mint, signal_type)
                
                tracking_completed_at_str = token.get("tracking_completed_at")
                if not tracking_completed_at_str:
                    continue
                
                try:
                    new_label_time = parser.isoparse(tracking_completed_at_str)
                except Exception:
                    log.warning(f"Could not parse tracking_completed_at for {composite_key}: {tracking_completed_at_str}")
                    continue
                
                existing_label = self._label_index.get(composite_key)
                if not existing_label:
                    self._label_index[composite_key] = token
                else:
                    try:
                        existing_label_time = parser.isoparse(existing_label.get("tracking_completed_at"))
                        if new_label_time > existing_label_time:
                            self._label_index[composite_key] = token
                    except Exception:
                        self._label_index[composite_key] = token
        
        log.info(f"Built targeted label index with {len(self._label_index)} labeled token-signals.")

        # Process due snapshots in batches
        batch_size = self.config.AGGREGATOR_BATCH_SIZE
        for i in range(0, len(due_snapshots), batch_size):
            batch = due_snapshots[i:i+batch_size]
            tasks = []
            for filename, snapshot, remote_path in batch:
                tasks.append(self._process_snapshot(filename, snapshot, remote_path))
            
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _process_snapshot(self, filename: str, snapshot: Dict, remote_path: str):
        """Process a single snapshot: claim, check label, aggregate or reschedule."""
        try:
            if not await self._claim_snapshot(filename, snapshot, remote_path):
                log.debug(f"Failed to claim {filename}, skipping")
                return
            
            mint = snapshot['features']['mint']
            pipeline = snapshot['features']['signal_source'] # This is 'discovery' or 'alpha'
            now = datetime.now(timezone.utc)
            
            composite_key = get_composite_key(mint, pipeline)
            
            finalization = snapshot['finalization']
            finalize_deadline_str = finalization['finalize_deadline']
            finalize_deadline = parser.isoparse(finalize_deadline_str).astimezone(timezone.utc)
            
            label_data = self._label_index.get(composite_key)
            
            if label_data:
                log.info(f"Label found for {composite_key} ({pipeline}): {label_data['status']}")
                await self._aggregate_with_label(snapshot, label_data, filename, remote_path)
                
            elif now >= finalize_deadline:
                log.warning(f"Snapshot {filename} ({composite_key}) expired without label (Deadline: {finalize_deadline_str})")
                await self._aggregate_expired(snapshot, filename, remote_path)
                
            else:
                log.debug(f"No label yet for {composite_key} ({pipeline}), rescheduling (Deadline: {finalize_deadline_str})")
                await self._reschedule_snapshot(snapshot, filename, remote_path)
                
        except Exception as e:
            log.error(f"Error processing snapshot {filename}: {e}", exc_info=True)
            self.claimed_snapshots.discard(filename)
    
    async def _claim_snapshot(self, filename: str, snapshot: Dict, remote_path: str) -> bool:
        """Atomically claim a snapshot."""
        try:
            self.claimed_snapshots.add(filename)
            await self.supabase.delete_file(remote_path)
            
            snapshot['finalization']['claimed_by'] = 'aggregator'
            snapshot['finalization']['claimed_at'] = datetime.now(timezone.utc).isoformat()
            snapshot['finalization']['finalization_status'] = 'awaiting_label'
            
            local_path = os.path.join(self.config.SNAPSHOT_DIR_LOCAL, filename)
            
            def _save():
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                with open(local_path, 'w', encoding='utf-8') as f:
                    json.dump(snapshot, f, indent=2, default=str)
            
            await asyncio.to_thread(_save)
            await self.supabase.upload_file(local_path, remote_path)
            
            log.debug(f"Claimed snapshot: {filename}")
            return True
            
        except Exception as e:
            log.error(f"Failed to claim snapshot {filename}: {e}")
            self.claimed_snapshots.discard(filename)
            return False
    
    async def _aggregate_with_label(self, snapshot: Dict, label_data: Dict, 
                                   filename: str, remote_path: str):
        """Aggregate snapshot with label to dataset, then delete original."""
        mint = snapshot['features']['mint']
        pipeline = snapshot['features']['signal_source'] # 'discovery' or 'alpha'
        
        snapshot['label'] = label_data
        snapshot['finalization']['finalization_status'] = 'labeled'
        snapshot['finalization']['finalized_at'] = datetime.now(timezone.utc).isoformat()
        
        checked_at_str = snapshot['features']['checked_at_utc']
        checked_at = parser.isoparse(checked_at_str)
        date_str = checked_at.strftime('%Y-%m-%d')
        
        # This will save to 'datasets/discovery/...' or 'datasets/alpha/...'
        success = await self.persistence.save_dataset(snapshot, pipeline, date_str, mint, is_expired=False)
        
        if success:
            await self._delete_snapshot(filename, remote_path)
            log.info(f"Successfully aggregated {mint} ({pipeline}) to dataset {pipeline}/{date_str}")
        else:
            log.error(f"Failed to save dataset for {mint} ({pipeline}), keeping snapshot")
            self.claimed_snapshots.discard(filename)
    
    async def _aggregate_expired(self, snapshot: Dict, filename: str, remote_path: str):
        """Mark snapshot as expired and move to expired dataset folder."""
        mint = snapshot['features']['mint']
        pipeline = snapshot['features']['signal_source']
        
        snapshot['label'] = None
        snapshot['finalization']['finalization_status'] = 'expired_no_label'
        snapshot['finalization']['finalized_at'] = datetime.now(timezone.utc).isoformat()
        
        checked_at_str = snapshot['features']['checked_at_utc']
        checked_at = parser.isoparse(checked_at_str)
        date_str = checked_at.strftime('%Y-%m-%d')
        
        # This will save to 'datasets/discovery/expired_no_label/...' etc.
        success = await self.persistence.save_dataset(snapshot, pipeline, date_str, mint, is_expired=True)
        
        if success:
            await self._delete_snapshot(filename, remote_path)
            log.info(f"Moved expired snapshot {mint} ({pipeline}) to {pipeline}/expired_no_label/{filename}")
        else:
            log.error(f"Failed to save expired dataset for {mint} ({pipeline})")
            self.claimed_snapshots.discard(filename)
    
    async def _reschedule_snapshot(self, snapshot: Dict, filename: str, remote_path: str):
        """Update next_check_at and re-upload snapshot."""
        now = datetime.now(timezone.utc)
        check_interval = timedelta(minutes=self.config.CHECK_INTERVAL_MINUTES)
        next_check = now + check_interval
        
        snapshot['finalization']['next_check_at'] = next_check.isoformat()
        snapshot['finalization']['check_count'] = snapshot['finalization'].get('check_count', 0) + 1
        snapshot['finalization']['claimed_by'] = None
        snapshot['finalization']['claimed_at'] = None
        
        local_path = os.path.join(self.config.SNAPSHOT_DIR_LOCAL, filename)
        
        def _save():
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, 'w', encoding='utf-8') as f:
                json.dump(snapshot, f, indent=2, default=str)
        
        await asyncio.to_thread(_save)
        
        await self.supabase.delete_file(remote_path)
        await self.supabase.upload_file(local_path, remote_path)
        
        if self.config.CLEANUP_LOCAL_FILES:
            try:
                os.remove(local_path)
            except Exception as e:
                log.warning(f"Failed to cleanup {local_path}: {e}")
        
        self.claimed_snapshots.discard(filename)
        log.debug(f"Rescheduled {filename} for {next_check.isoformat()}")
    
    async def _delete_snapshot(self, filename: str, remote_path: str):
        """Delete snapshot files (both .json and .pkl, local and remote)."""
        base_name = filename.replace('.json', '')
        
        json_remote = remote_path
        pkl_remote = remote_path.replace('.json', '.pkl')
        
        try:
            await self.supabase.delete_file(json_remote)
            log.debug(f"Deleted remote {json_remote}")
        except Exception as e:
            log.warning(f"Failed to delete remote {json_remote}: {e}")
        
        try:
            await self.supabase.delete_file(pkl_remote)
            log.debug(f"Deleted remote {pkl_remote}")
        except Exception as e:
            log.warning(f"Failed to delete remote {pkl_remote}: {e}")
        
        local_json = os.path.join(self.config.SNAPSHOT_DIR_LOCAL, filename)
        local_pkl = local_json.replace('.json', '.pkl')
        
        for local_file in [local_json, local_pkl]:
            if os.path.exists(local_file):
                try:
                    os.remove(local_file)
                    log.debug(f"Deleted local {local_file}")
                except Exception as e:
                    log.warning(f"Failed to delete local {local_file}: {e}")
        
        self.claimed_snapshots.discard(filename)

# --- Main Collector Service ---

class CollectorService:
    def __init__(self, config: Config, session: aiohttp.ClientSession):
        self.config = config
        self.session = session
        
        self.supabase = SupabaseManager(config)
        self.persistence = PersistenceManager(config, self.supabase)
        self.dex_client = DexscreenerClient(session, config)
        self.rug_client = RugCheckClient(session, config)
        self.holiday_client = HolidayClient(session, config)
        self.computer = FeatureComputer()
        self.aggregator = SnapshotAggregator(config, self.supabase, self.persistence)
        
        self.process_semaphore = asyncio.Semaphore(config.PROCESSOR_CONCURRENCY)
        log.info(f"Processor concurrency limit set to: {config.PROCESSOR_CONCURRENCY}")

        log.info("CollectorService initialized.")

    async def _fetch_all_signals(self) -> List[Dict]:
        """Fetches and flattens all signals from both discovery and alpha files."""
        tasks = {
            "discovery": self.supabase.download_json_file(self.config.SIGNAL_FILE_DISCOVERY),
            "alpha": self.supabase.download_json_file(self.config.SIGNAL_FILE_ALPHA),
        }
        
        results = await asyncio.gather(*tasks.values())
        signal_data = dict(zip(tasks.keys(), results))
        
        all_signals = []
        for signal_type, data in signal_data.items():
            if not data or not isinstance(data, dict):
                log.warning(f"No valid data found for {signal_type} signals.")
                continue
            
            log.info(f"Processing {len(data)} mints from {signal_type} file...")
            for mint, history_list in data.items():
                if not isinstance(history_list, list):
                    continue
                for history_entry in history_list:
                    all_signals.append({
                        "signal_type": signal_type, # This will be 'discovery' or 'alpha'
                        "mint": mint,
                        "data": history_entry
                    })
        return all_signals

    def _build_canonical_snapshot(self, signal: Dict, features: Dict, 
                                  dex_raw: Optional[Dict], rug_raw: Optional[Dict], 
                                  holiday_check: bool, filename_base: str) -> Dict:
        """Assembles the final snapshot dictionary with finalization metadata."""
        
        token_age_hours_at_signal = None
        
        if 'token_age_at_signal_seconds' in features:
            token_age_hours_at_signal = features['token_age_at_signal_seconds'] / 3600.0
        
        if token_age_hours_at_signal is None:
            age_from_signal = signal['data'].get('token_age_hours')
            if isinstance(age_from_signal, (int, float)):
                token_age_hours_at_signal = float(age_from_signal)
                log.debug(f"Using token age from signal file for {filename_base}: {token_age_hours_at_signal:.2f}h")

        if (token_age_hours_at_signal is not None and 
            token_age_hours_at_signal < self.config.TOKEN_AGE_THRESHOLD_HOURS):
            finalize_window_hours = self.config.SHORT_FINALIZE_HOURS
            is_new_token = True
        else:
            finalize_window_hours = self.config.LONG_FINALIZE_HOURS
            is_new_token = False

        checked_at = parser.isoparse(features['checked_at_utc']).astimezone(timezone.utc)
        finalize_deadline = checked_at + timedelta(hours=finalize_window_hours)
        next_check_at = checked_at + timedelta(minutes=self.config.CHECK_INTERVAL_MINUTES)
        
        if next_check_at < datetime.now(timezone.utc):
            log.warning(f"Initial next_check_at for {filename_base} is in the past. Setting to now.")
            next_check_at = datetime.now(timezone.utc)
        
        return {
            "snapshot_id": filename_base,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "inputs": {
                "signal_data": signal['data'],
                "dexscreener_raw": dex_raw or {"error": "not_fetched_or_failed"},
                "rugcheck_raw": rug_raw or {"error": "not_fetched_or_failed"},
                "holiday_check": {
                    "countries": self.config.HOLIDAY_COUNTRY_CODES,
                    "is_holiday": holiday_check
                }
            },
            "features": {
                "mint": signal['mint'],
                **features # 'signal_source' is already in here from compute_features
            },
            "finalization": {
                "token_age_hours_at_signal": round(token_age_hours_at_signal, 2) if token_age_hours_at_signal is not None else None,
                "is_new_token": is_new_token,
                "finalize_window_hours": finalize_window_hours,
                "finalize_deadline": finalize_deadline.isoformat(),
                "check_interval_minutes": self.config.CHECK_INTERVAL_MINUTES,
                "next_check_at": next_check_at.isoformat(),
                "check_count": 0,
                "finalization_status": "pending",
                "claimed_by": None,
                "claimed_at": None,
                "finalized_at": None
            },
            "label": None
        }

    async def process_signal(self, signal: Dict):
        """
        Main processing pipeline for a single signal.
        Fetches live data ONLY if it's missing from the signal file.
        """
        mint = signal['mint']
        signal_type = signal['signal_type'] # This is 'discovery' or 'alpha'
        history_entry = signal['data']

        dex_data = self.computer._safe_get_dex_data(history_entry)
        rug_data = self.computer._safe_get_rug_data(history_entry)

        features, checked_at_dt = self.computer.compute_features(
            signal_type, history_entry, dex_data, rug_data, False
        )
        
        if not features or not checked_at_dt:
            log.warning(f"Skipping signal for mint {mint} ({signal_type}) due to missing base features (timestamp).")
            return

        safe_timestamp = features['checked_at_utc'].replace(':', '-').replace('+', '_')
        filename_base = f"{mint}_{safe_timestamp}_{signal_type}"
        
        remote_json_path = f"{self.config.SNAPSHOT_DIR_REMOTE}/{filename_base}.json"

        try:
            if await self.supabase.check_file_exists(remote_json_path):
                log.debug(f"Skipping already processed signal: {filename_base}")
                return
        except Exception as e:
            log.error(f"Failed idempotency check for {filename_base}: {e}. Skipping.")
            return

        log.info(f"Processing new signal: {filename_base}")

        tasks_to_run = {}
        
        if not dex_data:
            log.warning(f"No pre-fetched Dex data for {filename_base}. Fetching live.")
            tasks_to_run["dex_data"] = self.dex_client.get_token_data(mint)
        
        if not rug_data:
            log.warning(f"No pre-fetched Rug data for {filename_base}. Fetching live.")
            tasks_to_run["rug_data"] = self.rug_client.get_token_report(mint)
            
        tasks_to_run["is_holiday"] = self.holiday_client.is_holiday(checked_at_dt, self.config.HOLIDAY_COUNTRY_CODES)
        
        try:
            if tasks_to_run:
                task_keys = list(tasks_to_run.keys())
                results = await asyncio.gather(*tasks_to_run.values(), return_exceptions=False)
                results_dict = dict(zip(task_keys, results))
            else:
                results_dict = {}

            dex_data = dex_data or results_dict.get("dex_data")
            rug_data = rug_data or results_dict.get("rug_data")
            is_holiday = results_dict.get("is_holiday", False)

        except Exception as e:
            log.error(f"Data-gathering failed for {filename_base}: {e}", exc_info=False)
            return

        final_features, _ = self.computer.compute_features(
            signal_type, history_entry, dex_data, rug_data, is_holiday
        )

        if not final_features:
            log.error(f"Failed to compute final features for {filename_base}. Skipping.")
            return

        snapshot = self._build_canonical_snapshot(
            signal, final_features, dex_data, rug_data, is_holiday, filename_base
        )
        
        await self.persistence.save_snapshot(snapshot, filename_base)

    async def run_process_with_semaphore(self, signal: Dict):
        """Wrapper for process_signal that acquires the semaphore before running."""
        mint = signal.get('mint', 'unknown_mint')
        signal_type = signal.get('signal_type', 'unknown_type')
        try:
            async with self.process_semaphore:
                log.debug(f"Semaphore acquired for: {mint} ({signal_type})")
                await self.process_signal(signal)
            log.debug(f"Semaphore released for: {mint} ({signal_type})")
        except Exception as e:
            log.error(f"CRITICAL error during process_signal for {mint} ({signal_type}): {e}", exc_info=True)
            raise

    async def run(self):
        """Main service loop with both signal processing and aggregation."""
        log.info(f"Starting collector service. Poll interval: {self.config.POLL_INTERVAL}s, Aggregator interval: {self.config.AGGREGATOR_INTERVAL}s")
        
        last_aggregation = time.monotonic()
        
        while True:
            try:
                start_time = time.monotonic()
                log.info("Starting new polling cycle...")
                
                signals = await self._fetch_all_signals()
                log.info(f"Found {len(signals)} total signals to check.")
                
                if signals:
                    tasks = [self.run_process_with_semaphore(sig) for sig in signals]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    exceptions = [r for r in results if isinstance(r, Exception)]
                    if exceptions:
                        log.error(f"{len(exceptions)} signals failed during processing:")
                        unique_errors = set(str(ex) for ex in exceptions)
                        for i, ex_str in enumerate(list(unique_errors)[:5]):
                            log.error(f"  - Unique Error {i+1}: {ex_str}")

                if (start_time - last_aggregation) >= self.config.AGGREGATOR_INTERVAL:
                    log.info("Running snapshot aggregation...")
                    try:
                        await self.aggregator.scan_and_aggregate()
                        last_aggregation = time.monotonic()
                    except Exception as e:
                        log.error(f"Aggregator failed: {e}", exc_info=True)
                        last_aggregation = time.monotonic()
                else:
                    log.debug(f"Skipping aggregation, {time.monotonic() - last_aggregation:.0f}s / {self.config.AGGREGATOR_INTERVAL}s elapsed.")


                cycle_duration = time.monotonic() - start_time
                log.info(f"Polling cycle finished in {cycle_duration:.2f}s.")
                
                sleep_time = max(0, self.config.POLL_INTERVAL - cycle_duration)
                log.info(f"Sleeping for {sleep_time:.2f}s...")
                await asyncio.sleep(sleep_time)
                
            except Exception as e:
                log.critical(f"CRITICAL ERROR in main loop: {e}", exc_info=True)
                log.info("Restarting loop after 60s...")
                await asyncio.sleep(60)

# --- CLI and Test Functions ---

async def run_tests(config: Config, session: aiohttp.ClientSession):
    """Runs connectivity tests for all external clients."""
    log.info("--- Running API Connectivity Tests ---")
    SAMPLE_MINT = "Sg4k4iFaEeqhv5866cQmsFTMhRx8sVCPAq2j8Xcpump"
    
    try:
        supa = SupabaseManager(config)
        log.info("Testing Supabase connection (checking for alpha file)...")
        exists = await supa.check_file_exists(config.SIGNAL_FILE_ALPHA)
        log.info(f"Supabase test: Check for {config.SIGNAL_FILE_ALPHA} -> {exists} (TEST PASSED)")
        log.info("Testing Supabase connection (checking for discovery file)...")
        exists_disc = await supa.check_file_exists(config.SIGNAL_FILE_DISCOVERY)
        log.info(f"Supabase test: Check for {config.SIGNAL_FILE_DISCOVERY} -> {exists_disc} (TEST PASSED)")
    except Exception as e:
        log.error(f"Supabase test: FAILED - {e}")

    try:
        dex = DexscreenerClient(session, config)
        log.info(f"Testing Dexscreener with mint: {SAMPLE_MINT}")
        data = await dex.get_token_data(SAMPLE_MINT)
        if data and data.get("pairs"):
            log.info(f"Dexscreener test: SUCCESS - Found pair: {data['pairs'][0].get('pairAddress')} (TEST PASSED)")
        else:
            log.error("Dexscreener test: FAILED - No pairs found or error.")
    except Exception as e:
        log.error(f"Dexscreener test: FAILED - {e}")

    try:
        rug = RugCheckClient(session, config)
        log.info(f"Testing RugCheck with mint: {SAMPLE_MINT}")
        data = await rug.get_token_report(SAMPLE_MINT)
        if data and data.get("ok"):
            log.info(f"RugCheck test: SUCCESS - Risk level: {data.get('data', {}).get('risk', {}).get('level')} (TEST PASSED)")
        else:
            log.error("RugCheck test: FAILED - API call unsuccessful.")
    except Exception as e:
        log.error(f"RugCheck test: FAILED - {e}")
        
    try:
        holiday = HolidayClient(session, config)
        log.info("Testing Holiday API for US...")
        today = datetime.now(timezone.utc)
        is_hol = await holiday.is_holiday(today, ["US"])
        log.info(f"Holiday API test: Is today a US holiday? -> {is_hol} (TEST PASSED)")
    except Exception as e:
        log.error(f"Holiday API test: FAILED - {e}")
        
    log.info("--- API Tests Complete ---")


async def main():
    """Main entry point for the service."""
    parser = argparse.ArgumentParser(description="Solana Snapshot Collector Service with Label Aggregation")
    parser.add_argument(
        "command",
        choices=["run", "test-apis"],
        default="run",
        nargs="?",
        help="Command to run: 'run' (default) or 'test-apis'."
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default=None,
        help="Override log level (e.g., DEBUG, INFO, WARNING)."
    )
    args = parser.parse_args()

    try:
        config = Config()
        log_level = args.log_level or config.LOG_LEVEL
        global log
        log = setup_logging(log_level)
    except Exception as e:
        print(f"FATAL: Configuration error: {e}")
        exit(1)

    async with aiohttp.ClientSession() as session:
        if args.command == "test-apis":
            await run_tests(config, session)
        elif args.command == "run":
            service = CollectorService(config, session)
            await service.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nService stopped manually.")
        if 'log' in globals():
            log.info("Service stopped manually.") 