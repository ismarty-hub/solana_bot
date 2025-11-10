#!/usr/bin/env python3
"""
snapshot_collector.py

Production-ready data-collector service with label aggregation.
Watches new token signals from Supabase (overlap_results.json, 
overlap_results_alpha.json), enriches each signal with live market
data (Dexscreener), security data (RugCheck), and holiday/time context,
computes derived features, and persists a canonical snapshot per signal
as .json locally and to Supabase.

NEW: Implements efficient, per-token aggregation. Each snapshot has a
dynamic `finalize_deadline` based on its age at signal time. The aggregator
intelligently scans *only* the required analytics daily files for snapshots
that are due for a recheck, rather than a full 7-day lookback.

(CORRECTED) The service now extracts pre-fetched Dexscreener and RugCheck
data from the signal files instead of making redundant live API calls.
It only fetches live data if it's missing or invalid (e.g., missing 'pairs').

(*** BUGFIX ***) The SnapshotAggregator now uses a composite key
(mint + signal_type) to look up labels, matching the logic
in analytics_tracker.py. This ensures the correct label (e.g.,
from 'mint_alpha') is applied to the correct snapshot (e.g.,
'snapshot_mint_alpha').

(*** REVISION ***) SupabaseManager now uses private signed URLs and
conditional GETs (ETag/Last-Modified) for all downloads, falling back
to a local file cache.

(*** MODIFIED ***) This script only creates or uploads .json files.
No .pkl files are used.

(*** MODIFIED ***) Service now checks for *active* signals. If a snapshot
for a given (mint, signal_type) pair is already being tracked, new
signals for that same pair are ignored until the first one is finalized.
"""

import os
import json
# import pickle <-- REMOVED
import asyncio
import aiohttp
import logging
import argparse
import time
import requests # Added for private, conditional downloads
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
    
    # --- Label aggregation settings (New Efficient Model) ---
    
    # Interval to re-check a snapshot for a label
    CHECK_INTERVAL_MINUTES: int = field(default_factory=lambda: int(os.getenv("CHECK_INTERVAL_MINUTES", "30")))
    
    # Age threshold to determine if a token is "new"
    TOKEN_AGE_THRESHOLD_HOURS: float = field(default_factory=lambda: float(os.getenv("TOKEN_AGE_THRESHOLD_HOURS", "12.0")))
    
    # Finalization window for "new" tokens (age < threshold)
    SHORT_FINALIZE_HOURS: int = field(default_factory=lambda: int(os.getenv("SHORT_FINALIZE_HOURS", "24")))
    
    # Finalization window for "old" tokens (age >= threshold or unknown)
    LONG_FINALIZE_HOURS: int = field(default_factory=lambda: int(os.getenv("LONG_FINALIZE_HOURS", "168")))
    
    # Cache TTL for downloaded analytics files *during an aggregation pass*
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

# --- (*** NEW ***) Keying Utility Function ---

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
    def __init__(self, config: Config, session: aiohttp.ClientSession):
        self.config = config
        self.session = session # aiohttp session for external API calls
        try:
            self.client: Client = create_client(config.SUPABASE_URL, config.SUPABASE_KEY)
            log.info("Supabase client initialized.")
        except Exception as e:
            log.critical(f"Failed to initialize Supabase client: {e}")
            raise
        
        # Cache for conditional GET headers (ETag, Last-Modified)
        self._file_cache_headers: Dict[str, Dict[str, str]] = {}
        # Local file cache directory
        self._local_cache_dir = os.path.join(config.DATASET_DIR_LOCAL, ".cache")
        os.makedirs(self._local_cache_dir, exist_ok=True)
    
    async def download_json_file(self, remote_path: str) -> Optional[Dict]:
        """
        Downloads and parses a JSON file from a private Supabase storage
        using signed URLs and conditional GETs (ETag / Last-Modified).
        Caches file locally to read from on 304 Not Modified.
        """
        # Create a unique-ish but stable local file path
        local_save_path = os.path.join(
            self._local_cache_dir, 
            remote_path.replace('/', '_').replace('\\', '_')
        )
        
        try:
            # 1. Generate signed URL (blocking call, run in thread)
            log.debug(f"Generating signed URL for: {remote_path}")
            signed_url_response = await asyncio.to_thread(
                self.client.storage.from_(self.config.SUPABASE_BUCKET).create_signed_url,
                remote_path,
                60 # 60 second expiry
            )
            signed_url = signed_url_response.get('signedURL')
            if not signed_url:
                log.error(f"Could not generate signed URL for '{remote_path}'. Response: {signed_url_response}")
                return None

            # 2. Prepare headers for conditional GET
            headers = {}
            cached_headers = self._file_cache_headers.get(remote_path, {})
            if cached_headers.get('Last-Modified'):
                headers['If-Modified-Since'] = cached_headers['Last-Modified']
            if cached_headers.get('ETag'):
                headers['If-None-Match'] = cached_headers['ETag']

            log.debug(f"Downloading file: {remote_path} (URL: {signed_url[:50]}...)")
            
            # 3. Perform the HTTP request (blocking, run in thread)
            # We use `requests` here as it's simpler for blocking calls
            # in threads than managing the `aiohttp` session inside.
            def _blocking_get():
                return requests.get(signed_url, headers=headers, timeout=30)
            
            response = await asyncio.to_thread(_blocking_get)
                
            # 4. Handle response
            if response.status_code == 304:
                # 304 Not Modified
                log.debug(f"File '{remote_path}': No change detected (304 Not Modified).")
                if os.path.exists(local_save_path):
                    log.debug(f"Loading from local cache: '{local_save_path}'")
                    
                    def _read_local():
                        with open(local_save_path, "rb") as f:
                            return f.read()
                    
                    file_bytes = await asyncio.to_thread(_read_local)
                    return json.loads(file_bytes)
                else:
                    log.warning(f"File '{remote_path}' not modified, but local file '{local_save_path}' missing. Forcing re-download on next poll.")
                    # Remove bad cache headers to force full download next time
                    self._file_cache_headers.pop(remote_path, None)
                    return None # Return None for this attempt

            elif response.status_code == 200:
                # 200 OK
                log.debug(f"File '{remote_path}': File updated — new data loaded.")
                file_bytes = response.content

                # Update cache headers
                new_last_modified = response.headers.get('Last-Modified')
                new_etag = response.headers.get('ETag')
                new_headers_to_cache = {}
                if new_last_modified:
                    new_headers_to_cache['Last-Modified'] = new_last_modified
                if new_etag:
                    new_headers_to_cache['ETag'] = new_etag
                if new_headers_to_cache:
                    self._file_cache_headers[remote_path] = new_headers_to_cache

                # Save new content to local cache (blocking, run in thread)
                def _save_local():
                    with open(local_save_path, "wb") as f:
                        f.write(file_bytes)
                
                await asyncio.to_thread(_save_local)
                log.debug(f"Saved to local cache: '{local_save_path}'")
                
                return json.loads(file_bytes)

            else:
                # Handle errors
                log.error(f"Failed to download {remote_path}: Status {response.status_code} {response.text[:200]}")
                # Fallback to local cache if it exists
                if os.path.exists(local_save_path):
                     log.warning(f"Loading from local cache as fallback: '{local_save_path}'")
                     def _read_local():
                        with open(local_save_path, "rb") as f:
                            return f.read()
                     file_bytes = await asyncio.to_thread(_read_local)
                     return json.loads(file_bytes)
                return None

        except requests.exceptions.RequestException as e:
            log.error(f"Requests Error downloading {remote_path}: {e}")
        except json.JSONDecodeError as e:
            log.error(f"Failed to decode JSON from {remote_path} (or local cache {local_save_path}): {e}")
        except Exception as e:
            if "404" in str(e) or "not_found" in str(e).lower():
                log.debug(f"File not found: {remote_path}")
            else:
                log.error(f"Failed to download {remote_path}: {e}", exc_info=True)
        
        # Final fallback
        if os.path.exists(local_save_path):
             log.warning(f"Loading from local cache as final fallback: '{local_save_path}'")
             try:
                 def _read_local():
                    with open(local_save_path, "rb") as f:
                        return f.read()
                 file_bytes = await asyncio.to_thread(_read_local)
                 return json.loads(file_bytes)
             except Exception as e:
                log.error(f"Failed to read final fallback cache {local_save_path}: {e}")
        
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
        """Uploads a local file to Supabase storage, overwriting if exists."""
        try:
            # 1. Remove existing file (best practice for overwrite)
            try:
                await asyncio.to_thread(
                    self.client.storage.from_(self.config.SUPABASE_BUCKET).remove,
                    [remote_path]
                )
                log.debug(f"Removed existing remote file: {remote_path}")
            except Exception as e:
                # This often fails if file doesn't exist, which is fine.
                log.debug(f"Pre-delete failed (likely file not found, this is OK): {remote_path}, {e}")

            # 2. Upload new file
            await asyncio.to_thread(
                self.client.storage.from_(self.config.SUPABASE_BUCKET).upload,
                remote_path,
                local_path,
                {"content-type": "application/json"} # <-- Specify content type
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
        """Saves snapshot as .json locally, then uploads."""
        json_path = os.path.join(self.config.SNAPSHOT_DIR_LOCAL, f"{filename_base}.json")
        # pkl_path = os.path.join(self.config.SNAPSHOT_DIR_LOCAL, f"{filename_base}.pkl") # <-- REMOVED

        try:
            def _save_files():
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(snapshot_data, f, indent=2, default=str)
                # with open(pkl_path, 'wb') as f: # <-- REMOVED
                #     pickle.dump(snapshot_data, f) # <-- REMOVED
            
            await asyncio.to_thread(_save_files)
            log.debug(f"Saved snapshot locally: {filename_base}.json")

        except Exception as e:
            log.error(f"Failed to save local snapshot {filename_base}: {e}")
            return

        if self.config.UPLOAD_TO_SUPABASE:
            remote_json_path = f"{self.config.SNAPSHOT_DIR_REMOTE}/{filename_base}.json"
            # remote_pkl_path = f"{self.config.SNAPSHOT_DIR_REMOTE}/{filename_base}.pkl" # <-- REMOVED
            
            try:
                await self.supabase.upload_file(json_path, remote_json_path)
                # await self.supabase.upload_file(pkl_path, remote_pkl_path) # <-- REMOVED
                log.info(f"Uploaded snapshot to Supabase: {filename_base}.json")
            except Exception as e:
                log.error(f"Failed to upload snapshot {filename_base}: {e}")

        if self.config.CLEANUP_LOCAL_FILES:
            try:
                os.remove(json_path)
                # os.remove(pkl_path) # <-- REMOVED
                log.debug(f"Cleaned up local snapshot: {filename_base}.json")
            except Exception as e:
                log.warning(f"Failed to clean up local file {filename_base}: {e}")
    
    async def save_dataset(self, dataset_data: Dict, pipeline: str, date_str: str, mint: str, is_expired: bool = False):
        """Saves labeled dataset as .json, uploads, returns success status."""
        subfolder_name = "expired_no_label" if is_expired else date_str
        dataset_dir_local = os.path.join(self.config.DATASET_DIR_LOCAL, pipeline, subfolder_name)
        os.makedirs(dataset_dir_local, exist_ok=True)
        
        safe_timestamp = dataset_data['features']['checked_at_utc'].replace(':', '-').replace('+', '_')
        filename_base = f"{mint}_{safe_timestamp}"
        
        json_path = os.path.join(dataset_dir_local, f"{filename_base}.json")
        # pkl_path = os.path.join(dataset_dir_local, f"{filename_base}.pkl") # <-- REMOVED
        
        try:
            def _save_files():
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(dataset_data, f, indent=2, default=str)
                # with open(pkl_path, 'wb') as f: # <-- REMOVED
                #     pickle.dump(dataset_data, f) # <-- REMOVED
            
            await asyncio.to_thread(_save_files)
            log.debug(f"Saved dataset locally: {filename_base}.json")
        except Exception as e:
            log.error(f"Failed to save local dataset {filename_base}: {e}")
            return False
        
        if self.config.UPLOAD_TO_SUPABASE:
            remote_json_path = f"{self.config.DATASET_DIR_REMOTE}/{pipeline}/{subfolder_name}/{filename_base}.json"
            # remote_pkl_path = f"{self.config.DATASET_DIR_REMOTE}/{pipeline}/{subfolder_name}/{filename_base}.pkl" # <-- REMOVED
            
            try:
                await self.supabase.upload_file(json_path, remote_json_path)
                # await self.supabase.upload_file(pkl_path, remote_pkl_path) # <-- REMOVED
                log.info(f"Uploaded dataset to Supabase: {pipeline}/{subfolder_name}/{filename_base}.json")
            except Exception as e:
                log.error(f"Failed to upload dataset {filename_base}: {e}")
                return False
        
        if self.config.CLEANUP_LOCAL_FILES:
            try:
                os.remove(json_path)
                # os.remove(pkl_path) # <-- REMOVED
                log.debug(f"Cleaned up local dataset: {filename_base}.json")
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

    # --- (*** CORRECTION ***) ---
    def _safe_get_dex_data(self, entry: dict) -> Optional[Dict]:
        """
        Safely extract Dexscreener data from a history entry.
        (***CORRECTED***) Now validates data contains a 'pairs' list.
        """
        if not isinstance(entry, dict): return None
        
        # Path 1: Top-level key (discovery-style)
        dex_data = entry.get("dexscreener")
        if isinstance(dex_data, dict) and isinstance(dex_data.get("pairs"), list):
            log.debug("Found valid 'dexscreener' data (with 'pairs') at top level.")
            return dex_data
        
        # Path 2: Nested in result (alpha-style)
        if isinstance(entry.get("result"), dict):
            dex_data = entry["result"].get("dexscreener")
            if isinstance(dex_data, dict) and isinstance(dex_data.get("pairs"), list):
                log.debug("Found valid 'dexscreener' data (with 'pairs') in result.")
                return dex_data
        
        log.debug("No valid (containing 'pairs') pre-fetched 'dexscreener' block found.")
        return None
    # --- END CORRECTION ---

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
            pass # Ignore errors from path traversal
        
        # Path 2: Discovery-style (top-level 'rugcheck_raw')
        raw_data = entry.get("rugcheck_raw")
        if isinstance(raw_data, dict) and "ok" in raw_data:
            log.debug("Found 'rugcheck_raw' data at top level.")
            return raw_data
            
        # Path 3: Fallback to 'rugcheck' key if it looks like the raw report
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
            log.warning(f"Could not parse timestamp from signal: {json.dumps(history_entry, default=str)}")
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
            # Find the best pair (highest liquidity) to use for creation date
            best_pair = max(
                (p for p in dex_data["pairs"] if p is not None), # Filter out None entries
                key=lambda p: float(p.get("liquidity", {}).get("usd", 0.0) or 0.0),
                default=None
            )
            
            if best_pair:
                pair_created_at_str = best_pair.get("pairCreatedAt")
                if pair_created_at_str:
                    try:
                        created_at_val = int(pair_created_at_str)
                        # Timestamps from dexscreener are in milliseconds
                        if created_at_val > 999999999999:
                             pair_created_at_timestamp = created_at_val // 1000
                        else: # Handle seconds just in case
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
        
        # (CORRECTION) Use the `price` from the rugcheck_raw block if price_usd is missing
        if market_features.get("price_usd", 0.0) == 0.0 and rug_data and rug_data.get("ok"):
            try:
                # Path from overlap_results_alpha.json: rugcheck_raw.raw.price
                # Fallback to top-level 'price'
                rug_price = float(rug_data.get("raw", {}).get("price", 0.0) or rug_data.get("price", 0.0) or 0.0)
                if rug_price > 0:
                    market_features["price_usd"] = rug_price
                    log.debug(f"Using fallback price from rug_data: {rug_price}")
            except Exception:
                pass # Ignore if price isn't there or invalid

        security_features = {}
        if rug_data and rug_data.get("ok"):
            # (CORRECTION) Handle both 'data' (v2) and 'raw' (v1) structures
            data = rug_data.get("data", rug_data.get("raw", {})) # Fallback to 'raw'
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
    
    def __init__(self, config: Config, supabase: SupabaseManager, persistence: 'PersistenceManager', active_snapshot_cache: Set[str]):
        self.config = config
        self.supabase = supabase
        self.persistence = persistence
        self.claimed_snapshots: Set[str] = set()
        
        # (*** NEW ***) Store reference to the main service's active snapshot cache
        self.active_snapshot_cache = active_snapshot_cache
        
        # Caches for a single aggregation pass
        self._file_cache: Dict[str, Tuple[Optional[Dict], float]] = {}
        self._cache_lock = asyncio.Lock()
        # (*** MODIFIED ***) Label index is keyed by composite_key
        self._label_index: Dict[str, Dict] = {} # {composite_key: {latest_label_data}}

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
        # Use the robust download_json_file method
        data = await self.supabase.download_json_file(remote_path)
        
        async with self._cache_lock:
            self._file_cache[remote_path] = (data, now + ttl)
        return data

    async def scan_and_aggregate(self):
        """Main aggregation loop: scan snapshots, check labels, aggregate or reschedule."""
        log.info("Starting snapshot aggregation scan...")
        
        self._clear_caches()
        
        # List all snapshot files
        snapshot_files = await self.supabase.list_files(self.config.SNAPSHOT_DIR_REMOTE, limit=1000)
        
        if not snapshot_files:
            log.info("No snapshot files found.")
            return
        
        now = datetime.now(timezone.utc)
        due_snapshots = []
        
        # 1. Filter for .json files that are due for checking
        for file_info in snapshot_files:
            filename = file_info.get('name', '')
            if not filename.endswith('.json'):
                continue
            
            # Skip if already claimed in this batch
            if filename in self.claimed_snapshots:
                continue
            
            # Download and check if due
            remote_path = f"{self.config.SNAPSHOT_DIR_REMOTE}/{filename}"
            # Use the robust download_json_file method
            snapshot = await self.supabase.download_json_file(remote_path)
            
            if not snapshot:
                log.warning(f"Failed to download snapshot {remote_path} for aggregation check, skipping.")
                continue
            
            finalization = snapshot.get('finalization', {})
            finalization_status = finalization.get("finalization_status", "pending")
            next_check_str = finalization.get('next_check_at')

            # Skip snapshots that are already finalized
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
        
        # 2. Build the set of *required* analytics files to download
        dates_to_scan: Set[str] = set()
        pipelines_to_scan: Set[str] = set()

        for _, snapshot, _ in due_snapshots:
            try:
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

        log.info(f"Scanning {len(dates_to_scan)} unique dates ({min(dates_to_scan)} to {max(dates_to_scan)}) and {len(pipelines_to_scan)} pipelines.")
        
        # 3. Download required files and build targeted label index
        fetch_tasks = []
        for pipeline in pipelines_to_scan:
            for date_str in dates_to_scan:
                remote_path = f"analytics/{pipeline}/daily/{date_str}.json"
                fetch_tasks.append(self._fetch_analytics_file(remote_path))
        
        file_contents = await asyncio.gather(*fetch_tasks, return_exceptions=True)
        
        # (*** MODIFIED ***) Build label index using composite key
        for content in file_contents:
            if isinstance(content, Exception) or not content or not isinstance(content.get("tokens"), list):
                continue
            
            for token in content["tokens"]:
                if not isinstance(token, dict): continue
                mint = token.get("mint")
                signal_type = token.get("signal_type") # Get signal_type
                
                # Check for both mint and signal_type
                if not mint or not signal_type or token.get("status") not in ("win", "loss"):
                    continue
                
                composite_key = get_composite_key(mint, signal_type) # Create key
                
                # Check for latest tracking_completed_at
                tracking_completed_at_str = token.get("tracking_completed_at")
                if not tracking_completed_at_str:
                    continue # Need this for comparison
                
                try:
                    new_label_time = parser.isoparse(tracking_completed_at_str)
                except Exception:
                    log.warning(f"Could not parse tracking_completed_at for {composite_key}: {tracking_completed_at_str}")
                    continue # Bad data
                
                existing_label = self._label_index.get(composite_key) # Use key
                if not existing_label:
                    self._label_index[composite_key] = token
                else:
                    try:
                        existing_label_time = parser.isoparse(existing_label.get("tracking_completed_at"))
                        if new_label_time > existing_label_time:
                            self._label_index[composite_key] = token # Update with newer label
                    except Exception:
                        self._label_index[composite_key] = token # Overwrite if old label was bad
        
        log.info(f"Built targeted label index with {len(self._label_index)} labeled token-signals.")
        # (*** END MODIFICATION ***)

        # 4. Process due snapshots in batches
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
            # 1. Claim the snapshot atomically
            if not await self._claim_snapshot(filename, snapshot, remote_path):
                log.debug(f"Failed to claim {filename}, skipping")
                return
            
            mint = snapshot['features']['mint']
            pipeline = snapshot['features']['signal_source']
            now = datetime.now(timezone.utc)
            
            # (*** MODIFIED ***) Create composite key for lookup
            composite_key = get_composite_key(mint, pipeline)
            
            finalization = snapshot['finalization']
            finalize_deadline_str = finalization['finalize_deadline']
            finalize_deadline = parser.isoparse(finalize_deadline_str).astimezone(timezone.utc)
            
            # 2. Look up label in our targeted index using composite key
            label_data = self._label_index.get(composite_key)
            
            if label_data:
                # LABEL FOUND - Aggregate to dataset
                log.info(f"Label found for {composite_key}: {label_data['status']}")
                await self._aggregate_with_label(snapshot, label_data, filename, remote_path)
                
            elif now >= finalize_deadline:
                # EXPIRED - No label found before deadline
                log.warning(f"Snapshot {filename} ({composite_key}) expired without label (Deadline: {finalize_deadline_str})")
                await self._aggregate_expired(snapshot, filename, remote_path)
                
            else:
                # NOT YET - Reschedule for next check
                log.debug(f"No label yet for {composite_key}, rescheduling (Deadline: {finalize_deadline_str})")
                await self._reschedule_snapshot(snapshot, filename, remote_path)
            # (*** END MODIFICATION ***)
                
        except Exception as e:
            log.error(f"Error processing snapshot {filename}: {e}", exc_info=True)
            # Release claim on error
            self.claimed_snapshots.discard(filename)
    
    async def _claim_snapshot(self, filename: str, snapshot: Dict, remote_path: str) -> bool:
        """
        Atomically claim a snapshot by:
        1. Marking as claimed in memory
        2. Deleting from remote
        3. Modifying locally
        4. Re-uploading modified version
        
        Returns True if claim successful, False otherwise.
        """
        try:
            # Mark as claimed
            self.claimed_snapshots.add(filename)
            
            # Delete remote (this "locks" it)
            await self.supabase.delete_file(remote_path)
            
            # Update claimed_by and claimed_at
            snapshot['finalization']['claimed_by'] = 'aggregator'
            snapshot['finalization']['claimed_at'] = datetime.now(timezone.utc).isoformat()
            snapshot['finalization']['finalization_status'] = 'awaiting_label' # Mark as actively being checked
            
            # Save locally and re-upload
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
        pipeline = snapshot['features']['signal_source']
        
        # 1. Attach label to snapshot
        snapshot['label'] = label_data
        snapshot['finalization']['finalization_status'] = 'labeled'
        snapshot['finalization']['finalized_at'] = datetime.now(timezone.utc).isoformat()
        
        # 2. Determine date for dataset folder
        checked_at_str = snapshot['features']['checked_at_utc']
        checked_at = parser.isoparse(checked_at_str)
        date_str = checked_at.strftime('%Y-%m-%d')
        
        # 3. Save as dataset
        success = await self.persistence.save_dataset(snapshot, pipeline, date_str, mint, is_expired=False)
        
        if success:
            # 4. Delete original snapshot (local + remote)
            await self._delete_snapshot(filename, remote_path)
            log.info(f"Successfully aggregated {mint} ({pipeline}) to dataset {pipeline}/{date_str}")
        else:
            log.error(f"Failed to save dataset for {mint} ({pipeline}), keeping snapshot")
            # Release claim but don't delete
            self.claimed_snapshots.discard(filename)
    
    async def _aggregate_expired(self, snapshot: Dict, filename: str, remote_path: str):
        """Mark snapshot as expired and move to expired dataset folder."""
        mint = snapshot['features']['mint']
        pipeline = snapshot['features']['signal_source']
        
        # 1. Mark as expired
        snapshot['label'] = None
        snapshot['finalization']['finalization_status'] = 'expired_no_label'
        snapshot['finalization']['finalized_at'] = datetime.now(timezone.utc).isoformat()
        
        # 2. Use signal date for folder structure, not current date
        checked_at_str = snapshot['features']['checked_at_utc']
        checked_at = parser.isoparse(checked_at_str)
        date_str = checked_at.strftime('%Y-%m-%d')
        
        # 3. Save to expired dataset
        success = await self.persistence.save_dataset(snapshot, pipeline, date_str, mint, is_expired=True)
        
        if success:
            # 4. Delete original snapshot
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
        # Increment check_count (as requested by 'checks_attempted')
        snapshot['finalization']['check_count'] = snapshot['finalization'].get('check_count', 0) + 1
        snapshot['finalization']['claimed_by'] = None # Release claim
        snapshot['finalization']['claimed_at'] = None
        
        # Save locally
        local_path = os.path.join(self.config.SNAPSHOT_DIR_LOCAL, filename)
        
        def _save():
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, 'w', encoding='utf-8') as f:
                json.dump(snapshot, f, indent=2, default=str)
        
        await asyncio.to_thread(_save)
        
        # Delete remote and re-upload (atomic update)
        await self.supabase.delete_file(remote_path)
        await self.supabase.upload_file(local_path, remote_path)
        
        # Cleanup local and release claim
        if self.config.CLEANUP_LOCAL_FILES:
            try:
                os.remove(local_path)
            except Exception as e:
                log.warning(f"Failed to cleanup {local_path}: {e}")
        
        self.claimed_snapshots.discard(filename)
        log.debug(f"Rescheduled {filename} for {next_check.isoformat()}")
    
    async def _delete_snapshot(self, filename: str, remote_path: str):
        """Delete snapshot files (.json only, local and remote)."""
        # base_name = filename.replace('.json', '') # <-- Not needed
        
        # Delete remote file
        json_remote = remote_path
        # pkl_remote = remote_path.replace('.json', '.pkl') # <-- REMOVED
        
        try:
            await self.supabase.delete_file(json_remote)
            log.debug(f"Deleted remote {json_remote}")
        except Exception as e:
            log.warning(f"Failed to delete remote {json_remote}: {e}")
        
        # try: # <-- REMOVED
        #     await self.supabase.delete_file(pkl_remote)
        #     log.debug(f"Deleted remote {pkl_remote}")
        # except Exception as e:
        #     log.warning(f"Failed to delete remote {pkl_remote}: {e}")
        
        # Delete local file if it exists
        local_json = os.path.join(self.config.SNAPSHOT_DIR_LOCAL, filename)
        # local_pkl = local_json.replace('.json', '.pkl') # <-- REMOVED
        
        if os.path.exists(local_json):
            try:
                os.remove(local_json)
                log.debug(f"Deleted local {local_json}")
            except Exception as e:
                log.warning(f"Failed to delete local {local_json}: {e}")

        # Release claim
        self.claimed_snapshots.discard(filename)
        
        # (*** NEW ***) Remove from the main service's active cache
        try:
            self.active_snapshot_cache.remove(filename)
            log.debug(f"Removed '{filename}' from active snapshot cache.")
        except KeyError:
            log.warning(f"Could not remove '{filename}' from cache, not found.")

# --- Main Collector Service ---

class CollectorService:
    def __init__(self, config: Config, session: aiohttp.ClientSession):
        self.config = config
        self.session = session
        
        # Pass the aiohttp session to SupabaseManager
        self.supabase = SupabaseManager(config, self.session)
        self.persistence = PersistenceManager(config, self.supabase)
        self.dex_client = DexscreenerClient(session, config)
        self.rug_client = RugCheckClient(session, config)
        self.holiday_client = HolidayClient(session, config)
        self.computer = FeatureComputer()
        
        # (*** NEW ***) Cache for active snapshots to prevent duplicates
        self.active_snapshot_files: Set[str] = set()
        
        # Pass the cache to the aggregator
        self.aggregator = SnapshotAggregator(config, self.supabase, self.persistence, self.active_snapshot_files)
        
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
            
            for mint, history_list in data.items():
                if not isinstance(history_list, list):
                    continue
                for history_entry in history_list:
                    all_signals.append({
                        "signal_type": signal_type,
                        "mint": mint,
                        "data": history_entry
                    })
        return all_signals

    def _build_canonical_snapshot(self, signal: Dict, features: Dict, 
                                  dex_raw: Optional[Dict], rug_raw: Optional[Dict], 
                                  holiday_check: bool, filename_base: str) -> Dict:
        """Assembles the final snapshot dictionary with finalization metadata."""
        
        # --- Per-Token Deadline Logic ---
        token_age_hours_at_signal = None
        
        # 1. Fallback Order: Dexscreener pairCreatedAt
        if 'token_age_at_signal_seconds' in features:
            token_age_hours_at_signal = features['token_age_at_signal_seconds'] / 3600.0
        
        # 2. Fallback Order: token_age_hours from signal file
        if token_age_hours_at_signal is None:
            age_from_signal = signal['data'].get('token_age_hours')
            if isinstance(age_from_signal, (int, float)):
                token_age_hours_at_signal = float(age_from_signal)
                log.debug(f"Using token age from signal file for {filename_base}: {token_age_hours_at_signal:.2f}h")

        # 3. Fallback Order: null (handled by else block)

        # Determine finalize window
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
        
        # If next_check_at is in the past, set it to now to process immediately
        if next_check_at < datetime.now(timezone.utc):
            log.warning(f"Initial next_check_at for {filename_base} is in the past. Setting to now.")
            next_check_at = datetime.now(timezone.utc)
        
        # --- End of Deadline Logic ---

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
                **features
            },
            "finalization": {
                "token_age_hours_at_signal": round(token_age_hours_at_signal, 2) if token_age_hours_at_signal is not None else None,
                "is_new_token": is_new_token,
                "finalize_window_hours": finalize_window_hours,
                "finalize_deadline": finalize_deadline.isoformat(),
                "check_interval_minutes": self.config.CHECK_INTERVAL_MINUTES,
                "next_check_at": next_check_at.isoformat(),
                "check_count": 0,
                "finalization_status": "pending", # Will become 'awaiting_label' on first check
                "claimed_by": None,
                "claimed_at": None,
                "finalized_at": None
            },
            "label": None
        }

    async def process_signal(self, signal: Dict):
        """
        (CORRECTED) Main processing pipeline for a single signal.
        Fetches live data ONLY if it's missing from the signal file.
        (*** NEW ***) Checks active_snapshot_files cache to prevent
        processing a (mint, type) pair that is already active.
        """
        mint = signal['mint']
        signal_type = signal['signal_type']
        history_entry = signal['data']

        # --- (CORRECTION) START ---
        # 1. Try to extract pre-fetched data from the signal
        dex_data = self.computer._safe_get_dex_data(history_entry)
        rug_data = self.computer._safe_get_rug_data(history_entry)
        # --- (CORRECTION) END ---

        # 2. Generate unique ID and check for idempotency
        # We compute features *first* using any available data.
        features, checked_at_dt = self.computer.compute_features(
            signal_type, history_entry, dex_data, rug_data, False # is_holiday=False for now
        )
        
        if not features or not checked_at_dt:
            log.warning(f"Skipping signal for mint {mint} due to missing base features (timestamp).")
            return

        safe_timestamp = features['checked_at_utc'].replace(':', '-').replace('+', '_')
        filename_base = f"{mint}_{safe_timestamp}_{signal_type}"
        
        remote_json_path = f"{self.config.SNAPSHOT_DIR_REMOTE}/{filename_base}.json"

        try:
            # 3. Idempotency Check 1: Exact signal (fast, remote check)
            if await self.supabase.check_file_exists(remote_json_path):
                log.debug(f"Skipping already processed exact signal: {filename_base}")
                return
        except Exception as e:
            log.error(f"Failed idempotency check for {filename_base}: {e}. Skipping.")
            return

        # 4. Idempotency Check 2: Active signal for same (mint, type) (local cache check)
        # (*** THIS IS THE NEW LOGIC YOU REQUESTED ***)
        search_prefix = f"{mint}_"
        search_suffix = f"_{signal_type}.json"
        for filename in self.active_snapshot_files:
            if filename.startswith(search_prefix) and filename.endswith(search_suffix):
                log.warning(f"Skipping new signal for ({mint}, {signal_type}) because an active snapshot is already being tracked: {filename}")
                return

        log.info(f"Processing new signal (passed active check): {filename_base}")

        # --- (CORRECTION) START ---
        # 5. Fetch *only missing* external data
        tasks_to_run = {}
        
        if not dex_data:
            log.warning(f"No pre-fetched Dex data for {filename_base}. Fetching live.")
            tasks_to_run["dex_data"] = self.dex_client.get_token_data(mint)
        
        if not rug_data:
            log.warning(f"No pre-fetched Rug data for {filename_base}. Fetching live.")
            tasks_to_run["rug_data"] = self.rug_client.get_token_report(mint)
            
        # Holiday data is always fetched live as it's not in the signal file
        tasks_to_run["is_holiday"] = self.holiday_client.is_holiday(checked_at_dt, self.config.HOLIDAY_COUNTRY_CODES)
        
        try:
            if tasks_to_run:
                task_keys = list(tasks_to_run.keys())
                results = await asyncio.gather(*tasks_to_run.values(), return_exceptions=False)
                results_dict = dict(zip(task_keys, results))
            else:
                results_dict = {}

            # Merge pre-fetched and newly-fetched data
            dex_data = dex_data or results_dict.get("dex_data")
            rug_data = rug_data or results_dict.get("rug_data")
            is_holiday = results_dict.get("is_holiday", False) # is_holiday is always fetched

        except Exception as e:
            log.error(f"Data-gathering failed for {filename_base}: {e}", exc_info=False)
            return
        # --- (CORRECTION) END ---


        # 6. Compute final features with all data
        final_features, _ = self.computer.compute_features(
            signal_type, history_entry, dex_data, rug_data, is_holiday
        )

        if not final_features:
            log.error(f"Failed to compute final features for {filename_base}. Skipping.")
            return

        # 7. Build and save snapshot with finalization metadata
        snapshot = self._build_canonical_snapshot(
            signal, final_features, dex_data, rug_data, is_holiday, filename_base
        )
        
        await self.persistence.save_snapshot(snapshot, filename_base)
        
        # (*** NEW ***) Add to the active cache *after* successful save
        self.active_snapshot_files.add(f"{filename_base}.json")

    async def run_process_with_semaphore(self, signal: Dict):
        """Wrapper for process_signal that acquires the semaphore before running."""
        mint = signal.get('mint', 'unknown_mint')
        try:
            async with self.process_semaphore:
                log.debug(f"Semaphore acquired for: {mint}")
                await self.process_signal(signal)
            log.debug(f"Semaphore released for: {mint}")
        except Exception as e:
            log.error(f"CRITICAL error during process_signal for {mint}: {e}", exc_info=True)
            # Do not re-raise, allow the loop to continue

    async def run(self):
        """Main service loop with both signal processing and aggregation."""
        log.info(f"Starting collector service. Poll interval: {self.config.POLL_INTERVAL}s, Aggregator interval: {self.config.AGGREGATOR_INTERVAL}s")
        
        last_aggregation = time.monotonic()
        
        while True:
            try:
                start_time = time.monotonic()
                log.info("Starting new polling cycle...")

                # (*** NEW ***) Update active snapshot file cache
                log.info("Updating active snapshot file cache...")
                try:
                    snapshot_files_list = await self.supabase.list_files(self.config.SNAPSHOT_DIR_REMOTE, limit=5000) # Increased limit
                    # Rebuild the set from scratch
                    self.active_snapshot_files = {f['name'] for f in snapshot_files_list if f.get('name') and f['name'].endswith('.json')}
                    log.info(f"Cached {len(self.active_snapshot_files)} active snapshot filenames.")
                except Exception as e:
                    log.error(f"Failed to update active snapshot cache: {e}. Proceeding with potentially stale cache.")
                
                # 1. Process new signals
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

                # 2. Run aggregator if interval elapsed
                if (start_time - last_aggregation) >= self.config.AGGREGATOR_INTERVAL:
                    log.info("Running snapshot aggregation...")
                    try:
                        await self.aggregator.scan_and_aggregate()
                        last_aggregation = time.monotonic() # Reset timer *after* completion
                    except Exception as e:
                        log.error(f"Aggregator failed: {e}", exc_info=True)
                        last_aggregation = time.monotonic() # Reset timer even on failure to avoid spam
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
    
    # 1. Supabase
    try:
        supa = SupabaseManager(config, session) # Pass session
        log.info("Testing Supabase connection (checking for alpha file)...")
        exists = await supa.check_file_exists(config.SIGNAL_FILE_ALPHA)
        log.info(f"Supabase test: Check for {config.SIGNAL_FILE_ALPHA} -> {exists} (TEST PASSED)")
        
        log.info("Testing Supabase download (downloading alpha file)...")
        data = await supa.download_json_file(config.SIGNAL_FILE_ALPHA)
        if data:
             log.info(f"Supabase download test: SUCCESS - downloaded {len(data)} tokens. (TEST PASSED)")
        else:
             log.error("Supabase download test: FAILED - no data returned.")
            
    except Exception as e:
        log.error(f"Supabase test: FAILED - {e}")

    # 2. Dexscreener
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

    # 3. RugCheck
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
        
    # 4. Holiday API
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

    # Load config and setup logging
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