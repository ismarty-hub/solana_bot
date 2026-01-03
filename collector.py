#!/usr/bin/env python3
"""
snapshot_collector.py - CORRECTED VERSION

Fixed to properly extract ALL fields from both RugCheck and Dexscreener data,
handling both discovery-style and alpha-style signal file structures.

Key fixes:
1. Comprehensive RugCheck field extraction from multiple nested paths
2. Proper handling of both v1 (raw) and v2 (data) RugCheck structures
3. Complete Dexscreener token metadata extraction
4. Fallback logic for missing fields
5. Type safety and validation for all extracted values
"""

import os
import json
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
    
    POLL_INTERVAL: int = field(default_factory=lambda: int(os.getenv("POLL_INTERVAL", "180")))
    AGGREGATOR_INTERVAL: int = field(default_factory=lambda: int(os.getenv("AGGREGATOR_INTERVAL", "60")))
    
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
    
    CHECK_INTERVAL_MINUTES: int = field(default_factory=lambda: int(os.getenv("CHECK_INTERVAL_MINUTES", "30")))
    TOKEN_AGE_THRESHOLD_HOURS: float = field(default_factory=lambda: float(os.getenv("TOKEN_AGE_THRESHOLD_HOURS", "12.0")))
    SHORT_FINALIZE_HOURS: int = field(default_factory=lambda: int(os.getenv("SHORT_FINALIZE_HOURS", "24")))
    LONG_FINALIZE_HOURS: int = field(default_factory=lambda: int(os.getenv("LONG_FINALIZE_HOURS", "168")))
    ANALYTICS_INDEX_TTL: int = field(default_factory=lambda: int(os.getenv("ANALYTICS_INDEX_TTL", "600")))
    AGGREGATOR_BATCH_SIZE: int = field(default_factory=lambda: int(os.getenv("AGGREGATOR_BATCH_SIZE", "10")))
    
    SNAPSHOT_RETENTION_DAYS: int = field(default_factory=lambda: int(os.getenv("SNAPSHOT_RETENTION_DAYS", "7")))
    DATASET_RETENTION_DAYS: int = field(default_factory=lambda: int(os.getenv("DATASET_RETENTION_DAYS", "90")))
    CLEANUP_INTERVAL_HOURS: int = field(default_factory=lambda: int(os.getenv("CLEANUP_INTERVAL_HOURS", "24")))

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

# --- Utility Functions ---

def get_composite_key(mint: str, signal_type: str) -> str:
    """Create a unique key for tracking a token per signal type."""
    return f"{mint}_{signal_type}"

def safe_float(value: Any, default: float = 0.0) -> float:
    """Safely convert value to float."""
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

def safe_int(value: Any, default: int = 0) -> int:
    """Safely convert value to int."""
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default

def safe_bool(value: Any, default: bool = False) -> bool:
    """Safely convert value to bool."""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ('true', '1', 'yes')
    return bool(value)

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
                        log.debug(f"[Cache HIT] for {func.__name__}")
                        return result
                    else:
                        del cache[key]

            result = await func(*args, **kwargs)
            async with lock:
                cache[key] = (result, now + ttl_seconds)
            return result
        return wrapper
    return decorator

# --- Base API Client ---

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
                        await asyncio.sleep(retry_after)
                    elif resp.status >= 500:
                        delay = (2 ** attempt) + (0.1 * (attempt + 1))
                        await asyncio.sleep(delay)
                    elif resp.status == 404:
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

        log.error(f"[{self.name}] Failed after {self.max_retries} attempts.")
        return None

# --- API Clients ---

class DexscreenerClient(BaseAPIClient):
    BASE_URL = "https://api.dexscreener.com/latest/dex/tokens"

    def __init__(self, session: aiohttp.ClientSession, config: Config):
        super().__init__(session, config.API_MAX_RETRIES, "Dexscreener")
        self.timeout = config.API_TIMEOUT_DEX

    @async_ttl_cache(ttl_seconds=120)
    async def get_token_data(self, mint: str) -> Optional[Dict]:
        url = f"{self.BASE_URL}/{mint}"
        return await self.async_get(url, self.timeout)

class RugCheckClient(BaseAPIClient):
    BASE_URL = "https://api.rugcheck.xyz/v1/tokens"

    def __init__(self, session: aiohttp.ClientSession, config: Config):
        super().__init__(session, config.API_MAX_RETRIES, "RugCheck")
        self.timeout = config.API_TIMEOUT_RUG

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
        url = f"{self.BASE_URL}/PublicHolidays/{year}/{country_code}"
        data = await self.async_get(url, self.timeout)
        if data and isinstance(data, list):
            return {item['date'] for item in data if 'date' in item and 'types' in item and 'Public' in item['types']}
        return set()

    async def is_holiday(self, dt: datetime, country_codes: List[str]) -> bool:
        date_str = dt.strftime('%Y-%m-%d')
        year = dt.year

        tasks = [self._fetch_holidays_for_year(year, code) for code in country_codes]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for res in results:
            if isinstance(res, set) and date_str in res:
                return True
        return False

# --- Supabase Manager (keeping your existing implementation) ---

class SupabaseManager:
    """SupabaseManager with sequential operations."""
    def __init__(self, config: Config, session: aiohttp.ClientSession):
        self.config = config
        self.session = session
        
        try:
            self.client: Client = create_client(config.SUPABASE_URL, config.SUPABASE_KEY)
            log.info("Supabase client initialized.")
        except Exception as e:
            log.critical(f"Failed to initialize Supabase client: {e}")
            raise
        
        self._file_cache_headers: Dict[str, Dict[str, str]] = {}
        self._local_cache_dir = os.path.join(config.DATASET_DIR_LOCAL, ".cache")
        os.makedirs(self._local_cache_dir, exist_ok=True)
        
        self._download_lock = asyncio.Lock()
        self._failed_paths: Dict[str, float] = {}
        self._backoff_seconds = 10.0
        self._last_operation_time = 0.0
        self._min_delay_between_ops = 0.5
    
    async def _enforce_rate_limit(self):
        now = time.monotonic()
        elapsed = now - self._last_operation_time
        if elapsed < self._min_delay_between_ops:
            sleep_time = self._min_delay_between_ops - elapsed
            await asyncio.sleep(sleep_time)
        self._last_operation_time = time.monotonic()
    
    async def download_json_file(self, remote_path: str) -> Optional[Dict]:
        now = time.monotonic()
        if remote_path in self._failed_paths:
            if now < self._failed_paths[remote_path]:
                return await self._load_from_local_cache(remote_path)
            else:
                del self._failed_paths[remote_path]
        
        async with self._download_lock:
            await self._enforce_rate_limit()
            return await self._download_json_file_impl(remote_path)
    
    async def _load_from_local_cache(self, remote_path: str) -> Optional[Dict]:
        local_save_path = os.path.join(
            self._local_cache_dir, 
            remote_path.replace('/', '_').replace('\\', '_')
        )
        
        if not os.path.exists(local_save_path):
            return None
        
        try:
            def _read_local():
                with open(local_save_path, "rb") as f:
                    return f.read()
            
            file_bytes = await asyncio.to_thread(_read_local)
            return json.loads(file_bytes)
        except Exception:
            return None
    
    async def _download_json_file_impl(self, remote_path: str) -> Optional[Dict]:
        local_save_path = os.path.join(
            self._local_cache_dir, 
            remote_path.replace('/', '_').replace('\\', '_')
        )
        
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                try:
                    signed_url_response = await asyncio.wait_for(
                        asyncio.to_thread(
                            self.client.storage.from_(self.config.SUPABASE_BUCKET).create_signed_url,
                            remote_path,
                            60
                        ),
                        timeout=15.0
                    )
                except asyncio.TimeoutError:
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2 ** attempt)
                        continue
                    else:
                        self._failed_paths[remote_path] = time.monotonic() + self._backoff_seconds
                        return await self._load_from_local_cache(remote_path)
                
                signed_url = signed_url_response.get('signedURL')
                if not signed_url:
                    return await self._load_from_local_cache(remote_path)

                headers = {}
                cached_headers = self._file_cache_headers.get(remote_path, {})
                if cached_headers.get('Last-Modified'):
                    headers['If-Modified-Since'] = cached_headers['Last-Modified']
                if cached_headers.get('ETag'):
                    headers['If-None-Match'] = cached_headers['ETag']

                try:
                    async with self.session.get(
                        signed_url, 
                        headers=headers, 
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as response:
                        if response.status == 304:
                            return await self._load_from_local_cache(remote_path)
                        
                        elif response.status == 200:
                            file_bytes = await response.read()
                            
                            new_headers = {}
                            if 'Last-Modified' in response.headers:
                                new_headers['Last-Modified'] = response.headers['Last-Modified']
                            if 'ETag' in response.headers:
                                new_headers['ETag'] = response.headers['ETag']
                            if new_headers:
                                self._file_cache_headers[remote_path] = new_headers
                            
                            def _save_local():
                                os.makedirs(os.path.dirname(local_save_path), exist_ok=True)
                                with open(local_save_path, "wb") as f:
                                    f.write(file_bytes)
                            
                            await asyncio.to_thread(_save_local)
                            return json.loads(file_bytes)
                        
                        elif response.status == 404:
                            return None
                        
                        else:
                            if attempt < max_retries - 1:
                                await asyncio.sleep(2 ** attempt)
                                continue
                            return await self._load_from_local_cache(remote_path)
                
                except (aiohttp.ClientError, asyncio.TimeoutError):
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2 ** attempt)
                        continue
                    else:
                        self._failed_paths[remote_path] = time.monotonic() + self._backoff_seconds
                        return await self._load_from_local_cache(remote_path)

            except json.JSONDecodeError:
                return None
            except Exception as e:
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                else:
                    self._failed_paths[remote_path] = time.monotonic() + self._backoff_seconds
                    return await self._load_from_local_cache(remote_path)
        
        return await self._load_from_local_cache(remote_path)

    async def check_file_exists(self, remote_path: str) -> bool:
        async with self._download_lock:
            await self._enforce_rate_limit()
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
        async with self._download_lock:
            await self._enforce_rate_limit()
            try:
                try:
                    await asyncio.to_thread(
                        self.client.storage.from_(self.config.SUPABASE_BUCKET).remove,
                        [remote_path]
                    )
                except Exception:
                    pass

                await asyncio.to_thread(
                    self.client.storage.from_(self.config.SUPABASE_BUCKET).upload,
                    remote_path,
                    local_path,
                    {"content-type": "application/json"}
                )
                log.debug(f"Uploaded {local_path} to {remote_path}")
            except Exception as e:
                log.error(f"Failed to upload {local_path}: {e}")
                raise
    
    async def delete_file(self, remote_path: str):
        async with self._download_lock:
            await self._enforce_rate_limit()
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
        async with self._download_lock:
            await self._enforce_rate_limit()
            try:
                files = await asyncio.to_thread(
                    self.client.storage.from_(self.config.SUPABASE_BUCKET).list,
                    folder,
                    {"limit": limit, "sortBy": {"column": "created_at", "order": "desc"}}
                )
                return files if files else []
            except Exception as e:
                log.error(f"Failed to list files in {folder}: {e}")
                return []
    
    async def cleanup_old_files(self, folder: str, retention_days: int) -> int:
        """Delete files older than retention_days. Returns count of deleted files."""
        try:
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=retention_days)
            log.info(f"Cleaning files older than {retention_days} days ({cutoff_date.isoformat()}) in {folder}")
            
            files_to_delete = []
            files = await self.list_files(folder, limit=5000)
            
            for file_info in files:
                try:
                    created_at_str = file_info.get('created_at')
                    if not created_at_str:
                        continue
                    
                    created_at = parser.isoparse(created_at_str)
                    if created_at < cutoff_date:
                        files_to_delete.append(file_info['name'])
                except Exception:
                    continue
            
            if not files_to_delete:
                log.info(f"No files to delete in {folder}")
                return 0
            
            log.info(f"Found {len(files_to_delete)} files to delete in {folder}")
            
            deleted_count = 0
            for filename in files_to_delete:
                try:
                    file_path = f"{folder}/{filename}"
                    await self.delete_file(file_path)
                    deleted_count += 1
                except Exception as e:
                    log.warning(f"Failed to delete {file_path}: {e}")
            
            log.info(f"Deleted {deleted_count}/{len(files_to_delete)} files from {folder}")
            return deleted_count
            
        except Exception as e:
            log.error(f"Cleanup failed for {folder}: {e}")
            return 0

# --- Persistence Manager ---

class PersistenceManager:
    """Handles saving snapshots and datasets."""
    def __init__(self, config: Config, supabase_manager: SupabaseManager):
        self.config = config
        self.supabase = supabase_manager

    async def save_snapshot(self, snapshot_data: Dict, filename_base: str):
        json_path = os.path.join(self.config.SNAPSHOT_DIR_LOCAL, f"{filename_base}.json")

        try:
            def _save_files():
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(snapshot_data, f, indent=2, default=str)
            
            await asyncio.to_thread(_save_files)
            log.debug(f"Saved snapshot locally: {filename_base}.json")

        except Exception as e:
            log.error(f"Failed to save local snapshot {filename_base}: {e}")
            return

        if self.config.UPLOAD_TO_SUPABASE:
            remote_json_path = f"{self.config.SNAPSHOT_DIR_REMOTE}/{filename_base}.json"
            
            try:
                await self.supabase.upload_file(json_path, remote_json_path)
                log.info(f"Uploaded snapshot to Supabase: {filename_base}.json")
            except Exception as e:
                log.error(f"Failed to upload snapshot {filename_base}: {e}")

        if self.config.CLEANUP_LOCAL_FILES:
            try:
                os.remove(json_path)
                log.debug(f"Cleaned up local snapshot: {filename_base}.json")
            except Exception as e:
                log.warning(f"Failed to clean up local file {filename_base}: {e}")
    
    async def save_dataset(self, dataset_data: Dict, pipeline: str, date_str: str, mint: str, is_expired: bool = False):
        subfolder_name = "expired_no_label" if is_expired else date_str
        dataset_dir_local = os.path.join(self.config.DATASET_DIR_LOCAL, pipeline, subfolder_name)
        os.makedirs(dataset_dir_local, exist_ok=True)
        
        safe_timestamp = dataset_data['features']['checked_at_utc'].replace(':', '-').replace('+', '_')
        filename_base = f"{mint}_{safe_timestamp}"
        
        json_path = os.path.join(dataset_dir_local, f"{filename_base}.json")
        
        try:
            def _save_files():
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(dataset_data, f, indent=2, default=str)
            
            await asyncio.to_thread(_save_files)
            log.debug(f"Saved dataset locally: {filename_base}.json")
        except Exception as e:
            log.error(f"Failed to save local dataset {filename_base}: {e}")
            return False
        
        if self.config.UPLOAD_TO_SUPABASE:
            remote_json_path = f"{self.config.DATASET_DIR_REMOTE}/{pipeline}/{subfolder_name}/{filename_base}.json"
            
            try:
                await self.supabase.upload_file(json_path, remote_json_path)
                log.info(f"Uploaded dataset to Supabase: {pipeline}/{subfolder_name}/{filename_base}.json")
            except Exception as e:
                log.error(f"Failed to upload dataset {filename_base}: {e}")
                return False
        
        if self.config.CLEANUP_LOCAL_FILES:
            try:
                os.remove(json_path)
                log.debug(f"Cleaned up local dataset: {filename_base}.json")
            except Exception as e:
                log.warning(f"Failed to clean up local dataset {filename_base}: {e}")
        
        return True

# --- CORRECTED Feature Computer ---

class FeatureComputer:
    """
    Computes derived features from raw signal and API data.
    CORRECTED to extract ALL fields from RugCheck and Dexscreener.
    """

    def _safe_get_timestamp(self, entry: dict) -> Optional[str]:
        """Extract timestamp from a history entry."""
        if not isinstance(entry, dict):
            return None
        for field in ["ts", "timestamp", "checked_at", "created_at", "updated_at"]:
            ts = entry.get(field)
            if isinstance(ts, str):
                return ts
        result = entry.get("result", {})
        if isinstance(result, dict):
            for field in ["discovered_at", "checked_at", "timestamp"]:
                ts = result.get(field)
                if isinstance(ts, str):
                    return ts
        return None

    def _safe_get_grade(self, entry: dict) -> str:
        """Extract grade from a history entry."""
        if not isinstance(entry, dict):
            return "UNKNOWN"
        if isinstance(entry.get("result"), dict):
            grade = entry["result"].get("grade")
            if isinstance(grade, str):
                return grade
        if isinstance(entry.get("grade"), str):
            return entry["grade"]
        for path in [["overlap_result", "grade"], ["data", "grade"], ["analysis", "grade"]]:
            obj = entry
            for key in path:
                obj = obj.get(key) if isinstance(obj, dict) else None
            if isinstance(obj, str):
                return obj
        return "UNKNOWN"

    def _safe_get_dex_data(self, entry: dict) -> Optional[Dict]:
        """
        Extract Dexscreener data from a history entry.
        Must contain 'pairs' list to be valid.
        """
        if not isinstance(entry, dict):
            return None
        
        # Path 1: Top-level (discovery-style)
        dex_data = entry.get("dexscreener")
        if isinstance(dex_data, dict) and isinstance(dex_data.get("pairs"), list):
            log.debug("Found dexscreener data at top level")
            return dex_data
        
        # Path 2: Nested in result (alpha-style)
        if isinstance(entry.get("result"), dict):
            dex_data = entry["result"].get("dexscreener")
            if isinstance(dex_data, dict) and isinstance(dex_data.get("pairs"), list):
                log.debug("Found dexscreener data in result")
                return dex_data
        
        return None

    def _safe_get_rug_data(self, entry: dict) -> Optional[Dict]:
        """
        Extract RugCheck API response from a history entry.
        Handles multiple nesting paths.
        """
        if not isinstance(entry, dict):
            return None
        
        # Path 1: Alpha-style (result.security.rugcheck_raw)
        try:
            raw_data = entry.get("result", {}).get("security", {}).get("rugcheck_raw")
            if isinstance(raw_data, dict) and "ok" in raw_data:
                log.debug("Found rugcheck_raw in result.security")
                return raw_data
        except Exception:
            pass
        
        # Path 2: Discovery-style (top-level rugcheck_raw)
        raw_data = entry.get("rugcheck_raw")
        if isinstance(raw_data, dict) and "ok" in raw_data:
            log.debug("Found rugcheck_raw at top level")
            return raw_data
            
        # Path 3: Fallback to 'rugcheck' key
        raw_data = entry.get("rugcheck")
        if isinstance(raw_data, dict) and "ok" in raw_data:
            log.debug("Found rugcheck at top level")
            return raw_data
        
        return None

    def _extract_dex_features(self, dex_data: Optional[Dict]) -> Dict[str, Any]:
        """
        CORRECTED: Extract ALL Dexscreener fields.
        Returns complete market features dict.
        """
        features = {
            # Market data
            "price_usd": 0.0,
            "fdv_usd": 0.0,
            "market_cap_usd": 0.0,
            "liquidity_usd": 0.0,
            "volume_h24_usd": 0.0,
            "volume_h6_usd": 0.0,
            "volume_h1_usd": 0.0,
            "volume_m5_usd": 0.0,
            "price_change_h24_pct": 0.0,
            "price_change_h6_pct": 0.0,
            "price_change_h1_pct": 0.0,
            "price_change_m5_pct": 0.0,
            
            # Pair metadata
            "pair_created_at_timestamp": None,
            "pair_address": None,
            "dex_id": None,
            "chain_id": None,
            
            # Trading activity
            "txns_h24_buys": 0,
            "txns_h24_sells": 0,
            "txns_h6_buys": 0,
            "txns_h6_sells": 0,
            "txns_h1_buys": 0,
            "txns_h1_sells": 0,
            "txns_m5_buys": 0,
            "txns_m5_sells": 0,
            
            # Token info
            "token_name": None,
            "token_symbol": None,
        }
        
        if not dex_data or not isinstance(dex_data.get("pairs"), list) or len(dex_data["pairs"]) == 0:
            return features
        
        # Find best pair (highest liquidity)
        best_pair = max(
            (p for p in dex_data["pairs"] if p is not None),
            key=lambda p: safe_float(p.get("liquidity", {}).get("usd")),
            default=None
        )
        
        if not best_pair:
            return features
        
        # Extract pair creation timestamp
        pair_created_at_str = best_pair.get("pairCreatedAt")
        if pair_created_at_str:
            try:
                created_at_val = int(pair_created_at_str)
                # Dexscreener timestamps are in milliseconds
                if created_at_val > 999999999999:
                    features["pair_created_at_timestamp"] = created_at_val // 1000
                else:
                    features["pair_created_at_timestamp"] = created_at_val
            except Exception as e:
                log.warning(f"Failed to parse pairCreatedAt '{pair_created_at_str}': {e}")
        
        # Market data
        features["price_usd"] = safe_float(best_pair.get("priceUsd"))
        features["fdv_usd"] = safe_float(best_pair.get("fdv"))
        features["market_cap_usd"] = safe_float(best_pair.get("marketCap"))
        features["liquidity_usd"] = safe_float(best_pair.get("liquidity", {}).get("usd"))
        
        # Volume data
        volume = best_pair.get("volume", {})
        features["volume_h24_usd"] = safe_float(volume.get("h24"))
        features["volume_h6_usd"] = safe_float(volume.get("h6"))
        features["volume_h1_usd"] = safe_float(volume.get("h1"))
        features["volume_m5_usd"] = safe_float(volume.get("m5"))
        
        # Price changes
        price_change = best_pair.get("priceChange", {})
        features["price_change_h24_pct"] = safe_float(price_change.get("h24"))
        features["price_change_h6_pct"] = safe_float(price_change.get("h6"))
        features["price_change_h1_pct"] = safe_float(price_change.get("h1"))
        features["price_change_m5_pct"] = safe_float(price_change.get("m5"))
        
        # Transaction counts
        txns = best_pair.get("txns", {})
        h24 = txns.get("h24", {})
        h6 = txns.get("h6", {})
        h1 = txns.get("h1", {})
        m5 = txns.get("m5", {})
        
        features["txns_h24_buys"] = safe_int(h24.get("buys"))
        features["txns_h24_sells"] = safe_int(h24.get("sells"))
        features["txns_h6_buys"] = safe_int(h6.get("buys"))
        features["txns_h6_sells"] = safe_int(h6.get("sells"))
        features["txns_h1_buys"] = safe_int(h1.get("buys"))
        features["txns_h1_sells"] = safe_int(h1.get("sells"))
        features["txns_m5_buys"] = safe_int(m5.get("buys"))
        features["txns_m5_sells"] = safe_int(m5.get("sells"))
        
        # Pair metadata
        features["pair_address"] = best_pair.get("pairAddress")
        features["dex_id"] = best_pair.get("dexId")
        features["chain_id"] = best_pair.get("chainId")
        
        # Token info (from baseToken)
        base_token = best_pair.get("baseToken", {})
        features["token_name"] = base_token.get("name")
        features["token_symbol"] = base_token.get("symbol")
        
        return features

    def _extract_rug_features(self, rug_data: Optional[Dict]) -> Dict[str, Any]:
        """
        CORRECTED: Extract ALL RugCheck fields.
        Handles both v1 (raw) and v2 (data) structures.
        """
        features = {
            # Risk assessment
            "rugcheck_risk_level": "unknown",
            "rugcheck_risk_score": 0,
            "is_rugged": False,
            
            # Authority flags
            "has_mint_authority": False,
            "has_freeze_authority": False,
            "has_lp_authority": False,
            
            # Token distribution
            "creator_balance_pct": 0.0,
            "creator_balance_tokens": 0.0,
            "top_10_holders_pct": 0.0,
            "total_supply": 0.0,
            
            # Liquidity
            "is_lp_locked_95_plus": False,
            "total_lp_locked_usd": 0.0,
            "lp_locked_pct": 0.0,
            
            # Market data (from RugCheck)
            "rug_market_cap_usd": 0.0,
            "rug_liquidity_usd": 0.0,
            "rug_price_usd": 0.0,
            
            # Token metadata
            "token_meta_name": None,
            "token_meta_symbol": None,
            "token_meta_description": None,
            "token_meta_image_uri": None,
            
            # Market information
            "primary_market_name": None,
            "primary_market_lp_burned": False,
            "num_markets": 0,
            
            # Top holders details
            "top_holders_count": 0,
        }
        
        if not rug_data or not rug_data.get("ok"):
            return features
        
        # Handle both v1 (raw) and v2 (data) structures
        data = rug_data.get("data", rug_data.get("raw", {}))
        if not isinstance(data, dict):
            return features
        
        # Risk assessment
        risk = data.get("risk", {})
        if isinstance(risk, dict):
            features["rugcheck_risk_level"] = risk.get("level", "unknown")
            features["rugcheck_risk_score"] = safe_int(risk.get("score"))
        
        features["is_rugged"] = safe_bool(data.get("rugged"))
        
        # Authorities
        features["has_mint_authority"] = bool(data.get("mintAuthority"))
        features["has_freeze_authority"] = bool(data.get("freezeAuthority"))
        features["has_lp_authority"] = bool(data.get("lpAuthority"))
        
        # Creator balance
        creator_balance = data.get("creatorBalance")
        if isinstance(creator_balance, dict):
            features["creator_balance_pct"] = safe_float(creator_balance.get("pct"))
            features["creator_balance_tokens"] = safe_float(creator_balance.get("amount"))
        elif creator_balance is not None:
            # Handle case where it's just a number
            features["creator_balance_pct"] = safe_float(creator_balance)
        
        # Top holders
        top_holders = data.get("topHolders", [])
        if isinstance(top_holders, list):
            features["top_holders_count"] = len(top_holders)
            features["top_10_holders_pct"] = sum(
                safe_float(h.get("pct")) for h in top_holders[:10] if isinstance(h, dict)
            )
        
        # Total supply
        features["total_supply"] = safe_float(data.get("totalSupply"))
        
        # Markets and liquidity
        markets = data.get("markets", [])
        if isinstance(markets, list):
            features["num_markets"] = len(markets)
            
            # Calculate LP locked metrics
            total_lp_locked_usd = 0.0
            max_locked_pct = 0.0
            lp_locked_95_plus = False
            
            for market in markets:
                if not isinstance(market, dict):
                    continue
                
                lp = market.get("lp", {})
                if isinstance(lp, dict):
                    locked_usd = safe_float(lp.get("lpLockedUSD"))
                    locked_pct = safe_float(lp.get("lockedPct"))
                    
                    total_lp_locked_usd += locked_usd
                    max_locked_pct = max(max_locked_pct, locked_pct)
                    
                    if locked_pct >= 95.0:
                        lp_locked_95_plus = True
            
            features["total_lp_locked_usd"] = total_lp_locked_usd
            features["lp_locked_pct"] = max_locked_pct
            features["is_lp_locked_95_plus"] = lp_locked_95_plus
            
            # Primary market info
            if len(markets) > 0:
                primary_market = markets[0]
                if isinstance(primary_market, dict):
                    features["primary_market_name"] = primary_market.get("name")
                    lp = primary_market.get("lp", {})
                    if isinstance(lp, dict):
                        features["primary_market_lp_burned"] = safe_bool(lp.get("lpBurn"))
        
        # Market data from RugCheck
        features["rug_market_cap_usd"] = safe_float(data.get("marketCap"))
        features["rug_liquidity_usd"] = safe_float(data.get("liquidity"))
        features["rug_price_usd"] = safe_float(data.get("price"))
        
        # Token metadata
        token_meta = data.get("tokenMeta", {})
        if isinstance(token_meta, dict):
            features["token_meta_name"] = token_meta.get("name")
            features["token_meta_symbol"] = token_meta.get("symbol")
            features["token_meta_description"] = token_meta.get("description")
            features["token_meta_image_uri"] = token_meta.get("image")
        
        return features

    def compute_features(self, signal_type: str, history_entry: Dict, 
                         dex_data: Optional[Dict], rug_data: Optional[Dict], 
                         is_holiday: bool) -> Tuple[Optional[Dict], Optional[datetime]]:
        """
        CORRECTED: Computes all derived features with complete extraction.
        """
        
        checked_at_str = self._safe_get_timestamp(history_entry)
        if not checked_at_str:
            log.warning(f"Could not parse timestamp from signal")
            return None, None
        
        try:
            checked_at_dt = parser.isoparse(checked_at_str).astimezone(timezone.utc)
            checked_at_timestamp = int(checked_at_dt.timestamp())
        except Exception as e:
            log.warning(f"Failed to parse timestamp '{checked_at_str}': {e}")
            return None, None

        # Time features
        time_features = {
            "checked_at_utc": checked_at_dt.isoformat(),
            "checked_at_timestamp": checked_at_timestamp,
            "time_of_day_utc": checked_at_dt.hour,
            "day_of_week_utc": checked_at_dt.weekday(),
            "is_weekend_utc": checked_at_dt.weekday() >= 5,
            "is_public_holiday_any": is_holiday,
        }

        # Signal features
        signal_features = {
            "signal_source": signal_type,
            "grade": self._safe_get_grade(history_entry)
        }

        # Extract market features from Dexscreener
        market_features = self._extract_dex_features(dex_data)
        
        # Extract security features from RugCheck
        security_features = self._extract_rug_features(rug_data)
        
        # Use RugCheck price as fallback if Dexscreener price is 0
        if market_features["price_usd"] == 0.0 and security_features["rug_price_usd"] > 0.0:
            market_features["price_usd"] = security_features["rug_price_usd"]
            log.debug(f"Using RugCheck price as fallback: {security_features['rug_price_usd']}")
        
        # Compute derived features
        derived_features = {}
        
        # Token age at signal
        pair_created_at_timestamp = market_features.get("pair_created_at_timestamp")
        if pair_created_at_timestamp:
            derived_features["token_age_at_signal_seconds"] = max(
                0, checked_at_timestamp - pair_created_at_timestamp
            )
            derived_features["token_age_at_signal_hours"] = derived_features["token_age_at_signal_seconds"] / 3600.0
        
        # Liquidity to market cap ratio
        if market_features["market_cap_usd"] > 0:
            derived_features["liquidity_to_mcap_ratio"] = (
                market_features["liquidity_usd"] / market_features["market_cap_usd"]
            )
        else:
            derived_features["liquidity_to_mcap_ratio"] = 0.0
        
        # Volume to liquidity ratio (24h)
        if market_features["liquidity_usd"] > 0:
            derived_features["volume_to_liquidity_ratio_h24"] = (
                market_features["volume_h24_usd"] / market_features["liquidity_usd"]
            )
        else:
            derived_features["volume_to_liquidity_ratio_h24"] = 0.0
        
        # Buy/sell ratio
        total_txns_h24 = market_features["txns_h24_buys"] + market_features["txns_h24_sells"]
        if total_txns_h24 > 0:
            derived_features["buy_ratio_h24"] = market_features["txns_h24_buys"] / total_txns_h24
        else:
            derived_features["buy_ratio_h24"] = 0.0
        
        # Combine all features
        all_features = {
            **time_features,
            **signal_features,
            **market_features,
            **security_features,
            **derived_features,
        }
        
        return all_features, checked_at_dt

# --- Snapshot Aggregator (keeping your existing implementation) ---

class SnapshotAggregator:
    """Handles efficient aggregation of snapshots with labels."""
    
    def __init__(self, config: Config, supabase: SupabaseManager, persistence: 'PersistenceManager', active_snapshot_cache: Set[str]):
        self.config = config
        self.supabase = supabase
        self.persistence = persistence
        self.claimed_snapshots: Set[str] = set()
        self.active_snapshot_cache = active_snapshot_cache
        self._file_cache: Dict[str, Tuple[Optional[Dict], float]] = {}
        self._cache_lock = asyncio.Lock()
        self._label_index: Dict[str, Dict] = {}
        self._existing_daily_folders: Dict[str, Set[str]] = {}

    def _clear_caches(self):
        log.debug("Clearing aggregator pass caches.")
        self._file_cache.clear()
        self._label_index.clear()
        self._existing_daily_folders.clear()

    async def _fetch_analytics_file(self, remote_path: str) -> Optional[Dict]:
        now = time.monotonic()
        ttl = self.config.ANALYTICS_INDEX_TTL
        
        async with self._cache_lock:
            if remote_path in self._file_cache:
                data, expiry = self._file_cache[remote_path]
                if now < expiry:
                    return data
        
        data = await self.supabase.download_json_file(remote_path)
        
        async with self._cache_lock:
            self._file_cache[remote_path] = (data, now + ttl)
        return data

    async def scan_and_aggregate(self):
        """Sequential snapshot aggregation scan."""
        log.info("Starting sequential snapshot aggregation scan...")
        
        self._clear_caches()
        now = datetime.now(timezone.utc)
        
        # Stage 1: Build label index
        log.info("Loading 'active_tracking.json'...")
        active_tracking_data = await self.supabase.download_json_file("analytics/active_tracking.json")
        
        if active_tracking_data and isinstance(active_tracking_data, dict):
            active_wins_found = 0
            for composite_key, token_data in active_tracking_data.items():
                if token_data.get("status") == "win":
                    self._label_index[composite_key] = token_data
                    active_wins_found += 1
            log.info(f"Found {active_wins_found} active 'win' labels.")
        
        await asyncio.sleep(1.0)
            
        # Discover pipelines
        try:
            analytics_folders = await self.supabase.list_files("analytics/")
            pipelines_to_scan = {
                f['name'] for f in analytics_folders 
                if f['name'] not in ('snapshots', 'overall', 'active_tracking.json')
            }
            log.info(f"Found pipelines: {pipelines_to_scan}")
        except Exception as e:
            log.error(f"Failed to discover pipelines: {e}")
            pipelines_to_scan = set()
        
        await asyncio.sleep(1.0)

        # Collect daily file paths
        daily_file_paths = []
        for pipeline in pipelines_to_scan:
            daily_folder_path = f"analytics/{pipeline}/daily"
            try:
                files = await self.supabase.list_files(daily_folder_path, limit=2000)
                for file_info in files:
                    filename = file_info.get('name', '')
                    if filename.endswith('.json'):
                        daily_file_paths.append(f"{daily_folder_path}/{filename}")
                await asyncio.sleep(0.5)
            except Exception as e:
                log.error(f"Failed to list daily files for {pipeline}: {e}")

        log.info(f"Found {len(daily_file_paths)} daily files. Processing sequentially...")

        # Download daily files sequentially
        finalized_labels_found = 0
        
        for i, file_path in enumerate(daily_file_paths):
            if i > 0 and i % 10 == 0:
                log.info(f"Progress: {i}/{len(daily_file_paths)} daily files processed")
            
            try:
                content = await self.supabase.download_json_file(file_path)
                
                if not content or not isinstance(content.get("tokens"), list):
                    continue
                
                for token in content["tokens"]:
                    if not isinstance(token, dict):
                        continue
                        
                    mint = token.get("mint")
                    signal_type = token.get("signal_type")
                    
                    if not mint or not signal_type or token.get("status") not in ("win", "loss"):
                        continue
                    
                    composite_key = get_composite_key(mint, signal_type)
                    self._label_index[composite_key] = token
                    finalized_labels_found += 1
            
            except Exception as e:
                log.warning(f"Failed to process daily file {file_path}: {e}")

        log.info(f"Found {finalized_labels_found} finalized labels from daily files.")
        log.info(f"Total labels indexed: {len(self._label_index)}")

        if not self._label_index:
            log.info("No labels found. Nothing to aggregate.")
            return

        await asyncio.sleep(1.0)

        # Stage 2: Match snapshots
        log.info("Listing snapshot files...")
        snapshot_files = await self.supabase.list_files(self.config.SNAPSHOT_DIR_REMOTE, limit=5000)
        
        if not snapshot_files:
            log.info("No snapshot files found.")
            return

        log.info(f"Found {len(snapshot_files)} snapshot files. Matching against labels...")

        snapshots_to_process = []

        for file_info in snapshot_files:
            filename = file_info.get('name', '')
            if not filename.endswith('.json'):
                continue
            
            try:
                parts = filename.replace('.json', '').split('_')
                if len(parts) < 3:
                    continue
                
                mint = parts[0]
                signal_type = parts[-1]
                composite_key = get_composite_key(mint, signal_type)
                
                if composite_key in self._label_index:
                    remote_path = f"{self.config.SNAPSHOT_DIR_REMOTE}/{filename}"
                    snapshots_to_process.append((filename, remote_path, composite_key))
                    
            except Exception as e:
                log.warning(f"Error parsing filename {filename}: {e}")
        
        if not snapshots_to_process:
            log.info("No snapshots match the label index.")
            return
            
        log.info(f"Found {len(snapshots_to_process)} snapshots with labels.")
        
        await asyncio.sleep(1.0)

        # Stage 3: Process snapshots sequentially
        processed_count = 0
        failed_count = 0
        
        for i, (filename, remote_path, composite_key) in enumerate(snapshots_to_process):
            try:
                if i > 0 and i % 10 == 0:
                    log.info(f"Progress: {i}/{len(snapshots_to_process)} snapshots processed "
                            f"({processed_count} successful, {failed_count} failed)")
                
                snapshot_data = await self.supabase.download_json_file(remote_path)
                
                if not snapshot_data:
                    failed_count += 1
                    continue
                
                label_data = self._label_index.get(composite_key)
                if not label_data:
                    failed_count += 1
                    continue
                
                finalization_status = snapshot_data.get('finalization', {}).get("finalization_status", "pending")
                if finalization_status in ("labeled", "expired_no_label"):
                    log.warning(f"Snapshot {filename} already finalized. Deleting.")
                    await self._delete_snapshot(filename, remote_path)
                    continue

                log.info(f"[{i+1}/{len(snapshots_to_process)}] Aggregating {filename} with label '{label_data.get('status')}'")
                await self._aggregate_with_label(snapshot_data, label_data, filename, remote_path)
                processed_count += 1
                
            except Exception as e:
                log.error(f"Failed to aggregate {filename}: {e}", exc_info=True)
                failed_count += 1
        
        log.info(f"Aggregation complete: {processed_count} successful, {failed_count} failed")
    
    async def _aggregate_with_label(self, snapshot: Dict, label_data: Dict, 
                                filename: str, remote_path: str):
        mint = snapshot['features']['mint']
        pipeline = snapshot['features']['signal_source']
        
        snapshot['label'] = label_data
        snapshot['finalization']['finalization_status'] = 'labeled'
        snapshot['finalization']['finalized_at'] = datetime.now(timezone.utc).isoformat()
        
        tracking_completed_str = label_data.get('tracking_completed_at')
        if tracking_completed_str:
            try:
                tracking_completed = parser.isoparse(tracking_completed_str)
                date_str = tracking_completed.strftime('%Y-%m-%d')
            except Exception:
                checked_at_str = snapshot['features']['checked_at_utc']
                checked_at = parser.isoparse(checked_at_str)
                date_str = checked_at.strftime('%Y-%m-%d')
        else:
            checked_at_str = snapshot['features']['checked_at_utc']
            checked_at = parser.isoparse(checked_at_str)
            date_str = checked_at.strftime('%Y-%m-%d')
        
        success = await self.persistence.save_dataset(snapshot, pipeline, date_str, mint, is_expired=False)
        
        if success:
            await self._delete_snapshot(filename, remote_path)
            log.info(f"Successfully aggregated {mint} ({pipeline}) to dataset {pipeline}/{date_str}")
        else:
            log.error(f"Failed to save dataset for {mint} ({pipeline})")
    
    async def _delete_snapshot(self, filename: str, remote_path: str):
        try:
            await self.supabase.delete_file(remote_path)
        except Exception as e:
            log.warning(f"Failed to delete remote {remote_path}: {e}")
        
        local_json = os.path.join(self.config.SNAPSHOT_DIR_LOCAL, filename)
        
        if os.path.exists(local_json):
            try:
                os.remove(local_json)
            except Exception:
                pass

        self.claimed_snapshots.discard(filename)
        
        try:
            if filename in self.active_snapshot_cache:
                self.active_snapshot_cache.remove(filename)
        except Exception:
            pass

# --- Main Collector Service ---

class CollectorService:
    def __init__(self, config: Config, session: aiohttp.ClientSession):
        self.config = config
        self.session = session
        
        self.supabase = SupabaseManager(config, self.session)
        self.persistence = PersistenceManager(config, self.supabase)
        self.dex_client = DexscreenerClient(session, config)
        self.rug_client = RugCheckClient(session, config)
        self.holiday_client = HolidayClient(session, config)
        self.computer = FeatureComputer()
        
        self.active_snapshot_files: Set[str] = set()
        self.processing_keys: Set[str] = set()  # Track signals currently being processed
        self.aggregator = SnapshotAggregator(config, self.supabase, self.persistence, self.active_snapshot_files)
        
        self.process_semaphore = asyncio.Semaphore(config.PROCESSOR_CONCURRENCY)
        # Initialize to past to trigger cleanup on first run, then every CLEANUP_INTERVAL_HOURS
        self.last_cleanup = time.monotonic() - (config.CLEANUP_INTERVAL_HOURS * 3600)
        log.info(f"Processor concurrency: {config.PROCESSOR_CONCURRENCY}")
        log.info(f"Retention: Snapshots={config.SNAPSHOT_RETENTION_DAYS}d, Datasets={config.DATASET_RETENTION_DAYS}d")
        log.info("CollectorService initialized.")

    async def _fetch_all_signals(self) -> List[Dict]:
        """Fetches and flattens all signals from both files, keeping only the latest for each token."""
        tasks = {
            "discovery": self.supabase.download_json_file(self.config.SIGNAL_FILE_DISCOVERY),
            "alpha": self.supabase.download_json_file(self.config.SIGNAL_FILE_ALPHA),
        }
        
        results = await asyncio.gather(*tasks.values())
        signal_data = dict(zip(tasks.keys(), results))
        
        latest_signals = {}  # (mint, signal_type) -> (timestamp, signal_dict)
        
        for signal_type, data in signal_data.items():
            if not data or not isinstance(data, dict):
                continue
            
            for mint, history_list in data.items():
                if not isinstance(history_list, list):
                    continue
                
                for history_entry in history_list:
                    timestamp_str = self.computer._safe_get_timestamp(history_entry)
                    if not timestamp_str:
                        continue
                    
                    try:
                        timestamp_dt = parser.isoparse(timestamp_str)
                        key = (mint, signal_type)
                        
                        # Only keep the latest signal for this token/type pair
                        if key not in latest_signals or timestamp_dt > latest_signals[key][0]:
                            latest_signals[key] = (timestamp_dt, {
                                "signal_type": signal_type,
                                "mint": mint,
                                "data": history_entry
                            })
                    except Exception:
                        continue
                        
        return [item[1] for item in latest_signals.values()]

    def _build_canonical_snapshot(self, signal: Dict, features: Dict, 
                                  dex_raw: Optional[Dict], rug_raw: Optional[Dict], 
                                  holiday_check: bool, filename_base: str) -> Dict:
        """Assembles the final snapshot with finalization metadata."""
        
        token_age_hours_at_signal = features.get("token_age_at_signal_hours")
        
        if token_age_hours_at_signal is None:
            age_from_signal = signal['data'].get('token_age_hours')
            if isinstance(age_from_signal, (int, float)):
                token_age_hours_at_signal = float(age_from_signal)

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
                "finalization_status": "pending",
                "claimed_by": None,
                "claimed_at": None,
                "finalized_at": None
            },
            "label": None
        }

    async def process_signal(self, signal: Dict):
        """
        CORRECTED: Main processing pipeline with robust duplicate prevention.
        """
        mint = signal['mint']
        signal_type = signal['signal_type']
        history_entry = signal['data']
        
        # 1. In-memory Lifecycle Lock (Prevents race conditions in the same batch)
        processing_key = f"{mint}_{signal_type}"
        if processing_key in self.processing_keys:
            log.debug(f"Already processing internal: {processing_key}")
            return
            
        self.processing_keys.add(processing_key)
        
        try:
            # 2. Extract timestamp and check basic features
            features, checked_at_dt = self.computer.compute_features(
                signal_type, history_entry, None, None, False
            )
            
            if not features or not checked_at_dt:
                log.warning(f"Skipping signal for {mint} - missing base features")
                return

            safe_timestamp = features['checked_at_utc'].replace(':', '-').replace('+', '_')
            filename_base = f"{mint}_{safe_timestamp}_{signal_type}"
            filename_json = f"{filename_base}.json"
            remote_json_path = f"{self.config.SNAPSHOT_DIR_REMOTE}/{filename_json}"

            # 3. Robust Idempotency Check (Active Monitoring Check)
            # Check if ANY active snapshot exists for this (mint, signal_type)
            search_prefix = f"{mint}_"
            search_suffix = f"_{signal_type}.json"
            
            existing_active = [f for f in self.active_snapshot_files 
                               if f.startswith(search_prefix) and f.endswith(search_suffix)]
            
            if existing_active:
                log.info(f"Skipping: active snapshot already exists for {processing_key} -> {existing_active[0]}")
                return

            # Extra safety: Check Supabase directly if not in local cache
            try:
                if await self.supabase.check_file_exists(remote_json_path):
                    log.debug(f"Skipping: file already exists on Supabase: {filename_json}")
                    self.active_snapshot_files.add(filename_json)
                    return
            except Exception as e:
                log.error(f"Failed remote idempotency check for {filename_base}: {e}")
                return

            log.info(f"Processing new signal: {filename_base}")

            # 4. Data Gathering (Fetch live data if missing)
            dex_data = self.computer._safe_get_dex_data(history_entry)
            rug_data = self.computer._safe_get_rug_data(history_entry)
            
            tasks_to_run = {}
            if not dex_data:
                tasks_to_run["dex_data"] = self.dex_client.get_token_data(mint)
            if not rug_data:
                tasks_to_run["rug_data"] = self.rug_client.get_token_report(mint)
            
            tasks_to_run["is_holiday"] = self.holiday_client.is_holiday(checked_at_dt, self.config.HOLIDAY_COUNTRY_CODES)
            
            results_dict = {}
            if tasks_to_run:
                task_keys = list(tasks_to_run.keys())
                results = await asyncio.gather(*tasks_to_run.values(), return_exceptions=False)
                results_dict = dict(zip(task_keys, results))

            dex_data = dex_data or results_dict.get("dex_data")
            rug_data = rug_data or results_dict.get("rug_data")
            is_holiday = results_dict.get("is_holiday", False)

            # 5. Final Feature Computation and Persistence
            final_features, _ = self.computer.compute_features(
                signal_type, history_entry, dex_data, rug_data, is_holiday
            )

            if not final_features:
                log.error(f"Failed to compute final features for {filename_base}")
                return

            snapshot = self._build_canonical_snapshot(
                signal, final_features, dex_data, rug_data, is_holiday, filename_base
            )
            
            success = await self.persistence.save_snapshot(snapshot, filename_base)
            if success:
                self.active_snapshot_files.add(filename_json)
                log.info(f"Successfully created snapshot: {filename_json}")

        except Exception as e:
            log.error(f"Unexpected error in process_signal for {mint}: {e}", exc_info=True)
        finally:
            self.processing_keys.discard(processing_key)

    async def run_process_with_semaphore(self, signal: Dict):
        """Wrapper for process_signal with semaphore."""
        mint = signal.get('mint', 'unknown_mint')
        try:
            async with self.process_semaphore:
                await self.process_signal(signal)
        except Exception as e:
            log.error(f"Error processing {mint}: {e}", exc_info=True)

    async def run_cleanup(self):
        """Run storage cleanup if interval has elapsed."""
        current_time = time.monotonic()
        cleanup_interval_seconds = self.config.CLEANUP_INTERVAL_HOURS * 3600
        
        if current_time - self.last_cleanup >= cleanup_interval_seconds:
            log.info("Starting storage cleanup cycle...")
            try:
                # Cleanup snapshots
                deleted_snapshots = await self.supabase.cleanup_old_files(
                    self.config.SNAPSHOT_DIR_REMOTE, 
                    self.config.SNAPSHOT_RETENTION_DAYS
                )
                
                # Cleanup datasets
                deleted_datasets = await self.supabase.cleanup_old_files(
                    self.config.DATASET_DIR_REMOTE, 
                    self.config.DATASET_RETENTION_DAYS
                )
                
                log.info(f"Storage cleanup complete: Deleted {deleted_snapshots} snapshots + {deleted_datasets} datasets")
            except Exception as e:
                log.error(f"Storage cleanup failed: {e}", exc_info=True)
            finally:
                self.last_cleanup = time.monotonic()

    async def run(self):
        """Main service loop."""
        log.info(f"Starting collector service. Poll: {self.config.POLL_INTERVAL}s, Aggregator: {self.config.AGGREGATOR_INTERVAL}s")
        
        last_aggregation = time.monotonic() - self.config.AGGREGATOR_INTERVAL - 1
        
        while True:
            try:
                start_time = time.monotonic()
                log.info("Starting new polling cycle...")

                # Update active snapshot cache
                log.info("Updating active snapshot file cache...")
                try:
                    snapshot_files_list = await self.supabase.list_files(self.config.SNAPSHOT_DIR_REMOTE, limit=5000)
                    self.active_snapshot_files = {f['name'] for f in snapshot_files_list if f.get('name') and f['name'].endswith('.json')}
                    log.info(f"Cached {len(self.active_snapshot_files)} active snapshot filenames.")
                except Exception as e:
                    log.error(f"Failed to update active snapshot cache: {e}")
                
                # Process new signals
                signals = await self._fetch_all_signals()
                log.info(f"Found {len(signals)} total signals to check.")
                
                if signals:
                    tasks = [self.run_process_with_semaphore(sig) for sig in signals]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    exceptions = [r for r in results if isinstance(r, Exception)]
                    if exceptions:
                        log.error(f"{len(exceptions)} signals failed during processing")

                # Run aggregator if interval elapsed
                elapsed_since_last_agg = start_time - last_aggregation
                
                if elapsed_since_last_agg >= self.config.AGGREGATOR_INTERVAL:
                    log.info("Running snapshot aggregation...")
                    try:
                        await self.aggregator.scan_and_aggregate()
                        last_aggregation = time.monotonic()
                        log.info("Aggregation completed successfully.")
                    except Exception as e:
                        log.error(f"Aggregator failed: {e}", exc_info=True)
                        last_aggregation = time.monotonic()
                
                # Run storage cleanup if interval elapsed
                await self.run_cleanup()

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
    
    # Supabase
    try:
        supa = SupabaseManager(config, session)
        log.info("Testing Supabase connection...")
        exists = await supa.check_file_exists(config.SIGNAL_FILE_ALPHA)
        log.info(f"Supabase check: {config.SIGNAL_FILE_ALPHA} exists -> {exists}")
        
        data = await supa.download_json_file(config.SIGNAL_FILE_ALPHA)
        if data:
            log.info(f"Supabase download: SUCCESS - downloaded {len(data)} tokens")
        else:
            log.error("Supabase download: FAILED")
    except Exception as e:
        log.error(f"Supabase test: FAILED - {e}")

    # Dexscreener
    try:
        dex = DexscreenerClient(session, config)
        log.info(f"Testing Dexscreener with mint: {SAMPLE_MINT}")
        data = await dex.get_token_data(SAMPLE_MINT)
        if data and data.get("pairs"):
            log.info(f"Dexscreener test: SUCCESS - Found {len(data['pairs'])} pairs")
        else:
            log.error("Dexscreener test: FAILED")
    except Exception as e:
        log.error(f"Dexscreener test: FAILED - {e}")

    # RugCheck
    try:
        rug = RugCheckClient(session, config)
        log.info(f"Testing RugCheck with mint: {SAMPLE_MINT}")
        data = await rug.get_token_report(SAMPLE_MINT)
        if data and data.get("ok"):
            risk_data = data.get('data', data.get('raw', {}))
            risk = risk_data.get('risk', {})
            log.info(f"RugCheck test: SUCCESS - Risk level: {risk.get('level')}")
        else:
            log.error("RugCheck test: FAILED")
    except Exception as e:
        log.error(f"RugCheck test: FAILED - {e}")
        
    # Holiday API
    try:
        holiday = HolidayClient(session, config)
        log.info("Testing Holiday API for US...")
        today = datetime.now(timezone.utc)
        is_hol = await holiday.is_holiday(today, ["US"])
        log.info(f"Holiday API test: Is today a US holiday? -> {is_hol}")
    except Exception as e:
        log.error(f"Holiday API test: FAILED - {e}")
        
    log.info("--- API Tests Complete ---")


async def main():
    """Main entry point."""
    parser_cli = argparse.ArgumentParser(description="Solana Snapshot Collector Service")
    parser_cli.add_argument(
        "command",
        choices=["run", "test-apis"],
        default="run",
        nargs="?",
        help="Command: 'run' (default) or 'test-apis'"
    )
    parser_cli.add_argument(
        "--log-level",
        type=str,
        default=None,
        help="Override log level (DEBUG, INFO, WARNING)"
    )
    args = parser_cli.parse_args()

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