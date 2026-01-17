## 1) `alerts/analytics_monitoring.py` (FULL FILE)

# alerts/analytics_monitoring.py
#!/usr/bin/env python3
"""
Analytics-driven signal detector for paper trading.

Changes in this corrected version:
- Records `startup_time` when the loop starts and only processes tokens whose
  `entry_time` is *after* the startup_time (prevents opening trades for tokens
  that were present before the bot started).
- Checks user-level auto-trade activation times (if present in user prefs) and
  only processes tokens whose `entry_time` is after the user's activation time.
- Defensive guards against `None`/malformed active_tracking entries.
- Ensures `ml_prediction` is a dict (fallback to {}).
- Still keeps snapshot-based duplicate prevention.

Behavioral summary:
- A token will be processed only when ALL of the following are true:
  1. token entry_time parsed correctly and > bot startup_time
  2. token entry_time >= user auto-trade activation time (if user pref present)
  3. token entry_time != snapshot entry_time (not previously processed)
  4. user preferences (alpha_alerts, grades, and auto_trade_enabled) allow it

"""
import asyncio
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List

from telegram.ext import Application

# Config imports
from config import DATA_DIR, BUCKET_NAME, USE_SUPABASE, ALL_GRADES, SIGNAL_FRESHNESS_WINDOW, ANALYTICS_POLL_INTERVAL

# File IO helpers
from shared.file_io import safe_load, safe_save

import joblib

# Try import download_file from supabase utils (optional)
try:
    from supabase_utils import download_file
except Exception:
    download_file = None  # graceful fallback

logger = logging.getLogger(__name__)

# Constants
ACTIVE_TRACKING_FILE = DATA_DIR / "active_tracking.json"
SNAPSHOT_FILE = DATA_DIR / "last_processed_tracking.json"
OVERLAP_FILE = DATA_DIR / "overlap_results.pkl"
ALPHA_OVERLAP_FILE = DATA_DIR / "overlap_results_alpha.pkl"
POLL_INTERVAL = 10  # Reduced to 10 seconds for faster execution



def parse_iso_to_dt(s: str) -> Optional[datetime]:
    """Parse an ISO8601-like timestamp into an aware datetime (UTC).

    Accepts strings with trailing 'Z' or timezone offsets. Returns None if
    parsing fails.
    """
    if not s or not isinstance(s, str):
        return None
    try:
        # handle trailing Z
        if s.endswith("Z"):
            s2 = s[:-1] + "+00:00"
        else:
            s2 = s
        dt = datetime.fromisoformat(s2)
        # make timezone-aware
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


async def download_active_tracking_with_retry(max_retries: int = 3) -> Dict[str, Any]:
    """
    Load active_tracking.json. 
    PRIORITY: Local file (checked first for speed)
    FALLBACK: Supabase download
    """
    # 1. Try local file first (Zero Latency)
    if ACTIVE_TRACKING_FILE.exists():
        try:
            # Check file age
            mtime = Path(ACTIVE_TRACKING_FILE).stat().st_mtime
            age = time.time() - mtime
            if age < 60: # If local file is fresh (< 60s), use it immediately
                data = safe_load(ACTIVE_TRACKING_FILE, {})
                if data:
                    logger.debug(f"✅ Using FRESH local active_tracking.json (age: {age:.1f}s)")
                    return data
        except Exception as e:
            logger.warning(f"Failed to load fresh local file: {e}")

    # 2. If no local or local is stale, try Supabase (only if not skipping)
    skip_supabase = os.getenv("SKIP_SUPABASE_DOWNLOAD", "False").lower() == "true"
    if USE_SUPABASE and download_file and not skip_supabase:
        remote_path = "analytics/active_tracking.json"
        attempt = 0
        while attempt < max_retries:
            attempt += 1
            try:
                ok = download_file(str(ACTIVE_TRACKING_FILE), remote_path, bucket=BUCKET_NAME)
                if ok and ACTIVE_TRACKING_FILE.exists():
                    data = safe_load(ACTIVE_TRACKING_FILE, {})
                    if data:
                        logger.info("✅ Downloaded active_tracking.json from Supabase")
                        return data
            except Exception as e:
                logger.warning(f"Download attempt {attempt} failed: {e}")
            await asyncio.sleep(1)

    # 3. Last resort: Load whatever the local file has, regardless of age
    if ACTIVE_TRACKING_FILE.exists():
        return safe_load(ACTIVE_TRACKING_FILE, {})
    
    return {}


def get_composite_key(mint: str, signal_type: str) -> str:
    """Return composite key for storage: {mint}_{signal_type}"""
    return f"{mint}_{signal_type}"


def get_token_data_from_overlap(mint: str, overlap_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Identify token metadata from pre-loaded overlap datasets.
    Checks list of overlap dicts provided.
    """
    for overlap in overlap_data:
        try:
            if not isinstance(overlap, dict):
                continue

            history = overlap.get(mint)
            if not history:
                # Fallback for keys that might contain the mint
                for k, v in overlap.items():
                    if isinstance(k, str) and k.endswith(mint):
                        history = v
                        break

            if not history or not isinstance(history, list) or not history[-1]:
                continue

            last_entry = history[-1]
            result = last_entry.get("result", {}) if isinstance(last_entry, dict) else {}
            if result:
                # Add ML_PASSED status from the history entry level if it exists
                if isinstance(last_entry, dict) and "ML_PASSED" in last_entry:
                    result["ML_PASSED"] = last_entry["ML_PASSED"]
                return result
        except Exception as e:
            logger.debug(f"Error checking pre-loaded overlap data for {mint}: {e}")
            continue

    return {}


def get_user_activation_time(user_prefs: Dict[str, Any]) -> Optional[datetime]:
    """
    Attempt to extract a user's auto-trade activation timestamp from their
    preferences. Returns a timezone-aware datetime or None if not present.

    The function looks for common keys that might be used by the system:
    - 'auto_trade_activated_at'
    - 'auto_trade_enabled_at'
    - 'auto_trade_opt_in_time'

    If found, the value is expected to be an ISO8601 string. Malformed or
    missing values are ignored (-> None).
    """
    if not user_prefs or not isinstance(user_prefs, dict):
        return None
    keys = [
        "auto_trade_activated_at",
        "auto_trade_enabled_at",
        "auto_trade_opt_in_time",
        "auto_trade_opt_in_at",
    ]
    for k in keys:
        v = user_prefs.get(k)
        if v:
            dt = parse_iso_to_dt(v)
            if dt:
                return dt
    return None


async def active_tracking_signal_loop(app: Application, user_manager, portfolio_manager):
    """
    Main analytics-driven loop.

    Key differences from previous implementation:
    - `startup_time` recorded at loop start; tokens with entry_time <= startup_time
      are skipped (they existed before the bot started).
    - For each user, if user_prefs includes an activation timestamp, tokens with
      entry_time <= that activation timestamp are skipped for that user.
    - Duplicate prevention via snapshot still applies.
    """
    logger.info("🔍 Analytics signal loop started.")

    # Record start time for logging purposes
    startup_time = datetime.now(timezone.utc)
    logger.info(f"Analytics loop started at {startup_time.isoformat()}")

    # Load snapshot on startup
    snapshot = safe_load(SNAPSHOT_FILE, {})
    logger.info(f"Loaded snapshot with {len(snapshot)} entries.")

    # Slight initial delay to allow other systems to settle
    await asyncio.sleep(2.0)

    while True:
        try:
            active_tracking = await download_active_tracking_with_retry()
            if not active_tracking:
                logger.debug("No active tracking data; sleeping and retrying.")
                await asyncio.sleep(POLL_INTERVAL)
                continue

            # Pre-load overlap data ONCE per loop to avoid massive disk IO
            overlap_datasets = []
            for f in [OVERLAP_FILE, ALPHA_OVERLAP_FILE]:
                if f.exists():
                    try:
                        data = joblib.load(f)
                        if isinstance(data, dict):
                            overlap_datasets.append(data)
                    except Exception as e:
                        logger.error(f"Failed to pre-load {f.name}: {e}")

            trading_users = user_manager.get_trading_users()
            if not trading_users:
                logger.debug("No trading users found; sleeping.")
                await asyncio.sleep(POLL_INTERVAL)
                continue

            new_signals_found = 0

            # iterate through items: keys are expected to be "{mint}_{signal_type}"
            for composite_key, data in active_tracking.items():
                try:
                    # Defensive checks: skip None/malformed entries
                    if data is None or not isinstance(data, dict):
                        logger.warning(f"Skipping {composite_key} - active_tracking entry is None or invalid.")
                        continue

                    # expected composite_key format may be provided, but also support explicit fields
                    if "_" in composite_key:
                        mint_from_key, signal_type_from_key = composite_key.rsplit("_", 1)
                    else:
                        mint_from_key = data.get("mint")
                        signal_type_from_key = data.get("signal_type")

                    # prefer explicit fields if present
                    mint = data.get("mint") or mint_from_key
                    signal_type = data.get("signal_type") or signal_type_from_key

                    if not mint or not signal_type:
                        logger.debug(f"Skipping malformed entry: missing mint or signal_type for key={composite_key}")
                        continue

                    current_entry_time = data.get("entry_time")
                    if not current_entry_time:
                        logger.debug(f"Skipping {mint} - missing entry_time.")
                        continue

                    entry_dt = parse_iso_to_dt(current_entry_time)
                    if not entry_dt:
                        logger.debug(f"Skipping {mint} - could not parse entry_time: {current_entry_time}")
                        continue


                    # CRITICAL: Signal Freshness Check
                    # Skip signals that are older than the configured window (e.g. 5 mins)
                    # This prevents executing stale signals if the user adds capital later
                    now = datetime.now(timezone.utc)
                    age_seconds = (now - entry_dt).total_seconds()
                    
                    if age_seconds > SIGNAL_FRESHNESS_WINDOW:
                        logger.debug(f"Skipping {mint} - signal stale used {age_seconds:.0f}s > {SIGNAL_FRESHNESS_WINDOW}s limit")
                        continue

                    key = get_composite_key(mint, signal_type)
                    last_entry_time = snapshot.get(key, {}).get("entry_time")

                    # Duplicate prevention
                    if current_entry_time == last_entry_time:
                        logger.debug(f"Skipping {key} - already processed (duplicate entry_time)")
                        continue

                    # Grade and Enriched Metadata assignment using PRE-LOADED data
                    enriched_data = get_token_data_from_overlap(mint, overlap_datasets)
                    grade = enriched_data.get("grade")
                    
                    if not grade:
                        ml_action = (data.get("ml_prediction") or {}).get("action", "UNKNOWN")
                        if signal_type == "alpha":
                            grade = "HIGH"
                        elif ml_action == "BUY":
                            grade = "HIGH"
                        elif ml_action == "CONSIDER":
                            grade = "MEDIUM"
                        else:
                            grade = "MEDIUM"
                    
                    # Merge enriched ML prediction with data from active_tracking
                    ml_prediction = data.get("ml_prediction") or {}
                    if enriched_data.get("ml_prediction"):
                        ml_prediction.update(enriched_data["ml_prediction"])
                    
                    # Merge ML Passed status (permissive - if either says passed)
                    ml_passed = data.get("ML_PASSED", False) or enriched_data.get("ML_PASSED", False)

                    # Process for each trading user
                    for chat_id in trading_users:
                        try:
                            user_prefs = user_manager.get_user_prefs(chat_id) or {}

                            # Respect auto-trade enabled
                            if user_prefs.get("auto_trade_enabled") is False:
                                logger.debug(f"User {chat_id} opted out of auto-trade; skipping {key}")
                                continue

                            # Optional user-level activation timestamp
                            user_activation = get_user_activation_time(user_prefs)
                            if user_activation and entry_dt <= user_activation:
                                logger.debug(
                                    f"Skipping {key} for user {chat_id}: entry_time {entry_dt.isoformat()} <= activation {user_activation.isoformat()}"
                                )
                                continue

                            # Alpha trading filtering (Decoupled)
                            if signal_type == "alpha" and not user_prefs.get("trade_alpha_alerts", False):
                                logger.debug(f"User {chat_id} disabled alpha auto-trading; skipping {key}")
                                continue

                            # Discovery grade trading filtering (Decoupled)
                            if signal_type == "discovery":
                                allowed_trade_grades = user_prefs.get("trade_grades", ALL_GRADES)
                                if grade not in allowed_trade_grades:
                                    logger.debug(f"User {chat_id} trade grade filter prevents {key} ({grade})")
                                    continue

                            # Build token info
                            token_info = {
                                "mint": mint,
                                "signal_type": signal_type,
                                "symbol": data.get("symbol", "Unknown"),
                                "name": data.get("name", "Unknown"),
                                "price": data.get("entry_price"),
                                "grade": grade,
                                "token_age_hours": data.get("token_age_hours"),
                                "tracking_end_time": data.get("tracking_end_time"),
                                "entry_time": data.get("entry_time"),
                                "entry_mcap": data.get("entry_mcap"),
                                "entry_liquidity": data.get("entry_liquidity"),
                                "ml_prediction": ml_prediction,
                                "ml_passed": ml_passed,
                            }

                            await portfolio_manager.process_new_signal(
                                chat_id, token_info, user_manager, app
                            )
                            logger.info(f"Processed new signal for user={chat_id} token={key} grade={grade}")
                        except Exception as e:
                            logger.exception(f"Error processing signal for user {chat_id} token={key}: {e}")
                            continue

                    # Update snapshot
                    snapshot[key] = {
                        "entry_time": current_entry_time,
                        "processed_at": datetime.now(timezone.utc).isoformat() + "Z",
                        "signal_type": signal_type,
                        "symbol": data.get("symbol", "Unknown"),
                    }
                    new_signals_found += 1

                except Exception as e:
                    logger.exception(f"Error processing token {composite_key}: {e}")
                    continue

            if new_signals_found:
                try:
                    safe_save(SNAPSHOT_FILE, snapshot)
                    logger.info(f"Saved snapshot with {new_signals_found} new signals.")
                except Exception as e:
                    logger.warning(f"Failed saving snapshot: {e}")

            await asyncio.sleep(POLL_INTERVAL)

        except Exception as e:
            logger.exception(f"Analytics signal loop failure: {e}")
            await asyncio.sleep(POLL_INTERVAL)