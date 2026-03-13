#!/usr/bin/env python3
"""
delete_analytics_dates.py

Utility script to delete analytics data from specific dates in Supabase.
Use this to surgically remove bad/corrupt data for days where there were issues.

FOLDER STRUCTURE TARGETED:
    analytics/{signal_type}/daily/{date}.json

SIGNAL TYPES (by default):
    - discovery
    - alpha

USAGE EXAMPLES:

  # Dry run (preview what would be deleted, no actual deletion):
  python delete_analytics_dates.py --dates 2026-02-28 2026-03-01 --dry-run

  # Delete specific dates for ALL signal types:
  python delete_analytics_dates.py --dates 2026-02-28 2026-03-01

  # Delete specific dates for only the 'alpha' signal type:
  python delete_analytics_dates.py --dates 2026-02-28 --signal-types alpha

  # Delete a range of dates:
  python delete_analytics_dates.py --date-range 2026-02-20 2026-02-25
"""

import os
import sys
import argparse
import logging
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration — mirrors your existing setup
# ---------------------------------------------------------------------------

BUCKET_NAME         = os.getenv("SUPABASE_BUCKET", "monitor-data")
DEFAULT_SIGNAL_TYPES = ["discovery", "alpha"]
ANALYTICS_BASE_PATH  = "analytics"  # Supabase folder root

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.FileHandler("delete_analytics_dates.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("AnalyticsDeleter")


# ---------------------------------------------------------------------------
# Supabase helpers
# ---------------------------------------------------------------------------

def get_client() -> Client:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        log.critical("SUPABASE_URL and SUPABASE_KEY must be set in .env")
        sys.exit(1)
    return create_client(url, key)


def file_exists(client: Client, remote_path: str) -> bool:
    """Check whether a specific file exists in the bucket."""
    try:
        folder = os.path.dirname(remote_path).replace("\\", "/")
        filename = os.path.basename(remote_path)
        result = client.storage.from_(BUCKET_NAME).list(folder, {"search": filename})
        return any(f.get("name") == filename for f in (result or []))
    except Exception as e:
        log.warning(f"  Could not check existence of {remote_path}: {e}")
        return False


def delete_file(client: Client, remote_path: str) -> bool:
    """Delete a single file from the bucket. Returns True on success."""
    try:
        client.storage.from_(BUCKET_NAME).remove([remote_path])
        return True
    except Exception as e:
        log.error(f"  Failed to delete {remote_path}: {e}")
        return False


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def build_target_paths(dates: list[str], signal_types: list[str]) -> list[dict]:
    """
    Build the list of Supabase file paths to target.

    Path pattern:  analytics/{signal_type}/daily/{date}.json
    """
    targets = []
    for signal in signal_types:
        for d in dates:
            path = f"{ANALYTICS_BASE_PATH}/{signal}/daily/{d}.json"
            targets.append({"signal_type": signal, "date": d, "path": path})
    return targets


def run(dates: list[str], signal_types: list[str], dry_run: bool) -> None:
    log.info("=" * 60)
    log.info("  Analytics Date Deletion Utility")
    log.info("=" * 60)
    log.info(f"  Bucket       : {BUCKET_NAME}")
    log.info(f"  Signal types : {', '.join(signal_types)}")
    log.info(f"  Dates        : {', '.join(dates)}")
    log.info(f"  Dry run      : {'YES — no files will be deleted' if dry_run else 'NO  — files WILL be deleted'}")
    log.info("=" * 60)

    client = get_client()
    targets = build_target_paths(dates, signal_types)

    found      = []
    not_found  = []

    log.info("\nScanning for files...")
    for t in targets:
        if file_exists(client, t["path"]):
            log.info(f"  [FOUND]     {t['path']}")
            found.append(t)
        else:
            log.info(f"  [NOT FOUND] {t['path']}")
            not_found.append(t)

    log.info(f"\nScan complete: {len(found)} file(s) found, {len(not_found)} file(s) missing.")

    if not found:
        log.info("Nothing to delete. Exiting.")
        return

    # Confirmation prompt (unless dry-run)
    if not dry_run:
        log.info(f"\nAbout to permanently delete {len(found)} file(s).")
        confirm = input("Type 'yes' to confirm deletion: ").strip().lower()
        if confirm != "yes":
            log.info("Deletion cancelled by user.")
            return

    deleted  = 0
    failed   = 0

    log.info("\nProcessing deletions...")
    for t in found:
        if dry_run:
            log.info(f"  [DRY RUN] Would delete: {t['path']}")
            deleted += 1
        else:
            success = delete_file(client, t["path"])
            if success:
                log.info(f"  [DELETED]  {t['path']}")
                deleted += 1
            else:
                log.error(f"  [FAILED]   {t['path']}")
                failed += 1

    log.info("\n" + "=" * 60)
    action = "Would have deleted" if dry_run else "Deleted"
    log.info(f"  {action} : {deleted} file(s)")
    if not dry_run:
        log.info(f"  Failed     : {failed} file(s)")
        if failed == 0:
            log.info("  All deletions successful ✓")
        else:
            log.warning("  Some deletions failed — check the log above.")
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# Date range helper
# ---------------------------------------------------------------------------

def expand_date_range(start: str, end: str) -> list[str]:
    """Return a list of YYYY-MM-DD strings from start to end (inclusive)."""
    try:
        start_dt = datetime.strptime(start, "%Y-%m-%d").date()
        end_dt   = datetime.strptime(end,   "%Y-%m-%d").date()
    except ValueError as e:
        log.critical(f"Invalid date format: {e}. Use YYYY-MM-DD.")
        sys.exit(1)

    if start_dt > end_dt:
        log.critical("--date-range: start date must be before or equal to end date.")
        sys.exit(1)

    result = []
    current = start_dt
    while current <= end_dt:
        result.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return result


def validate_dates(dates: list[str]) -> list[str]:
    """Validate date formats and return cleaned list."""
    valid = []
    for d in dates:
        try:
            datetime.strptime(d, "%Y-%m-%d")
            valid.append(d)
        except ValueError:
            log.warning(f"Skipping invalid date '{d}' — must be YYYY-MM-DD format.")
    if not valid:
        log.critical("No valid dates provided. Exiting.")
        sys.exit(1)
    return valid


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Delete analytics data from specific dates in Supabase.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument(
        "--dates",
        nargs="+",
        metavar="YYYY-MM-DD",
        help="One or more specific dates to delete (space-separated).",
    )
    date_group.add_argument(
        "--date-range",
        nargs=2,
        metavar=("START", "END"),
        help="Delete all dates from START to END inclusive (YYYY-MM-DD YYYY-MM-DD).",
    )

    parser.add_argument(
        "--signal-types",
        nargs="+",
        default=DEFAULT_SIGNAL_TYPES,
        metavar="TYPE",
        help=f"Signal type folders to target (default: {' '.join(DEFAULT_SIGNAL_TYPES)}).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be deleted without actually deleting anything.",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    # Resolve dates
    if args.dates:
        dates = validate_dates(args.dates)
    else:
        dates = expand_date_range(args.date_range[0], args.date_range[1])

    run(dates=dates, signal_types=args.signal_types, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
