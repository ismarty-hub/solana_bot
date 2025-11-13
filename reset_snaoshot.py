#!/usr/bin/env python3
"""
Script to reset finalization status of ALL snapshots
This allows the aggregator to re-evaluate them properly
"""
import os
import json
import asyncio
from datetime import datetime, timezone
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

async def reset_all_snapshots():
    """Reset ALL snapshots back to processable state."""
    
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")
    SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET", "monitor-data")
    SNAPSHOT_DIR = "analytics/snapshots"
    
    client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # 1. List all snapshots
    print("Fetching snapshot file list...")
    files = await asyncio.to_thread(
        client.storage.from_(SUPABASE_BUCKET).list,
        SNAPSHOT_DIR,
        {"limit": 2000}
    )
    
    json_files = [f for f in files if f.get('name', '').endswith('.json')]
    print(f"Found {len(json_files)} snapshot files")
    
    # 2. Process each snapshot
    print("\nProcessing snapshots...")
    reset_count = 0
    skipped_count = 0
    error_count = 0
    
    status_before = {}
    
    for i, file_info in enumerate(json_files):
        filename = file_info['name']
        remote_path = f"{SNAPSHOT_DIR}/{filename}"
        
        if (i + 1) % 50 == 0:
            print(f"  Processed {i + 1}/{len(json_files)} files...")
        
        try:
            # Download snapshot
            file_bytes = await asyncio.to_thread(
                client.storage.from_(SUPABASE_BUCKET).download,
                remote_path
            )
            snapshot = json.loads(file_bytes)
            
            # Check current status
            current_status = snapshot.get('finalization', {}).get('finalization_status', 'unknown')
            status_before[current_status] = status_before.get(current_status, 0) + 1
            
            # Only reset if it's marked as finalized (don't touch pending/awaiting_label)
            if current_status not in ('labeled', 'expired_no_label'):
                skipped_count += 1
                continue
            
            # Extract info for logging
            mint = snapshot.get('features', {}).get('mint', 'unknown')
            signal_type = snapshot.get('features', {}).get('signal_source', 'unknown')
            
            # Reset the snapshot
            if reset_count < 5:  # Log first 5 resets
                print(f"\n  Resetting: {filename}")
                print(f"    Mint: {mint}")
                print(f"    Signal: {signal_type}")
                print(f"    Old status: {current_status}")
            
            # Update finalization metadata
            snapshot['finalization']['finalization_status'] = 'pending'
            snapshot['finalization']['next_check_at'] = datetime.now(timezone.utc).isoformat()
            snapshot['finalization']['check_count'] = 0
            snapshot['finalization']['claimed_by'] = None
            snapshot['finalization']['claimed_at'] = None
            snapshot['finalization']['finalized_at'] = None
            
            # Save to temp file
            temp_path = f"./temp_{filename}"
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(snapshot, f, indent=2, default=str)
            
            # Delete old file
            await asyncio.to_thread(
                client.storage.from_(SUPABASE_BUCKET).remove,
                [remote_path]
            )
            
            # Upload new file
            await asyncio.to_thread(
                client.storage.from_(SUPABASE_BUCKET).upload,
                remote_path,
                temp_path,
                {"content-type": "application/json", "upsert": "true"}
            )
            
            # Cleanup temp file
            os.remove(temp_path)
            
            reset_count += 1
            
            if reset_count < 5:
                print(f"    ✅ Reset successfully")
            
        except Exception as e:
            error_count += 1
            if error_count < 5:  # Log first 5 errors
                print(f"  ❌ Error processing {filename}: {e}")
    
    # 3. Summary
    print("\n" + "="*60)
    print("RESET SUMMARY:")
    print("="*60)
    print(f"Total snapshots checked: {len(json_files)}")
    print(f"\nStatus breakdown BEFORE reset:")
    for status, count in sorted(status_before.items()):
        print(f"  {status}: {count}")
    print(f"\nSnapshots reset: {reset_count}")
    print(f"Snapshots skipped (already pending/awaiting): {skipped_count}")
    print(f"Errors: {error_count}")
    
    print("\n" + "="*60)
    print("NEXT STEPS:")
    print("="*60)
    print("1. Run: python collector.py")
    print("2. The aggregator will now re-evaluate all {0} reset snapshots".format(reset_count))
    print("3. Snapshots will be:")
    print("   - Matched with labels from active_tracking.json and daily files")
    print("   - Moved to dataset folders if labels exist")
    print("   - Rescheduled if labels don't exist yet")
    print("   - Marked as expired if deadline has passed with no label")

if __name__ == "__main__":
    print("="*60)
    print("SNAPSHOT STATUS RESET SCRIPT")
    print("="*60)
    print("\nThis script will:")
    print("1. Find ALL snapshots with 'labeled' or 'expired_no_label' status")
    print("2. Reset their finalization_status to 'pending'")
    print("3. Set next_check_at to NOW for immediate processing")
    print("4. Clear all finalization metadata")
    print("\nThe aggregator will then:")
    print("- Match snapshots with labels (from active_tracking.json and daily files)")
    print("- Create datasets for labeled tokens")
    print("- Re-expire tokens that still have no labels past deadline")
    print("\n⚠️  WARNING: This will modify ALL finalized snapshot files in Supabase")
    print("⚠️  Snapshots that were already correctly moved to datasets will be re-evaluated")
    print("     (This is safe - duplicates will be detected and skipped)")
    
    response = input("\nContinue? (yes/no): ")
    if response.lower() != 'yes':
        print("Aborted.")
        exit(0)
    
    asyncio.run(reset_all_snapshots())