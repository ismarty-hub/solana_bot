#!/usr/bin/env python3
"""
Diagnostic script to check dataset folders and compare with active_tracking.json
"""
import os
import json
import asyncio
from supabase import create_client
from dotenv import load_dotenv
from collections import defaultdict

load_dotenv()

async def analyze_datasets():
    """Analyze dataset folders and compare with active tracking."""
    
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")
    SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET", "monitor-data")
    
    client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # 1. Load active_tracking.json
    print("Loading active_tracking.json...")
    try:
        file_bytes = await asyncio.to_thread(
            client.storage.from_(SUPABASE_BUCKET).download,
            "analytics/active_tracking.json"
        )
        active_tracking = json.loads(file_bytes)
        
        win_tokens = {k: v for k, v in active_tracking.items() if v.get('status') == 'win'}
        print(f"Found {len(win_tokens)} tokens with 'win' status in active_tracking.json")
        
        # Sample first 5 wins
        print("\nSample win tokens:")
        for i, (key, data) in enumerate(list(win_tokens.items())[:5]):
            mint = data.get('mint', 'unknown')
            signal_type = data.get('signal_type', 'unknown')
            print(f"  {i+1}. {key} -> mint: {mint}, signal: {signal_type}")
        
    except Exception as e:
        print(f"Error loading active_tracking.json: {e}")
        win_tokens = {}
    
    # 2. Check dataset folders
    print("\n" + "="*60)
    print("CHECKING DATASET FOLDERS:")
    print("="*60)
    
    pipelines = ['discovery', 'alpha']
    dataset_counts = defaultdict(int)
    
    for pipeline in pipelines:
        print(f"\nPipeline: {pipeline}")
        print("-" * 40)
        
        # Check daily folders (labeled datasets)
        daily_path = f"datasets/{pipeline}"
        try:
            folders = await asyncio.to_thread(
                client.storage.from_(SUPABASE_BUCKET).list,
                daily_path,
                {"limit": 100}
            )
            
            # Folders are the date strings
            date_folders = [f['name'] for f in folders if f.get('name') and f['name'] != 'expired_no_label']
            
            print(f"  Daily folders: {len(date_folders)}")
            if date_folders:
                print(f"  Date range: {min(date_folders)} to {max(date_folders)}")
            
            # Count files in each date folder
            total_labeled = 0
            for date_folder in date_folders:
                try:
                    files = await asyncio.to_thread(
                        client.storage.from_(SUPABASE_BUCKET).list,
                        f"{daily_path}/{date_folder}",
                        {"limit": 1000}
                    )
                    json_files = [f for f in files if f.get('name', '').endswith('.json')]
                    count = len(json_files)
                    total_labeled += count
                    print(f"    {date_folder}: {count} datasets")
                except Exception as e:
                    print(f"    {date_folder}: Error - {e}")
            
            dataset_counts[f"{pipeline}_labeled"] = total_labeled
            
        except Exception as e:
            print(f"  Error accessing daily folders: {e}")
        
        # Check expired folder
        expired_path = f"datasets/{pipeline}/expired_no_label"
        try:
            files = await asyncio.to_thread(
                client.storage.from_(SUPABASE_BUCKET).list,
                expired_path,
                {"limit": 1000}
            )
            json_files = [f for f in files if f.get('name', '').endswith('.json')]
            count = len(json_files)
            dataset_counts[f"{pipeline}_expired"] = count
            print(f"  Expired (no label): {count} datasets")
        except Exception as e:
            print(f"  Error accessing expired folder: {e}")
    
    # 3. Summary
    print("\n" + "="*60)
    print("SUMMARY:")
    print("="*60)
    print(f"Active wins in tracking: {len(win_tokens)}")
    print(f"Total labeled datasets: {dataset_counts['discovery_labeled'] + dataset_counts['alpha_labeled']}")
    print(f"  - Discovery pipeline: {dataset_counts['discovery_labeled']}")
    print(f"  - Alpha pipeline: {dataset_counts['alpha_labeled']}")
    print(f"Total expired datasets: {dataset_counts['discovery_expired'] + dataset_counts['alpha_expired']}")
    print(f"  - Discovery pipeline: {dataset_counts['discovery_expired']}")
    print(f"  - Alpha pipeline: {dataset_counts['alpha_expired']}")
    
    # 4. Cross-check: Are the win tokens in datasets?
    print("\n" + "="*60)
    print("CHECKING IF WIN TOKENS ARE IN DATASETS:")
    print("="*60)
    
    if win_tokens:
        print("Checking first 5 win tokens...")
        for i, (key, data) in enumerate(list(win_tokens.items())[:5]):
            mint = data.get('mint', 'unknown')
            signal_type = data.get('signal_type', 'unknown')
            
            # Try to find this token in datasets
            found = False
            for pipeline in pipelines:
                daily_path = f"datasets/{pipeline}"
                try:
                    folders = await asyncio.to_thread(
                        client.storage.from_(SUPABASE_BUCKET).list,
                        daily_path,
                        {"limit": 100}
                    )
                    
                    for folder in folders:
                        folder_name = folder.get('name', '')
                        if folder_name == 'expired_no_label':
                            continue
                        
                        # List files in this folder
                        files = await asyncio.to_thread(
                            client.storage.from_(SUPABASE_BUCKET).list,
                            f"{daily_path}/{folder_name}",
                            {"limit": 1000}
                        )
                        
                        # Check if any file starts with the mint
                        for file in files:
                            if file.get('name', '').startswith(mint):
                                print(f"  ✅ {key} -> Found in {pipeline}/{folder_name}")
                                found = True
                                break
                        
                        if found:
                            break
                    
                    if found:
                        break
                        
                except Exception as e:
                    pass
            
            if not found:
                print(f"  ❌ {key} -> NOT FOUND in any dataset folder")

if __name__ == "__main__":
    asyncio.run(analyze_datasets())