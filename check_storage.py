import os
import asyncio
import aiohttp
from collector import Config, SupabaseManager

async def check_storage():
    config = Config()
    async with aiohttp.ClientSession() as session:
        supa = SupabaseManager(config, session)
        
        folders = ["datasets", "analytics", "analytics/discovery", "analytics/alpha", "analytics/snapshots"]
        
        for folder in folders:
            print(f"\n--- Listing {folder} ---")
            files = await supa.list_files(folder)
            if not files:
                print("Empty")
                continue
            
            for f in files:
                name = f.get('name')
                created = f.get('created_at', 'N/A')
                size = f.get('metadata', {}).get('size', 0)
                print(f"- {name} ({size} bytes, created: {created})")

if __name__ == "__main__":
    asyncio.run(check_storage())
