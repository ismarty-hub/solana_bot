#!/usr/bin/env python3
"""
main.py - FastAPI entrypoint for Render deployment.
It imports and runs the Telegram bot (from bot.py) in a background thread,
and exposes HTTP endpoints for uptime pings and additional integrations.
"""

import logging
import threading
import asyncio
from fastapi import FastAPI
import uvicorn

# Import the bot module (we'll call bot.main() inside a thread)
import bot

# If you have another script (e.g. metrics.py, worker.py), import it here:
# import my_other_script


# ----------------------
# Logging Setup
# ----------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# ----------------------
# FastAPI app setup
# ----------------------
app = FastAPI(title="Solana Bot Service", version="1.0.0")


@app.on_event("startup")
async def startup_event():
    """Start Telegram bot (and optionally other scripts) when the FastAPI app starts."""
    async def run_bot_async():
        """Run the bot in an executor (non-blocking, async-safe)."""
        try:
            logging.info("🚀 Starting Telegram bot (async executor)...")
            loop = asyncio.get_event_loop()
            # run bot.main() (a blocking call) in a separate thread safely
            await loop.run_in_executor(None, bot.main)
        except Exception as e:
            logging.error(f"Bot crashed: {e}")

    # Schedule it inside FastAPI’s event loop
    asyncio.create_task(run_bot_async())

    # You can add other scripts here too
    # asyncio.create_task(loop.run_in_executor(None, my_other_script.run))

    logging.info("✅ Background services (bot + others) started successfully.")


@app.get("/")
async def root():
    return {"message": "Solana Bot Service is running!"}

@app.head("/health")
@app.get("/health")
async def health_check():
    """Endpoint for uptime monitoring tools like RobotPinger or UptimeRobot."""
    return {"status": "ok", "service": "Solana Bot", "details": "Bot and API running smoothly"}


# ----------------------
# Run locally (Render will use Procfile instead)
# ----------------------
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000)
