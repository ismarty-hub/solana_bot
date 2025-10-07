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
    def run_bot():
        """Runs the existing bot.py main() function (blocking call)."""
        try:
            logging.info("🚀 Starting Telegram bot thread...")
            bot.main()
        except Exception as e:
            logging.error(f"Bot crashed: {e}")

    # Start bot in background thread so FastAPI remains responsive
    threading.Thread(target=run_bot, daemon=True).start()

    # Example placeholder for your other script
    # threading.Thread(target=my_other_script.run, daemon=True).start()

    logging.info("✅ Background services (bot + others) started successfully.")


@app.get("/")
async def root():
    return {"message": "Solana Bot Service is running!"}


@app.get("/health")
async def health_check():
    """Endpoint for uptime monitoring tools like RobotPinger or UptimeRobot."""
    return {"status": "ok", "service": "Solana Bot", "details": "Bot and API running smoothly"}


# ----------------------
# Run locally (Render will use Procfile instead)
# ----------------------
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000)
