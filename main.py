#!/usr/bin/env python3
"""
main.py - FastAPI entrypoint for Render deployment.
It imports and runs the Telegram bot (from bot.py) in a background thread,
and exposes HTTP endpoints for uptime pings and additional integrations.
"""

import logging
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
import uvicorn

# Import the bot module
import bot

# ----------------------
# Logging Setup
# ----------------------
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

# Global variable to store the bot task
bot_task = None


# ----------------------
# Lifespan context manager for startup/shutdown
# ----------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage bot lifecycle with proper startup and shutdown."""
    global bot_task
    
    # Startup
    logging.info("🚀 Starting Telegram bot...")
    bot_task = asyncio.create_task(bot.main())
    
    yield  # FastAPI runs here
    
    # Shutdown
    logging.info("🛑 Shutting down Telegram bot...")
    if bot_task and not bot_task.done():
        bot_task.cancel()
        try:
            await bot_task
        except asyncio.CancelledError:
            logging.info("✅ Bot task cancelled successfully")
        except Exception as e:
            logging.error(f"Error during bot shutdown: {e}")


# ----------------------
# FastAPI app setup
# ----------------------
app = FastAPI(
    title="Solana Bot Service",
    version="1.0.0",
    lifespan=lifespan
)


@app.get("/", methods=["GET", "HEAD"])
async def root():
    return {"message": "Solana Bot Service is running!"}


@app.head("/health")
@app.get("/health")
async def health_check():
    """Endpoint for uptime monitoring tools like RobotPinger or UptimeRobot."""
    bot_status = "running" if bot_task and not bot_task.done() else "stopped"
    return {
        "status": "ok",
        "service": "Solana Bot",
        "bot_status": bot_status,
        "details": "Bot and API running smoothly"
    }


# ----------------------
# Run locally (Render will use Procfile instead)
# ----------------------
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)