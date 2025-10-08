#!/usr/bin/env python3
"""
main.py - FastAPI entrypoint for Render deployment.
It imports and runs the Telegram bot (from bot.py) in a background thread,
and exposes HTTP endpoints for uptime pings and additional integrations
(merged in from api.py).
"""

import logging
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
import uvicorn

# --- Additional imports from api.py ---
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import uuid
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from datetime import datetime
import json
import joblib
from supabase import create_client, Client
# Import the pipeline function (same as api.py)
from alpha import run_pipeline

# Import the bot module
import bot

# ----------------------
# Logging Setup
# ----------------------
# Keep one logging configuration
LOGLEVEL = os.environ.get("LOGLEVEL", "DEBUG")
logging.basicConfig(level=LOGLEVEL, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("main")

# ----------------------
# Job / executor setup (from api.py)
# ----------------------
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
BUCKET_NAME = "monitor-data"
JOBS_FOLDER = "jobs"

MAX_WORKERS = int(os.environ.get("API_MAX_WORKERS", "2"))
EXECUTOR = ThreadPoolExecutor(max_workers=MAX_WORKERS)
JOB_STATUS_DIR = Path("job_status")
JOB_STATUS_DIR.mkdir(exist_ok=True)

# In-memory map of job futures
JOB_FUTURES: Dict[str, Any] = {}

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
    logger.info("🚀 Starting Telegram bot...")
    # run bot.main() in background; it's expected to be async
    bot_task = asyncio.create_task(bot.main())

    yield  # FastAPI runs here

    # Shutdown
    logger.info("🛑 Shutting down Telegram bot...")
    if bot_task and not bot_task.done():
        bot_task.cancel()
        try:
            await bot_task
        except asyncio.CancelledError:
            logger.info("✅ Bot task cancelled successfully")
        except Exception as e:
            logger.error(f"Error during bot shutdown: {e}")


# ----------------------
# FastAPI app setup
# ----------------------
app = FastAPI(
    title="Solana Bot Service (with Trader ROI API)",
    version="1.0.0",
    lifespan=lifespan,
    description="Combined service running the Telegram bot and ROI analysis endpoints."
)

# ----------------------
# Root / Health endpoints (unified)
# ----------------------
@app.api_route("/", methods=["GET", "HEAD"])
async def root():
    return {"message": "Solana Bot Service is running!"}


@app.head("/health")
@app.get("/health")
async def health_check():
    """Endpoint for uptime monitoring tools like RobotPinger or UptimeRobot."""
    bot_status = "running" if bot_task and not bot_task.done() else "stopped"
    # Basic job stats
    running_jobs = sum(1 for f in JOB_FUTURES.values() if not f.done()) if JOB_FUTURES else 0
    return {
        "status": "ok",
        "service": "Solana Bot + Trader ROI API",
        "bot_status": bot_status,
        "running_jobs": running_jobs,
        "details": "Bot and API running smoothly"
    }

# ----------------------
# Supabase upload helper (from api.py)
# ----------------------
def upload_to_supabase(job_id: str, data: Any, suffix: str = "pkl"):
    """Upload data to Supabase storage in monitor-data/jobs folder"""
    try:
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise ValueError("Supabase credentials not configured")

        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

        # Create a temporary file
        temp_file = f"temp_{job_id}.{suffix}"
        joblib.dump(data, temp_file)

        # Upload to Supabase
        with open(temp_file, "rb") as f:
            file_path = f"{JOBS_FOLDER}/{job_id}.{suffix}"
            supabase.storage.from_(BUCKET_NAME).upload(
                path=file_path,
                file=f,
                file_options={"content-type": "application/octet-stream"}
            )

        # Clean up temp file
        os.remove(temp_file)
        logger.info(f"Uploaded {suffix} file for job {job_id} to Supabase")

    except Exception as e:
        logger.error(f"Failed to upload to Supabase: {e}")
        raise

# ----------------------
# Request model (from api.py)
# ----------------------
class AnalysisRequest(BaseModel):
    tokens: List[str]
    min_buy: Optional[float] = 100.0
    min_num_tokens_in_profit: Optional[int] = 1
    window: Optional[int] = None
    trader_type: Optional[str] = "all"

# ----------------------
# API endpoints (from api.py)
# ----------------------
@app.post("/analyze")
def analyze(req: AnalysisRequest):
    """Starts run_pipeline in a background thread and returns a jobId immediately."""
    try:
        job_id = uuid.uuid4().hex

        # Save initial job status to disk
        status_file = JOB_STATUS_DIR / f"{job_id}.json"
        status_data = {
            "jobId": job_id,
            "status": "running",
            "startedAt": datetime.now().isoformat(),
            "tokens": req.tokens
        }
        with open(status_file, "w") as f:
            json.dump(status_data, f)

        # Submit the long-running pipeline to background thread
        future = EXECUTOR.submit(
            run_pipeline,
            tokens=req.tokens,
            early_trading_window_hours=req.window if (req.trader_type == "early" and req.window) else None,
            minimum_initial_buy_usd=req.min_buy,
            min_profitable_trades=req.min_num_tokens_in_profit,
            job_id=job_id
        )

        # Store future in memory
        JOB_FUTURES[job_id] = future

        # Callback to update status when complete
        def _on_complete(fut):
            status_file = JOB_STATUS_DIR / f"{job_id}.json"
            try:
                result = fut.result()
                status = "done"
                error = None
                supabase_path = None

                # Upload result to Supabase if successful
                if result is not None:
                    try:
                        upload_to_supabase(job_id, result)
                        supabase_path = f"{BUCKET_NAME}/{JOBS_FOLDER}/{job_id}.pkl"
                    except Exception as e:
                        logger.exception(f"Failed to upload job {job_id} result to Supabase: {e}")
                        # keep supabase_path = None

            except Exception as e:
                logger.exception(f"Job {job_id} failed")
                status = "failed"
                error = str(e)
                supabase_path = None

            # Update status file
            status_data = {
                "jobId": job_id,
                "status": status,
                "error": error,
                "completedAt": datetime.now().isoformat(),
                "supabasePath": supabase_path
            }
            try:
                with open(status_file, "w") as f:
                    json.dump(status_data, f)
            except Exception:
                logger.exception(f"Failed to write status file for job {job_id}")

        future.add_done_callback(_on_complete)

        logger.info(f"Submitted job {job_id} for tokens={req.tokens}")
        return {"jobId": job_id}

    except Exception as e:
        logger.exception("Failed to submit job")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/job/{job_id}")
def job_status(job_id: str):
    """Check job status and include Supabase storage path if complete"""
    logger.info(f"Checking status for job {job_id}")

    # Check status file first (most reliable source)
    status_file = JOB_STATUS_DIR / f"{job_id}.json"
    if status_file.exists():
        try:
            with open(status_file) as f:
                status_data = json.load(f)
            return status_data
        except Exception as e:
            logger.error(f"Error reading status file: {e}")

    # Check in-memory future
    fut = JOB_FUTURES.get(job_id)
    if fut is not None:
        if fut.running():
            return {"jobId": job_id, "status": "running"}
        elif fut.done():
            try:
                result = fut.result()
                return {
                    "jobId": job_id,
                    "status": "done",
                    "supabasePath": f"{BUCKET_NAME}/{JOBS_FOLDER}/{job_id}.pkl"
                }
            except Exception as e:
                logger.exception(f"Error getting result for job {job_id}")
                return {"jobId": job_id, "status": "failed", "error": str(e)}

    # Job ID not found
    return {
        "jobId": job_id,
        "status": "unknown",
        "message": "Job not found. It may have been completed in a previous server session."
    }

# ----------------------
# Run locally (Render will use Procfile instead)
# ----------------------
if __name__ == "__main__":
    # Use uvicorn to run the app; keep reload for local dev
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), reload=True)
