#!/usr/bin/env python3
"""
main.py - FastAPI entrypoint for Render deployment.

It imports and runs:
1. The Telegram bot (from bot.py)
2. The analytics tracker (from analytics_tracker.py)
3. The snapshot collector (from collector.py)
...all in background tasks.

It also exposes HTTP endpoints for uptime pings and additional integrations
(merged in from api.py and collector.py).
"""

import logging
import asyncio
import aiohttp
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

# Import analytics tracker
import analytics_tracker

# Import collector
try:
    import collector
except ImportError:
    logging.getLogger("main").error("--- 'collector.py' not found. Collector service will not start. ---")
    collector = None # Set to None to handle gracefully

# ----------------------
# Logging Setup
# ----------------------
# Keep one logging configuration
LOGLEVEL = os.environ.get("LOGLEVEL", "DEBUG")
# Note: collector.py also sets up logging, which might add file handlers.
# This basicConfig will apply first.
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

# Global variables to store background tasks
bot_task = None
analytics_task = None
collector_task = None
collector_session = None # For collector's aiohttp session
collector_log = None # To store the collector's logger instance

# ----------------------
# Collector Service Runner
# ----------------------
async def run_collector_service():
    """Initializes and runs the CollectorService in a loop."""
    global collector_session, collector_log
    
    if not collector:
        logger.error("Collector module not loaded. Service cannot start.")
        return

    try:
        config = collector.Config()
        
        # collector.py's setup_logging() configures the root logger AND
        # returns its own logger. It also sets a global 'log' variable
        # within its own module, which its components rely on.
        try:
            # This sets collector.log and returns the logger instance
            collector.log = collector.setup_logging(config.LOG_LEVEL)
        except Exception as log_e:
            logger.warning(f"Could not configure collector's custom logging: {log_e}. It may use main's logging.")
            # Fallback: get the logger it *would* have used
            collector.log = logging.getLogger("CollectorService")

        collector_log = collector.log # Get a reference to it
        
        collector_log.info("Collector service starting...")
        
        async with aiohttp.ClientSession() as session:
            collector_session = session # Store for graceful shutdown
            service = collector.CollectorService(config, session)
            await service.run() # This is the infinite loop
            
    except asyncio.CancelledError:
        if collector_log:
            collector_log.info("Collector service loop cancelled.")
        else:
            logger.info("Collector service loop cancelled.")
    except Exception as e:
        logger.error(f"Collector service failed critically: {e}", exc_info=True)
    finally:
        if collector_session and not collector_session.closed:
            await collector_session.close()
            if collector_log:
                collector_log.info("Collector session closed.")
            else:
                logger.info("Collector session closed.")
        logger.info("Collector service shut down complete.")


# ----------------------
# Lifespan context manager for startup/shutdown
# ----------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage bot, analytics, and collector lifecycles with proper startup and shutdown."""
    global bot_task, analytics_task, collector_task

    # Startup
    logger.info("🚀 Starting Telegram bot...")
    bot_task = asyncio.create_task(bot.main())
    
    logger.info("🚀 Starting Analytics Tracker...")
    analytics_task = asyncio.create_task(analytics_tracker.main_loop())

    # --- NEW: Start Collector Service ---
    if collector:
        logger.info("🚀 Starting Snapshot Collector Service...")
        collector_task = asyncio.create_task(run_collector_service())
    else:
        logger.warning("Collector module not loaded, skipping collector service startup.")
    # ------------------------------------

    yield  # FastAPI runs here

    # Shutdown
    logger.info("🛑 Shutting down services...")
    
    # Shutdown bot
    if bot_task and not bot_task.done():
        logger.info("🛑 Shutting down bot...")
        bot_task.cancel()
        try:
            await bot_task
        except asyncio.CancelledError:
            logger.info("✅ Bot task cancelled successfully")
        except Exception as e:
            logger.error(f"Error during bot shutdown: {e}")
    
    # Shutdown analytics tracker
    if analytics_task and not analytics_task.done():
        logger.info("🛑 Shutting down analytics tracker...")
        analytics_task.cancel()
        try:
            await analytics_task
        except asyncio.CancelledError:
            logger.info("✅ Analytics tracker cancelled successfully")
        except Exception as e:
            logger.error(f"Error during analytics tracker shutdown: {e}")

    # --- NEW: Shutdown Collector Service ---
    if collector_task and not collector_task.done():
        logger.info("🛑 Shutting down collector service...")
        collector_task.cancel()
        try:
            await collector_task
        except asyncio.CancelledError:
            logger.info("✅ Collector task cancelled successfully")
        except Exception as e:
            logger.error(f"Error during collector shutdown: {e}")
    # Session cleanup is handled in run_collector_service's finally block
    # ---------------------------------------
    
    # Cleanup HTTP session from analytics tracker
    if analytics_tracker.http_session and not analytics_tracker.http_session.closed:
        await analytics_tracker.http_session.close()
        logger.info("✅ Analytics tracker HTTP session closed")


# ----------------------
# FastAPI app setup
# ----------------------
app = FastAPI(
    title="Solana Bot Service (Trader ROI API, Analytics & Collector)",
    version="1.1.0",
    lifespan=lifespan,
    description="Combined service running the Telegram bot, analytics tracker, ROI analysis, and snapshot collector."
)

# ----------------------
# Root / Health endpoints (unified)
# ----------------------
@app.api_route("/", methods=["GET", "HEAD"])
async def root():
    return {"message": "Solana Bot Service with Analytics & Collector is running!"}


@app.head("/health")
@app.get("/health")
async def health_check():
    """Endpoint for uptime monitoring tools like RobotPinger or UptimeRobot."""
    bot_status = "running" if bot_task and not bot_task.done() else "stopped"
    analytics_status = "running" if analytics_task and not analytics_task.done() else "stopped"
    
    collector_status = "not_loaded" # Default if collector is None
    if collector:
         collector_status = "running" if collector_task and not collector_task.done() else "stopped"
    
    # Basic job stats
    running_jobs = sum(1 for f in JOB_FUTURES.values() if not f.done()) if JOB_FUTURES else 0
    
    # Analytics tracker stats
    active_tokens = len(analytics_tracker.active_tracking) if hasattr(analytics_tracker, 'active_tracking') else 0
    
    return {
        "status": "ok",
        "service": "Solana Bot + Trader ROI API + Analytics + Collector",
        "bot_status": bot_status,
        "analytics_status": analytics_status,
        "collector_status": collector_status,
        "running_jobs": running_jobs,
        "active_tracking_tokens": active_tokens,
        "details": "All services running smoothly"
    }


@app.get("/analytics/status")
async def analytics_status():
    """Get detailed status of the analytics tracker."""
    try:
        active_tokens = analytics_tracker.active_tracking if hasattr(analytics_tracker, 'active_tracking') else {}
        
        # Group by signal type
        discovery_count = sum(1 for t in active_tokens.values() if t.get("signal_type") == "discovery")
        alpha_count = sum(1 for t in active_tokens.values() if t.get("signal_type") == "alpha")
        
        # Count by status
        active_count = sum(1 for t in active_tokens.values() if t.get("status") == "active")
        win_count = sum(1 for t in active_tokens.values() if t.get("status") == "win")
        
        return {
            "status": "running" if analytics_task and not analytics_task.done() else "stopped",
            "total_active_tokens": len(active_tokens),
            "by_signal_type": {
                "discovery": discovery_count,
                "alpha": alpha_count
            },
            "by_status": {
                "active": active_count,
                "wins": win_count
            },
            "tokens": list(active_tokens.keys())[:10]  # Show first 10 tokens
        }
    except Exception as e:
        logger.error(f"Error getting analytics status: {e}")
        return {"status": "error", "message": str(e)}


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
@app.post("/analyze", tags=["Trader Analysis"])
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


@app.get("/job/{job_id}", tags=["Trader Analysis"])
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
# NEW: Collector API Endpoints
# ----------------------

@app.post("/collector/test-apis", tags=["Collector"])
async def run_collector_api_tests():
    """
    Triggers the collector's 'test-apis' command from its main() function.
    This runs connectivity tests for Supabase, Dexscreener, RugCheck, and HolidayAPI.
    Results are printed to the server logs.
    """
    if not collector:
        raise HTTPException(status_code=503, detail="Collector module is not loaded.")
    
    logger.info("--- Triggering Collector API tests via endpoint ---")
    
    try:
        config = collector.Config()
        
        # Ensure the collector's logger is available for the test run
        # (It's normally set in the run_collector_service task)
        if not hasattr(collector, 'log') or not collector.log:
             collector.log = collector.setup_logging(config.LOG_LEVEL)

        # run_tests is an async function that needs a session
        async with aiohttp.ClientSession() as session:
            await collector.run_tests(config, session)
        
        logger.info("--- Collector API tests complete ---")
        return {"status": "ok", "message": "Collector API tests triggered. Check server logs for results."}
    
    except Exception as e:
        logger.error(f"Failed to run collector API tests: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to run tests: {str(e)}")


# ----------------------
# Run locally (Render will use Procfile instead)
# ----------------------
if __name__ == "__main__":
    # Use uvicorn to run the app; keep reload for local dev
    port = int(os.environ.get("PORT", 8000))
    logger.info(f"Starting server locally on http://0.0.0.0:{port}")
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)