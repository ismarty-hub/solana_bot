# 🚀 Solana Analytics & Trading Bot

A professional, modular, and analytics-driven Telegram bot for monitoring Solana tokens, executing paper trades, and leveraging Machine Learning for price predictions.

![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![Solana](https://img.shields.io/badge/Solana-DeFi-green)
![Supabase](https://img.shields.io/badge/Supabase-Database-orange)
![Telegram](https://img.shields.io/badge/Telegram-Bot-blue)

## 📖 Overview

This bot provides real-time analytics, alerts, and paper trading capabilities for Solana tokens. It monitors blockchain signals, integrates with Supabase for data persistence, and uses a Machine Learning model to predict token movements. Designed for both individual traders and communities, it supports detailed portfolio tracking and configurable alerts.

## ✨ Key Features

### 🔍 Analytics & Signals
- **Discovery Alerts**: Real-time notifications for new token discoveries based on on-chain data.
- **Alpha Alerts**: High-quality "Alpha" signals for promising tokens.
- **ML Predictions**: Integration with a FastAPI ML service to predict `BUY`, `CONSIDER`, or `AVOID` actions.
- **Grade Filtering**: Filter alerts by grades (`CRITICAL`, `HIGH`, `MEDIUM`, `LOW`).

### 📈 Paper Trading Engine
- **Virtual Portfolio**: Complete paper trading system with tracking for Capital, PnL, and ROI.
- **Auto-Trading**: Configurable auto-trade settings based on signal types and ML confidence.
- **Trade Management**:
  - `TP/SL` (Take Profit / Stop Loss) management with dynamic targets (Mean/Median/Mode ATH).
  - Manual Close / Close All positions.
  - Periodic monitoring of active positions.

### 🛠 Architecture & Tech Stack
- **Modular Design**: Separated concerns (alerts, trading, monitoring, analytics).
- **Supabase Integration**: Centralized storage for tracking data, user stats, and shared analytics.
- **Persistent State**: Local caching with cloud sync for robustness.
- **AsyncIO**: Fully asynchronous core for high-performance monitoring.

## 🧠 Detailed Logic & Workflows

### 1. Signal Detection Pipeline (`analytics_monitoring.py`)
The bot employs a sophisticated multi-stage pipeline to filter noise and identify high-quality tokens:

1.  **Pre-Filtering**:
    *   **Startup Check**: Ignores tokens that existed before the bot started (prevents stale trades).
    *   **User Activation**: Ignores tokens generated before a user specifically enabled auto-trading.
    *   **Freshness Check**: Discards signals older than `5 minutes` to avoid late entries.

2.  **Grading & ML Integration**:
    *   **Primary Source**: Attempts to read the "Grade" (CRITICAL/HIGH/MEDIUM/LOW) from the `overlap_results` analysis.
    *   **ML Fallback**: If the primary grade is unavailable, it uses the **ML Prediction** to determine quality:
        *   `BUY` Prediction ➔ **HIGH** Grade
        *   `CONSIDER` Prediction ➔ **MEDIUM** Grade
        *   `AVOID/SKIP` ➔ **LOW** Grade

3.  **User Routing**:
    *   Checks if the user has subscribed to the assigned Grade.
    *   Checks if the User has sufficient capital reserves.

### 2. Trade Execution Engine (`trade_manager.py`)
Once a signal is approved, the Portfolio Manager executes the trade virtually:

1.  **Capital Validation**:
    *   Ensures `Available Balance > Min Trade Size`.
    *   Ensures `Available Balance > User Reserve Setting`.

2.  **Position Sizing**:
    *   **Fixed Mode**: Uses a static USD amount per trade (e.g., $50).
    *   **Percent Mode**: Uses a percentage of *Available Capital* (e.g., 10% of $1000 = $100).
    *   **Caps**: Enforces a hard cap (default $150) to prevent over-exposure on one token.

3.  **Exit Strategy (Managed by `trade_monitor.py`)**:
    *   **Dynamic Take Profit (TP)**: Targets can be set to Fixed % (e.g., 50%) or Dynamic stats (Median/Mean/Mode ATH of past winners).
    *   **Stop Loss (SL)**: Triggers if price drops below user preference.
    *   **Time-Based Expiry**: Automatically closes positions after a set duration (Default: 24h for Discovery, 7d for Alpha) if no targets are hit.

| File | Description |
|------|-------------|
| **`bot.py`** | **Main Entry Point**. Initializes the bot, commands, background loops, and database syncing. |
| **`config.py`** | **Configuration Hub**. Manages Env vars, file paths, alert settings, and logic for Prod/Local modes. |
| **`main.py`** | Alternate entry point for certain deployment configurations (often wraps `bot.py`). |
| **`requirements.txt`** | List of Python dependencies (Telegram, Supabase, Pandas, etc). |

### 🚨 Alerts & Monitoring (`alerts/`)

| File | Description |
|------|-------------|
| **`analytics_monitoring.py`** | **Core Detection Loop**. Scans for new "Discovery" signals and integrates ML predictions. |
| **`alpha_monitoring.py`** | **Alpha Loop**. Scans for high-quality "Alpha" signals separately. |
| **`trade_monitor.py`** | **Trade Watchdog**. Monitors active paper trades for TP/SL hits and tracking expiry. |
| **`commands.py`** | **User Command Handlers**. Logic for `/papertrade`, `/portfolio`, `/stats`, and alert management. |
| **`admin_commands.py`** | **Admin Tools**. Logic for `/admin`, `/broadcast`, and user management. |
| **`user_manager.py`** | **User State**. Manages user preferences, subscriptions, and stats. |
| **`price_fetcher.py`** | **Price Logic**. Robust fetching from Jupiter API with Dexscreener fallbacks. |

### 📊 Trading & Analytics

| File | Description |
|------|-------------|
| **`trade_manager.py`** | **Portfolio Engine**. Manages virtual capital, executes trades, calculates PnL, and tracks history. |
| **`analytics_tracker.py`** | **Data Persistence**. Handles downloading/uploading daily analytics files and tracking token lifecycle. |
| **`collector.py`** | **Data Collection**. Scripts for gathering historical data or initial datasets. |
| **`clean_tokens.py`** | **Maintenance**. Utility to remove specific tokens from all historical records and recalculate stats. |

### 🔧 Utilities & maintenance

| File | Description |
|------|-------------|
| **`supabase_utils.py`** | **Database**. Helper functions for uploading/downloading files to Supabase Storage. |
| **`shared/file_io.py`** | **IO Helpers**. Safe file loading/saving with error handling. |
| **`diagnostic.py`** | **Health Check**. Script to check system status and data integrity. |
| **`repair_analytics.py`** | **Repair Tool**. Utility to fix corrupted analytics files or re-sync data. |

### 📁 Data Directory (`data/`)
*Local cache of persistent data (synced with Supabase)*
- `active_tracking.json`: Currently tracked tokens.
- `bot_user_prefs.pkl`: User settings.
- `bot_portfolios.pkl`: Paper trading portfolios.
- `overlap_results.pkl`: Cached signal results.

## ⚙️ Configuration & Environment

The bot is configured via environment variables (loaded from `.env` in local dev).

| Variable | Required | Description |
|----------|----------|-------------|
| `BOT_TOKEN` | **Yes** | Telegram Bot Token from @BotFather. |
| `ADMIN_USER_ID` | No | Comma-separated list of Telegram User IDs for admin access. |
| `SUPABASE_URL` | **Yes** | URL of your Supabase project. |
| `SUPABASE_KEY` | **Yes** | Service_role or anon key for Supabase storage access. |
| `ML_API_URL` | No | URL of the external ML Prediction API (e.g. `http://localhost:8000`). |
| `ML_API_TIMEOUT` | No | Timeout in seconds for ML requests (Default: 30). |
| `RENDER` | No | Set to `true` if deploying on Render (adjusts logging/paths). |

## 🏗️ Data Architecture

The system uses a **Hybrid State Model** to ensure reliability and persistence:

1.  **Source of Truth (Cloud)**: Supabase Storage.
    *   `active_tracking.json`: The master list of all currently tracked tokens.
    *   `bot_user_prefs.pkl`: User settings (alert grades, capital).
    *   `bot_portfolios.pkl`: All user paper-trading accounts.
    *   `overlap_results.pkl`: The latest batch of signals to process.

2.  **Local Cache (Runtime)**: `data/` directory.
    *   On startup, the bot **Downloads** all state files from Supabase.
    *   During operation, it reads/writes to local files for speed.
    *   **Background Sync**: Periodically (and on critical events), local state is **Uploaded** back to Supabase.

3.  **Concurrency**:
    *   `analytics_tracker.py` (if running separately) and `bot.py` both access Supabase.
    *   **Note**: To clean tokens manually, use `clean_tokens.py` but **stop the bot** first to prevent race conditions.

## 🚀 Getting Started

### Prerequisites
- Python 3.10+
- A Telegram Bot Token (from @BotFather)
- Supabase Project credentials

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd solana_bot
   ```

2. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure Environment**
   Create a `.env` file in the root directory:
   ```env
   BOT_TOKEN=your_telegram_bot_token
   ADMIN_USER_ID=12345678,87654321
   
   # Supabase (Required)
   SUPABASE_URL=your_supabase_url
   SUPABASE_KEY=your_supabase_key
   SUPABASE_BUCKET=monitor-data
   
   # ML Service (Optional)
   ML_API_URL=http://localhost:8000
   ML_API_TIMEOUT=30
   
   # Deployment
   DATA_DIR=./data
   ```

4. **Run the Bot**
   ```bash
   python bot.py
   ```

## 🎮 User Commands

| Category | Command | Description |
|----------|---------|-------------|
| **General** | `/start` | Initialize and configure the bot |
| | `/help` | Show all available commands |
| | `/stats` | View personal usage statistics |
| **Alerts** | `/myalerts` | specific alert settings |
| | `/setalerts` | Configure which grades to receive |
| | `/stop` | Pause all alerts |
| **Trading** | `/papertrade <amount>` | Enable paper trading with initial capital |
| | `/portfolio` | View current positions and balance |
| | `/pnl` | Check unrealized Profit & Loss |
| | `/history` | View trade history |
| | `/closeposition <mint>` | Manually close a specific position |
| | `/closeall` | Liquidate all open positions |
| **Alpha** | `/alpha_subscribe` | Subscribe to Alpha specific signals |
| **ML** | `/predict <token>` | Get ML-based price prediction |

## 🛠 Admin Commands

| Command | Description |
|---------|-------------|
| `/admin` | View system-wide statistics |
| `/broadcast <msg>` | Send a message to all users |
| `/adduser <id>` | Manually authorize a user |
| `/forcedownload` | Force sync data from Supabase |
| `/debugsystem` | View internal system health |

## ☁️ Deployment

This project is optimized for deployment on **Render** or **Heroku**.

- **Config**: Uses `config.py` to detect environment (`IS_RENDER`).
- **Data Persistence**: Configured to use `/opt/render/project/data` for persistent storage on Render.
- **Supabase Sync**: Automatically syncs local state to Supabase for backup.

## ⚠️ Disclaimer

**This software is for educational and paper-trading purposes only.**
Crypto trading involves significant risk. The authors are not responsible for any financial losses incurred from using this bot. Always do your own research (DYOR).

---
*Built with ❤️ for the Solana Community*
