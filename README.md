# 🚀 SmartyMetrics: Solana Analytics & Trading Bot

A high-performance, modular Telegram bot designed for real-time Solana token monitoring, advanced analytics, and automated paper trading leveraging Machine Learning.

![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![Solana](https://img.shields.io/badge/Solana-DeFi-green)
![Supabase](https://img.shields.io/badge/Supabase-Database-orange)
![Telegram](https://img.shields.io/badge/Telegram-Bot-blue)

## 📖 Overview

SmartyMetrics monitors the Solana blockchain (via Discovery and Alpha streams), processes signals through an ML-driven security and probability gate, and provides users with actionable alerts and a robust virtual trading environment. It features a hybrid cloud-local state management system using Supabase for high availability.

## ✨ Key Features

### 🔍 Intelligence & Alerts
- **Discovery Alerts**: Real-time identification of new token opportunities.
- **Alpha Alerts**: Curated, high-conviction signals with deep-dive analysis.
- **ML Filtering**: Automated "ML Passed" gate using a FastAPI service to predict win probability.
- **Role-Based Access**: Granular permissions for Users, Admins, and Superadmins.

### 📈 Advanced Paper Trading
- **Virtual Portfolio**: Track Capital, PnL, and ROI without financial risk.
- **Decoupled Filters**: Independently configure which grades you **receive as notifications** vs which ones the bot **automatically trades**.
- **Smart Take Profit (TP)**: Choose between fixed percentages or statistical targets:
  - `median`, `mean`, `mode` - Based on recent historical ATH ROI.
  - `smart` - Targets reached by 75% of past winning signals.
- **Dynamic Risk**: Configurable Stop Loss, Reserve Balances, and Minimum Trade Sizes.

### 🛠 Robust Infrastructure
- **Callback Data Hashing**: Optimized Telegram button handling to support long token names and complex UI states within Telegram's 64-byte limit.
- **Global Error Handler**: Centralized recovery for network timeouts and expired button queries.
- **Hybrid Persistence**: Periodic background syncing between local JSON state and Supabase Storage.

## 🧠 System Architecture

### 1. Signal Pipeline (`alerts/monitoring/`)
The bot processes tokens through several security and probability layers:
1. **Detection**: `analytics_monitoring.py` and `alpha_monitoring.py` poll for new signal data.
2. **Security Gate**: High-speed security checks (Mint/Freeze authority, LP burn, etc.).
3. **ML Prediction**: Tokens are sent to the ML API; only those passing the threshold are broadcast.
4. **Broadcast**: Validated signals are sent to users based on their Grade preferences (Low, Medium, High, Critical).

### 2. Execution Engine (`trade_manager.py` & `portfolio_manager.py`)
If "Both Modes" or "Trading Only" is enabled:
1. **Validation**: Checks for sufficient virtual funds and user-defined reserve balance.
2. **Entry**: Executes a paper trade based on configured trade size (Fixed or % of portfolio).
3. **Monitoring**: `trade_monitor.py` tracks live prices via Jupiter API.
4. **Exit**: Automatically triggers TP/SL hits or closes expired trades after a defined period (e.g., 24h).

## 🎮 Navigation & Commands

### User Commands
- `/start` - Main menu and onboarding.
- `/portfolio` - View current virtual balance and active position summary.
- `/pnl` - Interactive page for live unrealized Profit & Loss and manual closing.
- `/history` - View closed trades and performance metrics.
- `/myalerts` - Configure notification grades and alpha settings.
- `/settings` - Deep-dive configuration for trading capital, filters, and TP/SL.

### Admin Commands
- `/admin` - System-wide user and performance statistics.
- `/broadcast` - Send announcements to the entire user base.
- `/adduser` - Manage subscriptions and expiry dates.

## ⚙️ Setup & Deployment

1. **Prerequisites**: Python 3.10+, Telegram Bot Token, Supabase URL/Key.
2. **Installation**:
   ```bash
   pip install -r requirements.txt
   ```
3. **Configuration**: Use `.env` or Environment Variables:
   - `BOT_TOKEN`: Telegram Token.
   - `SUPABASE_URL` / `SUPABASE_KEY`: Database credentials.
   - `ML_API_URL`: ML service endpoint.
4. **Run**:
   ```bash
   python bot.py
   ```

## 🏗️ Performance Optimization
- **Multiprocessing**: Heavy analytics can be run as a separate process from the Telegram bot for maximum responsiveness.
- **Caching**: Aggressive local caching minimizes API calls to Jupiter and Supabase.

---
⚠️ **Disclaimer**: This project is for **educational and paper-trading purposes only**. The authors are not responsible for any financial decisions or real-world losses.
