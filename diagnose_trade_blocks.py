#!/usr/bin/env python3
"""
Diagnostic tool to identify why trades aren't opening despite ML_PASSED: true

This script analyzes:
1. Active tracking data
2. Portfolio state
3. User preferences
4. Recent signals
And identifies which blocking conditions are preventing trades
"""

import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from config import DATA_DIR, SIGNAL_FRESHNESS_WINDOW, PORTFOLIOS_FILE
from shared.file_io import safe_load

def diagnose_trading_blocks():
    """Analyze active_tracking.json to find blocking conditions"""
    
    ACTIVE_TRACKING_FILE = DATA_DIR / "active_tracking.json"
    
    print("=" * 80)
    print("🔍 TRADE BLOCKING DIAGNOSTIC")
    print("=" * 80)
    
    # Load active tracking
    active_tracking = safe_load(ACTIVE_TRACKING_FILE, {})
    if not active_tracking:
        print("❌ No active_tracking.json found or empty")
        return
    
    print(f"\n📊 Total tokens in active_tracking: {len(active_tracking)}")
    
    # Load portfolios to check capital
    portfolios = safe_load(PORTFOLIOS_FILE, {})
    print(f"📈 Total portfolios: {len(portfolios)}")
    
    # Analyze by signal type
    discovery_tokens = {k: v for k, v in active_tracking.items() if "_discovery" in k}
    alpha_tokens = {k: v for k, v in active_tracking.items() if "_alpha" in k}
    
    print(f"\n  Discovery tokens: {len(discovery_tokens)}")
    print(f"  Alpha tokens: {len(alpha_tokens)}")
    
    # Find ML_PASSED tokens
    ml_passed_tokens = {k: v for k, v in active_tracking.items() if v.get("ML_PASSED") is True}
    ml_failed_tokens = {k: v for k, v in active_tracking.items() if v.get("ML_PASSED") is False}
    
    print(f"\n🤖 ML Status:")
    print(f"  ML_PASSED=True:  {len(ml_passed_tokens)}")
    print(f"  ML_PASSED=False: {len(ml_failed_tokens)}")
    
    if not ml_passed_tokens:
        print("\n⚠️  WARNING: No tokens with ML_PASSED=True found!")
        return
    
    # Analyze blocking conditions for ML_PASSED tokens
    print(f"\n🔎 Analyzing {len(ml_passed_tokens)} ML_PASSED tokens for blocking conditions...")
    
    now = datetime.now(timezone.utc)
    stale_signals = []
    active_positions = []
    old_signals = []
    
    for key, token_data in ml_passed_tokens.items():
        mint = token_data.get("mint", "N/A")
        symbol = token_data.get("symbol", "N/A")
        status = token_data.get("status", "unknown")
        
        # Check 1: Signal freshness
        entry_time_str = token_data.get("entry_time")
        if entry_time_str:
            try:
                entry_dt = datetime.fromisoformat(entry_time_str.replace("Z", "+00:00"))
                age_seconds = (now - entry_dt).total_seconds()
                
                if age_seconds > SIGNAL_FRESHNESS_WINDOW:
                    stale_signals.append({
                        "key": key,
                        "symbol": symbol,
                        "age_seconds": age_seconds,
                        "reason": f"Signal too old ({age_seconds:.0f}s > {SIGNAL_FRESHNESS_WINDOW}s)"
                    })
                elif age_seconds > 300:  # 5 minutes
                    old_signals.append({
                        "key": key,
                        "symbol": symbol,
                        "age_seconds": age_seconds
                    })
            except Exception as e:
                print(f"  ⚠️  Could not parse entry_time for {symbol}: {e}")
        
        # Check 2: Active positions (already exists)
        if status == "active":
            active_positions.append({
                "key": key,
                "symbol": symbol,
                "reason": "Position already exists (duplicate prevention)"
            })
    
    # Print findings
    if stale_signals:
        print(f"\n🛑 STALE SIGNALS ({len(stale_signals)}):")
        print(f"  These signals are too old to trade (> {SIGNAL_FRESHNESS_WINDOW}s):")
        for sig in stale_signals[:5]:
            print(f"    • {sig['symbol']:10} | Age: {sig['age_seconds']:.0f}s | {sig['reason']}")
        if len(stale_signals) > 5:
            print(f"    ... and {len(stale_signals) - 5} more")
    
    if old_signals:
        print(f"\n⏳ SIGNALS NEARING STALE THRESHOLD ({len(old_signals)}):")
        print(f"  These are older than 5 minutes but still valid:")
        for sig in old_signals[:5]:
            age_min = sig['age_seconds'] / 60
            print(f"    • {sig['symbol']:10} | Age: {age_min:.1f} minutes")
    
    if active_positions:
        print(f"\n✅ ACTIVE POSITIONS ({len(active_positions)}):")
        print(f"  These are already trading (duplicate prevention):")
        for pos in active_positions[:5]:
            print(f"    • {pos['symbol']:10} | {pos['reason']}")
        if len(active_positions) > 5:
            print(f"    ... and {len(active_positions) - 5} more")
    
    # Check portfolio capital
    print(f"\n💰 PORTFOLIO CAPITAL STATUS:")
    for chat_id, portfolio in portfolios.items():
        capital = portfolio.get("capital_usd", 0)
        positions = portfolio.get("positions", {})
        
        print(f"\n  User {chat_id[:8]}...:")
        print(f"    Available Capital: ${capital:,.2f}")
        print(f"    Open Positions: {len(positions)}")
        
        # Check if capital is low
        if capital < 10:
            print(f"    ⚠️  WARNING: Very low capital! Trades may be blocked.")
        elif capital < 50:
            print(f"    ⚠️  Low capital - may prevent trade size allocation")
    
    # Summary
    print(f"\n" + "=" * 80)
    print("📋 SUMMARY:")
    print("=" * 80)
    
    fresh_ml_tokens = len(ml_passed_tokens) - len(stale_signals) - len(active_positions)
    
    print(f"✅ Fresh ML_PASSED tokens available for trading: {fresh_ml_tokens}")
    print(f"🛑 Stale signals (freshness expired): {len(stale_signals)}")
    print(f"✅ Already active positions: {len(active_positions)}")
    
    if fresh_ml_tokens == 0:
        print(f"\n⚠️  ACTION REQUIRED: No fresh signals available to trade!")
        print(f"   Consider checking:")
        print(f"   1. Are new tokens being detected? (check active_tracking.json)")
        print(f"   2. Are they passing ML checks? (check ML_PASSED status)")
        print(f"   3. Check SIGNAL_FRESHNESS_WINDOW setting (currently {SIGNAL_FRESHNESS_WINDOW}s)")
    else:
        print(f"\n✅ There are {fresh_ml_tokens} tokens ready to trade!")
        print(f"   Check user preferences for other blocks (min_trade_size, capital, etc.)")

if __name__ == "__main__":
    diagnose_trading_blocks()
