#!/usr/bin/env python3
"""
alerts/formatters.py - Alert formatting functions
"""

from typing import Optional, Dict, Any
from shared.utils import format_marketcap_display, fetch_marketcap_and_fdv, truncate_address

# --- New Imports for Alpha Alerts ---
import aiohttp
import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
import math
from config import DATA_DIR
from shared.file_io import safe_load, safe_save

# Define new state file path
ALPHA_ALERTS_STATE_FILE = Path(DATA_DIR) / "alerts_state_alpha.json"
logger = logging.getLogger(__name__)
# --- End New Imports ---


def format_alert_html(
    token_data: Dict[str, Any],
    alert_type: str,
    previous_grade: Optional[str] = None,
    initial_mc: Optional[float] = None,
    initial_fdv: Optional[float] = None,
    first_alert_at: Optional[str] = None
) -> str:
    """
    Format token alert as an HTML message with a tappable address.
    
    Args:
        token_data: Token information dictionary
        alert_type: "NEW" or "CHANGE"
        previous_grade: Previous grade (for CHANGE alerts)
        initial_mc: Initial market cap (for comparison)
        initial_fdv: Initial FDV (for comparison)
        first_alert_at: Timestamp of first alert
    
    Returns:
        Formatted HTML string
    """
    token_meta = token_data.get("token_metadata") or {}
    name = token_meta.get("name") or token_data.get("token") or "Unknown"
    symbol = token_meta.get("symbol") or ""
    grade = token_data.get("grade", "NONE")
    mint = token_meta.get("mint", "") or token_data.get("token", "")

    current_mc, current_fdv, current_liquidity = fetch_marketcap_and_fdv(mint)

    # Build Market Cap / FDV line based on alert_type
    mc_line = ""
    if alert_type == "NEW":
        if current_mc is not None:
            mc_line = f"💰 <b>Market Cap:</b> {format_marketcap_display(current_mc)}"
        elif current_fdv is not None:
            mc_line = f"🏷️ <b>FDV:</b> {format_marketcap_display(current_fdv)}"
        else:
            mc_line = "💰 <b>Market Cap/FDV:</b> Unknown"

    elif alert_type == "CHANGE":
        if current_mc is not None:
            if initial_mc is not None:
                mc_line = f"💰 <b>Market Cap:</b> {format_marketcap_display(current_mc)} (was {format_marketcap_display(initial_mc)})"
            else:
                mc_line = f"💰 <b>Market Cap:</b> {format_marketcap_display(current_mc)}"
        elif current_fdv is not None:
            if initial_fdv is not None:
                mc_line = f"🏷️ <b>FDV:</b> {format_marketcap_display(current_fdv)} (was {format_marketcap_display(initial_fdv)})"
            else:
                mc_line = f"🏷️ <b>FDV:</b> {format_marketcap_display(current_fdv)}"
        else:
            mc_line = "💰 <b>Market Cap/FDV:</b> Unknown"

    # Build the full alert
    lines = [
        "🚀 <b>New Token Detected</b>" if alert_type == "NEW" else "🔔 <b>Grade Changed</b>",
        f"<b>{name}</b> ({symbol})" if symbol else f"<b>{name}</b>",
        f"<b>Grade:</b> {grade}" + (f" (was {previous_grade})" if previous_grade and alert_type == "CHANGE" else ""),
        mc_line,
        f"💧 <b>Liquidity:</b> {format_marketcap_display(current_liquidity)}" if current_liquidity else "💧 <b>Liquidity:</b> Unknown",
        f"<b>Concentration:</b> {token_data.get('concentration')}%"
    ]

    if first_alert_at:
        lines.append(f"🕐 <b>First alert:</b> {first_alert_at[:10]}")

    lines.append("")
    if mint:
        # ✅ IMPROVEMENT: Wrap the address in <code> tags to make it tappable/copyable
        lines.append(f"<b>Token: (tap to copy)</b> <code>{mint}</code>")
        lines.append(
            f'<a href="https://solscan.io/token/{mint}">Solscan</a> | '
            f'<a href="https://gmgn.ai/sol/token/{mint}">GMGN</a> | '
            f'<a href="https://dexscreener.com/solana/{mint}">DexScreener</a>'
        )

    return "\n".join(lines)


# --- NEW FUNCTIONS FOR ALPHA ALERTS ---

# --- Helper: Aiohttp Session ---
# Use a single session for all DexScreener calls in this module
_http_session = None

async def _get_http_session():
    """Get or create a persistent aiohttp session."""
    global _http_session
    if _http_session is None or _http_session.closed:
        _http_session = aiohttp.ClientSession()
    return _http_session

async def _close_http_session():
    """Close the persistent aiohttp session."""
    global _http_session
    if _http_session:
        await _http_session.close()
        _http_session = None

# --- Helper: DexScreener Fetch ---
async def _get_dexscreener_data(mint: str) -> Dict[str, Any]:
    """Fetch real-time data from DexScreener."""
    session = await _get_http_session()
    url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
    try:
        async with session.get(url, timeout=10) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data and data.get("pairs"):
                    # Use the first pair as the canonical source
                    return data["pairs"][0]
            logger.warning(f"Failed to fetch DexScreener data for {mint}, status: {resp.status}")
            return {}
    except asyncio.TimeoutError:
        logger.error(f"Timeout fetching DexScreener for {mint}")
        return {}
    except Exception as e:
        logger.exception(f"Error fetching DexScreener for {mint}: {e}")
        return {}

# --- Helper: Formatting Utilities ---

def _format_time_ago(dt: datetime) -> str:
    """Format a datetime into a 'time ago' string."""
    if not dt:
        return "Unknown"
    now = datetime.now(timezone.utc)
    # Ensure dt is offset-aware for comparison
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
        
    diff = now - dt
    
    seconds = diff.total_seconds()
    if seconds < 60:
        return f"{int(seconds)}s ago"
    minutes = seconds / 60
    if minutes < 60:
        return f"{int(minutes)}m ago"
    hours = minutes / 60
    if hours < 24:
        return f"{hours:.1f}h ago"
    days = hours / 24
    return f"{int(days)}d ago"

def _format_usd(value: float) -> str:
    """Format a float into a compact USD string."""
    if not isinstance(value, (int, float)) or value == 0:
        return "$0"
    if value > 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    if value > 1_000:
        return f"${value / 1_000:.2f}K"
    return f"${value:.2f}"

def _format_pct(value: float) -> str:
    """Format a float into a percentage string."""
    if not isinstance(value, (int, float)):
        return "N/A"
    return f"{value:.2f}%"

def _get_rugcheck_assessment(score_normalised: int) -> str:
    """Get emoji and text for RugCheck score (lower is better)."""
    if not isinstance(score_normalised, (int, float)) or score_normalised < 0:
        return "N/A"
    if score_normalised <= 10:
        return f"EXCELLENT ✅ ({score_normalised}/100)"
    if score_normalised <= 30:
        return f"GOOD 🟢 ({score_normalised}/100)"
    if score_normalised <= 50:
        return f"MODERATE ⚠️ ({score_normalised}/100)"
    return f"HIGH RISK ❌ ({score_normalised}/100)"

def _get_graduation_status(markets: list, market_cap: float) -> str:
    """Determine if a token is on a bonding curve, pre-graduation, or graduated."""
    if not markets:
        markets = []
        
    has_dex = any(
        m.get("marketType") in ["raydium", "orca"] for m in markets
    )
    
    if has_dex:
        return "Graduated ✅"
    if market_cap < 69_000:
        return "Pre-Graduation 🎓"
    return "Bonding Curve 📈"

def _get_lp_mc_ratio(liquidity_usd: float, market_cap: float) -> str:
    """Calculate and assess the LP/MC ratio."""
    if not market_cap or not liquidity_usd or market_cap == 0:
        return "N/A"
    
    ratio = (liquidity_usd / market_cap) * 100
    assessment = ""
    if ratio < 5:
        assessment = " (Very Low ⚠️)"
    elif ratio < 10:
        assessment = " (Low)"
    elif ratio > 30:
        assessment = " (Healthy ✅)"
    
    return f"{ratio:.2f}%{assessment}"

# --- New Main Formatter Functions ---

async def format_alpha_alert(
    mint: str, 
    entry: Dict[str, Any]
) -> (Optional[str], Optional[Dict[str, Any]]):
    """
    Format a new alpha token alert message.
    Fetches real-time DexScreener data and uses static RugCheck data from the entry.
    """
    try:
        # 1. Fetch real-time data from DexScreener
        dex_data = await _get_dexscreener_data(mint)
        if not dex_data:
            logger.warning(f"No DexScreener data for {mint}, skipping alert.")
            return None, None

        # 2. Extract data from the .pkl entry
        result = entry.get("result", {})
        security = result.get("security", {})
        rugcheck = security.get("rugcheck", {})
        rugcheck_raw_outer = security.get("rugcheck_raw", {})
        # Handle different nesting levels for 'raw'
        rugcheck_raw = rugcheck_raw_outer.get("raw", rugcheck_raw_outer)
        
        probation_meta = rugcheck_raw.get("probation_meta", {})
        
        # 3. Extract DexScreener Data
        pair = dex_data
        name = pair.get("baseToken", {}).get("name", "Unknown")
        symbol = pair.get("baseToken", {}).get("symbol", "N/A")
        try:
            price_usd = float(pair.get("priceUsd", 0))
            market_cap = float(pair.get("fdv", 0)) # Use FDV as Market Cap
            liquidity_usd = float(pair.get("liquidity", {}).get("usd", 0))
            pair_created_at_ts = pair.get("pairCreatedAt", 0) / 1000
        except (ValueError, TypeError):
            logger.warning(f"Could not parse numeric DexScreener data for {mint}")
            return None, None
            
        txns = pair.get("txns", {}).get("h24", {})
        buys = txns.get("buys", 0)
        sells = txns.get("sells", 0)
        pair_dt = datetime.fromtimestamp(pair_created_at_ts, tz=timezone.utc)
        
        # 4. Extract RugCheck/Entry Data
        overlap_grade = result.get("grade", "N/A")
        holder_count = rugcheck.get("holder_count", 0)
        lp_locked_pct = rugcheck.get("lp_locked_pct", 0)
        freeze_authority = rugcheck.get("freeze_authority")
        mint_authority = rugcheck.get("mint_authority")
        score_normalised = rugcheck_raw.get("score_normalised", -1)
        risks = sorted(
            rugcheck_raw.get("risks", []),
            key=lambda x: x.get("score", 0),
            reverse=True
        )
        # Use .get() for probation_meta fields for safety
        top3_pct = probation_meta.get("top3HoldersPct", probation_meta.get("top_n_pct", 0))
        top10_pct = probation_meta.get("top10HoldersPct", 0)
        
        # 5. Calculations
        age_str = _format_time_ago(pair_dt)
        total_txns = buys + sells
        buy_pct = (buys / total_txns * 100) if total_txns > 0 else 0
        sell_pct = 100 - buy_pct
        graduation_status = _get_graduation_status(
            rugcheck_raw.get("markets", []), market_cap
        )
        lp_mc_ratio_str = _get_lp_mc_ratio(liquidity_usd, market_cap)
        rugcheck_assessment = _get_rugcheck_assessment(score_normalised)
        
        risk_lines = []
        for i, risk in enumerate(risks[:5]): # Show top 5 risks
            r_level = risk.get('level', 'info')
            emoji = "❌" if r_level == 'danger' else "⚠️" if r_level == 'warn' else "ℹ️"
            risk_lines.append(f"  {emoji} {risk.get('name')}")
        if len(risks) > 5:
            risk_lines.append(f"  ...and {len(risks) - 5} more risks")
            
        risk_str = "\n".join(risk_lines) if risk_lines else "  No significant risks found ✅"
        
        # 6. Build Message
        msg = f"""🚀 <b>Alpha Alert: ${symbol}</b> 🚀

<b>{name}</b>
<code>{mint}</code>

<b>Status:</b> {graduation_status}
<b>Overlap Grade:</b> <b>{overlap_grade.upper()}</b>

--- 📈 <b>Market Data</b> ---
<b>Price:</b> ${price_usd:,.8f}
<b>Market Cap:</b> {_format_usd(market_cap)}
<b>Liquidity:</b> {_format_usd(liquidity_usd)}
<b>LP / MC Ratio:</b> {lp_mc_ratio_str}
<b>Age:</b> {age_str}

--- 📊 <b>Volume & Holders</b> ---
<b>Buy/Sell (24h):</b> {_format_pct(buy_pct)} / {_format_pct(sell_pct)}
<b>Total Holders:</b> {holder_count:,}
<b>Top 3 Holders:</b> {_format_pct(top3_pct)}
<b>Top 10 Holders:</b> {_format_pct(top10_pct)}

--- 🛡️ <b>Safety Check</b> ---
<b>RugCheck Score:</b> {rugcheck_assessment}
<b>LP Locked:</b> {_format_pct(lp_locked_pct)}
<b>Mint Authority:</b> {'RENOUNCED ✅' if not mint_authority else 'ACTIVE ❌'}
<b>Freeze Authority:</b> {'RENOUNCED ✅' if not freeze_authority else 'ACTIVE ❌'}

--- ⚠️ <b>Top Risks</b> ---
{risk_str}

--- 🔗 <b>Links</b> ---
<a href="https://solscan.io/token/{mint}">Solscan</a> | <a href="https://gmgn.ai/sol/token/{mint}">GMGN</a> | <a href="https://dexscreener.com/solana/{mint}">DexScreener</a> | <a href="https://rugcheck.xyz/tokens/{mint}">RugCheck</a>
"""
        
        # 7. Create initial state for storage
        initial_data = {
            "first_alert_at": datetime.now(timezone.utc).isoformat(),
            "initial_market_cap": market_cap,
            "initial_liquidity": liquidity_usd,
            "initial_holders": holder_count,
            "pair_created_at": pair_dt.isoformat(),
            "symbol": symbol
        }
        
        return msg, initial_data

    except Exception as e:
        logger.exception(f"Error in format_alpha_alert for {mint}: {e}")
        return None, None


async def format_alpha_refresh(
    mint: str, 
    initial_state: Dict[str, Any]
) -> str:
    """Format a refresh message comparing current data to initial data."""
    try:
        # 1. Fetch fresh DexScreener data
        dex_data = await _get_dexscreener_data(mint)
        if not dex_data:
            return "❌ Failed to fetch fresh data. Please try again in a moment."

        # 2. Extract initial data from state
        symbol = initial_state.get("symbol", "N/A")
        try:
            initial_mc = float(initial_state.get("initial_market_cap", 0))
            initial_liq = float(initial_state.get("initial_liquidity", 0))
            initial_holders = int(initial_state.get("initial_holders", 0))
            first_alert_at_dt = datetime.fromisoformat(initial_state.get("first_alert_at"))
        except Exception as e:
            logger.error(f"Failed to parse initial_state for {mint}: {e}")
            return "Error: Could not parse initial token data."
            
        # 3. Extract current data
        current_price = float(dex_data.get("priceUsd", 0))
        current_mc = float(dex_data.get("fdv", 0))
        current_liq = float(dex_data.get("liquidity", {}).get("usd", 0))
        
        # Note: DexScreener doesn't provide holder count. 
        # We will show the initial holder count for reference.
        
        # 4. Calculate changes
        mc_change_pct = ((current_mc - initial_mc) / initial_mc * 100) if initial_mc > 0 else 0
        liq_change_pct = ((current_liq - initial_liq) / initial_liq * 100) if initial_liq > 0 else 0
        
        time_elapsed_str = _format_time_ago(first_alert_at_dt)
        
        def format_change(pct: float) -> str:
            if pct > 0:
                return f"+{pct:.2f}% 📈"
            elif pct < 0:
                return f"{pct:.2f}% 📉"
            else:
                return "No Change"

        mc_change_str = format_change(mc_change_pct)
        liq_change_str = format_change(liq_change_pct)

        # 5. Build message
        msg = f"""🔄 <b>Refresh: ${symbol}</b>
(Stats vs. first alert {time_elapsed_str})

<b>Price:</b> ${current_price:,.8f}

--- <b>Market Cap</b> ---
<b>Now:</b> {_format_usd(current_mc)}
<b>Initial:</b> {_format_usd(initial_mc)}
<b>Change:</b> {mc_change_str}

--- <b>Liquidity</b> ---
<b>Now:</b> {_format_usd(current_liq)}
<b>Initial:</b> {_format_usd(initial_liq)}
<b>Change:</b> {liq_change_str}

--- <b>Holders</b> ---
<b>Initial:</b> {initial_holders:,}
<i>(Holder count not available on refresh)</i>

<a href="https://dexscreener.com/solana/{mint}">View on DexScreener</a>
"""
        return msg

    except Exception as e:
        logger.exception(f"Error in format_alpha_refresh for {mint}: {e}")
        return "Error: An exception occurred during refresh."