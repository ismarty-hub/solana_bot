#!/usr/bin/env python3
"""
alerts/formatters.py - Alert formatting functions

Contains formatters for:
1. ALPHA ALERTS - High-priority overlap alerts with detailed security analysis
2. REGULAR ALERTS - Standard token overlap alerts (NEW/CHANGE)

Recent fixes:
- Fixed raw \n characters showing instead of line breaks
- Removed debug data from messages
- Proper HTML formatting for Telegram
- Shows ALL risks for alpha alerts
- Fixed refresh button functionality
- (CORRECTION) Removed live data fetch from format_alert_html, uses pre-fetched data instead.
"""

from typing import Optional, Dict, Any, Tuple
import aiohttp
import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
import html
from config import DATA_DIR
from shared.file_io import safe_load, safe_save
# (CORRECTION) Removed fetch_marketcap_and_fdv, it's no longer called
from shared.utils import format_marketcap_display, truncate_address

# Define state file path
ALPHA_ALERTS_STATE_FILE = Path(DATA_DIR) / "alerts_state_alpha.json"
logger = logging.getLogger(__name__)

# ============================================================================
# ALPHA ALERTS - High-priority overlap alerts with security analysis
# ============================================================================

# Global HTTP session
_http_session = None

async def _get_http_session():
    global _http_session
    if _http_session is None or _http_session.closed:
        _http_session = aiohttp.ClientSession()
    return _http_session

async def _close_http_session():
    global _http_session
    if _http_session:
        await _http_session.close()
        _http_session = None

async def _get_dexscreener_data(mint: str) -> Dict[str, Any]:
    """Fetch token data from DexScreener API."""
    session = await _get_http_session()
    url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
    try:
        async with session.get(url, timeout=10) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data and data.get("pairs"):
                    return data["pairs"][0]
            logger.warning(f"Failed to fetch DexScreener data for {mint}, status: {resp.status}")
            return {}
    except asyncio.TimeoutError:
        logger.error(f"Timeout fetching DexScreener for {mint}")
        return {}
    except Exception as e:
        logger.exception(f"Error fetching DexScreener for {mint}: {e}")
        return {}

# Formatting helpers

def _format_time_ago(dt: datetime) -> str:
    """Format datetime as 'Xd ago' or 'Xh ago'."""
    if not dt:
        return "Unknown"
    now = datetime.now(timezone.utc)
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
        return f"{int(hours)}h ago"
    days = hours / 24
    return f"{int(days)}d ago"


def _format_usd(value: float) -> str:
    """Format USD values compactly."""
    try:
        if not isinstance(value, (int, float)) or value == 0:
            return "$0"
        if value >= 1_000_000:
            return f"${value / 1_000_000:.2f}M"
        if value >= 1_000:
            return f"${value / 1_000:.2f}K"
        return f"${value:.2f}"
    except Exception:
        return "$0"


def _format_pct(value: float) -> str:
    """Format percentage values."""
    try:
        if not isinstance(value, (int, float)):
            return "N/A"
        return f"{value:.2f}%"
    except Exception:
        return "N/A"


def _get_graduation_status(markets: list, market_cap: float) -> str:
    """Determine if token has graduated from bonding curve."""
    if not markets:
        markets = []
    has_dex = any(
        m.get("marketType") in ["raydium", "orca", "meteora_damm_v2"] for m in markets
    )
    if has_dex:
        return "Graduated ✅"
    if market_cap < 69_000:
        return "Pre-Graduation 🎓"
    return "Bonding Curve 📈"


def format_alpha_alert(mint: str, entry: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    """
    SYNCHRONOUS version - returns (message_string, metadata_dict)
    This must be called with asyncio.run() or await if needed.
    """
    # This is a wrapper that will be called by monitoring loop
    # The actual async work is done in _format_alpha_alert_async
    import asyncio
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If loop is already running, we need to create task
            return asyncio.create_task(_format_alpha_alert_async(mint, entry))
        else:
            return loop.run_until_complete(_format_alpha_alert_async(mint, entry))
    except RuntimeError:
        # No loop exists, create one
        return asyncio.run(_format_alpha_alert_async(mint, entry))


async def _format_alpha_alert_async(mint: str, entry: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    """
    Format alpha alert - ASYNC version that does the actual work.
    Returns (message_html_string, initial_state_dict)
    """
    try:
        # Fetch current market data
        dex_data = await _get_dexscreener_data(mint)
        if not dex_data:
            logger.warning(f"No DexScreener data for {mint}, skipping alert.")
            return None, None

        # Extract token metadata
        result = entry.get("result", {})
        security = result.get("security", {})
        rugcheck = security.get("rugcheck", {})
        rugcheck_raw_outer = security.get("rugcheck_raw", {})
        rugcheck_raw = rugcheck_raw_outer.get("raw", rugcheck_raw_outer)

        pair = dex_data
        name = pair.get("baseToken", {}).get("name", "Unknown")
        symbol = pair.get("baseToken", {}).get("symbol", "N/A")

        # Parse numeric values safely
        try:
            price_usd = float(pair.get("priceUsd", 0) or 0)
        except:
            price_usd = 0.0
        
        try:
            market_cap = float(pair.get("fdv", 0) or 0)
        except:
            market_cap = 0.0
        
        try:
            liquidity_usd = float(pair.get("liquidity", {}).get("usd", 0) or 0)
        except:
            liquidity_usd = 0.0

        # Parse timestamps
        pair_created_at = pair.get("pairCreatedAt", 0)
        try:
            pair_created_at_ts = float(pair_created_at) / 1000
            pair_dt = datetime.fromtimestamp(pair_created_at_ts, tz=timezone.utc)
        except:
            pair_dt = None

        # Transaction data
        txns = pair.get("txns", {}).get("h24", {})
        buys = txns.get("buys", 0)
        sells = txns.get("sells", 0)

        # Security metrics
        overlap_grade = result.get("grade", "N/A")
        holder_count = rugcheck.get("holder_count", 0)
        lp_locked_pct = rugcheck.get("lp_locked_pct", 0)
        freeze_authority = rugcheck.get("freeze_authority")
        mint_authority = rugcheck.get("mint_authority")
        score_normalised = rugcheck_raw.get("score_normalised", -1)
        
        # Get ALL risks
        risks = sorted(
            rugcheck_raw.get("risks", []),
            key=lambda x: x.get("score", 0),
            reverse=True
        )

        # Calculate metrics
        age_str = _format_time_ago(pair_dt) if pair_dt else "Unknown"
        total_txns = (buys or 0) + (sells or 0)
        buy_pct = (buys / total_txns * 100) if total_txns > 0 else 0
        sell_pct = 100 - buy_pct
        
        graduation_status = _get_graduation_status(
            rugcheck_raw.get("markets", []), 
            market_cap
        )
        
        lp_mc_ratio = (liquidity_usd / market_cap * 100) if market_cap > 0 else 0

        # Get risk score assessment
        if score_normalised <= 10:
            score_text = f"EXCELLENT ✅ ({score_normalised}/100)"
        elif score_normalised <= 30:
            score_text = f"GOOD 🟢 ({score_normalised}/100)"
        elif score_normalised <= 50:
            score_text = f"MODERATE ⚠️ ({score_normalised}/100)"
        else:
            score_text = f"HIGH RISK ❌ ({score_normalised}/100)"

        # Escape HTML for safe display
        esc_name = html.escape(str(name))
        esc_symbol = html.escape(str(symbol))
        esc_mint = html.escape(str(mint))

        # Format ALL risks
        risk_lines = []
        for risk in risks:
            level = risk.get('level', 'info')
            if level == 'danger':
                emoji = "⚠️"
            elif level == 'warn':
                emoji = "⚠️"
            else:
                emoji = "ℹ️"
            desc = html.escape(str(risk.get('description', '')))
            risk_lines.append(f"{emoji} {desc}")
        
        risk_str = "\n".join(risk_lines) if risk_lines else "✅ No significant risks"

        # Build the message - THIS MUST RETURN A PLAIN STRING
        msg = f"""🚀 <b>Alpha Alert: ${esc_symbol}</b> 🚀

<b>{esc_name}</b>
<code>{esc_mint}</code>

<b>Status:</b> {html.escape(graduation_status)}
<b>Overlap Grade:</b> <b>{html.escape(str(overlap_grade).upper())}</b>

--- 📈 <b>Market Data</b> ---
<b>Price:</b> ${price_usd:.8f}
<b>Market Cap:</b> {_format_usd(market_cap)}
<b>Liquidity:</b> {_format_usd(liquidity_usd)}
<b>LP / MC Ratio:</b> {lp_mc_ratio:.2f}%
<b>Age:</b> {age_str}

--- 📊 <b>Volume &amp; Holders</b> ---
<b>Buy/Sell (24h):</b> {buy_pct:.2f}% / {sell_pct:.2f}%
<b>Total Holders:</b> {int(holder_count):,}

--- 🛡️ <b>Safety Check</b> ---
<b>Score:</b> {score_text}
<b>LP Locked:</b> {_format_pct(lp_locked_pct)}
<b>Mint Authority:</b> {'RENOUNCED ✅' if not mint_authority else 'ACTIVE ❌'}
<b>Freeze Authority:</b> {'RENOUNCED ✅' if not freeze_authority else 'ACTIVE ❌'}

--- ⚠️ <b>Top Risks</b> ---
{risk_str}

--- 🔗 <b>Links</b> ---
<a href="https://solscan.io/token/{mint}">Solscan</a> | <a href="https://gmgn.ai/sol/token/{mint}">GMGN</a> | <a href="https://dexscreener.com/solana/{mint}">DexScreener</a>"""

        # Create initial state for refresh functionality
        initial_state = {
            "first_alert_at": datetime.now(timezone.utc).isoformat(),
            "initial_market_cap": market_cap,
            "initial_liquidity": liquidity_usd,
            "initial_holders": holder_count,
            "pair_created_at": pair_dt.isoformat() if pair_dt else None,
            "symbol": symbol,
            "name": name,
            "mint": mint
        }

        # Return JUST the string and dict - no tuples, no extra wrapping
        return msg, initial_state

    except Exception as e:
        logger.exception(f"Error in _format_alpha_alert_async for {mint}: {e}")
        return None, None


async def format_alpha_refresh(mint: str, initial_state: Dict[str, Any]) -> str:
    """
    Format refresh message showing changes since initial alert.
    Returns a plain HTML string for Telegram.
    """
    try:
        # Fetch current data
        dex_data = await _get_dexscreener_data(mint)
        if not dex_data:
            return "❌ Failed to fetch fresh data. Please try again in a moment."

        # Get initial values
        symbol = initial_state.get("symbol", "N/A")
        name = initial_state.get("name", "Unknown")
        
        try:
            initial_mc = float(initial_state.get("initial_market_cap", 0) or 0)
            initial_liq = float(initial_state.get("initial_liquidity", 0) or 0)
            initial_holders = int(initial_state.get("initial_holders", 0) or 0)
            first_alert_at_str = initial_state.get("first_alert_at")
            first_alert_at_dt = datetime.fromisoformat(first_alert_at_str.replace('Z', '+00:00'))
        except Exception as e:
            logger.error(f"Failed to parse initial_state for {mint}: {e}")
            return "❌ Error: Could not parse initial token data."

        # Get current values
        current_price = float(dex_data.get("priceUsd", 0) or 0)
        current_mc = float(dex_data.get("fdv", 0) or 0)
        current_liq = float(dex_data.get("liquidity", {}).get("usd", 0) or 0)

        # Calculate changes
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

        # Build refresh message as plain string
        msg = f"""🔄 <b>Refresh: ${html.escape(str(symbol))}</b>
<i>Stats vs. first alert {time_elapsed_str}</i>

<b>Price:</b> ${current_price:.8f}

--- <b>Market Cap</b> ---
<b>Now:</b> {_format_usd(current_mc)}
<b>Initial:</b> {_format_usd(initial_mc)}
<b>Change:</b> {mc_change_str}

--- <b>Liquidity</b> ---
<b>Now:</b> {_format_usd(current_liq)}
<b>Initial:</b> {_format_usd(initial_liq)}
<b>Change:</b> {liq_change_str}

<a href="https://dexscreener.com/solana/{mint}">View on DexScreener</a>"""
        
        return msg

    except Exception as e:
        logger.exception(f"Error in format_alpha_refresh for {mint}: {e}")
        return "❌ Error: An exception occurred during refresh."


# ============================================================================
# REGULAR TOKEN ALERTS (Non-Alpha)
# ============================================================================

def format_alert_html(
    token_data: Dict[str, Any],
    alert_type: str,
    previous_grade: Optional[str] = None,
    initial_mc: Optional[float] = None,
    initial_fdv: Optional[float] = None,
    first_alert_at: Optional[str] = None
) -> str:
    """
    Format regular token alert as an HTML message with a tappable address.
    Uses data ALREADY FETCHED by the monitoring loop.
    
    Args:
        token_data: Token information dictionary (MUST include 'dexscreener' and 'rugcheck' keys)
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

    # --- (CORRECTION) REMOVED LIVE FETCH ---
    # current_mc, current_fdv, current_liquidity = fetch_marketcap_and_fdv(mint)
    
    # --- (CORRECTION) ADDED: Get data from token_data ---
    dex_data = token_data.get("dexscreener", {})
    rugcheck_data = token_data.get("rugcheck", {})

    # Use Dexscreener's market cap (which is FDV from their API)
    current_mc = dex_data.get("market_cap_usd")
    
    # Use RugCheck's aggregated liquidity
    current_liquidity = rugcheck_data.get("total_liquidity_usd")
    
    # We will use 'current_mc' for both MC and FDV display.
    current_fdv = current_mc 
    # --- END CORRECTION ---


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
        # (CORRECTION) Handle 'None' for liquidity
        f"💧 <b>Liquidity:</b> {format_marketcap_display(current_liquidity)}" if current_liquidity is not None else "💧 <b>Liquidity:</b> Unknown",
        f"<b>Concentration:</b> {token_data.get('concentration')}%"
    ]

    if first_alert_at:
        lines.append(f"🕐 <b>First alert:</b> {first_alert_at[:10]}")

    lines.append("")
    if mint:
        # Wrap the address in <code> tags to make it tappable/copyable
        lines.append(f"<b>Token: (tap to copy)</b> <code>{mint}</code>")
        lines.append(
            f'<a href="https://solscan.io/token/{mint}">Solscan</a> | '
            f'<a href="https://gmgn.ai/sol/token/{mint}">GMGN</a> | '
            f'<a href="https://dexscreener.com/solana/{mint}">DexScreener</a>'
        )

    return "\n".join(lines)