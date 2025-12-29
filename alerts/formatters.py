#!/usr/bin/env python3
"""
alerts/formatters.py - Alert formatting functions with enhanced ML insights

Contains formatters for:
1. ALPHA ALERTS - High-priority overlap alerts with detailed security analysis
2. REGULAR ALERTS - Standard token overlap alerts (NEW/CHANGE)

Enhanced ML insights with beautiful UX
"""

from typing import Optional, Dict, Any, Tuple
import aiohttp
import asyncio
import logging
from datetime import datetime, timezone
import html

from config import DATA_DIR, ALPHA_ALERTS_STATE_FILE
from shared.file_io import safe_load, safe_save
from shared.utils import format_marketcap_display, truncate_address

logger = logging.getLogger(__name__)

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


# ============================================================================
# ML INSIGHT FORMATTING (SHARED)
# ============================================================================

def format_ml_insight_for_alert(ml_data: Dict[str, Any]) -> str:
    """
    Format ML prediction data as a beautiful, compact section for alerts.
    
    Args:
        ml_data: ML prediction data from API/monitoring
    
    Returns:
        Formatted HTML string for inclusion in alerts
    """
    if not ml_data or not isinstance(ml_data, dict):
        return ""
    
    # Extract values (handle both "probability" and "win_probability" keys)
    prob = ml_data.get("probability") or ml_data.get("win_probability")
    confidence = ml_data.get("confidence")
    risk_tier = ml_data.get("risk_tier")
    action = ml_data.get("action")
    
    # If no data, return empty
    if prob is None and not confidence and not risk_tier:
        return ""
    
    # Action emoji mapping
    action_styles = {
        "BUY": ("🟢", "STRONG BUY"),
        "CONSIDER": ("🟡", "CONSIDER"),
        "SKIP": ("🟠", "SKIP"),
        "AVOID": ("🔴", "AVOID")
    }
    
    # Build ML insight section
    lines = ["", "--- 🤖 <b>ML Insight</b> ---"]
    
    # Win probability with visual bar
    if prob is not None:
        try:
            prob_pct = float(prob) * 100
            
            # Visual progress bar
            filled = int(prob_pct / 20)  # 0-5 blocks
            bar = "█" * filled + "░" * (5 - filled)
            
            # Color based on probability
            if prob_pct >= 70:
                color = "🟢"
            elif prob_pct >= 60:
                color = "🟡"
            elif prob_pct >= 45:
                color = "🟠"
            else:
                color = "🔴"
            
            lines.append(f"{color} <b>Win Chance:</b> {prob_pct:.1f}% {bar}")
        except Exception:
            pass
    
    # Action recommendation with styled text
    if action:
        action_emoji, action_text = action_styles.get(action, ("⚪", action))
        lines.append(f"{action_emoji} <b>Signal:</b> {action_text}")
    
    # Confidence level
    if confidence:
        # Emoji based on confidence
        if confidence == "HIGH":
            conf_emoji = "🎯"
        elif confidence == "MEDIUM":
            conf_emoji = "📊"
        else:
            conf_emoji = "📉"
        
        lines.append(f"{conf_emoji} <b>Confidence:</b> {html.escape(str(confidence))}")
    
    # Risk tier with clear interpretation
    if risk_tier:
        risk_tier_str = str(risk_tier)
        
        # Emoji and styling based on risk
        if "LOW" in risk_tier_str.upper():
            risk_emoji = "🛡️"
            risk_label = "Low Risk"
        elif "MEDIUM" in risk_tier_str.upper():
            risk_emoji = "⚠️"
            risk_label = "Moderate Risk"
        elif "HIGH" in risk_tier_str.upper() and "VERY" not in risk_tier_str.upper():
            risk_emoji = "🚨"
            risk_label = "High Risk"
        else:
            risk_emoji = "☠️"
            risk_label = "Very High Risk"
        
        lines.append(f"{risk_emoji} <b>Risk:</b> {risk_label}")
    
    return "\n".join(lines)


# ============================================================================
# ALPHA ALERTS - High-priority overlap alerts with security analysis
# ============================================================================

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


def _format_time_ago(dt: datetime) -> str:
    """
    Format datetime as age.
    - If < 24 hours: show as hours, minutes, seconds (e.g., "2h 30m 15s ago")
    - If >= 24 hours: show as days and hours (e.g., "1d 3h ago")
    """
    if not dt:
        return "Unknown"
    now = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    diff = now - dt
    total_seconds = diff.total_seconds()
    
    # If less than 24 hours, show hours:minutes:seconds format
    if total_seconds < 86400:  # 86400 seconds = 24 hours
        hours = int(total_seconds // 3600)
        remaining = int(total_seconds % 3600)
        minutes = remaining // 60
        seconds = remaining % 60
        
        parts = []
        if hours > 0:
            parts.append(f"{hours}h")
        if minutes > 0:
            parts.append(f"{minutes}m")
        if seconds > 0 or not parts:  # Always show seconds if no hours/minutes
            parts.append(f"{seconds}s")
        
        return " ".join(parts) + " ago"
    
    # If >= 24 hours, show days and hours format
    total_hours = total_seconds / 3600
    days = int(total_hours // 24)
    remaining_hours = int(total_hours % 24)
    
    if remaining_hours > 0:
        return f"{days}d {remaining_hours}h ago"
    else:
        return f"{days}d ago"


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
    """Wrapper for async alpha alert formatting."""
    import asyncio
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            return _format_alpha_alert_async(mint, entry)
        else:
            return loop.run_until_complete(_format_alpha_alert_async(mint, entry))
    except RuntimeError:
        return asyncio.run(_format_alpha_alert_async(mint, entry))


async def _format_alpha_alert_async(mint: str, entry: Dict[str, Any]) -> Tuple[str, Dict[str, Any], Optional[str]]:
    """
    Format alpha alert with enhanced ML insights.
    Returns (message_html_string, initial_state_dict, image_url)
    """
    try:
        # Fetch current market data
        dex_data = await _get_dexscreener_data(mint)
        if not dex_data:
            logger.warning(f"No DexScreener data for {mint}, skipping alert.")
            return None, None, None

        # Extract token metadata
        result = entry.get("result", {})
        security = result.get("security", {})
        rugcheck = security.get("rugcheck", {})
        rugcheck_raw_outer = security.get("rugcheck_raw", {})
        rugcheck_raw = rugcheck_raw_outer.get("raw", rugcheck_raw_outer)
        
        # Extract image URL from dexscreener data in the entry
        dex_entry = result.get("dexscreener", {})
        image_url = dex_entry.get("info", {}).get("imageUrl")

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

        # Risk score assessment
        if score_normalised <= 10:
            score_text = f"EXCELLENT ✅ ({score_normalised}/100)"
        elif score_normalised <= 30:
            score_text = f"GOOD 🟢 ({score_normalised}/100)"
        elif score_normalised <= 50:
            score_text = f"MODERATE ⚠️ ({score_normalised}/100)"
        else:
            score_text = f"HIGH RISK ❌ ({score_normalised}/100)"

        # Escape HTML
        esc_name = html.escape(str(name))
        esc_symbol = html.escape(str(symbol))
        esc_mint = html.escape(str(mint))

        # Format risks (show top 5)
        risk_lines = []
        for risk in risks[:5]:
            level = risk.get('level', 'info')
            if level == 'danger':
                emoji = "🔴"
            elif level == 'warn':
                emoji = "⚠️"
            else:
                emoji = "ℹ️"
            desc = html.escape(str(risk.get('description', '')))
            risk_lines.append(f"{emoji} {desc}")
        
        risk_str = "\n".join(risk_lines) if risk_lines else "✅ No significant risks"

        # Get ML prediction data
        ml_data = result.get("ml_prediction", {})
        ml_section = format_ml_insight_for_alert(ml_data)

        # Build the message
        msg = f"""🚀 <b>Alpha Alert: ${esc_symbol}</b> 🚀

<b>{esc_name}</b>
<code>{esc_mint}</code>

<b>Status:</b> {html.escape(graduation_status)}
<b>Overlap Grade:</b> <b>{html.escape(str(overlap_grade).upper())}</b>

--- 📈 <b>Market Data</b> ---
<b>💰 Price:</b> ${price_usd:.8f}
<b>📊 Market Cap:</b> {_format_usd(market_cap)}
<b>💧 Liquidity:</b> {_format_usd(liquidity_usd)}
<b>📉 LP/MC Ratio:</b> {lp_mc_ratio:.2f}%
<b>⏰ Age:</b> {age_str}

--- 📊 <b>Activity</b> ---
<b>Buy/Sell:</b> {buy_pct:.0f}% / {sell_pct:.0f}%
<b>👥 Holders:</b> {int(holder_count):,}

--- 🛡️ <b>Safety</b> ---
<b>Score:</b> {score_text}
<b>🔒 LP Locked:</b> {_format_pct(lp_locked_pct)}
<b>Mint:</b> {'✅ Renounced' if not mint_authority else '❌ Active'}
<b>Freeze:</b> {'✅ Renounced' if not freeze_authority else '❌ Active'}

--- ⚠️ <b>Top Risks</b> ---
{risk_str}
{ml_section}

--- 🔗 <b>Links</b> ---
<a href="https://solscan.io/token/{mint}">Solscan</a> | <a href="https://gmgn.ai/sol/token/{mint}">GMGN</a> | <a href="https://dexscreener.com/solana/{mint}">DexScreener</a>"""

        # Create initial state
        initial_state = {
            "first_alert_at": datetime.now(timezone.utc).isoformat(),
            "initial_market_cap": market_cap,
            "initial_liquidity": liquidity_usd,
            "initial_holders": holder_count,
            "pair_created_at": pair_dt.isoformat() if pair_dt else None,
            "symbol": symbol,
            "name": name,
            "mint": mint,
            "image_url": image_url
        }

        return msg, initial_state, image_url

    except Exception as e:
        logger.exception(f"Error in _format_alpha_alert_async for {mint}: {e}")
        return None, None, None


async def format_alpha_refresh(mint: str, initial_state: Dict[str, Any]) -> str:
    """Format refresh message showing changes since initial alert."""
    try:
        dex_data = await _get_dexscreener_data(mint)
        if not dex_data:
            return "❌ Failed to fetch fresh data. Please try again in a moment."

        symbol = initial_state.get("symbol", "N/A")
        name = initial_state.get("name", "Unknown")
        
        try:
            initial_mc = float(initial_state.get("initial_market_cap", 0) or 0)
            initial_liq = float(initial_state.get("initial_liquidity", 0) or 0)
            first_alert_at_str = initial_state.get("first_alert_at")
            first_alert_at_dt = datetime.fromisoformat(first_alert_at_str.replace('Z', '+00:00'))
        except Exception as e:
            logger.error(f"Failed to parse initial_state for {mint}: {e}")
            return "❌ Error: Could not parse initial token data."

        current_price = float(dex_data.get("priceUsd", 0) or 0)
        current_mc = float(dex_data.get("fdv", 0) or 0)
        current_liq = float(dex_data.get("liquidity", {}).get("usd", 0) or 0)

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

        msg = f"""🔄 <b>Refresh: ${html.escape(str(symbol))}</b>
<i>Stats vs. first alert {time_elapsed_str}</i>

<b>💰 Price:</b> ${current_price:.8f}

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
    Format regular token alert with enhanced ML insights.
    
    Args:
        token_data: Token information dictionary
        alert_type: "NEW" or "CHANGE"
        previous_grade: Previous grade (for CHANGE alerts)
        initial_mc: Initial market cap
        initial_fdv: Initial FDV
        first_alert_at: Timestamp of first alert
    
    Returns:
        Formatted HTML string
    """
    token_meta = token_data.get("token_metadata") or {}
    name = token_meta.get("name") or token_data.get("token") or "Unknown"
    symbol = token_meta.get("symbol") or ""
    grade = token_data.get("grade", "NONE")
    mint = token_meta.get("mint", "") or token_data.get("token", "")

    dex_data = token_data.get("dexscreener", {})
    rugcheck_data = token_data.get("rugcheck", {})

    current_mc = dex_data.get("market_cap_usd")
    current_liquidity = rugcheck_data.get("total_liquidity_usd")
    current_fdv = current_mc 

    # Build Market Cap line
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

    # Build alert
    lines = [
        "🚀 <b>New Token Detected</b>" if alert_type == "NEW" else "🔔 <b>Grade Changed</b>",
        f"<b>{name}</b> ({symbol})" if symbol else f"<b>{name}</b>",
        f"<b>Grade:</b> {grade}" + (f" (was {previous_grade})" if previous_grade and alert_type == "CHANGE" else ""),
        mc_line,
        f"💧 <b>Liquidity:</b> {format_marketcap_display(current_liquidity)}" if current_liquidity is not None else "💧 <b>Liquidity:</b> Unknown",
        f"<b>Concentration:</b> {token_data.get('concentration')}%"
    ]

    if first_alert_at:
        lines.append(f"🕐 <b>First alert:</b> {first_alert_at[:10]}")

    lines.append("")
    if mint:
        lines.append(f"<b>Token: (tap to copy)</b> <code>{mint}</code>")

        # Add ML insight
        ml_data = token_data.get("ml_prediction", {})
        ml_section = format_ml_insight_for_alert(ml_data)
        if ml_section:
            lines.append(ml_section)
        
        lines.append("")
        lines.append(
            f'<a href="https://solscan.io/token/{mint}">Solscan</a> | '
            f'<a href="https://gmgn.ai/sol/token/{mint}">GMGN</a> | '
            f'<a href="https://dexscreener.com/solana/{mint}">DexScreener</a>'
        )

    return "\n".join(lines)