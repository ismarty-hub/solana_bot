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
    Format ML prediction data as a single compact line for alerts.
    As per bot_spec.md: "🤖 ML: 64% WIN CHANCE | Action: WATCH"
    """
    if not ml_data or not isinstance(ml_data, dict):
        return ""
    
    prob = ml_data.get("probability") or ml_data.get("win_probability")
    action = ml_data.get("action", "WATCH")
    
    if prob is None:
        return ""
    
    try:
        prob_pct = float(prob) * 100
        return f"🤖 <b>ML:</b> {prob_pct:.0f}% WIN CHANCE  |  <b>Action:</b> {action}"
    except Exception:
        return ""


def alpha_bar(score: int) -> str:
    """ASCII progress bar: 10 blocks (score/10)"""
    score = min(100, max(0, int(score)))
    filled = score // 10
    empty = 10 - filled
    return "█" * filled + "░" * empty


# ============================================================================
# SMART MONEY INSIGHT FORMATTING
# ============================================================================

def format_smart_money_insight(sm_data: Dict[str, Any], conviction: Optional[Dict[str, Any]] = None) -> str:
    """
    Format Smart Money conviction data for alerts.
    Prioritizes the new "Gold Standard" conviction summary if available.
    """
    # 1. Check for conviction summary first (new Gold Standard system)
    if conviction and conviction.get("alpha_score") is not None:
        return _format_gold_standard_conviction(conviction)
    
    # 2. Fallback to older raw sm_data / transitional conviction
    if conviction and conviction.get("sm_weighted_score") is not None:
        return _format_smart_money_from_conviction(conviction)
    
    if not sm_data or not sm_data.get("enabled"):
        return ""
    
    boost_tier = sm_data.get("boost_tier", "NONE")
    if boost_tier in ("NONE", "FILTERED"):
        return ""

    lines = ["", "--- 🧠 <b>Smart Money</b> ---"]
    
    # Conviction Score Bar
    score = sm_data.get("smart_money_weighted_score", 0.0)
    score_display = min(int(score), 100)
    filled = min(5, max(1, int(score_display / 20)))
    bar = "🟩" * filled + "⬜" * (5 - filled)
    lines.append(f"<b>Conviction:</b> {score_display}/100 {bar}")

    if sm_data.get("has_cluster"):
        cluster_len = len(sm_data.get("cluster_wallets", []))
        lines.append(f"🔥 <b>Smart Cluster:</b> {cluster_len} Entities Active")
    
    return "\n".join(lines)


def _format_gold_standard_conviction(conv: Dict[str, Any]) -> str:
    """Helper to format detailed Smart Money section (Phase 7 Redesign)."""
    overlap_count = conv.get("overlap_count", 0)
    if overlap_count == 0:
        return ""

    lines = ["", "🏆 <b>SMART MONEY</b>"]
    
    # 1. Winners & Conviction
    conv_pct = conv.get("wallet_conviction_pct", 0)
    lines.append(f"Winners holding: {overlap_count}  |  Conviction: {conv_pct}%")
    
    # 2. Tiers (Elite / Strong / Active)
    tiers = conv.get("pnl_tier_breakdown", {})
    tier_parts = []
    if tiers.get("ELITE"): tier_parts.append(f"🏅 {tiers['ELITE']} Elite")
    if tiers.get("STRONG"): tier_parts.append(f"💪 {tiers['STRONG']} Strong")
    if tiers.get("ACTIVE"): tier_parts.append(f"✅ {tiers['ACTIVE']} Active")
    
    if tier_parts:
        lines.append("  ".join(tier_parts))
        
    # 3. Aggregates (Combined PnL & Win Rate)
    profit = conv.get("cluster_combined_profit_usd")
    wr = conv.get("cluster_avg_win_rate_pct")
    
    agg_parts = []
    if profit is not None:
        agg_parts.append(f"Combined PnL: +${profit:,.0f}")
    if wr is not None:
        agg_parts.append(f"Avg Win Rate: {wr:.0f}%")
        
    if agg_parts:
        lines.append("  |  ".join(agg_parts))
        
    # 4. Insiders / Snipers
    s_count = conv.get("sniper_count", 0)
    e_count = conv.get("early_buyer_count", 0)
    
    if s_count > 0:
        lines.append(f"🎯 {s_count} Snipers bought in first 5 mins")
    elif e_count > 0:
        lines.append(f"🎯 {e_count} Early Buyers (first 30 mins)")

    return "\n".join(lines)


def _format_smart_money_from_conviction(conv: Dict[str, Any]) -> str:
    """Fallback for transitional conviction summaries."""
    lines = ["", "--- 🧠 <b>Smart Money (PnL)</b> ---"]
    
    score = conv.get("sm_weighted_score", 0.0)
    score_display = min(int(score), 100)
    filled = min(5, max(1, int(score_display / 20)))
    bar = "🟩" * filled + "⬜" * (5 - filled)
    lines.append(f"<b>Conviction:</b> {score_display}/100 {bar}")

    top_wallet = conv.get("top_pnl_wallet")
    if top_wallet:
        profit = top_wallet.get("profit_usd")
        win_rate = top_wallet.get("win_rate")
        tier = top_wallet.get("pnl_tier", "ACTIVE")
        profit_str = f"${profit:,.0f}" if profit and profit >= 1 else "Checking..."
        wr_str = f"{win_rate:.0f}%" if win_rate is not None else "N/A"
        lines.append(f"💰 <b>Best Wallet:</b> {profit_str} Realized")
        lines.append(f"🏆 <b>Win Rate:</b> {wr_str} | <b>Tier:</b> {tier}")

    tiers = conv.get("pnl_tier_breakdown", {})
    if tiers:
        elite = tiers.get("ELITE", 0)
        strong = tiers.get("STRONG", 0)
        if elite > 0 or strong > 0:
            lines.append(f"🏅 <b>Support:</b> {elite} ELITE | {strong} STRONG")

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
        
        return " ".join(parts)
    
    # If >= 24 hours, show days and hours format
    total_hours = total_seconds / 3600
    days = int(total_hours // 24)
    remaining_hours = int(total_hours % 24)
    
    if remaining_hours > 0:
        return f"{days}d {remaining_hours}h"
    else:
        return f"{days}d"


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
        
        # Fallback to fresh dex_data if image_url is missing in historian entry
        if not image_url:
            image_url = dex_data.get("info", {}).get("imageUrl")

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
            risk_label = "EXCELLENT"
        elif score_normalised <= 30:
            risk_label = "GOOD"
        elif score_normalised <= 50:
            risk_label = "MODERATE"
        else:
            risk_label = "HIGH RISK"

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

        # Get Smart Money & Conviction data
        sm_data = result.get("smart_money", {})
        conviction = entry.get("conviction_summary", {})
        sm_section = format_smart_money_insight(sm_data, conviction=conviction)
        
        # Headline Formatting
        sentiment_raw = conviction.get("trader_sentiment", "NEUTRAL")
        # Strip emoji from end if present (e.g. "BULLISH ✅") and move to front
        sentiment_clean = sentiment_raw.replace("✅", "").replace("🔥", "").replace("⭐", "").replace("⚠️", "").replace("❌", "").strip()
        
        sentiment_emoji = "📊"
        if "EXTREME" in sentiment_clean: sentiment_emoji = "🔥"
        elif "STRONG" in sentiment_clean: sentiment_emoji = "⭐"
        elif "BULLISH" in sentiment_clean: sentiment_emoji = "✅"
        elif "CAUTIOUS" in sentiment_clean: sentiment_emoji = "⚠️"
        elif "BEARISH" in sentiment_clean: sentiment_emoji = "❌"
        
        headline_sentiment = f"{sentiment_emoji} {sentiment_clean}"
        if conviction.get("is_super_alpha"):
            headline_sentiment = f"🔥 SUPER-ALPHA | {headline_sentiment}"

        alpha_score = conviction.get("alpha_score", 0)
        confidence = conviction.get("sentiment_confidence", "LOW")

        # Shorten Mint
        short_mint = f"{mint[:8]}...{mint[-4:]}" if len(mint) > 12 else mint

        # Prepare safety line
        safety_line = risk_str.split('\n')[0] if risk_str.startswith('✅') else 'See Risks'
        
        # Build the message (Phase 7 Layout)
        msg = f"""━━━━━━━━━━━━━━━━━━━━━━━━━
<b>{headline_sentiment}  |  Grade: {overlap_grade.upper()}</b>
<b>${symbol}</b>  •  {graduation_status}  •  {age_str}
━━━━━━━━━━━━━━━━━━━━━━━━━

🧠 <b>Alpha Score:</b> {alpha_score}/100  <code>{alpha_bar(alpha_score)}</code>
<b>Sentiment:</b> {headline_sentiment}  ({confidence} confidence)
{sm_section}
{ml_section}

💰 <b>MARKET</b>
<b>Price:</b> ${price_usd:.8f}  |  <b>MCap:</b> {_format_usd(market_cap)}  |  <b>Liq:</b> {_format_usd(liquidity_usd)}
<b>Vol 1h:</b> {_format_usd(pair.get('volume', {}).get('h1', 0))}  |  <b>Buy pressure:</b> {buy_pct:.0f}%  |  <b>Age:</b> {age_str}

🔒 <b>SAFETY</b>
<b>LP:</b> {lp_locked_pct:.0f}% locked  |  <b>Holders:</b> {int(holder_count):,}  |  <b>Risk score:</b> {score_normalised}/100
<b>Mint:</b> {'✅' if not mint_authority else '❌'}  <b>Freeze:</b> {'✅' if not freeze_authority else '❌'}  {safety_line}

🔗 <a href="https://solscan.io/token/{mint}">Solscan</a>  |  <a href="https://gmgn.ai/sol/token/{mint}">GMGN</a>  |  <a href="https://dexscreener.com/solana/{mint}">DexScreener</a>
━━━━━━━━━━━━━━━━━━━━━━━━━"""

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
    sm_data = token_data.get("smart_money", {})
    conviction = token_data.get("conviction_summary", {})
    
    # Alert Label Priority
    default_label = "Grade Changed" if alert_type == "CHANGE" else "New Token Detected"
    sentiment = conviction.get("trader_sentiment")
    alert_label = sentiment or conviction.get("sm_alert_label") or sm_data.get("alert_label") or default_label
    
    lines = [
        f"🔔 <b>{html.escape(alert_label)}</b>",
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
        # Add Smart Money insight
        sm_section = format_smart_money_insight(sm_data, conviction=conviction)
        if sm_section:
            lines.append(sm_section)
        
        lines.append("")
        lines.append(
            f'<a href="https://solscan.io/token/{mint}">Solscan</a> | '
            f'<a href="https://gmgn.ai/sol/token/{mint}">GMGN</a> | '
            f'<a href="https://dexscreener.com/solana/{mint}">DexScreener</a>'
        )

    return "\n".join(lines)