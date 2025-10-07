#!/usr/bin/env python3
"""
alerts/formatters.py - Alert formatting functions
"""

from typing import Optional, Dict, Any
from shared.utils import format_marketcap_display, fetch_marketcap_and_fdv, truncate_address


def format_alert_html(
    token_data: Dict[str, Any],
    alert_type: str,
    previous_grade: Optional[str] = None,
    initial_mc: Optional[float] = None,
    initial_fdv: Optional[float] = None,
    first_alert_at: Optional[str] = None
) -> str:
    """
    Format token alert as HTML message.
    
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
        lines.append(f"<b>Token:</b> {mint}")
        lines.append(
            f'<a href="https://solscan.io/token/{mint}">Solscan</a> | '
            f'<a href="https://gmgn.ai/sol/token/{mint}">GMGN</a> | '
            f'<a href="https://dexscreener.com/solana/{mint}">DexScreener</a>'
        )

    return "\n".join(lines)