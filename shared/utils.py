"""
shared/utils.py

Utility functions shared across alerts and trading modules.
"""

import logging
import requests
from typing import Optional, Tuple


def truncate_address(addr: str, length: int = 6) -> str:
    """
    Truncate a Solana token address for display.
    
    Args:
        addr: Full token address
        length: Number of characters to show at start and end
        
    Returns:
        Truncated address like "ABC123...XYZ789"
        
    Example:
        >>> truncate_address("G8cGYUUdnwvQ8W1iMy37TMD2xpMnYS4NCh1YKQJepump")
        "G8cGYU...Jepump"
    """
    if not addr or len(addr) <= length * 2:
        return addr
    return f"{addr[:length]}...{addr[-length:]}"


def format_marketcap_display(value: Optional[float]) -> str:
    """
    Format market cap/FDV/liquidity values for display.
    
    Args:
        value: Numeric value in USD
        
    Returns:
        Formatted string with appropriate suffix (B/M/K)
        
    Example:
        >>> format_marketcap_display(1500000)
        "$1.50M"
        >>> format_marketcap_display(2500000000)
        "$2.50B"
    """
    if value is None:
        return "N/A"
    if value >= 1e9:
        return f"${value / 1e9:.2f}B"
    elif value >= 1e6:
        return f"${value / 1e6:.2f}M"
    elif value >= 1e3:
        return f"${value / 1e3:.2f}K"
    else:
        return f"${value:.2f}"


def fetch_marketcap_and_fdv(mint: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Fetch current market cap, FDV, and liquidity from DexScreener API.
    
    Args:
        mint: Token mint address
        
    Returns:
        Tuple of (market_cap, fdv, liquidity_usd) or (None, None, None) on error
        
    Example:
        >>> mc, fdv, liq = fetch_marketcap_and_fdv("ABC123...")
        >>> print(f"MC: ${mc:,.2f}, Liquidity: ${liq:,.2f}")
    """
    try:
        if not mint:
            return None, None, None

        url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
        resp = requests.get(url, timeout=10)

        if resp.status_code != 200:
            return None, None, None

        data = resp.json()
        pairs = data.get("pairs", [])
        if not pairs:
            return None, None, None

        # Use first pair (usually most liquid)
        pair = pairs[0]
        mc = pair.get("marketCap")
        fdv = pair.get("fdv")

        # Safely extract liquidity
        liquidity = pair.get("liquidity", {})
        lqd = liquidity.get("usd")
        lqd_float = float(lqd) if lqd is not None else None

        return mc, fdv, lqd_float

    except Exception as e:
        logging.error(f"Error fetching marketcap for {mint}: {e}")
        return None, None, None