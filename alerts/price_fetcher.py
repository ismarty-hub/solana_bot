#!/usr/bin/env python3
"""
alerts/price_fetcher.py - Fetch token prices from Jupiter and DexScreener

This module handles fetching token information (price, symbol, name) given a mint address.
It prioritizes Jupiter API for speed and falls back to DexScreener for coverage.
"""

import aiohttp
import logging
import asyncio
from typing import Dict, Optional, Any

logger = logging.getLogger(__name__)

class PriceFetcher:
    """Fetcher for token prices using Jupiter and DexScreener APIs."""
    
    JUPITER_API_URL = "https://api.jup.ag/price/v2?ids={mint}"
    DEXSCREENER_API_URL = "https://api.dexscreener.com/latest/dex/tokens/{mint}"
    RUGCHECK_API_URL = "https://api.rugcheck.xyz/v1/tokens/{mint}/report"
    
    @classmethod
    async def get_token_info(cls, mint: str) -> Optional[Dict[str, Any]]:
        """
        Fetch token info (price, symbol, name) for a given mint address.
        
        Returns:
            dict: {
                "price": float,
                "symbol": str,
                "name": str,
                "source": str ("jupiter" or "dexscreener")
            }
            or None if not found.
        """
        async with aiohttp.ClientSession() as session:
            # 1. Try Jupiter API first (Fastest)
            try:
                jup_data = await cls._fetch_jupiter(session, mint)
                if jup_data:
                    return jup_data
            except Exception as e:
                logger.warning(f"Jupiter API failed for {mint}: {e}")
            
            # 2. Fallback to DexScreener (More comprehensive)
            try:
                dex_data = await cls._fetch_dexscreener(session, mint)
                if dex_data:
                    return dex_data
            except Exception as e:
                logger.warning(f"DexScreener API failed for {mint}: {e}")
                
        return None

    @classmethod
    async def _fetch_jupiter(cls, session: aiohttp.ClientSession, mint: str) -> Optional[Dict[str, Any]]:
        """Fetch price from Jupiter v2 API."""
        url = cls.JUPITER_API_URL.format(mint=mint)
        async with session.get(url, timeout=5) as response:
            if response.status == 200:
                data = await response.json()
                # Jupiter v2 response format:
                # { "data": { "mint": { "id": "mint", "type": "token", "price": "1.23" } } }
                token_data = data.get("data", {}).get(mint)
                
                if token_data and token_data.get("price"):
                    return {
                        "price": float(token_data["price"]),
                        "symbol": "UNKNOWN", # Jupiter v2 price API often doesn't return symbol/name
                        "name": "Unknown Token",
                        "source": "jupiter"
                    }
        return None

    @classmethod
    async def _fetch_dexscreener(cls, session: aiohttp.ClientSession, mint: str) -> Optional[Dict[str, Any]]:
        """Fetch price and metadata from DexScreener API."""
        url = cls.DEXSCREENER_API_URL.format(mint=mint)
        async with session.get(url, timeout=10) as response:
            if response.status == 200:
                data = await response.json()
                pairs = data.get("pairs", [])
                
                if pairs:
                    # Get the most liquid pair (usually the first one)
                    best_pair = pairs[0]
                    return {
                        "price": float(best_pair.get("priceUsd", 0)),
                        "symbol": best_pair.get("baseToken", {}).get("symbol", "UNKNOWN"),
                        "name": best_pair.get("baseToken", {}).get("name", "Unknown Token"),
                        "source": "dexscreener",
                        # Extra details
                        "fdv": float(best_pair.get("fdv", 0)),
                        "volume24h": float(best_pair.get("volume", {}).get("h24", 0)),
                        "liquidity": float(best_pair.get("liquidity", {}).get("usd", 0)),
                        "price_change_24h": float(best_pair.get("priceChange", {}).get("h24", 0))
                    }
        return None

    @classmethod
    async def get_rugcheck_analysis(cls, mint: str) -> Optional[Dict[str, Any]]:
        """
        Fetch comprehensive security analysis from RugCheck API.
        """
        async with aiohttp.ClientSession() as session:
            try:
                url = cls.RUGCHECK_API_URL.format(mint=mint)
                async with session.get(url, timeout=10) as response:
                    if response.status != 200:
                        return None
                        
                    data = await response.json()
                    
                    # Parse RugCheck response
                    analysis = {
                        "score": data.get("score", 0),
                        "risks": data.get("risks", []),
                        "mint_authority": data.get("mintAuthority"),
                        "freeze_authority": data.get("freezeAuthority"),
                        "graph_insiders": data.get("graphInsidersDetected", 0),
                    }
                    
                    # Token meta
                    token_meta = data.get("tokenMeta", {})
                    analysis["token_name"] = token_meta.get("name", "Unknown")
                    analysis["token_symbol"] = token_meta.get("symbol", "UNKNOWN")
                    analysis["is_mutable"] = token_meta.get("mutable", True)
                    
                    # Top holders
                    top_holders = data.get("topHolders", [])
                    if top_holders:
                        # Sum top 10 holders percentage
                        analysis["top_holders_pct"] = sum(h.get("pct", 0) for h in top_holders[:10])
                        # Get top 1 holder pct
                        analysis["top_holder_pct"] = top_holders[0].get("pct", 0) if len(top_holders) > 0 else 0
                    else:
                        analysis["top_holders_pct"] = 0.0
                        analysis["top_holder_pct"] = 0.0
                    
                    # Markets/Liquidity
                    markets = data.get("markets", [])
                    total_locked_usd = 0
                    total_liquidity_usd = 0
                    lp_locked_pct = 0
                    
                    # Try to get LP locked % from the first market (usually the main one)
                    if markets:
                        first_market = markets[0]
                        lp = first_market.get("lp", {})
                        lp_locked_pct = lp.get("lpLockedPct", 0)
                        total_liquidity_usd = lp.get("liquidityUSD", 0)
                    
                    analysis["liquidity_locked_pct"] = lp_locked_pct
                    
                    # Insider/Creator information
                    analysis["insider_wallets_count"] = 0
                    analysis["insider_supply_pct"] = 0.0
                    analysis["dev_supply_pct"] = 0.0
                    analysis["dev_sold"] = False
                    
                    # Check for specific risk indicators
                    for risk in analysis["risks"]:
                        risk_name = risk.get("name", "").lower()
                        risk_description = risk.get("description", "").lower()
                        risk_value = risk.get("value")
                        
                        if "insider" in risk_name or "creator" in risk_name:
                            if isinstance(risk_value, (int, float)):
                                if "wallet" in risk_description:
                                    analysis["insider_wallets_count"] = int(risk_value)
                                elif "supply" in risk_description or "%" in risk_description:
                                    analysis["insider_supply_pct"] = float(risk_value)
                        
                        if "creator" in risk_name or "dev" in risk_name:
                            if "sold" in risk_description:
                                analysis["dev_sold"] = True
                            if isinstance(risk_value, (int, float)) and "%" in str(risk):
                                analysis["dev_supply_pct"] = float(risk_value)
                    
                    return analysis
                    
            except Exception as e:
                logger.error(f"RugCheck API error for {mint}: {e}")
                return None


# Simple test if run directly
if __name__ == "__main__":
    async def test():
        # Test with SOL
        sol_mint = "So11111111111111111111111111111111111111112"
        print(f"Fetching SOL ({sol_mint})...")
        info = await PriceFetcher.get_token_info(sol_mint)
        print(f"Result: {info}")
        
        # Test with a meme coin (e.g., BONK)
        bonk_mint = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"
        print(f"Fetching BONK ({bonk_mint})...")
        info = await PriceFetcher.get_token_info(bonk_mint)
        print(f"Result: {info}")

    logging.basicConfig(level=logging.INFO)
    asyncio.run(test())
