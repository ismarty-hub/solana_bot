#!/usr/bin/env python3
"""
alerts/ml_client.py - ML API client for token predictions with enhanced UX
"""

import logging
import aiohttp
import asyncio
from typing import Dict, Any, Optional, List
from config import ML_API_URL, ML_API_TIMEOUT

logger = logging.getLogger(__name__)

class MLAPIClient:
    """Client for interacting with the ML prediction API."""
    
    def __init__(self, base_url: str = ML_API_URL, timeout: int = ML_API_TIMEOUT):
        self.base_url = base_url.rstrip('/')
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self._session = None
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self.timeout)
        return self._session
    
    async def close(self):
        """Close the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()
    
    async def predict_token(
        self, 
        mint: str, 
        threshold: float = 0.70
    ) -> Optional[Dict[str, Any]]:
        """
        Get ML prediction for a single token.
        
        Args:
            mint: Token mint address
            threshold: Probability threshold for BUY signal (default 0.70)
        
        Returns:
            Dictionary with prediction results or None if failed
        """
        try:
            session = await self._get_session()
            url = f"{self.base_url}/token/{mint}/predict"
            params = {"threshold": threshold}
            
            logger.info(f"🤖 Requesting ML prediction for {mint[:8]}...")
            
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"✅ Got ML prediction for {mint[:8]}")
                    return data
                elif response.status == 503:
                    logger.warning(f"⚠️ ML service unavailable for {mint[:8]}")
                    return None
                else:
                    error_text = await response.text()
                    logger.error(
                        f"❌ ML API error for {mint[:8]}: "
                        f"status={response.status}, error={error_text}"
                    )
                    return None
                    
        except asyncio.TimeoutError:
            logger.error(f"⏱️ ML API timeout for {mint[:8]}")
            return None
        except Exception as e:
            logger.exception(f"❌ ML API request failed for {mint[:8]}: {e}")
            return None
    
    async def predict_batch(
        self, 
        mints: List[str], 
        threshold: float = 0.70
    ) -> Optional[Dict[str, Any]]:
        """
        Get ML predictions for multiple tokens (max 10).
        
        Args:
            mints: List of token mint addresses (max 10)
            threshold: Probability threshold for BUY signal
        
        Returns:
            Dictionary with batch prediction results or None if failed
        """
        if len(mints) > 10:
            logger.warning(f"⚠️ Batch size {len(mints)} exceeds limit, truncating to 10")
            mints = mints[:10]
        
        try:
            session = await self._get_session()
            url = f"{self.base_url}/token/predict/batch"
            params = {"threshold": threshold}
            payload = mints
            
            logger.info(f"🤖 Requesting batch ML prediction for {len(mints)} tokens...")
            
            async with session.post(url, json=payload, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info(
                        f"✅ Got batch predictions: "
                        f"{data.get('successful_predictions', 0)}/{len(mints)} successful"
                    )
                    return data
                elif response.status == 503:
                    logger.warning("⚠️ ML service unavailable for batch prediction")
                    return None
                else:
                    error_text = await response.text()
                    logger.error(
                        f"❌ Batch ML API error: "
                        f"status={response.status}, error={error_text}"
                    )
                    return None
                    
        except asyncio.TimeoutError:
            logger.error("⏱️ ML API batch timeout")
            return None
        except Exception as e:
            logger.exception(f"❌ Batch ML API request failed: {e}")
            return None
    
    async def get_ml_status(self) -> Optional[Dict[str, Any]]:
        """Get ML service status and model information."""
        try:
            session = await self._get_session()
            url = f"{self.base_url}/ml/status"
            
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return data
                else:
                    logger.warning(f"⚠️ ML status check failed: {response.status}")
                    return None
                    
        except Exception as e:
            logger.exception(f"❌ ML status check failed: {e}")
            return None


def _format_currency(value: float) -> str:
    """Format currency values with K/M suffixes."""
    if value >= 1_000_000:
        return f"${value/1_000_000:.2f}M"
    elif value >= 1_000:
        return f"${value/1_000:.1f}K"
    else:
        return f"${value:.2f}"


def _format_age(hours: float) -> str:
    """Format token age in human-readable format."""
    if hours < 1:
        return f"{int(hours * 60)}m"
    elif hours < 24:
        return f"{hours:.1f}h"
    else:
        days = hours / 24
        return f"{days:.1f}d"


def _get_risk_emoji(score: float) -> str:
    """Get emoji for risk score."""
    if score <= 20:
        return "🟢"
    elif score <= 40:
        return "🟡"
    elif score <= 60:
        return "🟠"
    else:
        return "🔴"


def _get_health_emoji(score: float) -> str:
    """Get emoji for health score."""
    if score >= 70:
        return "💪"
    elif score >= 50:
        return "👍"
    elif score >= 30:
        return "👌"
    else:
        return "⚠️"


def format_ml_prediction(prediction_data: Dict[str, Any], show_full: bool = True) -> str:
    """
    Format ML prediction data for Telegram with enhanced UX.
    
    Args:
        prediction_data: Prediction data from API
        show_full: If True, show all details; if False, show compact version
    
    Returns:
        Formatted HTML string
    """
    if not prediction_data:
        return "🤖 <i>ML prediction unavailable</i>"
    
    prediction = prediction_data.get("prediction", {})
    
    # Extract key fields
    action = prediction.get("action", "UNKNOWN")
    win_prob = prediction.get("win_probability", 0.0)
    confidence = prediction.get("confidence", "N/A")
    risk_tier = prediction.get("risk_tier", "N/A")
    
    # Action emoji and styling
    action_styles = {
        "BUY": ("🟢", "STRONG BUY"),
        "CONSIDER": ("🟡", "CONSIDER"),
        "SKIP": ("🟠", "SKIP"),
        "AVOID": ("🔴", "AVOID")
    }
    emoji, action_text = action_styles.get(action, ("⚪", action))
    
    if show_full:
        key_metrics = prediction.get("key_metrics", {})
        warnings = prediction.get("warnings", [])
        
        # Build header with clear verdict
        msg_parts = [
            f"🤖 <b>ML Token Analysis</b>",
            f"{emoji} <b>Verdict: {action_text}</b>\n",
            f"<b>📊 Win Probability:</b> {win_prob*100:.1f}%",
            f"<b>🎯 Confidence:</b> {confidence}",
            f"<b>🛡️ Risk Level:</b> {risk_tier}\n"
        ]
        
        # Token Basics
        if key_metrics:
            token_age = key_metrics.get("token_age_hours", 0)
            price_change = key_metrics.get("price_change_h24_pct", 0)
            
            msg_parts.append("<b>⏰ Token Basics</b>")
            msg_parts.append(f"• Age: {_format_age(token_age)}")
            
            price_emoji = "📈" if price_change > 0 else "📉"
            msg_parts.append(f"• 24h Price Change: {price_emoji} {price_change:+.1f}%\n")
            
            # Market Health
            liquidity = key_metrics.get("liquidity_usd", 0)
            volume = key_metrics.get("volume_h24_usd", 0)
            health_score = key_metrics.get("market_health_score", 0)
            
            health_emoji = _get_health_emoji(health_score)
            
            msg_parts.append(f"<b>💹 Market Health {health_emoji}</b>")
            msg_parts.append(f"• Liquidity: {_format_currency(liquidity)}")
            msg_parts.append(f"• 24h Volume: {_format_currency(volume)}")
            msg_parts.append(f"• Health Score: {health_score:.0f}/100\n")
            
            # Holder Analysis
            insider_pct = key_metrics.get("insider_supply_pct", 0)
            top10_pct = key_metrics.get("top_10_holders_pct", 0)
            
            # Determine holder risk level
            if insider_pct > 20 or top10_pct > 70:
                holder_risk = "🔴 HIGH RISK"
            elif insider_pct > 10 or top10_pct > 50:
                holder_risk = "🟡 MODERATE"
            else:
                holder_risk = "🟢 HEALTHY"
            
            msg_parts.append(f"<b>👥 Holder Distribution {holder_risk}</b>")
            msg_parts.append(f"• Insider Control: {insider_pct:.1f}%")
            msg_parts.append(f"• Top 10 Holders: {top10_pct:.1f}%\n")
            
            # Risk Assessment
            pump_risk = key_metrics.get("pump_dump_risk_score", 0)
            risk_emoji = _get_risk_emoji(pump_risk)
            
            msg_parts.append(f"<b>⚠️ Risk Analysis {risk_emoji}</b>")
            msg_parts.append(f"• Pump/Dump Risk: {pump_risk:.0f}/100")
            
            # Risk interpretation
            if pump_risk <= 20:
                risk_text = "Very Low ✅"
            elif pump_risk <= 40:
                risk_text = "Low 👍"
            elif pump_risk <= 60:
                risk_text = "Moderate ⚠️"
            elif pump_risk <= 80:
                risk_text = "High 🚨"
            else:
                risk_text = "Very High ❌"
            
            msg_parts.append(f"• Assessment: {risk_text}\n")
        
        # Warnings Section (only if warnings exist)
        if warnings:
            msg_parts.append("<b>🚨 Important Warnings</b>")
            
            # Categorize warnings
            critical_warnings = []
            other_warnings = []
            
            for warning in warnings:
                warning_lower = warning.lower()
                if any(keyword in warning_lower for keyword in ['high', 'dump', 'extreme', 'locked', 'authority']):
                    critical_warnings.append(f"🔴 {warning}")
                else:
                    other_warnings.append(f"⚠️ {warning}")
            
            # Show critical first
            for w in critical_warnings[:3]:
                msg_parts.append(w)
            for w in other_warnings[:2]:
                msg_parts.append(w)
            
            remaining = len(warnings) - len(critical_warnings[:3]) - len(other_warnings[:2])
            if remaining > 0:
                msg_parts.append(f"<i>...and {remaining} more warnings</i>")
            
            msg_parts.append("")
        
        # Trading Recommendation
        msg_parts.append("<b>💡 Recommendation</b>")
        
        if action == "BUY":
            msg_parts.append(
                f"✅ <b>Strong signal detected</b>\n"
                f"Win probability is {win_prob*100:.1f}% with {confidence} confidence.\n"
                f"Consider entering with appropriate position sizing."
            )
        elif action == "CONSIDER":
            msg_parts.append(
                f"🟡 <b>Moderate signal</b>\n"
                f"Token shows potential but proceed with caution.\n"
                f"Review warnings carefully before trading."
            )
        elif action == "SKIP":
            msg_parts.append(
                f"🟠 <b>Weak signal</b>\n"
                f"Risk may outweigh potential reward.\n"
                f"Better opportunities likely available."
            )
        else:  # AVOID
            msg_parts.append(
                f"🔴 <b>High risk detected</b>\n"
                f"Model recommends avoiding this token.\n"
                f"Multiple risk factors identified."
            )
        
        return "\n".join(msg_parts)
    
    else:
        # Compact inline format
        return (
            f"🤖 {emoji} <b>{action}</b> • "
            f"Win: {win_prob*100:.0f}% • "
            f"{confidence} • "
            f"{risk_tier}"
        )


# Global ML client instance
_ml_client = None

async def get_ml_client() -> MLAPIClient:
    """Get or create global ML client instance."""
    global _ml_client
    if _ml_client is None:
        _ml_client = MLAPIClient()
    return _ml_client

async def close_ml_client():
    """Close global ML client instance."""
    global _ml_client
    if _ml_client:
        await _ml_client.close()
        _ml_client = None