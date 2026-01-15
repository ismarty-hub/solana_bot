#!/usr/bin/env python3
"""
shared/tracking_utils.py - Utilities for tracking duration calculation.

Used by both alert deduplication and trade position tracking.
"""

from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional


def calculate_tracking_duration_hours(token_age_hours: float) -> int:
    """
    Calculate tracking duration based on token age at signal time.
    
    Args:
        token_age_hours: Token age in hours at the time of signal
        
    Returns:
        Tracking duration in hours (24 for young tokens, 168 for mature)
    """
    if token_age_hours < 12:
        return 24   # Young token: 24h tracking
    else:
        return 168  # Mature token: 7 days tracking


def get_token_age_hours(dex_data: Optional[Dict[str, Any]]) -> float:
    """
    Calculate token age in hours from dexscreener pairCreatedAt.
    
    Args:
        dex_data: Dexscreener data dict containing pairCreatedAt
        
    Returns:
        Token age in hours, or 999 if unknown (defaults to mature/7-day tracking)
    """
    if not dex_data:
        return 999.0  # Default to mature (7 days tracking)
    
    created_at = dex_data.get("pairCreatedAt")
    if not created_at:
        return 999.0
    
    try:
        # Handle both ISO format strings and Unix timestamps
        if isinstance(created_at, (int, float)):
            created_dt = datetime.fromtimestamp(created_at / 1000, tz=timezone.utc)
        else:
            created_dt = datetime.fromisoformat(str(created_at).rstrip("Z")).replace(tzinfo=timezone.utc)
        
        age_seconds = (datetime.now(timezone.utc) - created_dt).total_seconds()
        return max(0, age_seconds / 3600)
    except Exception:
        return 999.0  # Default to mature on parse error


def calculate_dedup_expiry(dex_data: Optional[Dict[str, Any]]) -> str:
    """
    Calculate deduplication expiry timestamp based on token age.
    
    Args:
        dex_data: Dexscreener data dict containing pairCreatedAt
        
    Returns:
        ISO format expiry timestamp string
    """
    token_age = get_token_age_hours(dex_data)
    duration_hours = calculate_tracking_duration_hours(token_age)
    expiry = datetime.now(timezone.utc) + timedelta(hours=duration_hours)
    return expiry.isoformat()


def is_dedup_expired(expires_at: Optional[str]) -> bool:
    """
    Check if a deduplication expiry timestamp has passed.
    
    Args:
        expires_at: ISO format expiry timestamp string
        
    Returns:
        True if expired (eligible for re-alert), False otherwise
    """
    if not expires_at:
        return False  # No expiry set = never expires (backwards compat)
    
    try:
        expires_dt = datetime.fromisoformat(expires_at.rstrip("Z")).replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) > expires_dt
    except Exception:
        return False  # Parse error = don't expire
