"""
shared/__init__.py

Shared utilities package for the Solana bot.
"""

from .file_io import safe_load, safe_save
from .utils import truncate_address, format_marketcap_display, fetch_marketcap_and_fdv

__all__ = [
    'safe_load',
    'safe_save',
    'truncate_address',
    'format_marketcap_display',
    'fetch_marketcap_and_fdv',
]