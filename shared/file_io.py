"""
shared/file_io.py

Thread-safe file I/O operations using joblib.
Used by both alerts and trading modules.
"""

import joblib
import logging
from pathlib import Path
from threading import Lock
from typing import Any

# Global file lock for thread-safe operations
file_lock = Lock()


def safe_load(path: Path, default: Any) -> Any:
    """
    Thread-safe load from pickle file.
    
    Args:
        path: Path to the pickle file
        default: Default value to return if file doesn't exist or load fails
        
    Returns:
        Loaded data or default value
    """
    with file_lock:
        try:
            if not path.exists():
                return default
            return joblib.load(path)
        except Exception as e:
            logging.exception(f"Failed loading {path}: {e}")
            return default


def safe_save(path: Path, data: Any) -> bool:
    """
    Thread-safe save to pickle file.
    
    Args:
        path: Path to the pickle file
        data: Data to save
        
    Returns:
        True if save was successful, False otherwise
    """
    with file_lock:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            joblib.dump(data, path)
            return True
        except Exception as e:
            logging.exception(f"Failed saving {path}: {e}")
            return False