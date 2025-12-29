"""
shared/file_io.py

Thread-safe file I/O operations.
Supports both JSON (.json) and pickle (.pkl) files.
"""

import json
import joblib
import logging
from pathlib import Path
from threading import Lock
from typing import Any

# Global file lock for thread-safe operations
file_lock = Lock()


def safe_load(path: Path, default: Any) -> Any:
    """
    Thread-safe load from file.
    Automatically detects JSON vs pickle based on file extension.
    
    Args:
        path: Path to the file
        default: Default value to return if file doesn't exist or load fails
        
    Returns:
        Loaded data or default value
    """
    with file_lock:
        try:
            path = Path(path)  # Ensure it's a Path object
            if not path.exists():
                return default
            
            # Detect file type by extension
            if path.suffix.lower() == '.json':
                with open(path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                # Default to joblib for .pkl and other files
                return joblib.load(path)
                
        except Exception as e:
            logging.exception(f"Failed loading {path}: {e}")
            return default


def safe_save(path: Path, data: Any) -> bool:
    """
    Thread-safe save to file.
    Automatically detects JSON vs pickle based on file extension.
    
    Args:
        path: Path to the file
        data: Data to save
        
    Returns:
        True if save was successful, False otherwise
    """
    with file_lock:
        try:
            path = Path(path)  # Ensure it's a Path object
            path.parent.mkdir(parents=True, exist_ok=True)
            
            # Detect file type by extension
            if path.suffix.lower() == '.json':
                with open(path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, default=str)
            else:
                # Default to joblib for .pkl and other files
                joblib.dump(data, path)
            
            return True
        except Exception as e:
            logging.exception(f"Failed saving {path}: {e}")
            return False