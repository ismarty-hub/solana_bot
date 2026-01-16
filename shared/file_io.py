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
    Process-safe and Thread-safe save to file.
    Uses atomic rename (os.replace) to prevent file corruption.
    Automatically detects JSON vs pickle based on file extension.
    
    Args:
        path: Path to the file
        data: Data to save
        
    Returns:
        True if save was successful, False otherwise
    """
    
    with file_lock:
        temp_fd = None
        temp_path = None
        try:
            path = Path(path)
            path.parent.mkdir(parents=True, exist_ok=True)
            
            # Create a temporary file in the same directory as the target
            # This ensures they are on the same filesystem for os.replace to be atomic
            temp_dir = path.parent
            fd, temp_path = tempfile.mkstemp(dir=temp_dir, suffix=".tmp")
            temp_fd = os.fdopen(fd, 'w' if path.suffix.lower() == '.json' else 'wb', encoding='utf-8' if path.suffix.lower() == '.json' else None)
            
            if path.suffix.lower() == '.json':
                json.dump(data, temp_fd, indent=2, default=str)
            else:
                joblib.dump(data, temp_fd)
            
            temp_fd.flush()
            os.fsync(temp_fd.fileno())
            temp_fd.close()
            temp_fd = None
            
            # Atomic swap
            os.replace(temp_path, path)
            return True
            
        except Exception as e:
            logging.exception(f"Failed atomic save to {path}: {e}")
            if temp_fd:
                temp_fd.close()
            if temp_path and os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except Exception:
                    pass
            return False