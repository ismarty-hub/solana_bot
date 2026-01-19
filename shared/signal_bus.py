
from collections import deque
from threading import Lock
from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)

class SignalBus:
    """
    In-memory signal queue for instant signal propagation between 
    analytics_tracker (producer) and analytics_monitoring (consumer).
    
    This eliminates the polling delay from file-based communication.
    """
    # Ring buffer to hold recent signals (prevent memory leaks if consumer is slow)
    _signals = deque(maxlen=200) 
    _lock = Lock()
    
    @classmethod
    def push_signal(cls, token_data: Dict[str, Any]):
        """Push a new signal to the bus."""
        with cls._lock:
            # Add timestamp arrival if not present, useful for debugging latency
            if "_bus_arrival" not in token_data:
                import time
                token_data["_bus_arrival"] = time.time()
                
            cls._signals.append(token_data)
            logger.debug(f"SignalBus: Pushed signal for {token_data.get('mint', 'unknown')}")
    
    @classmethod
    def pop_all(cls) -> List[Dict[str, Any]]:
        """Get all pending signals and clear the queue."""
        with cls._lock:
            if not cls._signals:
                return []
                
            signals = list(cls._signals)
            cls._signals.clear()
            return signals

    @classmethod
    def peek_count(cls) -> int:
        """Check how many signals are waiting (without removing)."""
        with cls._lock:
            return len(cls._signals)
