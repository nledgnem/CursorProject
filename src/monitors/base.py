"""Base monitor interface and helpers."""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from datetime import date
import pandas as pd


class MonitorBase(ABC):
    """Base class for regime monitors.
    
    All monitors must implement compute_regime() which returns a regime score (1-5)
    for a given date.
    """
    
    @abstractmethod
    def compute_regime(self, date: date, **kwargs) -> Dict[str, Any]:
        """
        Compute regime score for a given date.
        
        Args:
            date: Date to compute regime for
            **kwargs: Additional context/data needed by the monitor
            
        Returns:
            Dict with at least:
            - regime_1_5: int (1-5)
            - score_raw: float (optional, raw score before bucketing)
            - params_json: str (optional, JSON string of monitor parameters)
        """
        pass
    
    @abstractmethod
    def get_monitor_name(self) -> str:
        """Return monitor name identifier."""
        pass


def bucket_to_1_5(bucket: str) -> int:
    """Convert bucket name to numeric 1-5 scale.
    
    Mapping:
    - RED = 1 (worst)
    - ORANGE = 2
    - YELLOW = 3
    - YELLOWGREEN = 4
    - GREEN = 5 (best)
    
    Args:
        bucket: Bucket name (case-insensitive)
        
    Returns:
        Integer 1-5
    """
    mapping = {
        "RED": 1,
        "ORANGE": 2,
        "YELLOW": 3,
        "YELLOWGREEN": 4,
        "GREEN": 5,
    }
    return mapping.get(bucket.upper(), 3)  # Default to 3 if unknown


def score_to_bucket_1_5(score: float) -> int:
    """Convert regime score (0-100) to 1-5 bucket.
    
    Mapping:
    - 0-29: 1 (RED)
    - 30-44: 2 (ORANGE)
    - 45-54: 3 (YELLOW)
    - 55-69: 4 (YELLOWGREEN)
    - 70-100: 5 (GREEN)
    
    Args:
        score: Regime score (0-100 scale)
        
    Returns:
        Integer 1-5
    """
    if score >= 70:
        return 5  # GREEN
    elif score >= 55:
        return 4  # YELLOWGREEN
    elif score >= 45:
        return 3  # YELLOW
    elif score >= 30:
        return 2  # ORANGE
    else:
        return 1  # RED




