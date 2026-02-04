"""Funding persistence monitor (stub for future implementation)."""

from src.monitors.base import MonitorBase
from typing import Dict, Any
from datetime import date


class FundingPersistenceMonitor(MonitorBase):
    """Monitor based on funding rate persistence (stub)."""
    
    def get_monitor_name(self) -> str:
        return "funding_persistence"
    
    def compute_regime(self, date: date, **kwargs) -> Dict[str, Any]:
        """Stub implementation."""
        raise NotImplementedError("Funding persistence monitor not yet implemented")





