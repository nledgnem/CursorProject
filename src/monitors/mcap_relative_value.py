"""Market cap relative value monitor (stub for future implementation)."""

from src.monitors.base import MonitorBase
from typing import Dict, Any
from datetime import date


class McapRelativeValueMonitor(MonitorBase):
    """Monitor based on market cap relative value (stub)."""
    
    def get_monitor_name(self) -> str:
        return "mcap_relative_value"
    
    def compute_regime(self, date: date, **kwargs) -> Dict[str, Any]:
        """Stub implementation."""
        raise NotImplementedError("Market cap relative value monitor not yet implemented")





