"""Wrapper for existing regime monitor (OwnScripts/regime_backtest/regime_monitor.py)."""

import sys
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import date
import pandas as pd
import json

# Add OwnScripts to path to import existing monitor
repo_root = Path(__file__).parent.parent.parent
ownscripts_path = repo_root / "OwnScripts" / "regime_backtest"
sys.path.insert(0, str(ownscripts_path))

from src.monitors.base import MonitorBase, bucket_to_1_5, score_to_bucket_1_5


class ExistingMonitor(MonitorBase):
    """Wrapper for the existing regime monitor.
    
    Loads regime scores from CSV file (regime_history.csv) or can compute on-demand.
    """
    
    def __init__(self, regime_csv_path: Optional[Path] = None):
        """
        Initialize existing monitor wrapper.
        
        Args:
            regime_csv_path: Path to regime_history.csv. If None, tries default location.
        """
        if regime_csv_path is None:
            # Try default location
            default_path = ownscripts_path / "regime_history.csv"
            if default_path.exists():
                regime_csv_path = default_path
            else:
                raise ValueError(f"regime_csv_path not provided and default not found: {default_path}")
        
        self.regime_csv_path = Path(regime_csv_path)
        if not self.regime_csv_path.exists():
            raise FileNotFoundError(f"Regime CSV not found: {self.regime_csv_path}")
        
        # Load regime data
        self.regime_df = self._load_regime_data()
    
    def _load_regime_data(self) -> pd.DataFrame:
        """Load regime data from CSV and convert to standard format."""
        df = pd.read_csv(self.regime_csv_path)
        
        # Parse date column (could be date_iso or date)
        date_col = None
        for col in ["date_iso", "date"]:
            if col in df.columns:
                date_col = col
                break
        
        if date_col is None:
            raise ValueError("No date column found (expected date_iso or date)")
        
        # Convert to date index (handle ISO8601 format with microseconds)
        df[date_col] = pd.to_datetime(df[date_col], format='ISO8601')
        # Normalize to timezone-naive date-only index (for alignment with LS returns)
        df[date_col] = df[date_col].dt.tz_localize(None)  # Remove timezone
        df[date_col] = pd.to_datetime(df[date_col]).dt.date  # Convert to date
        df = df.set_index(date_col)
        df.index = pd.to_datetime(df.index)  # Convert back to DatetimeIndex (timezone-naive)
        df.index.name = "date"
        
        # Convert bucket to regime_1_5
        if "bucket" in df.columns:
            df["regime_1_5"] = df["bucket"].apply(bucket_to_1_5)
        elif "regime_score" in df.columns:
            # Convert score to bucket
            df["regime_1_5"] = df["regime_score"].apply(score_to_bucket_1_5)
        else:
            raise ValueError("No bucket or regime_score column found")
        
        # Keep score_raw if available
        if "regime_score" in df.columns:
            df["score_raw"] = df["regime_score"]
        
        return df
    
    def get_monitor_name(self) -> str:
        return "existing_regime_monitor"
    
    def compute_regime(self, date: date, **kwargs) -> Dict[str, Any]:
        """
        Get regime score for a given date from loaded CSV data.
        
        Args:
            date: Date to get regime for
            **kwargs: Ignored
            
        Returns:
            Dict with regime_1_5, score_raw (if available), params_json
        """
        # Convert date to datetime for matching
        if isinstance(date, date) and not isinstance(date, pd.Timestamp):
            date_dt = pd.Timestamp(date)
        else:
            date_dt = date
        
        # Find matching row (exact date match)
        matches = self.regime_df[self.regime_df.index.date == date_dt.date()]
        
        if len(matches) == 0:
            raise ValueError(f"No regime data found for date {date}")
        
        row = matches.iloc[0]
        
        result = {
            "regime_1_5": int(row["regime_1_5"]),
        }
        
        if "score_raw" in row:
            result["score_raw"] = float(row["score_raw"])
        
        # Add params_json if available (could include monitor parameters)
        result["params_json"] = json.dumps({"source": "existing_monitor"})
        
        return result
    
    def get_regime_series(self, start_date: Optional[date] = None, end_date: Optional[date] = None) -> pd.DataFrame:
        """
        Get regime series as DataFrame for evaluation.
        
        Args:
            start_date: Start date (optional filter)
            end_date: End date (optional filter)
            
        Returns:
            DataFrame with date index and columns: regime_1_5, score_raw (if available), monitor_name
        """
        df = self.regime_df.copy()
        
        if start_date:
            df = df[df.index >= pd.Timestamp(start_date)]
        if end_date:
            df = df[df.index <= pd.Timestamp(end_date)]
        
        result = pd.DataFrame(index=df.index)
        result["regime_1_5"] = df["regime_1_5"]
        if "score_raw" in df.columns:
            result["score_raw"] = df["score_raw"]
        result["monitor_name"] = self.get_monitor_name()
        
        return result

