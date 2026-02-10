"""Test QC gap filling functionality."""

import pytest
import pandas as pd
import numpy as np
from datetime import date, timedelta
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from qc_curate import apply_repairs, QC_CONFIG


def test_single_day_gap_fill():
    """Test that 1-day gaps are filled via forward fill."""
    dates = pd.date_range("2024-01-01", periods=5, freq="D")
    
    # Series with 1-day gap on day 2
    prices = [100.0, 105.0, np.nan, 110.0, 115.0]
    
    df = pd.DataFrame({
        "GAP": prices,
    }, index=dates)
    
    repair_log = []
    config = QC_CONFIG.copy()
    config["allow_ffill"] = True
    config["max_ffill_days"] = 2
    
    curated_df = apply_repairs(df, "prices", repair_log, config)
    
    # Gap should be filled with previous value (105.0)
    assert curated_df.loc[dates[2], "GAP"] == 105.0
    
    # Repair log should contain ffill entry
    ffill_entries = [entry for entry in repair_log 
                     if entry["action"] == "ffill" and entry["rule"] == "missing_gap"]
    assert len(ffill_entries) == 1
    assert ffill_entries[0]["symbol"] == "GAP"
    assert ffill_entries[0]["date"] == str(dates[2])
    assert ffill_entries[0]["old_value"] is None
    assert ffill_entries[0]["new_value"] == 105.0


def test_two_day_gap_fill():
    """Test that 2-day gaps are filled."""
    dates = pd.date_range("2024-01-01", periods=5, freq="D")
    
    # Series with 2-day gap
    prices = [100.0, 105.0, np.nan, np.nan, 115.0]
    
    df = pd.DataFrame({
        "GAP": prices,
    }, index=dates)
    
    repair_log = []
    config = QC_CONFIG.copy()
    config["allow_ffill"] = True
    config["max_ffill_days"] = 2
    
    curated_df = apply_repairs(df, "prices", repair_log, config)
    
    # Both gap days should be filled with 105.0
    assert curated_df.loc[dates[2], "GAP"] == 105.0
    assert curated_df.loc[dates[3], "GAP"] == 105.0
    
    # Repair log should contain 2 ffill entries
    ffill_entries = [entry for entry in repair_log 
                     if entry["action"] == "ffill" and entry["rule"] == "missing_gap"]
    assert len(ffill_entries) == 2


def test_large_gap_not_filled():
    """Test that gaps larger than max_ffill_days are NOT filled."""
    dates = pd.date_range("2024-01-01", periods=6, freq="D")
    
    # Series with 3-day gap (exceeds max_ffill_days=2)
    prices = [100.0, 105.0, np.nan, np.nan, np.nan, 120.0]
    
    df = pd.DataFrame({
        "LARGE_GAP": prices,
    }, index=dates)
    
    repair_log = []
    config = QC_CONFIG.copy()
    config["allow_ffill"] = True
    config["max_ffill_days"] = 2  # Only fill up to 2 days
    
    curated_df = apply_repairs(df, "prices", repair_log, config)
    
    # Large gap should NOT be filled (all should remain NA)
    assert pd.isna(curated_df.loc[dates[2], "LARGE_GAP"])
    assert pd.isna(curated_df.loc[dates[3], "LARGE_GAP"])
    assert pd.isna(curated_df.loc[dates[4], "LARGE_GAP"])
    
    # No repairs should be logged
    ffill_entries = [entry for entry in repair_log 
                     if entry["action"] == "ffill"]
    assert len(ffill_entries) == 0


def test_gap_at_start_not_filled():
    """Test that gaps at the start (no prior data) are NOT filled."""
    dates = pd.date_range("2024-01-01", periods=5, freq="D")
    
    # Series with gap at start
    prices = [np.nan, np.nan, 100.0, 105.0, 110.0]
    
    df = pd.DataFrame({
        "START_GAP": prices,
    }, index=dates)
    
    repair_log = []
    config = QC_CONFIG.copy()
    config["allow_ffill"] = True
    config["max_ffill_days"] = 2
    
    curated_df = apply_repairs(df, "prices", repair_log, config)
    
    # Gap at start should NOT be filled
    assert pd.isna(curated_df.loc[dates[0], "START_GAP"])
    assert pd.isna(curated_df.loc[dates[1], "START_GAP"])
    
    # No repairs should be logged
    ffill_entries = [entry for entry in repair_log 
                     if entry["action"] == "ffill"]
    assert len(ffill_entries) == 0


def test_no_fill_when_disabled():
    """Test that gap filling is skipped when allow_ffill=False."""
    dates = pd.date_range("2024-01-01", periods=5, freq="D")
    
    prices = [100.0, 105.0, np.nan, 110.0, 115.0]
    
    df = pd.DataFrame({
        "GAP": prices,
    }, index=dates)
    
    repair_log = []
    config = QC_CONFIG.copy()
    config["allow_ffill"] = False  # Disabled
    
    curated_df = apply_repairs(df, "prices", repair_log, config)
    
    # Gap should remain NA
    assert pd.isna(curated_df.loc[dates[2], "GAP"])
    
    # No repairs logged
    assert len(repair_log) == 0


def test_only_prices_filled():
    """Test that gap filling only applies to prices, not marketcap/volume."""
    dates = pd.date_range("2024-01-01", periods=5, freq="D")
    
    values = [100.0, 105.0, np.nan, 110.0, 115.0]
    
    df = pd.DataFrame({
        "SERIES": values,
    }, index=dates)
    
    repair_log = []
    config = QC_CONFIG.copy()
    config["allow_ffill"] = True
    config["max_ffill_days"] = 2
    
    # For marketcap dataset, should NOT fill
    curated_mcap = apply_repairs(df, "marketcap", repair_log, config)
    assert pd.isna(curated_mcap.loc[dates[2], "SERIES"])
    
    # For volume dataset, should NOT fill
    curated_vol = apply_repairs(df, "volume", repair_log, config)
    assert pd.isna(curated_vol.loc[dates[2], "SERIES"])
    
    # Only prices should fill
    curated_prices = apply_repairs(df, "prices", repair_log, config)
    assert curated_prices.loc[dates[2], "SERIES"] == 105.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
