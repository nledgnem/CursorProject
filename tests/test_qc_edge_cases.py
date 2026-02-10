"""Test QC edge cases: gap bridging, daily-range missingness."""

import pytest
import pandas as pd
import numpy as np
from datetime import date, timedelta
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from qc_curate import apply_outlier_flags, align_datasets, QC_CONFIG


def test_return_spike_not_triggered_across_gap():
    """Test that return spikes are NOT detected when bridging across missing data."""
    dates = pd.date_range("2024-01-01", periods=5, freq="D")
    
    # Series: 100 -> 200 (gap) -> 300
    # Without gap: 100 -> 200 (100% return), gap, 200 -> 300 (50% return)
    # With gap bridged incorrectly: 100 -> 300 (200% return) - WRONG
    # Correct behavior: no return calculated across gap, so no spike detected
    prices = [100.0, 200.0, np.nan, 300.0, 310.0]
    
    df = pd.DataFrame({
        "GAP_BRIDGE": prices,
    }, index=dates)
    
    repair_log = []
    config = QC_CONFIG.copy()
    config["RET_SPIKE"] = 1.5  # 150% threshold (lower for testing)
    
    curated_df = apply_outlier_flags(df, "prices", repair_log, config)
    
    # No spike should be detected at index 3 (300.0) even though
    # 100 -> 300 would be 200% if incorrectly bridged
    # Because pct_change(fill_method=None) returns NaN when t-1 is NaN
    
    # Check that 300.0 is NOT flagged (since return from NaN -> 300 is NaN, not computed)
    assert not pd.isna(curated_df.loc[dates[3], "GAP_BRIDGE"])
    
    # The spike entries should not include the date after the gap
    spike_entries = [entry for entry in repair_log if entry.get("rule") == "return_spike"]
    spike_dates = [entry["date"] for entry in spike_entries]
    assert str(dates[3]) not in spike_dates


def test_daily_range_alignment():
    """Test that alignment creates complete daily range with proper missingness."""
    # Create panels with non-contiguous dates
    dates1 = pd.date_range("2024-01-01", periods=3, freq="D")
    dates2 = pd.date_range("2024-01-05", periods=2, freq="D")  # Gap between
    
    df1 = pd.DataFrame({"A": [1.0, 2.0, 3.0]}, index=dates1)
    df2 = pd.DataFrame({"B": [10.0, 20.0]}, index=dates2)
    
    panels = {"prices": df1, "volume": df2}
    
    aligned = align_datasets(panels)
    
    # Should have complete daily range from 2024-01-01 to 2024-01-06
    expected_dates = pd.date_range("2024-01-01", "2024-01-06", freq="D")
    
    assert len(aligned["prices"]) == len(expected_dates)
    assert len(aligned["volume"]) == len(expected_dates)
    
    # Prices should have NA on missing dates (Jan 4-6)
    assert aligned["prices"].loc["2024-01-01", "A"] == 1.0
    assert aligned["prices"].loc["2024-01-02", "A"] == 2.0
    assert aligned["prices"].loc["2024-01-03", "A"] == 3.0
    assert pd.isna(aligned["prices"].loc["2024-01-04", "A"])
    assert pd.isna(aligned["prices"].loc["2024-01-05", "A"])
    assert pd.isna(aligned["prices"].loc["2024-01-06", "A"])
    
    # Volume should have NA on missing dates (Jan 1-4, 6)
    assert pd.isna(aligned["volume"].loc["2024-01-01", "B"])
    assert pd.isna(aligned["volume"].loc["2024-01-04", "B"])
    assert aligned["volume"].loc["2024-01-05", "B"] == 10.0
    assert aligned["volume"].loc["2024-01-06", "B"] == 20.0


def test_missingness_computation_uses_aligned_shape():
    """Test that missingness percentages use aligned dataframe shapes."""
    # Create a scenario where raw and curated have different date ranges after alignment
    dates_raw = pd.date_range("2024-01-01", periods=3, freq="D")
    dates_curated = pd.date_range("2024-01-01", periods=5, freq="D")  # Longer after alignment
    
    raw_df = pd.DataFrame({
        "A": [1.0, 2.0, 3.0],
        "B": [10.0, np.nan, 30.0],
    }, index=dates_raw)
    
    curated_df = pd.DataFrame({
        "A": [1.0, 2.0, 3.0, np.nan, np.nan],  # Extended with NAs
        "B": [10.0, np.nan, 30.0, np.nan, np.nan],
    }, index=dates_curated)
    
    # After alignment, both should have same shape (5 rows x 2 cols = 10 cells)
    panels_raw = {"prices": raw_df}
    panels_curated = {"prices": curated_df}
    
    aligned_raw = align_datasets(panels_raw)
    aligned_curated = align_datasets(panels_curated)
    
    # Both should now have same date range (union becomes full daily range)
    # If raw had min=Jan1, max=Jan3, and curated has min=Jan1, max=Jan5,
    # aligned should go from Jan1 to Jan5
    assert len(aligned_raw["prices"]) == len(aligned_curated["prices"])
    
    # Missingness should be computed from aligned shape
    raw_total = aligned_raw["prices"].shape[0] * aligned_raw["prices"].shape[1]
    curated_total = aligned_curated["prices"].shape[0] * aligned_curated["prices"].shape[1]
    
    assert raw_total == curated_total  # Same shape after alignment


def test_return_spike_requires_both_dates_non_na():
    """Test that return spikes only trigger when both t-1 and t are non-NA."""
    dates = pd.date_range("2024-01-01", periods=4, freq="D")
    
    # Series: 100 -> NaN -> 500 (huge jump, but t-1 is NaN, so no return computed)
    prices = [100.0, np.nan, 500.0, 510.0]
    
    df = pd.DataFrame({
        "JUMP_AFTER_GAP": prices,
    }, index=dates)
    
    repair_log = []
    config = QC_CONFIG.copy()
    config["RET_SPIKE"] = 2.0  # 200% threshold
    
    curated_df = apply_outlier_flags(df, "prices", repair_log, config)
    
    # Date at index 2 (500.0) should NOT be flagged because return from NaN is NaN
    # The return calculation requires t-1 to be non-NA
    assert not pd.isna(curated_df.loc[dates[2], "JUMP_AFTER_GAP"])
    
    # But date at index 3 (510.0) could potentially trigger if 500->510 is > threshold
    # Actually, 500->510 is only 2% return, so should be fine with 200% threshold
    assert not pd.isna(curated_df.loc[dates[3], "JUMP_AFTER_GAP"])


def test_daily_range_preserves_valid_dates():
    """Test that alignment to daily range preserves existing valid data."""
    # Create data with weekend gap (skip Saturday/Sunday if business days)
    dates = pd.date_range("2024-01-01", periods=3, freq="D")  # Mon, Tue, Wed
    
    df = pd.DataFrame({
        "A": [1.0, 2.0, 3.0],
    }, index=dates)
    
    panels = {"prices": df}
    aligned = align_datasets(panels)
    
    # Original dates should still have same values
    assert aligned["prices"].loc[dates[0], "A"] == 1.0
    assert aligned["prices"].loc[dates[1], "A"] == 2.0
    assert aligned["prices"].loc[dates[2], "A"] == 3.0
    
    # Index should be daily
    assert isinstance(aligned["prices"].index, pd.DatetimeIndex)
    assert aligned["prices"].index.freq == pd.tseries.offsets.Day()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
