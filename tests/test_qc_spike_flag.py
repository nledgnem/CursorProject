"""Test QC spike detection and flagging."""

import pytest
import pandas as pd
import numpy as np
from datetime import date, timedelta
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from qc_curate import apply_outlier_flags, apply_sanity_checks, QC_CONFIG


def test_return_spike_detection():
    """Test that return spikes are flagged and set to NA."""
    # Create synthetic price data with one absurd spike
    dates = pd.date_range("2024-01-01", periods=10, freq="D")
    
    # Normal series
    normal_prices = [100.0 + i for i in range(10)]
    
    # Series with spike on day 5 (going from 104 to 700 = ~573% return)
    spike_prices = [100.0 + i for i in range(5)]
    spike_prices.append(700.0)  # Spike!
    spike_prices.extend([106.0 + i for i in range(4)])
    
    df = pd.DataFrame({
        "NORMAL": normal_prices,
        "SPIKE": spike_prices,
    }, index=dates)
    
    repair_log = []
    config = QC_CONFIG.copy()
    config["RET_SPIKE"] = 5.0  # 500% threshold
    
    # Apply outlier flags
    curated_df = apply_outlier_flags(df, "prices", repair_log, config)
    
    # Normal series should be unchanged
    assert curated_df["NORMAL"].equals(df["NORMAL"])
    
    # Spike day should be NA in curated
    spike_date = dates[5]
    assert pd.isna(curated_df.loc[spike_date, "SPIKE"])
    
    # Other days should be unchanged
    assert curated_df.loc[dates[4], "SPIKE"] == df.loc[dates[4], "SPIKE"]
    assert curated_df.loc[dates[6], "SPIKE"] == df.loc[dates[6], "SPIKE"]
    
    # Repair log should contain the spike entry
    spike_entries = [entry for entry in repair_log if entry["rule"] == "return_spike"]
    assert len(spike_entries) == 1
    assert spike_entries[0]["symbol"] == "SPIKE"
    assert spike_entries[0]["date"] == str(spike_date)
    assert spike_entries[0]["action"] == "set_na"
    assert spike_entries[0]["old_value"] == 700.0
    assert spike_entries[0]["new_value"] is None


def test_jump_and_revert_detection():
    """Test that jump-and-revert patterns are flagged."""
    dates = pd.date_range("2024-01-01", periods=5, freq="D")
    
    # Series that jumps up then immediately reverts
    prices = [100.0, 100.0, 600.0, 100.0, 100.0]  # Day 2->3: 500% up, 3->4: -83% down
    
    df = pd.DataFrame({
        "JUMP_REVERT": prices,
    }, index=dates)
    
    repair_log = []
    config = QC_CONFIG.copy()
    config["RET_SPIKE"] = 5.0
    
    curated_df = apply_outlier_flags(df, "prices", repair_log, config)
    
    # Both spike days should be NA
    assert pd.isna(curated_df.loc[dates[2], "JUMP_REVERT"])  # Jump day
    assert pd.isna(curated_df.loc[dates[3], "JUMP_REVERT"])  # Revert day
    
    # Repair log should contain jump_revert entries
    jump_revert_entries = [entry for entry in repair_log if entry["rule"] == "jump_revert"]
    assert len(jump_revert_entries) >= 1
    assert any(entry["symbol"] == "JUMP_REVERT" for entry in jump_revert_entries)


def test_negative_price_flagging():
    """Test that negative prices are flagged."""
    dates = pd.date_range("2024-01-01", periods=5, freq="D")
    
    prices = [100.0, 50.0, -10.0, 60.0, 70.0]  # Day 2 is negative
    
    df = pd.DataFrame({
        "NEGATIVE": prices,
    }, index=dates)
    
    repair_log = []
    config = QC_CONFIG.copy()
    
    curated_df = apply_sanity_checks(df, "prices", repair_log, config)
    
    # Negative price should be NA
    assert pd.isna(curated_df.loc[dates[2], "NEGATIVE"])
    
    # Repair log should contain neg_price entry
    neg_entries = [entry for entry in repair_log if entry["rule"] == "neg_price"]
    assert len(neg_entries) == 1
    assert neg_entries[0]["symbol"] == "NEGATIVE"
    assert neg_entries[0]["old_value"] == -10.0
    assert neg_entries[0]["action"] == "set_na"


def test_zero_price_flagging():
    """Test that zero prices are flagged."""
    dates = pd.date_range("2024-01-01", periods=5, freq="D")
    
    prices = [100.0, 50.0, 0.0, 60.0, 70.0]  # Day 2 is zero
    
    df = pd.DataFrame({
        "ZERO": prices,
    }, index=dates)
    
    repair_log = []
    config = QC_CONFIG.copy()
    
    curated_df = apply_sanity_checks(df, "prices", repair_log, config)
    
    # Zero price should be NA
    assert pd.isna(curated_df.loc[dates[2], "ZERO"])
    
    # Repair log should contain zero_price entry
    zero_entries = [entry for entry in repair_log if entry["rule"] == "zero_price"]
    assert len(zero_entries) == 1
    assert zero_entries[0]["symbol"] == "ZERO"
    assert zero_entries[0]["old_value"] == 0.0
    assert zero_entries[0]["action"] == "set_na"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
