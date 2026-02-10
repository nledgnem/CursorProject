"""Tests for universe snapshot building."""

import pytest
import pandas as pd
import numpy as np
from datetime import date, timedelta
from pathlib import Path
import tempfile
import yaml

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.universe.snapshot import get_rebalance_dates, calculate_weights


def test_rebalance_dates_monthly():
    """Test monthly rebalance date generation."""
    start = date(2023, 1, 15)
    end = date(2023, 4, 15)
    dates = get_rebalance_dates(start, end, "monthly", day=1)
    
    # Should include Feb 1, Mar 1, Apr 1 (not Jan 15 since day=1)
    assert date(2023, 2, 1) in dates
    assert date(2023, 3, 1) in dates
    assert date(2023, 4, 1) in dates
    assert date(2023, 1, 15) not in dates


def test_rebalance_dates_quarterly():
    """Test quarterly rebalance date generation."""
    start = date(2023, 1, 15)
    end = date(2023, 10, 15)
    dates = get_rebalance_dates(start, end, "quarterly", day=1)
    
    # Should include Apr 1, Jul 1, Oct 1
    assert date(2023, 4, 1) in dates
    assert date(2023, 7, 1) in dates
    assert date(2023, 10, 1) in dates


def test_calculate_weights_cap_weighted():
    """Test cap-weighted weight calculation."""
    symbols = ["A", "B", "C"]
    mcaps = pd.Series([100, 200, 300], index=symbols)
    weights = calculate_weights(symbols, mcaps, "cap_weighted", max_weight=1.0)
    
    # Should be proportional to market cap
    assert abs(weights["C"] - 0.5) < 1e-6  # 300 / 600
    assert abs(weights["B"] - 1/3) < 1e-6  # 200 / 600
    assert abs(weights["A"] - 1/6) < 1e-6  # 100 / 600
    assert abs(weights.sum() - 1.0) < 1e-6


def test_calculate_weights_max_cap():
    """Test max weight capping."""
    symbols = ["A", "B", "C"]
    mcaps = pd.Series([10, 20, 970], index=symbols)  # C dominates
    weights = calculate_weights(symbols, mcaps, "cap_weighted", max_weight=0.10)
    
    # C should be capped at 0.10
    assert weights["C"] <= 0.10 + 1e-6
    # Weights should still sum to 1
    assert abs(weights.sum() - 1.0) < 1e-6


def test_calculate_weights_equal_weight_capped():
    """Test equal weight capped scheme."""
    symbols = ["A", "B", "C", "D"]
    mcaps = pd.Series([100, 200, 300, 400], index=symbols)
    weights = calculate_weights(symbols, mcaps, "equal_weight_capped", max_weight=0.30)
    
    # Should be equal weight (0.25 each), but capped at 0.30
    # Since 0.25 < 0.30, should remain equal
    assert abs(weights["A"] - 0.25) < 1e-6
    assert abs(weights.sum() - 1.0) < 1e-6


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

