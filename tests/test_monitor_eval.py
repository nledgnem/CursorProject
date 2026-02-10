"""Tests for monitor evaluation framework."""

import pytest
import pandas as pd
import numpy as np
from datetime import date, timedelta
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.evaluation.forward_returns import compute_forward_returns, align_regime_and_returns
from src.evaluation.regime_eval import compute_bucket_stats, compute_edge_stats
from src.monitors.base import bucket_to_1_5, score_to_bucket_1_5


def test_forward_returns_no_same_day():
    """Test that forward returns never use same-day returns."""
    # Create simple returns series
    dates = pd.date_range("2024-01-01", periods=10, freq="D")
    # Use known values: 1% per day
    returns = pd.Series([0.01] * 10, index=dates)
    
    # Compute forward returns for H=5
    fwd_returns = compute_forward_returns(returns, horizons=[5])
    
    # For date t=2024-01-01, fwd_ret should use returns from 2024-01-02 to 2024-01-06
    # Expected: (1.01^5) - 1 = 0.0510100501
    
    # Check first date
    first_date = dates[0]
    expected_fwd_5 = (1.01 ** 5) - 1
    actual_fwd_5 = fwd_returns.loc[first_date, "fwd_ret_5"]
    
    assert not pd.isna(actual_fwd_5), "Forward return should not be NaN"
    assert abs(actual_fwd_5 - expected_fwd_5) < 1e-6, f"Expected {expected_fwd_5}, got {actual_fwd_5}"
    
    # Verify that for date t, we're using t+1 to t+H (not including t)
    # If we were using same-day, the result would be different
    # For example, if we used t to t+H-1: (1.01^5) - 1 (same result by coincidence)
    # But if we use t+1 to t+H: we get the same (correct)
    
    # Better test: use different returns for each day
    dates2 = pd.date_range("2024-01-01", periods=10, freq="D")
    returns2 = pd.Series([0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.10], index=dates2)
    
    fwd_returns2 = compute_forward_returns(returns2, horizons=[3])
    
    # For date 2024-01-01, fwd_ret_3 should use returns from 2024-01-02 (0.02), 2024-01-03 (0.03), 2024-01-04 (0.04)
    # Expected: (1.02 * 1.03 * 1.04) - 1 = 0.093224
    first_date2 = dates2[0]
    expected_fwd_3 = (1.02 * 1.03 * 1.04) - 1
    actual_fwd_3 = fwd_returns2.loc[first_date2, "fwd_ret_3"]
    
    assert not pd.isna(actual_fwd_3), "Forward return should not be NaN"
    assert abs(actual_fwd_3 - expected_fwd_3) < 1e-5, f"Expected {expected_fwd_3:.6f}, got {actual_fwd_3:.6f}"


def test_alignment_inner_join():
    """Test that alignment uses inner join (only common dates)."""
    # Create regime data
    dates_regime = pd.date_range("2024-01-01", periods=5, freq="D")
    regime_df = pd.DataFrame({
        "regime_1_5": [1, 2, 3, 4, 5],
    }, index=dates_regime)
    
    # Create LS returns with overlapping but not identical dates
    dates_returns = pd.date_range("2024-01-02", periods=4, freq="D")  # Starts one day later
    ls_returns_df = pd.DataFrame({
        "ls_ret": [0.01, 0.02, 0.03, 0.04],
    }, index=dates_returns)
    
    # Align with inner join
    aligned = align_regime_and_returns(regime_df, ls_returns_df, drop_missing=True)
    
    # Should only have common dates: 2024-01-02, 2024-01-03, 2024-01-04, 2024-01-05
    expected_dates = pd.date_range("2024-01-02", periods=4, freq="D")
    assert len(aligned) == 4, f"Expected 4 dates, got {len(aligned)}"
    assert all(aligned.index == expected_dates), "Dates should match common dates only"
    
    # Verify no missing values in aligned data
    assert not aligned["regime_1_5"].isna().any(), "Regime should not have NaN"
    assert not aligned["ls_ret"].isna().any(), "LS returns should not have NaN"


def test_regime_bucket_coverage():
    """Test that bucket stats cover all regimes present in data."""
    # Create aligned data with all regimes
    dates = pd.date_range("2024-01-01", periods=15, freq="D")
    aligned_df = pd.DataFrame({
        "regime_1_5": [1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 1, 2, 3, 4, 5],
        "fwd_ret_5": [0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.10, 0.01, 0.02, 0.03, 0.04, 0.05],
    }, index=dates)
    
    # Compute bucket stats
    bucket_stats = compute_bucket_stats(aligned_df, horizon=5)
    
    # Should have stats for all 5 regimes
    assert len(bucket_stats) == 5, f"Expected 5 regimes, got {len(bucket_stats)}"
    assert set(bucket_stats["regime"]) == {1, 2, 3, 4, 5}, "Should cover all regimes"
    
    # Check that n (sample size) is correct
    for _, row in bucket_stats.iterrows():
        regime = int(row["regime"])
        expected_n = sum(aligned_df["regime_1_5"] == regime)
        assert row["n"] == expected_n, f"Regime {regime}: expected n={expected_n}, got {row['n']}"


def test_edge_stats_calculation():
    """Test edge stats calculation."""
    # Create aligned data with clear separation
    dates = pd.date_range("2024-01-01", periods=15, freq="D")
    aligned_df = pd.DataFrame({
        "regime_1_5": [1] * 5 + [3] * 5 + [5] * 5,
        "fwd_ret_5": [-0.05] * 5 + [0.0] * 5 + [0.05] * 5,  # Regime 1: -5%, Regime 3: 0%, Regime 5: +5%
    }, index=dates)
    
    # Compute edge stats
    edge_stats = compute_edge_stats(aligned_df, horizon=5)
    
    # Mean of all: (-0.05*5 + 0*5 + 0.05*5) / 15 = 0.0
    assert abs(edge_stats["mean_all"]) < 1e-6, "Mean of all should be 0"
    
    # Edge best (regime 5 vs all): 0.05 - 0.0 = 0.05
    assert abs(edge_stats["edge_best"] - 0.05) < 1e-6, f"Expected edge_best=0.05, got {edge_stats['edge_best']}"
    
    # Edge worst (regime 1 vs all): -0.05 - 0.0 = -0.05
    assert abs(edge_stats["edge_worst"] - (-0.05)) < 1e-6, f"Expected edge_worst=-0.05, got {edge_stats['edge_worst']}"
    
    # Spread (5 - 1): 0.05 - (-0.05) = 0.10
    assert abs(edge_stats["spread_1_5"] - 0.10) < 1e-6, f"Expected spread_1_5=0.10, got {edge_stats['spread_1_5']}"
    
    # Sample sizes
    assert edge_stats["n1"] == 5, "n1 should be 5"
    assert edge_stats["n5"] == 5, "n5 should be 5"
    assert edge_stats["n_all"] == 15, "n_all should be 15"


def test_bucket_to_1_5():
    """Test bucket name to numeric conversion."""
    assert bucket_to_1_5("RED") == 1
    assert bucket_to_1_5("ORANGE") == 2
    assert bucket_to_1_5("YELLOW") == 3
    assert bucket_to_1_5("YELLOWGREEN") == 4
    assert bucket_to_1_5("GREEN") == 5
    assert bucket_to_1_5("red") == 1  # Case insensitive
    assert bucket_to_1_5("UNKNOWN") == 3  # Default to 3


def test_score_to_bucket_1_5():
    """Test score to bucket conversion."""
    assert score_to_bucket_1_5(100) == 5  # GREEN
    assert score_to_bucket_1_5(70) == 5   # GREEN
    assert score_to_bucket_1_5(69) == 4   # YELLOWGREEN
    assert score_to_bucket_1_5(55) == 4   # YELLOWGREEN
    assert score_to_bucket_1_5(54) == 3   # YELLOW
    assert score_to_bucket_1_5(45) == 3   # YELLOW
    assert score_to_bucket_1_5(44) == 2   # ORANGE
    assert score_to_bucket_1_5(30) == 2   # ORANGE
    assert score_to_bucket_1_5(29) == 1   # RED
    assert score_to_bucket_1_5(0) == 1    # RED




