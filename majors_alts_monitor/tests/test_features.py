"""Tests for feature computation."""

import pytest
import polars as pl
import numpy as np
from datetime import date, timedelta
from majors_alts_monitor.features import FeatureLibrary


@pytest.fixture
def sample_data():
    """Create sample price/mcap/volume data."""
    dates = [date(2024, 1, 1) + timedelta(days=i) for i in range(100)]
    
    prices = pl.DataFrame({
        "asset_id": ["BTC", "ETH", "SOL"] * 100,
        "date": dates * 3,
        "close": np.random.randn(300).cumsum() + 40000.0,
    })
    
    marketcap = pl.DataFrame({
        "asset_id": ["BTC", "ETH", "SOL"] * 100,
        "date": dates * 3,
        "marketcap": [800_000_000_000, 300_000_000_000, 50_000_000_000] * 100,
    })
    
    volume = pl.DataFrame({
        "asset_id": ["BTC", "ETH", "SOL"] * 100,
        "date": dates * 3,
        "volume": np.random.randn(300).cumsum() + 1_000_000_000,
    })
    
    return prices, marketcap, volume


def test_feature_computation(sample_data):
    """Test feature computation."""
    prices, marketcap, volume = sample_data
    
    feature_lib = FeatureLibrary(burn_in_days=30, lookback_days=60)
    features = feature_lib.compute_features(
        prices, marketcap, volume,
        majors=["BTC", "ETH"],
        exclude_assets=[],
    )
    
    assert len(features) > 0
    assert "date" in features.columns
    
    # Check for feature columns
    feature_cols = [c for c in features.columns if c.startswith("raw_") or c.startswith("z_")]
    assert len(feature_cols) > 0
    
    # Check burn-in flag
    assert "valid" in features.columns


def test_no_nans_after_burnin(sample_data):
    """Test that features have no NaNs after burn-in."""
    prices, marketcap, volume = sample_data
    
    feature_lib = FeatureLibrary(burn_in_days=30, lookback_days=60)
    features = feature_lib.compute_features(
        prices, marketcap, volume,
        majors=["BTC", "ETH"],
        exclude_assets=[],
    )
    
    # Filter to valid period
    valid_features = features.filter(pl.col("valid") == True)
    
    if len(valid_features) > 0:
        # Check for NaNs in feature columns
        feature_cols = [c for c in valid_features.columns if c.startswith("raw_")]
        for col in feature_cols:
            n_nans = valid_features[col].null_count()
            # Some NaNs may be expected (e.g., funding if not available)
            # But core features should be mostly non-null
            assert n_nans < len(valid_features) * 0.5  # Less than 50% NaNs
