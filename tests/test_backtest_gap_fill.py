"""Tests for backtest gap filling."""

import pytest
import pandas as pd
import numpy as np
from datetime import date, timedelta
from pathlib import Path
import tempfile
import yaml

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.backtest.engine import apply_gap_fill, run_backtest


def test_gap_fill_mode_1d():
    """Test that gap_fill_mode='1d' fills 1-day gaps but not 2+ consecutive missing days."""
    # Create test data with gaps
    dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 10), freq="D")
    prices_df = pd.DataFrame({
        "BTC": [30000, 31000, np.nan, 32000, np.nan, np.nan, 33000, 34000, 35000, 36000],  # 1-day gap, then 2-day gap
        "ETH": [2000, 2100, 2200, np.nan, 2300, 2400, 2500, 2600, 2700, 2800],  # 1-day gap
    }, index=dates)
    
    # Apply gap fill mode "1d"
    filled_df = apply_gap_fill(prices_df, gap_fill_mode="1d")
    
    # Check 1-day gap was filled (BTC at 2023-01-03 should be filled with 31000)
    assert not pd.isna(filled_df.loc[dates[2], "BTC"])
    assert filled_df.loc[dates[2], "BTC"] == 31000
    
    # Check 2-day gap was NOT filled (BTC at 2023-01-05 and 2023-01-06 should remain NaN)
    assert pd.isna(filled_df.loc[dates[4], "BTC"])
    assert pd.isna(filled_df.loc[dates[5], "BTC"])
    
    # Check ETH 1-day gap was filled
    assert not pd.isna(filled_df.loc[dates[3], "ETH"])
    assert filled_df.loc[dates[3], "ETH"] == 2200


def test_gap_fill_mode_none():
    """Test that gap_fill_mode='none' preserves all NA values."""
    dates = pd.date_range(start=date(2023, 1, 1), end=date(2023, 1, 5), freq="D")
    prices_df = pd.DataFrame({
        "BTC": [30000, np.nan, 32000, np.nan, 34000],
    }, index=dates)
    
    filled_df = apply_gap_fill(prices_df, gap_fill_mode="none")
    
    # All NAs should be preserved
    assert pd.isna(filled_df.loc[dates[1], "BTC"])
    assert pd.isna(filled_df.loc[dates[3], "BTC"])


def test_gap_fill_integration():
    """Test gap filling in full backtest run."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        
        # Create test data with gaps
        start_date = date(2023, 1, 1)
        end_date = date(2023, 1, 10)
        dates = pd.date_range(start=start_date, end=end_date, freq="D")
        
        # Create prices with 1-day gap
        prices_df = pd.DataFrame({
            "BTC": [30000, 31000, np.nan, 32000, 33000, 34000, 35000, 36000, 37000, 38000],
        }, index=dates)
        
        # Create snapshots (simple: BTC only, equal weight)
        snapshots_df = pd.DataFrame({
            "rebalance_date": [start_date],
            "snapshot_date": [start_date],
            "symbol": ["BTC"],
            "weight": [1.0],
            "rank": [1],
            "marketcap": [600e9],
            "volume_14d": [1e9],
            "coingecko_id": ["bitcoin"],
            "venue": ["BINANCE"],
            "basket_name": ["test_TOP1"],
            "selection_version": ["v1"],
        })
        
        # Create config with gap_fill_mode="1d"
        config = {
            "strategy_name": "test",
            "start_date": str(start_date),
            "end_date": str(end_date),
            "rebalance_frequency": "monthly",
            "rebalance_day": 1,
            "base_asset": "BTC",
            "top_n": 1,
            "eligibility": {
                "must_have_perp": False,
                "min_listing_days": 0,
                "min_mcap_usd": None,
                "min_volume_usd": None,
            },
            "weighting": "equal_weight_capped",
            "max_weight_per_asset": 1.0,
            "cost_model": {"fee_bps": 5, "slippage_bps": 5},
            "backtest": {
                "gap_fill_mode": "1d",
            },
        }
        
        # Write files
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        prices_df.to_parquet(data_dir / "prices_daily.parquet")
        snapshots_df.to_parquet(tmp_path / "snapshots.parquet")
        
        config_path = tmp_path / "config.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config, f)
        
        # Run backtest
        output_dir = tmp_path / "outputs"
        metadata = run_backtest(
            config_path,
            data_dir / "prices_daily.parquet",
            tmp_path / "snapshots.parquet",
            output_dir,
        )
        
        # Check that gap fill mode was recorded in metadata
        assert metadata["backtest_assumptions"]["gap_fill_mode"] == "1d"
        
        # Check results (should have valid returns even with gap filled)
        results_path = output_dir / "backtest_results.csv"
        if results_path.exists():
            results_df = pd.read_csv(results_path)
            # Should have results for all dates (gap was filled)
            assert len(results_df) == len(dates)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
