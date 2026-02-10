"""Test that core functions always return dict metadata (not None).

This test enforces the "strictness loop" requirement:
- build_snapshots() must always return Dict[str, Any] with filter_thresholds and date_range
- run_backtest() must always return Dict[str, Any] with date_range and row counts

If either function returns None or a non-dict, these tests will fail, ensuring
metadata consistency for audit/reproducibility.
"""

import pytest
import pandas as pd
from datetime import date, timedelta
from pathlib import Path
import tempfile
import yaml

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.universe.snapshot import build_snapshots
from src.backtest.engine import run_backtest


def test_build_snapshots_returns_dict():
    """Test that build_snapshots() always returns a dict, even with empty data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        
        # Create minimal config
        config = {
            "start_date": "2024-01-01",
            "end_date": "2024-01-31",
            "rebalance_frequency": "monthly",
            "top_n": 5,
            "base_asset": "BTC",
            "strategy_name": "test",
            "eligibility": {
                "must_have_perp": True,
                "min_listing_days": 30,
                "min_mcap_usd": 1000000,
                "min_volume_usd": 100000,
            },
            "weighting": "equal_weight_capped",
            "max_weight_per_asset": 0.2,
        }
        config_path = tmp_path / "test_config.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config, f)
        
        # Create minimal empty data files
        prices_df = pd.DataFrame(index=pd.date_range("2024-01-01", "2024-01-31", freq="D"))
        mcaps_df = pd.DataFrame(index=pd.date_range("2024-01-01", "2024-01-31", freq="D"))
        volumes_df = pd.DataFrame(index=pd.date_range("2024-01-01", "2024-01-31", freq="D"))
        
        prices_path = tmp_path / "prices.parquet"
        mcaps_path = tmp_path / "mcaps.parquet"
        volumes_path = tmp_path / "volumes.parquet"
        
        prices_df.to_parquet(prices_path)
        mcaps_df.to_parquet(mcaps_path)
        volumes_df.to_parquet(volumes_path)
        
        # Create minimal allowlist
        allowlist_df = pd.DataFrame({
            "symbol": ["BTC", "ETH"],
            "coingecko_id": ["bitcoin", "ethereum"],
            "venue": ["BINANCE", "BINANCE"],
        })
        allowlist_path = tmp_path / "allowlist.csv"
        allowlist_df.to_csv(allowlist_path, index=False)
        
        output_path = tmp_path / "snapshots.parquet"
        
        # Call function - should return dict even with no eligible coins
        result = build_snapshots(
            config_path,
            prices_path,
            mcaps_path,
            volumes_path,
            allowlist_path,
            output_path,
        )
        
        # Assert it's a dict
        assert isinstance(result, dict), f"build_snapshots() returned {type(result)}, expected dict"
        
        # Assert it has required keys
        assert "filter_thresholds" in result, "Missing 'filter_thresholds' in return value"
        assert "date_range" in result, "Missing 'date_range' in return value"
        assert "num_snapshots" in result, "Missing 'num_snapshots' in return value"
        assert "row_count" in result, "Missing 'row_count' in return value"
        
        # Assert filter_thresholds is a dict
        assert isinstance(result["filter_thresholds"], dict), "filter_thresholds should be a dict"
        
        # Assert date_range is a dict
        assert isinstance(result["date_range"], dict), "date_range should be a dict"
        assert "start_date" in result["date_range"]
        assert "end_date" in result["date_range"]


def test_run_backtest_returns_dict():
    """Test that run_backtest() always returns a dict."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        
        # Create minimal config
        config = {
            "start_date": "2024-01-01",
            "end_date": "2024-01-10",
            "base_asset": "BTC",
            "strategy_name": "test",
            "cost_model": {
                "fee_bps": 5,
                "slippage_bps": 5,
            },
        }
        config_path = tmp_path / "test_config.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config, f)
        
        # Create minimal price data (BTC only, 10 days)
        dates = pd.date_range("2024-01-01", "2024-01-10", freq="D")
        prices_df = pd.DataFrame(
            {"BTC": [50000.0 + i * 100 for i in range(len(dates))]},
            index=dates
        )
        prices_path = tmp_path / "prices.parquet"
        prices_df.to_parquet(prices_path)
        
        # Create minimal snapshots (one rebalance on 2024-01-01)
        snapshots_df = pd.DataFrame({
            "rebalance_date": [date(2024, 1, 1)],
            "snapshot_date": [date(2024, 1, 1)],
            "symbol": ["ETH"],
            "coingecko_id": ["ethereum"],
            "venue": ["BINANCE"],
            "basket_name": ["test_TOP5"],
            "selection_version": ["v1"],
            "rank": [1],
            "weight": [1.0],
            "marketcap": [1000000000.0],
            "volume_14d": [10000000.0],
        })
        snapshots_path = tmp_path / "snapshots.parquet"
        snapshots_df.to_parquet(snapshots_path)
        
        output_dir = tmp_path / "outputs"
        
        # Call function
        result = run_backtest(
            config_path,
            prices_path,
            snapshots_path,
            output_dir,
        )
        
        # Assert it's a dict
        assert isinstance(result, dict), f"run_backtest() returned {type(result)}, expected dict"
        
        # Assert it has required keys
        assert "row_count" in result, "Missing 'row_count' in return value"
        assert "date_range" in result, "Missing 'date_range' in return value"
        assert "num_trading_days" in result, "Missing 'num_trading_days' in return value"
        assert "num_rebalance_dates" in result, "Missing 'num_rebalance_dates' in return value"
        
        # Assert date_range is a dict
        assert isinstance(result["date_range"], dict), "date_range should be a dict"
        assert "start_date" in result["date_range"]
        assert "end_date" in result["date_range"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

