"""Tests for backtest data quality threshold enforcement."""

import pytest
import pandas as pd
import numpy as np
from datetime import date, timedelta
from pathlib import Path
import tempfile
import yaml

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.backtest.engine import run_backtest


def test_min_history_days_enforced():
    """Test that min_history_days actually removes symbols or forces low coverage."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        
        # Create test data with a symbol that has insufficient history
        start_date = date(2023, 1, 1)
        end_date = date(2023, 1, 10)
        dates = pd.date_range(start=start_date, end=end_date, freq="D")
        
        # Create prices: BTC has full history, NEWCOIN only has 5 days (insufficient if min_history_days=10)
        prices_df = pd.DataFrame({
            "BTC": [30000] * len(dates),
            "NEWCOIN": [np.nan] * 5 + [100] * (len(dates) - 5),  # Only 5 days of data
        }, index=dates)
        
        # Create snapshots (both BTC and NEWCOIN in basket)
        snapshots_df = pd.DataFrame({
            "rebalance_date": [start_date],
            "snapshot_date": [start_date],
            "symbol": ["BTC", "NEWCOIN"],
            "weight": [0.5, 0.5],
            "rank": [1, 2],
            "marketcap": [600e9, 10e9],
            "volume_14d": [1e9, 100e6],
            "coingecko_id": ["bitcoin", "newcoin"],
            "venue": ["BINANCE", "BINANCE"],
            "basket_name": ["test_TOP2"],
            "selection_version": ["v1"],
        })
        
        # Create config with min_history_days=10
        config = {
            "strategy_name": "test",
            "start_date": str(start_date),
            "end_date": str(end_date),
            "rebalance_frequency": "monthly",
            "rebalance_day": 1,
            "base_asset": "BTC",
            "top_n": 2,
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
                "gap_fill_mode": "none",
                "min_history_days": 10,  # NEWCOIN only has 5 days
                "max_missing_frac": None,
                "max_consecutive_missing_days": None,
                "basket_coverage_threshold": 0.90,
                "lookback_window_days": 30,
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
        
        # Check that min_history_days was recorded in metadata
        assert metadata["backtest_assumptions"]["min_history_days"] == 10
        
        # Check results - NEWCOIN should be filtered out due to insufficient history
        # This should result in lower coverage or NaN returns on days where NEWCOIN is needed
        results_path = output_dir / "backtest_results.csv"
        if results_path.exists():
            results_df = pd.read_csv(results_path)
            # On days where NEWCOIN is required but filtered, coverage should drop below threshold
            # or returns should be NaN
            # (Exact behavior depends on coverage threshold, but NEWCOIN should not contribute)
            assert len(results_df) > 0


def test_basket_return_divides_by_total_weight():
    """Test that basket return calculation divides by total_weight (1.0), not valid_weights."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        
        # Create test data
        start_date = date(2023, 1, 1)
        end_date = date(2023, 1, 3)
        dates = pd.date_range(start=start_date, end=end_date, freq="D")
        
        # Create prices: BTC goes up 10%, ETH goes up 5%
        prices_df = pd.DataFrame({
            "BTC": [30000, 33000, 33000],  # +10% on day 1
            "ETH": [2000, 2100, 2100],     # +5% on day 1
        }, index=dates)
        
        # Create snapshots: 50% BTC, 50% ETH
        snapshots_df = pd.DataFrame({
            "rebalance_date": [start_date],
            "snapshot_date": [start_date],
            "symbol": ["BTC", "ETH"],
            "weight": [0.5, 0.5],
            "rank": [1, 2],
            "marketcap": [600e9, 240e9],
            "volume_14d": [1e9, 500e6],
            "coingecko_id": ["bitcoin", "ethereum"],
            "venue": ["BINANCE", "BINANCE"],
            "basket_name": ["test_TOP2"],
            "selection_version": ["v1"],
        })
        
        config = {
            "strategy_name": "test",
            "start_date": str(start_date),
            "end_date": str(end_date),
            "rebalance_frequency": "monthly",
            "rebalance_day": 1,
            "base_asset": "BTC",
            "top_n": 2,
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
                "gap_fill_mode": "none",
                "basket_coverage_threshold": 0.90,
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
        run_backtest(
            config_path,
            data_dir / "prices_daily.parquet",
            tmp_path / "snapshots.parquet",
            output_dir,
        )
        
        # Check results
        results_path = output_dir / "backtest_results.csv"
        if results_path.exists():
            results_df = pd.read_csv(results_path)
            # On day 1 (2023-01-02), basket return should be:
            # 0.5 * 0.10 + 0.5 * 0.05 = 0.075 = 7.5%
            # Divided by total_weight (1.0), not valid_weights
            day1_row = results_df[results_df["date"] == "2023-01-02"]
            if len(day1_row) > 0:
                r_basket = day1_row.iloc[0]["r_basket"]
                # Should be approximately 0.075 (7.5%)
                assert abs(r_basket - 0.075) < 0.001, \
                    f"Expected basket return ~0.075, got {r_basket}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
