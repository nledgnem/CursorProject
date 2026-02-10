"""Tests for snapshot sanity checks (weights sum to 1, both tables produced)."""

import pytest
import pandas as pd
import numpy as np
from datetime import date
from pathlib import Path
import tempfile
import yaml

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.universe.snapshot import build_snapshots


def test_snapshot_weights_sum_to_one():
    """Test that weights sum to 1.0 for each rebalance date."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        
        # Create test data
        start_date = date(2023, 1, 1)
        end_date = date(2023, 2, 1)
        dates = pd.date_range(start=start_date, end=end_date, freq="D")
        
        prices_df = pd.DataFrame({
            "BTC": [30000] * len(dates),
            "ETH": [2000] * len(dates),
            "SOL": [100] * len(dates),
        }, index=dates)
        
        mcaps_df = pd.DataFrame({
            "BTC": [600e9] * len(dates),
            "ETH": [240e9] * len(dates),
            "SOL": [10e9] * len(dates),
        }, index=dates)
        
        volumes_df = pd.DataFrame({
            "BTC": [1e9] * len(dates),
            "ETH": [500e6] * len(dates),
            "SOL": [100e6] * len(dates),
        }, index=dates)
        
        allowlist_df = pd.DataFrame({
            "symbol": ["BTC", "ETH", "SOL"],
            "coingecko_id": ["bitcoin", "ethereum", "solana"],
            "venue": ["BINANCE", "BINANCE", "BINANCE"],
        })
        
        config = {
            "strategy_name": "test",
            "start_date": str(start_date),
            "end_date": str(end_date),
            "rebalance_frequency": "monthly",
            "rebalance_day": 1,
            "base_asset": "BTC",
            "top_n": 2,  # Select top 2
            "eligibility": {
                "must_have_perp": False,
                "min_listing_days": 0,
                "min_mcap_usd": None,
                "min_volume_usd": None,
            },
            "weighting": "cap_weighted",
            "max_weight_per_asset": 0.10,
            "cost_model": {"fee_bps": 5, "slippage_bps": 5},
        }
        
        # Write files
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        prices_df.to_parquet(data_dir / "prices_daily.parquet")
        mcaps_df.to_parquet(data_dir / "marketcap_daily.parquet")
        volumes_df.to_parquet(data_dir / "volume_daily.parquet")
        allowlist_df.to_csv(tmp_path / "allowlist.csv", index=False)
        
        config_path = tmp_path / "config.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config, f)
        
        # Build snapshots
        output_path = tmp_path / "snapshots.parquet"
        universe_path = tmp_path / "universe_eligibility.parquet"
        
        build_snapshots(
            config_path,
            data_dir / "prices_daily.parquet",
            data_dir / "marketcap_daily.parquet",
            data_dir / "volume_daily.parquet",
            tmp_path / "allowlist.csv",
            output_path,
        )
        
        # Check that both files exist
        assert output_path.exists(), "Basket snapshots file should exist"
        assert universe_path.exists(), "Universe eligibility file should exist"
        
        # Check basket snapshots
        snapshots_df = pd.read_parquet(output_path)
        assert len(snapshots_df) > 0, "Basket snapshots should not be empty"
        
        # Check weights sum to 1.0 for each rebalance date
        for rebal_date in snapshots_df["rebalance_date"].unique():
            weights = snapshots_df[snapshots_df["rebalance_date"] == rebal_date]["weight"]
            weight_sum = weights.sum()
            assert abs(weight_sum - 1.0) < 1e-6, f"Weights for {rebal_date} sum to {weight_sum}, not 1.0"
        
        # Check universe eligibility
        universe_df = pd.read_parquet(universe_path)
        assert len(universe_df) > 0, "Universe eligibility should not be empty"
        
        # Check that universe eligibility has all candidates
        assert len(universe_df) >= len(snapshots_df), "Universe eligibility should have >= basket snapshots"


def test_snapshot_both_tables_produced():
    """Test that both universe_eligibility and basket snapshots are produced."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        
        # Create minimal test data
        start_date = date(2023, 1, 1)
        end_date = date(2023, 1, 1)  # Single rebalance
        dates = pd.date_range(start=start_date, end=start_date, freq="D")
        
        prices_df = pd.DataFrame({"ETH": [2000]}, index=dates)
        mcaps_df = pd.DataFrame({"ETH": [240e9]}, index=dates)
        volumes_df = pd.DataFrame({"ETH": [500e6]}, index=dates)
        
        allowlist_df = pd.DataFrame({
            "symbol": ["ETH"],
            "coingecko_id": ["ethereum"],
            "venue": ["BINANCE"],
        })
        
        config = {
            "strategy_name": "test",
            "start_date": str(start_date),
            "end_date": str(end_date),
            "rebalance_frequency": "monthly",
            "rebalance_day": 1,
            "base_asset": "BTC",  # ETH is not base asset, so eligible
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
        }
        
        # Write files
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        prices_df.to_parquet(data_dir / "prices_daily.parquet")
        mcaps_df.to_parquet(data_dir / "marketcap_daily.parquet")
        volumes_df.to_parquet(data_dir / "volume_daily.parquet")
        allowlist_df.to_csv(tmp_path / "allowlist.csv", index=False)
        
        config_path = tmp_path / "config.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config, f)
        
        # Build snapshots
        output_path = tmp_path / "snapshots.parquet"
        universe_path = tmp_path / "universe_eligibility.parquet"
        
        build_snapshots(
            config_path,
            data_dir / "prices_daily.parquet",
            data_dir / "marketcap_daily.parquet",
            data_dir / "volume_daily.parquet",
            tmp_path / "allowlist.csv",
            output_path,
        )
        
        # Both files should exist
        assert output_path.exists()
        assert universe_path.exists()
        
        # Both should have data
        snapshots_df = pd.read_parquet(output_path)
        universe_df = pd.read_parquet(universe_path)
        
        assert len(snapshots_df) > 0
        assert len(universe_df) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
