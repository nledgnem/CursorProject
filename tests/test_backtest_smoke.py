"""End-to-end smoke test for backtest engine."""

import pytest
import pandas as pd
import numpy as np
from datetime import date, timedelta
from pathlib import Path
import tempfile
import yaml
import json

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.backtest.engine import compute_turnover, run_backtest


def test_compute_turnover():
    """Test turnover calculation."""
    old_weights = pd.Series({"A": 0.5, "B": 0.5})
    new_weights = pd.Series({"A": 0.3, "B": 0.7})
    
    turnover = compute_turnover(old_weights, new_weights)
    
    # Change: A: 0.5 -> 0.3 (0.2), B: 0.5 -> 0.7 (0.2)
    # Turnover = (0.2 + 0.2) / 2 = 0.2
    assert abs(turnover - 0.2) < 1e-6


def test_compute_turnover_new_asset():
    """Test turnover with new asset."""
    old_weights = pd.Series({"A": 1.0})
    new_weights = pd.Series({"B": 1.0})
    
    turnover = compute_turnover(old_weights, new_weights)
    
    # Complete turnover: A -> B
    assert abs(turnover - 1.0) < 1e-6


def test_backtest_end_to_end():
    """End-to-end smoke test: create synthetic data, run backtest, verify outputs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        
        # Create minimal config
        config = {
            "start_date": "2024-01-01",
            "end_date": "2024-01-10",
            "base_asset": "BTC",
            "strategy_name": "test_smoke",
            "rebalance_frequency": "monthly",  # Required for report generation
            "cost_model": {
                "fee_bps": 5,
                "slippage_bps": 5,
            },
        }
        config_path = tmp_path / "test_config.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config, f)
        
        # Create synthetic price data (BTC + ETH, 10 days)
        dates = pd.date_range("2024-01-01", "2024-01-10", freq="D")
        prices_df = pd.DataFrame({
            "BTC": [50000.0 + i * 100 for i in range(len(dates))],
            "ETH": [3000.0 + i * 50 for i in range(len(dates))],
        }, index=dates)
        prices_path = tmp_path / "prices.parquet"
        prices_df.to_parquet(prices_path)
        
        # Create synthetic snapshots (two rebalances: 2024-01-01 and 2024-01-05)
        # Rebalance 1: 100% ETH
        # Rebalance 2: 50% ETH, 50% (new asset, but we'll use ETH again for simplicity)
        snapshots_data = [
            {
                "rebalance_date": date(2024, 1, 1),
                "snapshot_date": date(2024, 1, 1),
                "symbol": "ETH",
                "coingecko_id": "ethereum",
                "venue": "BINANCE",
                "basket_name": "test_TOP5",
                "selection_version": "v1",
                "rank": 1,
                "weight": 1.0,
                "marketcap": 1000000000.0,
                "volume_14d": 10000000.0,
            },
            {
                "rebalance_date": date(2024, 1, 5),
                "snapshot_date": date(2024, 1, 5),
                "symbol": "ETH",
                "coingecko_id": "ethereum",
                "venue": "BINANCE",
                "basket_name": "test_TOP5",
                "selection_version": "v1",
                "rank": 1,
                "weight": 1.0,  # Still 100% ETH for simplicity
                "marketcap": 1100000000.0,
                "volume_14d": 11000000.0,
            },
        ]
        snapshots_df = pd.DataFrame(snapshots_data)
        snapshots_path = tmp_path / "snapshots.parquet"
        snapshots_df.to_parquet(snapshots_path)
        
        # Verify weights sum to 1 per rebalance
        for rebal_date in snapshots_df["rebalance_date"].unique():
            weights = snapshots_df[snapshots_df["rebalance_date"] == rebal_date]["weight"]
            assert abs(weights.sum() - 1.0) < 1e-6, f"Weights for {rebal_date} sum to {weights.sum()}, not 1.0"
        
        output_dir = tmp_path / "outputs"
        
        # Run backtest
        result = run_backtest(
            config_path,
            prices_path,
            snapshots_path,
            output_dir,
        )
        
        # Assert return value is dict with required keys
        assert isinstance(result, dict), "run_backtest() must return a dict"
        assert "row_count" in result, "Missing 'row_count' in return value"
        assert "date_range" in result, "Missing 'date_range' in return value"
        assert "num_trading_days" in result, "Missing 'num_trading_days' in return value"
        assert "num_rebalance_dates" in result, "Missing 'num_rebalance_dates' in return value"
        
        # Assert row_count > 0
        assert result["row_count"] > 0, f"Expected row_count > 0, got {result['row_count']}"
        
        # Assert date_range matches config
        assert result["date_range"]["start_date"] == "2024-01-01"
        assert result["date_range"]["end_date"] == "2024-01-10"
        
        # Assert output files exist
        results_path = output_dir / "backtest_results.csv"
        report_path = output_dir / "report.md"
        
        assert results_path.exists(), f"backtest_results.csv not found at {results_path}"
        assert report_path.exists(), f"report.md not found at {report_path}"
        
        # Verify results CSV has data
        results_df = pd.read_csv(results_path)
        assert len(results_df) > 0, "backtest_results.csv is empty"
        assert len(results_df) == result["row_count"], "CSV row count doesn't match return value"
        
        # Verify required columns exist
        required_cols = ["date", "r_btc", "r_basket", "r_ls", "cost", "r_ls_net", "equity_curve"]
        for col in required_cols:
            assert col in results_df.columns, f"Missing column: {col}"
        
        # Verify equity curve starts at 1.0 (before first costs)
        equity = results_df["equity_curve"].values
        # First day may have costs, so equity might be < 1.0, but should be close
        # Check that equity is reasonable (between 0.9 and 1.0 for first day with costs)
        assert 0.9 <= equity[0] <= 1.0, f"Equity curve first value should be between 0.9 and 1.0, got {equity[0]}"
        
        # Verify report exists and has content
        with open(report_path) as f:
            report_content = f.read()
        assert len(report_content) > 0, "report.md is empty"
        assert "Backtest Report" in report_content, "report.md missing title"


def test_run_metadata_generation():
    """Test that run_metadata.json is generated with required fields."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        
        # Create minimal config
        config = {
            "start_date": "2024-01-01",
            "end_date": "2024-01-05",
            "base_asset": "BTC",
            "strategy_name": "test_metadata",
            "rebalance_frequency": "monthly",  # Required for report generation
            "cost_model": {
                "fee_bps": 5,
                "slippage_bps": 5,
            },
        }
        config_path = tmp_path / "test_config.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config, f)
        
        # Create minimal price data
        dates = pd.date_range("2024-01-01", "2024-01-05", freq="D")
        prices_df = pd.DataFrame({
            "BTC": [50000.0] * len(dates),
        }, index=dates)
        prices_path = tmp_path / "prices.parquet"
        prices_df.to_parquet(prices_path)
        
        # Create minimal snapshots
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
        
        # Import the script's metadata generation logic
        from src.utils.metadata import create_run_metadata, save_run_metadata
        
        # Simulate what the script does
        backtest_result = run_backtest(
            config_path,
            prices_path,
            snapshots_path,
            output_dir,
        )
        
        # Generate metadata (as the script would)
        row_counts = {}
        results_path = output_dir / "backtest_results.csv"
        if results_path.exists():
            results_df = pd.read_csv(results_path)
            row_counts["backtest_results"] = len(results_df)
        
        metadata = create_run_metadata(
            script_name="run_backtest.py",
            config_path=config_path,
            data_paths={
                "backtest_results": results_path,
                "snapshots": snapshots_path,
                "prices": prices_path,
            },
            row_counts=row_counts,
            date_range=backtest_result.get("date_range", {}),
            repo_root=tmp_path,  # Use tmp_path as repo root for test
        )
        
        metadata_path = output_dir / "run_metadata_backtest.json"
        save_run_metadata(metadata, metadata_path)
        
        # Assert metadata file exists
        assert metadata_path.exists(), f"run_metadata_backtest.json not found at {metadata_path}"
        
        # Load and verify metadata
        with open(metadata_path) as f:
            metadata_loaded = json.load(f)
        
        # Assert required fields exist
        assert "run_timestamp" in metadata_loaded, "Missing 'run_timestamp'"
        assert "script_name" in metadata_loaded, "Missing 'script_name'"
        assert metadata_loaded["script_name"] == "run_backtest.py"
        
        # git_commit_hash may be None if not in git repo, but field should exist
        assert "git_commit_hash" in metadata_loaded, "Missing 'git_commit_hash'"
        
        # config_hash should exist (even if None)
        assert "config_file" in metadata_loaded, "Missing 'config_file'"
        assert "config_hash" in metadata_loaded, "Missing 'config_hash'"
        
        # data_files should exist with hashes (data version identifiers)
        assert "data_files" in metadata_loaded, "Missing 'data_files'"
        assert "backtest_results" in metadata_loaded["data_files"], "Missing 'backtest_results' in data_files"
        assert "hash" in metadata_loaded["data_files"]["backtest_results"], "Missing 'hash' in data_files (data version identifier)"
        # Hash may be None if file doesn't exist, but field must be present
        
        # row_counts should exist
        assert "row_counts" in metadata_loaded, "Missing 'row_counts'"
        assert "backtest_results" in metadata_loaded["row_counts"], "Missing 'backtest_results' in row_counts"
        
        # date_range should exist
        assert "date_range" in metadata_loaded, "Missing 'date_range'"
        assert "start_date" in metadata_loaded["date_range"], "Missing 'start_date' in date_range"
        assert "end_date" in metadata_loaded["date_range"], "Missing 'end_date' in date_range"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
