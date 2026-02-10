"""Test that backtest cost calculation uses correct turnover (not always 100%)."""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
from datetime import date
import yaml
import tempfile

from src.backtest.engine import run_backtest


def test_costs_not_always_100_percent_turnover():
    """
    Test that cost calculation correctly uses prev_weights, not always 100% turnover.
    
    This test would fail if prev_weights is not updated correctly, causing
    costs to always be calculated as 100% turnover (first rebalance case).
    """
    # Create temporary config
    config = {
        "strategy_name": "test",
        "start_date": "2024-02-01",
        "end_date": "2024-03-31",
        "rebalance_frequency": "monthly",
        "rebalance_day": 1,
        "base_asset": "BTC",
        "cost_model": {
            "fee_bps": 5,
            "slippage_bps": 5,
        },
        "backtest": {
            "gap_fill_mode": "none",
            "basket_coverage_threshold": 0.90,
            "missing_price_policy": "nan",
        },
    }
    
    # Create minimal prices data (3 assets, 60 days)
    dates = pd.date_range("2024-02-01", "2024-03-31", freq="D")
    prices_data = {
        "BTC": np.ones(len(dates)) * 50000,
        "ETH": np.ones(len(dates)) * 3000,
        "SOL": np.ones(len(dates)) * 100,
    }
    prices_df = pd.DataFrame(prices_data, index=dates)
    prices_df.index = prices_df.index.date
    
    # Create snapshots with identical baskets (should have 0% turnover on second rebalance)
    snapshots_data = [
        {"rebalance_date": date(2024, 2, 1), "symbol": "ETH", "weight": 0.5},
        {"rebalance_date": date(2024, 2, 1), "symbol": "SOL", "weight": 0.5},
        {"rebalance_date": date(2024, 3, 1), "symbol": "ETH", "weight": 0.5},  # Same basket
        {"rebalance_date": date(2024, 3, 1), "symbol": "SOL", "weight": 0.5},  # Same basket
    ]
    snapshots_df = pd.DataFrame(snapshots_data)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        config_path = tmp_path / "config.yaml"
        prices_path = tmp_path / "prices.parquet"
        snapshots_path = tmp_path / "snapshots.parquet"
        output_dir = tmp_path / "outputs"
        
        # Write config and data
        with open(config_path, "w") as f:
            yaml.dump(config, f)
        prices_df.to_parquet(prices_path)
        snapshots_df.to_parquet(snapshots_path)
        
        # Run backtest
        result = run_backtest(config_path, prices_path, snapshots_path, output_dir)
        
        # Load results
        results_df = pd.read_csv(output_dir / "backtest_results.csv")
        turnover_df = pd.read_csv(output_dir / "rebalance_turnover.csv")
        
        # Check that second rebalance has 0% turnover (identical baskets)
        march_turnover = turnover_df[turnover_df["rebalance_date"] == "2024-03-01"]["turnover"].values[0]
        assert abs(march_turnover) < 1e-6, f"Expected 0% turnover for identical baskets, got {march_turnover:.2%}"
        
        # Check that costs on March 1 are NOT the same as costs on Feb 1
        # (Feb 1 should have cost from 100% turnover, March 1 should have cost from 0% turnover)
        feb_costs = results_df[results_df["date"] == "2024-02-01"]["cost"].values[0]
        march_costs = results_df[results_df["date"] == "2024-03-01"]["cost"].values[0]
        
        # March costs should be much lower (0% turnover vs 100% turnover)
        assert march_costs < feb_costs * 0.1, (
            f"Expected March costs ({march_costs:.6f}) to be much lower than Feb costs ({feb_costs:.6f}) "
            f"due to 0% turnover, but they're too similar. This suggests prev_weights is not being updated."
        )
        
        # Verify that costs are proportional to turnover
        expected_cost_bps = 5 + 5  # fee + slippage
        expected_feb_cost = expected_cost_bps / 10000.0 * 1.0  # 100% turnover
        expected_march_cost = expected_cost_bps / 10000.0 * 0.0  # 0% turnover
        
        assert abs(feb_costs - expected_feb_cost) < 1e-6, f"Feb cost should be {expected_feb_cost:.6f}, got {feb_costs:.6f}"
        assert abs(march_costs - expected_march_cost) < 1e-6, f"March cost should be {expected_march_cost:.6f}, got {march_costs:.6f}"


if __name__ == "__main__":
    test_costs_not_always_100_percent_turnover()
    print("Test passed: costs correctly use prev_weights, not always 100% turnover")

