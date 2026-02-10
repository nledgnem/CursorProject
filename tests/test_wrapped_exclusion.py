"""Test wrapped/synthetic asset exclusion in universe eligibility."""

import pytest
from pathlib import Path
import pandas as pd
import tempfile
import shutil

# Add src to path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.universe.snapshot import build_snapshots


@pytest.fixture
def temp_data_dir(tmp_path):
    """Create temporary data directory with minimal test data."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    
    # Create minimal price/mcap/volume data
    dates = pd.date_range("2024-01-01", "2024-01-10", freq="D")
    
    # Include a wrapped asset (stETH) and a normal asset (ETH)
    prices_df = pd.DataFrame(
        {
            "ETH": [2000.0] * len(dates),
            "stETH": [2000.0] * len(dates),  # Wrapped asset
            "BTC": [40000.0] * len(dates),
        },
        index=dates,
    )
    
    mcaps_df = pd.DataFrame(
        {
            "ETH": [200_000_000_000] * len(dates),
            "stETH": [20_000_000_000] * len(dates),
            "BTC": [800_000_000_000] * len(dates),
        },
        index=dates,
    )
    
    volumes_df = pd.DataFrame(
        {
            "ETH": [10_000_000_000] * len(dates),
            "stETH": [1_000_000_000] * len(dates),
            "BTC": [20_000_000_000] * len(dates),
        },
        index=dates,
    )
    
    prices_df.to_parquet(data_dir / "prices_daily.parquet")
    mcaps_df.to_parquet(data_dir / "marketcap_daily.parquet")
    volumes_df.to_parquet(data_dir / "volume_daily.parquet")
    
    return data_dir


@pytest.fixture
def temp_wrapped_file(tmp_path):
    """Create temporary wrapped.csv file."""
    wrapped_path = tmp_path / "wrapped.csv"
    wrapped_df = pd.DataFrame({
        "symbol": ["stETH"],
        "reason": ["Wrapped synthetic asset"],
    })
    wrapped_df.to_csv(wrapped_path, index=False)
    return wrapped_path


@pytest.fixture
def temp_allowlist(tmp_path):
    """Create temporary allowlist."""
    allowlist_path = tmp_path / "perp_allowlist.csv"
    allowlist_df = pd.DataFrame({
        "symbol": ["ETH", "stETH", "BTC"],
        "coingecko_id": ["ethereum", "staked-ether", "bitcoin"],
        "venue": ["BINANCE", "BINANCE", "BINANCE"],
    })
    allowlist_df.to_csv(allowlist_path, index=False)
    return allowlist_path


@pytest.fixture
def temp_config(tmp_path):
    """Create temporary config file."""
    config_path = tmp_path / "test_config.yaml"
    config_content = """start_date: '2024-01-01'
end_date: '2024-01-10'
rebalance_frequency: monthly
rebalance_day: 1
top_n: 10
base_asset: BTC
strategy_name: test
eligibility:
  must_have_perp: false
  min_listing_days: 0
  min_mcap_usd: 0
  min_volume_usd: 0
weighting: cap_weighted
max_weight_per_asset: 0.5
"""
    config_path.write_text(config_content)
    return config_path


def test_wrapped_exclusion(
    temp_data_dir, temp_wrapped_file, temp_allowlist, temp_config, tmp_path
):
    """Test that wrapped assets are excluded from eligibility."""
    output_path = tmp_path / "universe_snapshots.parquet"
    
    # Build snapshots with wrapped exclusion
    build_snapshots(
        config_path=temp_config,
        prices_path=temp_data_dir / "prices_daily.parquet",
        mcaps_path=temp_data_dir / "marketcap_daily.parquet",
        volumes_path=temp_data_dir / "volume_daily.parquet",
        allowlist_path=temp_allowlist,
        output_path=output_path,
        wrapped_path=temp_wrapped_file,
    )
    
    # Check universe_eligibility
    eligibility_path = output_path.parent / "universe_eligibility.parquet"
    assert eligibility_path.exists(), "universe_eligibility.parquet should be created"
    
    eligibility_df = pd.read_parquet(eligibility_path)
    
    # Check that stETH is excluded
    steth_rows = eligibility_df[eligibility_df["symbol"] == "stETH"]
    assert len(steth_rows) > 0, "stETH should appear in eligibility table"
    
    # Check exclusion reason
    steth_excluded = steth_rows[steth_rows["exclusion_reason"] == "wrapped_or_synthetic"]
    assert len(steth_excluded) > 0, "stETH should have exclusion_reason='wrapped_or_synthetic'"
    
    # Check is_wrapped flag
    assert steth_rows["is_wrapped"].all(), "stETH should have is_wrapped=True"
    
    # Check that stETH is not eligible
    steth_eligible = steth_rows[steth_rows["eligible"] == True]
    assert len(steth_eligible) == 0, "stETH should not be eligible"
    
    # Check that ETH (not wrapped) is eligible
    eth_rows = eligibility_df[eligibility_df["symbol"] == "ETH"]
    if len(eth_rows) > 0:
        eth_eligible = eth_rows[eth_rows["eligible"] == True]
        assert len(eth_eligible) > 0, "ETH (not wrapped) should be eligible"
    
    print("[PASS] Wrapped exclusion test passed")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

