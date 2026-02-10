"""Tests for exclusion ordering (stablecoin/blacklist should take precedence over perp eligibility)."""

import pytest
import pandas as pd
import numpy as np
from datetime import date, timedelta
from pathlib import Path
import tempfile
import yaml

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.universe.snapshot import build_snapshots


def test_stablecoin_exclusion_takes_precedence_over_perp():
    """Test that stablecoin exclusion reason is correct even when must_have_perp=True."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        
        # Create test data
        start_date = date(2023, 1, 1)
        end_date = date(2023, 1, 1)  # Single rebalance
        dates = pd.date_range(start=start_date, end=start_date, freq="D")
        
        # Create prices with USDT (a stablecoin)
        prices_df = pd.DataFrame({
            "BTC": [30000],
            "USDT": [1.0],  # Stablecoin
            "ETH": [2000],
        }, index=dates)
        
        mcaps_df = pd.DataFrame({
            "BTC": [600e9],
            "USDT": [80e9],
            "ETH": [240e9],
        }, index=dates)
        
        volumes_df = pd.DataFrame({
            "BTC": [1e9],
            "USDT": [10e9],
            "ETH": [500e6],
        }, index=dates)
        
        # Create allowlist (USDT is in allowlist but should be excluded as stablecoin)
        allowlist_df = pd.DataFrame({
            "symbol": ["BTC", "USDT", "ETH"],
            "coingecko_id": ["bitcoin", "tether", "ethereum"],
            "venue": ["BINANCE", "BINANCE", "BINANCE"],
        })
        
        # Create Binance perp listings (USDT has perp, but should still be excluded as stablecoin)
        perp_listings_df = pd.DataFrame({
            "symbol": ["BTCUSDT", "USDTUSDT", "ETHUSDT"],  # Binance format
            "onboard_date": [date(2020, 1, 1), date(2020, 1, 1), date(2020, 1, 1)],
            "source": ["binance_exchangeInfo"] * 3,
            "proxy_version": ["v0"] * 3,
        })
        
        # Create stablecoins list
        stablecoins_df = pd.DataFrame({
            "symbol": ["USDT"],
            "is_stable": [1],
        })
        
        # Create config with must_have_perp=True
        config = {
            "strategy_name": "test",
            "start_date": str(start_date),
            "end_date": str(end_date),
            "rebalance_frequency": "monthly",
            "rebalance_day": 1,
            "base_asset": "BTC",
            "top_n": 10,
            "eligibility": {
                "must_have_perp": True,  # Perp required
                "min_listing_days": 0,
                "min_mcap_usd": None,
                "min_volume_usd": None,
            },
            "weighting": "equal_weight_capped",
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
        perp_listings_df.to_parquet(tmp_path / "perp_listings.parquet")
        stablecoins_df.to_csv(tmp_path / "stablecoins.csv", index=False)
        
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
            stablecoins_path=tmp_path / "stablecoins.csv",
            perp_listings_path=tmp_path / "perp_listings.parquet",
        )
        
        # Check universe eligibility
        universe_df = pd.read_parquet(universe_path)
        
        # USDT should be excluded with reason "blacklist_or_stablecoin", NOT "perp_not_listed_yet"
        usdt_row = universe_df[universe_df["symbol"] == "USDT"].iloc[0]
        assert usdt_row["exclusion_reason"] == "blacklist_or_stablecoin", \
            f"Expected 'blacklist_or_stablecoin', got '{usdt_row['exclusion_reason']}'"
        assert usdt_row["is_stablecoin"] == True
        
        # ETH should be eligible (has perp, not stablecoin, not base asset)
        eth_row = universe_df[universe_df["symbol"] == "ETH"].iloc[0]
        assert eth_row["exclusion_reason"] is None or pd.isna(eth_row["exclusion_reason"]), \
            f"ETH should be eligible, but got exclusion_reason: {eth_row['exclusion_reason']}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
