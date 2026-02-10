"""Tests for Binance perp eligibility (point-in-time check)."""

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


def test_binance_perp_eligibility_point_in_time():
    """Test that assets with onboard_date after rebalance_date are excluded."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        
        # Create test data
        start_date = date(2023, 1, 1)
        end_date = date(2023, 2, 1)
        
        # Create prices, mcaps, volumes
        dates = pd.date_range(start=start_date, end=end_date, freq="D")
        prices_df = pd.DataFrame(
            {"BTC": [30000] * len(dates), "ETH": [2000] * len(dates), "NEWCOIN": [100] * len(dates)},
            index=dates
        )
        mcaps_df = pd.DataFrame(
            {"BTC": [600e9] * len(dates), "ETH": [240e9] * len(dates), "NEWCOIN": [10e9] * len(dates)},
            index=dates
        )
        volumes_df = pd.DataFrame(
            {"BTC": [1e9] * len(dates), "ETH": [500e6] * len(dates), "NEWCOIN": [100e6] * len(dates)},
            index=dates
        )
        
        # Create allowlist
        allowlist_df = pd.DataFrame({
            "symbol": ["BTC", "ETH", "NEWCOIN"],
            "coingecko_id": ["bitcoin", "ethereum", "newcoin"],
            "venue": ["BINANCE", "BINANCE", "BINANCE"],
        })
        
        # Create Binance perp listings: NEWCOIN onboarded on 2023-01-15 (after first rebalance on 2023-01-01)
        perp_listings_df = pd.DataFrame({
            "symbol": ["BTC", "ETH", "NEWCOIN"],
            "onboard_date": [date(2020, 1, 1), date(2020, 1, 1), date(2023, 1, 15)],  # NEWCOIN listed after first rebalance
            "source": ["binance_exchangeInfo"] * 3,
            "proxy_version": ["v0"] * 3,
        })
        
        # Create config
        config = {
            "strategy_name": "test",
            "start_date": str(start_date),
            "end_date": str(end_date),
            "rebalance_frequency": "monthly",
            "rebalance_day": 1,
            "base_asset": "BTC",
            "top_n": 10,
            "eligibility": {
                "must_have_perp": True,
                "min_listing_days": 0,  # No age requirement for simplicity
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
            perp_listings_path=tmp_path / "perp_listings.parquet",
        )
        
        # Check universe eligibility
        universe_df = pd.read_parquet(universe_path)
        
        # On 2023-01-01 rebalance: NEWCOIN should be excluded (onboard_date 2023-01-15 > 2023-01-01)
        jan_eligibility = universe_df[universe_df["rebalance_date"] == date(2023, 1, 1)]
        newcoin_jan = jan_eligibility[jan_eligibility["symbol"] == "NEWCOIN"].iloc[0]
        assert newcoin_jan["exclusion_reason"] == "perp_not_listed_yet"
        assert newcoin_jan["perp_eligible_proxy"] == False
        
        # On 2023-02-01 rebalance: NEWCOIN should be eligible (onboard_date 2023-01-15 < 2023-02-01)
        feb_eligibility = universe_df[universe_df["rebalance_date"] == date(2023, 2, 1)]
        newcoin_feb = feb_eligibility[feb_eligibility["symbol"] == "NEWCOIN"].iloc[0]
        assert newcoin_feb["exclusion_reason"] is None or pd.isna(newcoin_feb["exclusion_reason"])
        assert newcoin_feb["perp_eligible_proxy"] == True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
