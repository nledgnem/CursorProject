"""
Pytest configuration and shared fixtures for tests.

Provides helpers for creating test data in both wide format and data lake format.
"""

import pandas as pd
from pathlib import Path
from datetime import date
import tempfile


def create_test_data_wide(tmp_path: Path, dates: pd.DatetimeIndex) -> Path:
    """
    Create test data in wide format (for backward compatibility).
    
    Returns path to data directory.
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    
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
    
    prices_df.to_parquet(data_dir / "prices_daily.parquet")
    mcaps_df.to_parquet(data_dir / "marketcap_daily.parquet")
    volumes_df.to_parquet(data_dir / "volume_daily.parquet")
    
    return data_dir


def create_test_data_lake(tmp_path: Path, dates: pd.DatetimeIndex) -> Path:
    """
    Create test data in data lake format.
    
    Returns path to data_lake directory.
    """
    data_lake_dir = tmp_path / "data_lake"
    data_lake_dir.mkdir()
    
    # Create dimension table
    dim_asset = pd.DataFrame({
        "asset_id": ["BTC", "ETH", "SOL"],
        "symbol": ["BTC", "ETH", "SOL"],
        "name": ["Bitcoin", "Ethereum", "Solana"],
        "chain": [None, None, None],
        "contract_address": [None, None, None],
        "coingecko_id": ["bitcoin", "ethereum", "solana"],
        "is_stable": [False, False, False],
        "is_wrapped_stable": [False, False, False],
        "metadata_json": [None, None, None],
    })
    dim_asset.to_parquet(data_lake_dir / "dim_asset.parquet")
    
    # Create fact tables
    fact_rows = []
    for date_val in dates:
        date_obj = date_val.date() if hasattr(date_val, 'date') else date_val
        for asset_id, price, mcap, vol in [
            ("BTC", 30000, 600e9, 1e9),
            ("ETH", 2000, 240e9, 500e6),
            ("SOL", 100, 10e9, 100e6),
        ]:
            fact_rows.append({
                "asset_id": asset_id,
                "date": date_obj,
                "close": price,
                "source": "test"
            })
            fact_rows.append({
                "asset_id": asset_id,
                "date": date_obj,
                "marketcap": mcap,
                "source": "test"
            })
            fact_rows.append({
                "asset_id": asset_id,
                "date": date_obj,
                "volume": vol,
                "source": "test"
            })
    
    fact_df = pd.DataFrame(fact_rows)
    fact_price = fact_df[fact_df['close'].notna()][['asset_id', 'date', 'close', 'source']]
    fact_marketcap = fact_df[fact_df['marketcap'].notna()][['asset_id', 'date', 'marketcap', 'source']]
    fact_volume = fact_df[fact_df['volume'].notna()][['asset_id', 'date', 'volume', 'source']]
    
    fact_price.to_parquet(data_lake_dir / "fact_price.parquet")
    fact_marketcap.to_parquet(data_lake_dir / "fact_marketcap.parquet")
    fact_volume.to_parquet(data_lake_dir / "fact_volume.parquet")
    
    return data_lake_dir

