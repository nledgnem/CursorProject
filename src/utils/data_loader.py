"""
Data loading utilities for data lake format.

Provides functions to load fact table data in formats compatible with existing code.
"""

import pandas as pd
from pathlib import Path
from typing import Optional
from datetime import date


def load_fact_table_as_wide(
    fact_table_path: Path,
    value_column: str,
    asset_id_column: str = 'asset_id',
    date_column: str = 'date',
    dim_asset_path: Optional[Path] = None,
    symbol_column: str = 'symbol',
) -> pd.DataFrame:
    """
    Load a fact table and convert to wide format (date index, asset symbols as columns).
    
    This provides backward compatibility for code that expects wide format DataFrames.
    
    Args:
        fact_table_path: Path to fact table parquet file (e.g., fact_price.parquet)
        value_column: Column name containing the values (e.g., 'close', 'marketcap', 'volume')
        asset_id_column: Column name for asset IDs (default: 'asset_id')
        date_column: Column name for dates (default: 'date')
        dim_asset_path: Optional path to dim_asset.parquet to map asset_id -> symbol
        symbol_column: Column name in dim_asset for symbols (default: 'symbol')
    
    Returns:
        DataFrame with date index and asset symbols as columns (wide format)
    """
    # Load fact table
    fact_df = pd.read_parquet(fact_table_path)
    
    # Load dimension table to map asset_id -> symbol
    if dim_asset_path and dim_asset_path.exists():
        dim_asset = pd.read_parquet(dim_asset_path)
        id_to_symbol = dict(zip(dim_asset[asset_id_column], dim_asset[symbol_column]))
        
        # Map asset_id to symbol
        fact_df['symbol'] = fact_df[asset_id_column].map(id_to_symbol)
        
        # Drop rows where symbol mapping failed
        fact_df = fact_df.dropna(subset=['symbol'])
    else:
        # If no dim_asset, use asset_id as symbol
        fact_df['symbol'] = fact_df[asset_id_column]
    
    # Pivot to wide format: date index, symbols as columns
    wide_df = fact_df.pivot_table(
        index=date_column,
        columns='symbol',
        values=value_column,
        aggfunc='first'  # Take first value if duplicates
    )
    
    # Convert index to date if it's datetime
    if isinstance(wide_df.index, pd.DatetimeIndex):
        wide_df.index = wide_df.index.date
    
    # Sort by date
    wide_df = wide_df.sort_index()
    
    return wide_df


def load_prices_wide(data_lake_dir: Path, dim_asset_path: Optional[Path] = None) -> pd.DataFrame:
    """Load fact_price as wide format DataFrame."""
    fact_price_path = data_lake_dir / 'fact_price.parquet'
    if not fact_price_path.exists():
        raise FileNotFoundError(f"Fact price table not found: {fact_price_path}")
    
    return load_fact_table_as_wide(
        fact_price_path,
        value_column='close',
        dim_asset_path=dim_asset_path or (data_lake_dir / 'dim_asset.parquet'),
    )


def load_marketcap_wide(data_lake_dir: Path, dim_asset_path: Optional[Path] = None) -> pd.DataFrame:
    """Load fact_marketcap as wide format DataFrame."""
    fact_marketcap_path = data_lake_dir / 'fact_marketcap.parquet'
    if not fact_marketcap_path.exists():
        raise FileNotFoundError(f"Fact marketcap table not found: {fact_marketcap_path}")
    
    return load_fact_table_as_wide(
        fact_marketcap_path,
        value_column='marketcap',
        dim_asset_path=dim_asset_path or (data_lake_dir / 'dim_asset.parquet'),
    )


def load_volume_wide(data_lake_dir: Path, dim_asset_path: Optional[Path] = None) -> pd.DataFrame:
    """Load fact_volume as wide format DataFrame."""
    fact_volume_path = data_lake_dir / 'fact_volume.parquet'
    if not fact_volume_path.exists():
        raise FileNotFoundError(f"Fact volume table not found: {fact_volume_path}")
    
    return load_fact_table_as_wide(
        fact_volume_path,
        value_column='volume',
        dim_asset_path=dim_asset_path or (data_lake_dir / 'dim_asset.parquet'),
    )


def load_data_lake_wide(
    data_lake_dir: Path,
    prices: bool = True,
    marketcap: bool = True,
    volume: bool = True,
) -> dict:
    """
    Load all fact tables as wide format DataFrames.
    
    Returns a dict with keys: 'prices', 'marketcap', 'volume' (if requested)
    """
    dim_asset_path = data_lake_dir / 'dim_asset.parquet'
    if not dim_asset_path.exists():
        dim_asset_path = None  # Will use asset_id as symbol
    
    result = {}
    
    if prices:
        result['prices'] = load_prices_wide(data_lake_dir, dim_asset_path)
    
    if marketcap:
        result['marketcap'] = load_marketcap_wide(data_lake_dir, dim_asset_path)
    
    if volume:
        result['volume'] = load_volume_wide(data_lake_dir, dim_asset_path)
    
    return result

