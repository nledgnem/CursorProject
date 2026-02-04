#!/usr/bin/env python3
"""
Convert wide-format parquet files (dates x symbols) to normalized fact tables.

This script reads the curated wide-format files and converts them to:
- fact_price(asset_id, date, close, source)
- fact_marketcap(asset_id, date, marketcap, source)
- fact_volume(asset_id, date, volume, source)

It also builds dimension and mapping tables from the data.
"""

import sys
import argparse
from pathlib import Path
from datetime import date, timedelta
import pandas as pd

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.data_lake.schema import (
    FACT_PRICE_SCHEMA,
    FACT_MARKETCAP_SCHEMA,
    FACT_VOLUME_SCHEMA,
    create_empty_table,
)
from src.data_lake.mapping import (
    generate_asset_id,
    build_dim_asset_from_coingecko,
    build_map_provider_asset_coingecko,
    build_dim_instrument_from_binance_perps,
    build_map_provider_instrument_binance,
)


def load_stablecoins(stablecoins_path: Path) -> set:
    """Load stablecoin symbols."""
    if not stablecoins_path.exists():
        return set()
    
    df = pd.read_csv(stablecoins_path)
    if "symbol" in df.columns:
        return set(df["symbol"].str.upper().str.strip())
    return set()


def convert_wide_to_fact_table(
    wide_df: pd.DataFrame,
    fact_schema: dict,
    source: str,
    value_column_name: str,  # "close", "marketcap", "volume"
) -> pd.DataFrame:
    """
    Convert wide DataFrame (dates x symbols) to normalized fact table.
    
    Args:
        wide_df: DataFrame with dates as index, symbols as columns
        fact_schema: Schema for fact table
        source: Data source name (e.g., "coingecko")
        value_column_name: Name of value column in output (e.g., "close")
    
    Returns:
        DataFrame with fact table schema
    """
    rows = []
    
    # Normalize index to date objects
    if isinstance(wide_df.index, pd.DatetimeIndex):
        wide_df_index = wide_df.index.date
    else:
        wide_df_index = wide_df.index
    
    for date_val in wide_df_index:
        # Convert date_val to date object if needed
        if isinstance(date_val, pd.Timestamp):
            date_obj = date_val.date()
        elif isinstance(date_val, date):
            date_obj = date_val
        else:
            date_obj = pd.to_datetime(date_val).date()
        
        # Access using the original index (may be datetime or date)
        for symbol in wide_df.columns:
            # Try accessing with date_obj first, then try as datetime
            try:
                value = wide_df.loc[date_obj, symbol]
            except KeyError:
                # Try with datetime if index is DatetimeIndex
                if isinstance(wide_df.index, pd.DatetimeIndex):
                    value = wide_df.loc[pd.Timestamp(date_obj), symbol]
                else:
                    value = wide_df.loc[date_obj, symbol]
            
            # Skip NaN values
            if pd.isna(value):
                continue
            
            # Generate asset_id from symbol (for now, use symbol; improve later)
            asset_id = generate_asset_id(symbol=str(symbol))
            
            row = {
                "asset_id": asset_id,
                "date": date_obj,
                value_column_name: float(value),
                "source": source,
            }
            rows.append(row)
    
    df = pd.DataFrame(rows)
    return df


def build_dimensions_from_data(
    curated_dir: Path,
    stablecoins_path: Path,
    valid_from: date,
) -> tuple:
    """
    Build dimension and mapping tables from curated data.
    
    Returns:
        Tuple of (dim_asset, map_provider_asset) DataFrames
    """
    # Load stablecoins
    stablecoins = load_stablecoins(stablecoins_path)
    wrapped_stables = set()  # TODO: Load from config if available
    
    # Try to get symbols from fact_price if it exists, otherwise use wide format
    data_lake_dir = curated_dir.parent / "data_lake"
    fact_price_path = data_lake_dir / "fact_price.parquet"
    
    if fact_price_path.exists():
        # Use fact table to get unique asset_ids, then map to symbols via dim_asset
        fact_price = pd.read_parquet(fact_price_path)
        dim_asset_path = data_lake_dir / "dim_asset.parquet"
        if dim_asset_path.exists():
            dim_asset = pd.read_parquet(dim_asset_path)
            id_to_symbol = dict(zip(dim_asset['asset_id'], dim_asset['symbol']))
            unique_asset_ids = fact_price['asset_id'].unique()
            symbols = [id_to_symbol.get(aid, aid) for aid in unique_asset_ids if aid in id_to_symbol]
            print(f"  Loaded {len(symbols)} symbols from existing fact_price and dim_asset")
        else:
            # No dim_asset, use asset_ids as symbols
            symbols = fact_price['asset_id'].unique().tolist()
            print(f"  Loaded {len(symbols)} asset_ids from existing fact_price (no dim_asset found)")
    else:
        # Fallback to wide format
        prices_path = curated_dir / "prices_daily.parquet"
        if not prices_path.exists():
            raise FileNotFoundError(f"Neither fact_price nor prices_daily found. Tried: {fact_price_path}, {prices_path}")
        
        prices_df = pd.read_parquet(prices_path)
        symbols = prices_df.columns.tolist()
        print(f"  Loaded {len(symbols)} symbols from wide format prices_daily")
    
    # Create a simple coingecko_data structure from symbols
    # In a real implementation, we'd load this from CoinGecko metadata
    coingecko_data_rows = []
    for symbol in symbols:
        # Skip NaN/None values
        if pd.isna(symbol) or symbol is None:
            continue
        # Ensure symbol is a string
        symbol_str = str(symbol).strip()
        if not symbol_str:
            continue
        coingecko_data_rows.append({
            "symbol": symbol_str,
            "name": symbol_str,  # Placeholder
            "id": symbol_str.lower(),  # Placeholder - would be actual CoinGecko ID
        })
    coingecko_data = pd.DataFrame(coingecko_data_rows)
    
    # Build dimension tables
    dim_asset = build_dim_asset_from_coingecko(
        coingecko_data=coingecko_data,
        stablecoins=stablecoins,
        wrapped_stables=wrapped_stables,
    )
    
    map_provider_asset = build_map_provider_asset_coingecko(
        coingecko_data=coingecko_data,
        dim_asset=dim_asset,
        valid_from=valid_from,
    )
    
    return dim_asset, map_provider_asset


def main():
    parser = argparse.ArgumentParser(
        description="Convert wide-format parquet files to normalized fact tables",
    )
    parser.add_argument(
        "--curated-dir",
        type=Path,
        required=True,
        help="Directory with curated wide-format parquet files",
    )
    parser.add_argument(
        "--data-lake-dir",
        type=Path,
        required=True,
        help="Directory to write fact tables and dimension tables",
    )
    parser.add_argument(
        "--stablecoins",
        type=Path,
        default=Path("data/stablecoins.csv"),
        help="Path to stablecoins CSV file",
    )
    parser.add_argument(
        "--source",
        type=str,
        default="coingecko",
        help="Data source name (default: coingecko)",
    )
    parser.add_argument(
        "--perp-listings",
        type=Path,
        default=None,
        help="Path to Binance perp listings parquet file (optional)",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Incremental mode: only convert new dates/assets",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Start date for incremental conversion (YYYY-MM-DD). If not provided, auto-detect from existing fact tables.",
    )
    
    args = parser.parse_args()
    
    repo_root = Path(__file__).parent.parent
    curated_dir = (repo_root / args.curated_dir).resolve()
    data_lake_dir = (repo_root / args.data_lake_dir).resolve()
    stablecoins_path = (repo_root / args.stablecoins).resolve()
    
    data_lake_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 70)
    print("CONVERTING TO FACT TABLES")
    print("=" * 70)
    
    # Build dimension tables
    print("\n[Step 1] Building dimension tables...")
    valid_from = date.today()  # TODO: Use actual data start date
    dim_asset_new, map_provider_asset_new = build_dimensions_from_data(
        curated_dir=curated_dir,
        stablecoins_path=stablecoins_path,
        valid_from=valid_from,
    )
    
    # Preserve existing canonical IDs (idempotency)
    dim_asset_path = data_lake_dir / "dim_asset.parquet"
    if dim_asset_path.exists() and args.incremental:
        try:
            existing_dim_asset = pd.read_parquet(dim_asset_path)
            # Create lookup: symbol -> existing asset_id
            existing_symbol_to_asset_id = dict(zip(
                existing_dim_asset["symbol"].str.upper(),
                existing_dim_asset["asset_id"]
            ))
            
            # Preserve existing asset_ids for symbols that already exist
            for idx, row in dim_asset_new.iterrows():
                symbol_upper = row["symbol"].upper()
                if symbol_upper in existing_symbol_to_asset_id:
                    dim_asset_new.at[idx, "asset_id"] = existing_symbol_to_asset_id[symbol_upper]
            
            # Merge: existing takes precedence, append new
            existing_symbols = set(existing_dim_asset["symbol"].str.upper())
            new_symbols = set(dim_asset_new["symbol"].str.upper())
            symbols_to_add = new_symbols - existing_symbols
            
            if symbols_to_add:
                dim_asset_to_append = dim_asset_new[dim_asset_new["symbol"].str.upper().isin(symbols_to_add)]
                dim_asset = pd.concat([existing_dim_asset, dim_asset_to_append], ignore_index=True)
                print(f"  [INCREMENTAL] Preserved {len(existing_dim_asset)} existing assets, added {len(dim_asset_to_append)} new")
            else:
                dim_asset = existing_dim_asset
                print(f"  [INCREMENTAL] No new assets to add (all symbols already exist)")
        except Exception as e:
            print(f"  [WARN] Could not load existing dim_asset: {e}, using new")
            dim_asset = dim_asset_new
    else:
        dim_asset = dim_asset_new
    
    dim_asset.to_parquet(dim_asset_path, index=False)
    print(f"  Saved dim_asset: {len(dim_asset)} rows -> {dim_asset_path}")
    
    # Update map_provider_asset with preserved asset_ids
    symbol_to_asset_id = dict(zip(dim_asset["symbol"].str.upper(), dim_asset["asset_id"]))
    for idx, row in map_provider_asset_new.iterrows():
        symbol_upper = row.get("symbol", "").upper() if "symbol" in row else None
        if symbol_upper and symbol_upper in symbol_to_asset_id:
            # Update asset_id if symbol exists in dim_asset
            # Note: map_provider_asset uses coingecko_id, not symbol directly
            # We need to match via dim_asset lookup
            pass  # This is handled in build_map_provider_asset_coingecko
    
    # For map_provider_asset, append new mappings (don't overwrite existing)
    map_provider_asset_path = data_lake_dir / "map_provider_asset.parquet"
    if map_provider_asset_path.exists() and args.incremental:
        try:
            existing_map = pd.read_parquet(map_provider_asset_path)
            # Create set of existing (provider, provider_asset_id, valid_from) tuples
            existing_keys = set(zip(
                existing_map["provider"],
                existing_map["provider_asset_id"],
                existing_map["valid_from"]
            ))
            new_keys = set(zip(
                map_provider_asset_new["provider"],
                map_provider_asset_new["provider_asset_id"],
                map_provider_asset_new["valid_from"]
            ))
            keys_to_add = new_keys - existing_keys
            
            if keys_to_add:
                # Filter to only new mappings
                mask = map_provider_asset_new.apply(
                    lambda row: (row["provider"], row["provider_asset_id"], row["valid_from"]) in keys_to_add,
                    axis=1
                )
                map_to_append = map_provider_asset_new[mask]
                map_provider_asset = pd.concat([existing_map, map_to_append], ignore_index=True)
                print(f"  [INCREMENTAL] Preserved {len(existing_map)} existing mappings, added {len(map_to_append)} new")
            else:
                map_provider_asset = existing_map
                print(f"  [INCREMENTAL] No new mappings to add")
        except Exception as e:
            print(f"  [WARN] Could not load existing map_provider_asset: {e}, using new")
            map_provider_asset = map_provider_asset_new
    else:
        map_provider_asset = map_provider_asset_new
    
    map_provider_asset.to_parquet(map_provider_asset_path, index=False)
    print(f"  Saved map_provider_asset: {len(map_provider_asset)} rows -> {map_provider_asset_path}")
    
    # Build Binance instrument tables (if perp listings available)
    print("\n[Step 1.5] Building Binance instrument tables...")
    perp_listings_path = args.perp_listings
    if perp_listings_path is None:
        # Try default locations
        for default_path in [
            repo_root / "data" / "raw" / "perp_listings_binance.parquet",
            repo_root / "data" / "curated" / "perp_listings_binance.parquet",
            repo_root / "outputs" / "perp_listings_binance.parquet",
        ]:
            if default_path.exists():
                perp_listings_path = default_path
                break
    
    if perp_listings_path and Path(perp_listings_path).exists():
        try:
            perp_listings = pd.read_parquet(perp_listings_path)
            print(f"  Loading Binance perp listings from {perp_listings_path}...")
            
            # Build dim_instrument (with asset_id linkage)
            # Load dim_asset if available to link asset_id
            dim_asset_for_link = None
            dim_asset_path = data_lake_dir / "dim_asset.parquet"
            if dim_asset_path.exists():
                dim_asset_for_link = pd.read_parquet(dim_asset_path)
                print(f"    Loading dim_asset for asset_id linkage: {len(dim_asset_for_link)} assets")
            
            dim_instrument = build_dim_instrument_from_binance_perps(
                perp_listings,
                dim_asset=dim_asset_for_link,
            )
            dim_instrument_path = data_lake_dir / "dim_instrument.parquet"
            dim_instrument.to_parquet(dim_instrument_path, index=False)
            print(f"    Saved dim_instrument: {len(dim_instrument)} rows -> {dim_instrument_path}")
            if dim_asset_for_link is not None:
                linked_count = dim_instrument["asset_id"].notna().sum()
                print(f"    Linked {linked_count}/{len(dim_instrument)} instruments to asset_id")
            
            # Build map_provider_instrument
            map_provider_instrument = build_map_provider_instrument_binance(
                perp_listings=perp_listings,
                dim_instrument=dim_instrument,
                valid_from=valid_from,
            )
            map_provider_instrument_path = data_lake_dir / "map_provider_instrument.parquet"
            map_provider_instrument.to_parquet(map_provider_instrument_path, index=False)
            print(f"    Saved map_provider_instrument: {len(map_provider_instrument)} rows -> {map_provider_instrument_path}")
        except Exception as e:
            print(f"  [WARN] Failed to build Binance instrument tables: {e}")
            print(f"  [WARN] Continuing without instrument tables...")
    else:
        print(f"  [SKIP] Binance perp listings not found (optional)")
    
    # Convert fact tables
    print("\n[Step 2] Converting wide tables to fact tables...")
    
    # Determine date range for incremental conversion
    conversion_start_date = None
    if args.incremental:
        if args.start_date:
            conversion_start_date = date.fromisoformat(args.start_date)
        else:
            # Auto-detect: find last date in existing fact tables
            fact_price_path = data_lake_dir / "fact_price.parquet"
            if fact_price_path.exists():
                try:
                    existing_fact_price = pd.read_parquet(fact_price_path)
                    if len(existing_fact_price) > 0 and "date" in existing_fact_price.columns:
                        last_date = pd.to_datetime(existing_fact_price["date"]).max().date()
                        conversion_start_date = last_date + timedelta(days=1)
                        print(f"  [INCREMENTAL] Auto-detected start date: {conversion_start_date} (last date in fact_price: {last_date})")
                except Exception as e:
                    print(f"  [WARN] Could not auto-detect start date: {e}")
    
    # Prices
    prices_path = curated_dir / "prices_daily.parquet"
    if prices_path.exists():
        print(f"  Converting prices from {prices_path}...")
        prices_wide = pd.read_parquet(prices_path)
        
        # Filter to new dates if incremental
        if args.incremental and conversion_start_date:
            if isinstance(prices_wide.index, pd.DatetimeIndex):
                prices_wide = prices_wide[prices_wide.index >= pd.Timestamp(conversion_start_date)]
            else:
                prices_wide.index = pd.to_datetime(prices_wide.index)
                prices_wide = prices_wide[prices_wide.index >= pd.Timestamp(conversion_start_date)]
            if len(prices_wide) == 0:
                print(f"    [SKIP] No new dates to convert (all dates before {conversion_start_date})")
            else:
                print(f"    [INCREMENTAL] Converting {len(prices_wide)} new dates (from {conversion_start_date})")
        
        if len(prices_wide) > 0:
            fact_price_new = convert_wide_to_fact_table(
                wide_df=prices_wide,
                fact_schema=FACT_PRICE_SCHEMA,
                source=args.source,
                value_column_name="close",
            )
            
            # Append to existing if incremental
            fact_price_path = data_lake_dir / "fact_price.parquet"
            if args.incremental and fact_price_path.exists():
                try:
                    existing_fact_price = pd.read_parquet(fact_price_path)
                    # Remove duplicates (existing takes precedence)
                    existing_dates = set(existing_fact_price["date"].unique())
                    fact_price_new = fact_price_new[~fact_price_new["date"].isin(existing_dates)]
                    if len(fact_price_new) > 0:
                        fact_price = pd.concat([existing_fact_price, fact_price_new], ignore_index=True)
                        print(f"    [INCREMENTAL] Appending {len(fact_price_new):,} new rows to existing {len(existing_fact_price):,} rows")
                    else:
                        fact_price = existing_fact_price
                        print(f"    [INCREMENTAL] No new rows to append (all dates already exist)")
                except Exception as e:
                    print(f"    [WARN] Could not load existing fact_price: {e}, creating new")
                    fact_price = fact_price_new
            else:
                fact_price = fact_price_new
            
            fact_price.to_parquet(fact_price_path, index=False)
            print(f"    Saved fact_price: {len(fact_price):,} rows -> {fact_price_path}")
    else:
        print(f"  [SKIP] Prices file not found: {prices_path}")
    
    # Market cap
    mcap_path = curated_dir / "marketcap_daily.parquet"
    if mcap_path.exists():
        print(f"  Converting marketcap from {mcap_path}...")
        mcap_wide = pd.read_parquet(mcap_path)
        
        # Filter to new dates if incremental
        if args.incremental and conversion_start_date:
            if isinstance(mcap_wide.index, pd.DatetimeIndex):
                mcap_wide = mcap_wide[mcap_wide.index >= pd.Timestamp(conversion_start_date)]
            else:
                mcap_wide.index = pd.to_datetime(mcap_wide.index)
                mcap_wide = mcap_wide[mcap_wide.index >= pd.Timestamp(conversion_start_date)]
            if len(mcap_wide) == 0:
                print(f"    [SKIP] No new dates to convert")
            else:
                print(f"    [INCREMENTAL] Converting {len(mcap_wide)} new dates")
        
        if len(mcap_wide) > 0:
            fact_marketcap_new = convert_wide_to_fact_table(
                wide_df=mcap_wide,
                fact_schema=FACT_MARKETCAP_SCHEMA,
                source=args.source,
                value_column_name="marketcap",
            )
            
            # Append to existing if incremental
            fact_marketcap_path = data_lake_dir / "fact_marketcap.parquet"
            if args.incremental and fact_marketcap_path.exists():
                try:
                    existing_fact_marketcap = pd.read_parquet(fact_marketcap_path)
                    existing_dates = set(existing_fact_marketcap["date"].unique())
                    fact_marketcap_new = fact_marketcap_new[~fact_marketcap_new["date"].isin(existing_dates)]
                    if len(fact_marketcap_new) > 0:
                        fact_marketcap = pd.concat([existing_fact_marketcap, fact_marketcap_new], ignore_index=True)
                        print(f"    [INCREMENTAL] Appending {len(fact_marketcap_new):,} new rows")
                    else:
                        fact_marketcap = existing_fact_marketcap
                        print(f"    [INCREMENTAL] No new rows to append")
                except Exception as e:
                    print(f"    [WARN] Could not load existing fact_marketcap: {e}")
                    fact_marketcap = fact_marketcap_new
            else:
                fact_marketcap = fact_marketcap_new
            
            fact_marketcap.to_parquet(fact_marketcap_path, index=False)
            print(f"    Saved fact_marketcap: {len(fact_marketcap):,} rows -> {fact_marketcap_path}")
    else:
        print(f"  [SKIP] Marketcap file not found: {mcap_path}")
    
    # Volume
    volume_path = curated_dir / "volume_daily.parquet"
    if volume_path.exists():
        print(f"  Converting volume from {volume_path}...")
        volume_wide = pd.read_parquet(volume_path)
        
        # Filter to new dates if incremental
        if args.incremental and conversion_start_date:
            if isinstance(volume_wide.index, pd.DatetimeIndex):
                volume_wide = volume_wide[volume_wide.index >= pd.Timestamp(conversion_start_date)]
            else:
                volume_wide.index = pd.to_datetime(volume_wide.index)
                volume_wide = volume_wide[volume_wide.index >= pd.Timestamp(conversion_start_date)]
            if len(volume_wide) == 0:
                print(f"    [SKIP] No new dates to convert")
            else:
                print(f"    [INCREMENTAL] Converting {len(volume_wide)} new dates")
        
        if len(volume_wide) > 0:
            fact_volume_new = convert_wide_to_fact_table(
                wide_df=volume_wide,
                fact_schema=FACT_VOLUME_SCHEMA,
                source=args.source,
                value_column_name="volume",
            )
            
            # Append to existing if incremental
            fact_volume_path = data_lake_dir / "fact_volume.parquet"
            if args.incremental and fact_volume_path.exists():
                try:
                    existing_fact_volume = pd.read_parquet(fact_volume_path)
                    existing_dates = set(existing_fact_volume["date"].unique())
                    fact_volume_new = fact_volume_new[~fact_volume_new["date"].isin(existing_dates)]
                    if len(fact_volume_new) > 0:
                        fact_volume = pd.concat([existing_fact_volume, fact_volume_new], ignore_index=True)
                        print(f"    [INCREMENTAL] Appending {len(fact_volume_new):,} new rows")
                    else:
                        fact_volume = existing_fact_volume
                        print(f"    [INCREMENTAL] No new rows to append")
                except Exception as e:
                    print(f"    [WARN] Could not load existing fact_volume: {e}")
                    fact_volume = fact_volume_new
            else:
                fact_volume = fact_volume_new
            
            fact_volume.to_parquet(fact_volume_path, index=False)
            print(f"    Saved fact_volume: {len(fact_volume):,} rows -> {fact_volume_path}")
    else:
        print(f"  [SKIP] Volume file not found: {volume_path}")
    
    print("\n" + "=" * 70)
    print("CONVERSION COMPLETE")
    print("=" * 70)
    print(f"\nFact tables saved to: {data_lake_dir}")


if __name__ == "__main__":
    main()
