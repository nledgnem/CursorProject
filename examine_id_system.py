#!/usr/bin/env python3
"""Examine the ID system across all data lake tables."""

import polars as pl
from pathlib import Path

data_lake_dir = Path("data/curated/data_lake")

def safe_print(text: str) -> None:
    """Print text safely, handling Unicode encoding errors on Windows."""
    try:
        print(text, end="", flush=True)
    except UnicodeEncodeError:
        safe_text = text.encode('ascii', 'replace').decode('ascii')
        print(safe_text, end="", flush=True)

print("=" * 80)
print("DATA LAKE ID SYSTEM OVERVIEW")
print("=" * 80)
print()

# 1. Canonical IDs
print("1. CANONICAL IDs (Universal Bridge IDs)")
print("-" * 80)

# asset_id
if (data_lake_dir / "dim_asset.parquet").exists():
    df = pl.read_parquet(str(data_lake_dir / "dim_asset.parquet"))
    print(f"\n[asset_id] - Canonical Asset Identifier")
    print(f"  Location: dim_asset.parquet")
    print(f"  Count: {len(df):,} unique assets")
    print(f"  Format: Uppercase symbol (e.g., 'BTC', 'ETH') or 'CHAIN_ADDRESS' (e.g., 'ERC20_0xabc...')")
    print(f"  Examples:")
    sample = df.select(["asset_id", "symbol", "coingecko_id"]).head(10)
    for row in sample.to_dicts():
        safe_print(f"    - {row['asset_id']} (symbol: {row['symbol']}, coingecko: {row.get('coingecko_id', 'N/A')})\n")

# instrument_id
if (data_lake_dir / "dim_instrument.parquet").exists():
    df = pl.read_parquet(str(data_lake_dir / "dim_instrument.parquet"))
    print(f"\n[instrument_id] - Canonical Instrument Identifier")
    print(f"  Location: dim_instrument.parquet")
    print(f"  Count: {len(df):,} unique instruments")
    print(f"  Format: 'venue_type_symbol' (e.g., 'binance_perp_BTCUSDT')")
    print(f"  Examples:")
    sample = df.select(["instrument_id", "venue", "base_asset_symbol", "asset_id"]).head(10)
    for row in sample.to_dicts():
        safe_print(f"    - {row['instrument_id']} (venue: {row['venue']}, base: {row['base_asset_symbol']}, asset_id: {row.get('asset_id', 'N/A')})\n")

print("\n" + "=" * 80)
print("2. PROVIDER-SPECIFIC IDs (Mapped to Canonical IDs)")
print("-" * 80)

# Provider mappings
if (data_lake_dir / "map_provider_asset.parquet").exists():
    df = pl.read_parquet(str(data_lake_dir / "map_provider_asset.parquet"))
    providers = df["provider"].unique().to_list()
    print(f"\n[Provider Asset Mappings]")
    print(f"  Location: map_provider_asset.parquet")
    print(f"  Providers: {', '.join(providers)}")
    print(f"  Total mappings: {len(df):,}")
    print(f"  Purpose: Maps provider IDs (e.g., CoinGecko 'bitcoin') -> asset_id (e.g., 'BTC')")
    print(f"  Examples:")
    sample = df.head(10)
    for row in sample.to_dicts():
        safe_print(f"    - {row['provider']}: '{row['provider_asset_id']}' -> asset_id '{row['asset_id']}'\n")

if (data_lake_dir / "map_provider_instrument.parquet").exists():
    df = pl.read_parquet(str(data_lake_dir / "map_provider_instrument.parquet"))
    providers = df["provider"].unique().to_list()
    print(f"\n[Provider Instrument Mappings]")
    print(f"  Location: map_provider_instrument.parquet")
    print(f"  Providers: {', '.join(providers)}")
    print(f"  Total mappings: {len(df):,}")
    print(f"  Purpose: Maps provider instrument IDs -> instrument_id")

print("\n" + "=" * 80)
print("3. DOMAIN-SPECIFIC IDs (Not Universal, But Consistent Within Domain)")
print("-" * 80)

# exchange_id
if (data_lake_dir / "dim_exchanges.parquet").exists():
    df = pl.read_parquet(str(data_lake_dir / "dim_exchanges.parquet"))
    print(f"\n[exchange_id] - Exchange Identifier")
    print(f"  Location: dim_exchanges.parquet")
    print(f"  Count: {len(df):,} unique exchanges")
    print(f"  Format: Lowercase exchange name (e.g., 'binance', 'coinbase')")
    print(f"  Used in: fact_exchange_volume, fact_exchange_volume_history, fact_derivative_exchange_details")
    print(f"  Examples:")
    sample = df.select(["exchange_id", "exchange_name"]).head(10)
    for row in sample.to_dicts():
        safe_print(f"    - {row['exchange_id']} ({row['exchange_name']})\n")

# category_id
if (data_lake_dir / "fact_category_market.parquet").exists():
    df = pl.read_parquet(str(data_lake_dir / "fact_category_market.parquet"))
    categories = df["category_id"].unique().to_list()
    print(f"\n[category_id] - Category Identifier")
    print(f"  Location: fact_category_market.parquet")
    print(f"  Count: {len(categories):,} unique categories")
    print(f"  Format: CoinGecko category ID (e.g., 'defi', 'layer-1')")
    print(f"  Used in: fact_category_market")
    print(f"  Examples: {categories[:10]}")

# coingecko_id
print(f"\n[coingecko_id] - CoinGecko Provider ID")
print(f"  Format: CoinGecko's internal ID (e.g., 'bitcoin', 'ethereum')")
print(f"  Stored in: dim_asset.coingecko_id, dim_new_listings.coingecko_id, fact_markets_snapshot.coingecko_id")
print(f"  Mapped via: map_provider_asset where provider='coingecko'")
print(f"  Purpose: Reference to CoinGecko's asset identifier")

# item_id (trending searches)
if (data_lake_dir / "fact_trending_searches.parquet").exists():
    df = pl.read_parquet(str(data_lake_dir / "fact_trending_searches.parquet"))
    print(f"\n[item_id] - Trending Item Identifier")
    print(f"  Location: fact_trending_searches.parquet")
    print(f"  Format: CoinGecko ID or category ID (depends on item_type)")
    print(f"  Used in: fact_trending_searches")

print("\n" + "=" * 80)
print("4. ID RELATIONSHIPS & BRIDGING")
print("-" * 80)

print("""
PRIMARY BRIDGE: asset_id
----------------------------
asset_id is the UNIVERSAL bridge for all asset-related data:

  Provider IDs (coingecko_id, etc.)
    -> (via map_provider_asset)
  asset_id <- THE UNIVERSAL BRIDGE
    -> (used in all fact tables)
  fact_price, fact_marketcap, fact_volume, fact_ohlc, 
  fact_market_breadth, fact_markets_snapshot, etc.

SECONDARY BRIDGE: instrument_id
----------------------------
instrument_id bridges instrument-specific data:

  Provider Instrument IDs (binance symbols, etc.)
    -> (via map_provider_instrument)
  instrument_id <- INSTRUMENT BRIDGE
    -> (used in instrument fact tables)
  fact_funding (has both asset_id AND instrument_id)

DOMAIN-SPECIFIC IDs (No Universal Bridge)
----------------------------
These IDs are consistent within their domain but don't bridge to assets:

  exchange_id -> Used across exchange-related tables
    - dim_exchanges.exchange_id
    - fact_exchange_volume.exchange_id
    - fact_exchange_volume_history.exchange_id
    - fact_derivative_exchange_details.exchange_id
  
  category_id -> Used in category tables
    - fact_category_market.category_id
  
  item_id -> Used in trending searches
    - fact_trending_searches.item_id (may be coingecko_id or category_id)

SPECIAL CASES
----------------------------
Some tables use base_asset (symbol) instead of asset_id:
  - fact_derivative_volume.base_asset
  - fact_derivative_open_interest.base_asset
  
  These should ideally be converted to asset_id for full integration.
""")

print("\n" + "=" * 80)
print("5. SUMMARY: Universal IDs vs Domain IDs")
print("-" * 80)

print("""
✅ UNIVERSAL IDs (Bridge Everything):
  1. asset_id - Bridges ALL asset-related data
     - Used in: dim_asset, fact_price, fact_marketcap, fact_volume, 
                fact_ohlc, fact_market_breadth, fact_markets_snapshot,
                dim_new_listings, fact_funding, universe_eligibility, etc.
     - Mapped from: coingecko_id, provider IDs via map_provider_asset
  
  2. instrument_id - Bridges instrument-specific data
     - Used in: dim_instrument, fact_funding
     - Mapped from: provider instrument IDs via map_provider_instrument
     - Links to: asset_id via dim_instrument.asset_id

🔷 DOMAIN-SPECIFIC IDs (Consistent within domain, no universal bridge):
  1. exchange_id - Exchange identifiers
     - Used in: dim_exchanges, fact_exchange_volume, 
                fact_exchange_volume_history, fact_derivative_exchange_details
     - Does NOT bridge to assets directly
  
  2. category_id - Category identifiers
     - Used in: fact_category_market
     - Does NOT bridge to assets directly
  
  3. item_id - Trending item identifiers
     - Used in: fact_trending_searches
     - May be coingecko_id (which can map to asset_id) or category_id

⚠️  PROVIDER IDs (Mapped to canonical IDs):
  1. coingecko_id - CoinGecko's asset ID
     - Stored in: dim_asset, dim_new_listings, fact_markets_snapshot
     - Mapped to: asset_id via map_provider_asset

📝 RECOMMENDATION:
  To join data across tables:
  - Use asset_id for asset-related joins
  - Use instrument_id for instrument-related joins
  - Use exchange_id for exchange-related joins
  - Use map_provider_asset to convert provider IDs to asset_id
""")

print("=" * 80)
