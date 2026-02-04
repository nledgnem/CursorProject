# Data Directory

This directory contains raw and curated data files for the backtest platform.

## Proxy Datasets

The following files are **proxy datasets** (not from primary sources):

- **`perp_allowlist.csv`**: Proxy for perpetual futures eligibility
  - **Label**: `perp_listed_v0_allowlist_proxy`
  - **Description**: Manual allowlist of symbols that have perpetual futures contracts. This is a proxy until CoinGlass API integration is complete.
  - **Columns**: `symbol`, `coingecko_id`, `venue`
  - **Future**: Will be replaced by CoinGlass API integration

## Curated Data

- `curated/prices_daily.parquet`: Daily close prices (from CoinGecko)
- `curated/marketcap_daily.parquet`: Daily market caps (from CoinGecko)
- `curated/volume_daily.parquet`: Daily volumes (from CoinGecko)
- `curated/universe_snapshots.parquet`: Point-in-time universe snapshots

## Filter Files

- `blacklist.csv`: Symbols to exclude from universe (with reasons)
- `stablecoins.csv`: Stablecoin definitions (used for exclusion from alt-short basket)

