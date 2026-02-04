# Universe and Basket Snapshot Schemas

This document defines the schemas for the two parquet files produced by `build_universe_snapshots.py`:
1. `universe_eligibility.parquet` - All candidates with eligibility flags
2. `universe_snapshots.parquet` (or `basket_snapshots.parquet`) - Selected top-N basket members with weights

## Concept: Universe vs Basket

- **Universe**: The pool of all candidates considered for selection (all coins with market cap data)
- **Basket**: The selected subset from the universe at a rebalance date (top N by market cap)

## 1. Universe Eligibility Schema

**File**: `data/curated/universe_eligibility.parquet`

Contains **all candidates** for each rebalance date, with eligibility flags indicating why they passed or failed filters.

### Core Fields

| Column | Type | Description |
|--------|------|-------------|
| `rebalance_date` | date | Date on which the basket is rebalanced |
| `snapshot_date` | date | Date of data used for selection (usually same as rebalance_date, but can be earlier if data unavailable) |
| `symbol` | string | Asset symbol (e.g., "BTC", "ETH") |
| `coingecko_id` | string | CoinGecko ID (if available) |
| `venue` | string | Trading venue (default: "BINANCE") |

### Market Data Fields

| Column | Type | Description |
|--------|------|-------------|
| `marketcap` | float | Market capitalization in USD at snapshot_date |
| `volume_14d` | float | 14-day rolling average volume in USD (used for liquidity filter) |

### Eligibility Flags

These boolean flags indicate filter status:

| Column | Type | Description |
|--------|------|-------------|
| `is_stablecoin` | boolean | True if coin is classified as a stablecoin (excluded) |
| `is_blacklisted` | boolean | True if coin is in blacklist (excluded) |
| `perp_eligible_proxy` | boolean | True if coin is in perp_allowlist.csv (proxy for perp listing eligibility) |
| `meets_liquidity` | boolean | True if coin meets minimum volume threshold (min_volume_usd) |
| `meets_age` | boolean | True if coin meets minimum listing age (min_listing_days) |
| `meets_mcap` | boolean | True if coin meets minimum market cap threshold (min_mcap_usd) |

### Exclusion Tracking

| Column | Type | Description |
|--------|------|-------------|
| `exclusion_reason` | string | Reason for exclusion (or NULL if eligible). Values: `"base_asset"`, `"not_in_allowlist"`, `"blacklist_or_stablecoin"`, `"no_price_data"`, `"insufficient_listing_age"`, `"below_min_mcap"`, `"below_min_volume"` |

### Metadata Fields

| Column | Type | Description |
|--------|------|-------------|
| `first_seen_date` | date | First date this symbol appears in price data (proxy for listing date) |
| `data_proxy_label` | string | Label indicating data source/proxy (e.g., "perp_eligible_proxy_v1") |
| `proxy_version` | string | Version of proxy data (e.g., "v1") |
| `proxy_source` | string | Source file/identifier for proxy data (e.g., "perp_allowlist.csv") |

## 2. Basket Snapshots Schema

**File**: `data/curated/universe_snapshots.parquet`

Contains only the **selected top-N basket members** with weights and ranks.

### Core Fields

| Column | Type | Description |
|--------|------|-------------|
| `rebalance_date` | date | Date on which the basket is rebalanced |
| `snapshot_date` | date | Date of data used for selection |
| `symbol` | string | Asset symbol |
| `rank` | integer | Rank by market cap (1 = largest, 2 = second largest, etc.) |
| `weight` | float | Portfolio weight (0.0 to 1.0, sum = 1.0 per rebalance_date) |

### Market Data Fields

| Column | Type | Description |
|--------|------|-------------|
| `marketcap` | float | Market capitalization in USD at snapshot_date |
| `volume_14d` | float | 14-day rolling average volume in USD |

### Metadata Fields

| Column | Type | Description |
|--------|------|-------------|
| `coingecko_id` | string | CoinGecko ID (if available) |
| `venue` | string | Trading venue (default: "BINANCE") |
| `basket_name` | string | Name of the basket (e.g., "benchmark_ls_TOP30") |
| `selection_version` | string | Version of selection logic (e.g., "v1") |

### Schema Constraints

1. **Primary key**: `(rebalance_date, symbol)` - each coin appears once per rebalance
2. **Weight constraint**: For each `rebalance_date`, `SUM(weight) = 1.0`
3. **Rank constraint**: Ranks are unique per `rebalance_date` (1, 2, 3, ...)

## Example Query: Why Was Coin X Excluded?

```sql
-- Check exclusion reason for a specific coin on a specific date
SELECT 
    symbol,
    exclusion_reason,
    is_stablecoin,
    is_blacklisted,
    perp_eligible_proxy,
    meets_age,
    meets_mcap,
    meets_liquidity
FROM universe_eligibility
WHERE symbol = 'USDT'
  AND rebalance_date = '2024-01-01';

-- Find all coins excluded for a specific reason
SELECT 
    symbol,
    rebalance_date,
    exclusion_reason
FROM universe_eligibility
WHERE exclusion_reason = 'below_min_mcap'
ORDER BY rebalance_date, symbol;
```

## Filter Application Order

Filters are applied in this order:

1. **Base asset exclusion**: Base asset (e.g., "BTC") is always excluded (`exclusion_reason = "base_asset"`)
2. **Stablecoin exclusion**: Coins with `is_stablecoin = true` are excluded (`exclusion_reason = "blacklist_or_stablecoin"`)
3. **Blacklist exclusion**: Coins with `is_blacklisted = true` are excluded (`exclusion_reason = "blacklist_or_stablecoin"`)
4. **Perp eligibility**: If `must_have_perp = true`, coin must have `perp_eligible_proxy = true` (`exclusion_reason = "not_in_allowlist"`)
5. **Listing age**: Coin must have `meets_age = true` (`exclusion_reason = "insufficient_listing_age"`)
6. **Market cap**: Coin must have `meets_mcap = true` (`exclusion_reason = "below_min_mcap"`)
7. **Liquidity**: Coin must have `meets_liquidity = true` (`exclusion_reason = "below_min_volume"`)
8. **Ranking**: Remaining eligible coins (with `exclusion_reason = NULL`) are ranked by market cap
9. **Top N selection**: Top N coins by rank are selected and added to `basket_snapshots.parquet`
10. **Weighting**: Weights are calculated and capped at max_weight_per_asset

## Threshold Metadata

All filter thresholds used are recorded in `run_metadata_snapshots.json` under `filter_thresholds`:
- `min_listing_days`
- `min_mcap_usd`
- `min_volume_usd`
- `max_weight_per_asset`
- `top_n`
- `base_asset`
- `must_have_perp`

## Data Proxies and Labels

The system uses proxies for some data:
- **Perp eligibility**: `perp_allowlist.csv` is a proxy for actual perp listing data (labeled as `perp_listed_v0_allowlist_proxy`)
- **Listing date**: First appearance in price data is a proxy for actual listing date
- **Venue**: Default "BINANCE" unless specified in allowlist

These proxies are labeled in the `data_proxy_label` field for transparency.
