# Since Analyst Plan Upgrade – What We Added & Did

Summary of everything added or changed after upgrading to the CoinGecko Analyst plan.

---

## 1. New Data Lake Tables (Parquet Files)

| Table | Rows (approx) | Source Endpoint | Purpose |
|-------|----------------|-----------------|---------|
| **fact_trending_searches** | 28 | `/search/trending` | Sentiment / trend (coins, NFTs, categories) |
| **fact_category_market** | 659 | `/coins/categories` | Category-level market cap, volume, top coins |
| **fact_markets_snapshot** | 2,500 | `/coins/markets` | Snapshot of top coins (price, mcap, volume, ATH/ATL) |
| **fact_exchange_volume_history** | 558 | `/exchanges/{id}/volume_chart/range` | Historical exchange volume (31-day chunks) |
| **dim_exchanges** | 100 | `/exchanges` | Exchange metadata (rankings, trust score, volume) |
| **fact_derivative_exchange_details** | 20 | `/derivatives/exchanges/{id}` | Per-exchange derivative OI/volume |
| **dim_categories** | 739 | `/coins/categories/list` | Category ID + name (metadata only) |
| **fact_exchange_tickers** | 800 | `/exchanges/{id}` | Exchange tickers (pairs, price, volume, spread) |
| **fact_global_market_history** | 366 | `/global/market_cap_chart` | Historical total market cap (1 year) |
| **map_category_asset** | 2,711 | `/coins/{id}` (categories field) | Asset ↔ category (e.g. BTC → proof-of-work) |

**Total new/added tables:** 10 (all under `data/curated/data_lake/`).

---

## 2. New CoinGecko Endpoints Implemented

| Priority | Endpoint | Script / Module |
|----------|----------|------------------|
| High | `/search/trending` | `coingecko_analyst.fetch_trending_searches`, `fetch_high_priority_data.py` |
| High | `/coins/categories` | `coingecko_analyst.fetch_coins_categories`, `fetch_high_priority_data.py` |
| High | `/coins/markets` | `coingecko_analyst.fetch_coins_markets`, `fetch_high_priority_data.py` |
| High | `/exchanges/{id}/volume_chart/range` | `coingecko_analyst.fetch_exchange_volume_chart_range`, `fetch_high_priority_data.py` |
| Medium | `/exchanges` | `coingecko_analyst.fetch_exchanges_list`, `fetch_medium_priority_data.py` |
| Medium | `/derivatives/exchanges/{id}` | `coingecko_analyst.fetch_derivative_exchange_details`, `fetch_medium_priority_data.py` |
| Low | `/coins/categories/list` | `coingecko_analyst.fetch_categories_list`, `fetch_low_priority_data.py` |
| Low | `/exchanges/{id}` | `coingecko_analyst.fetch_exchange_details`, `fetch_low_priority_data.py` |
| Low | `/derivatives/exchanges/list` | `coingecko_analyst.fetch_derivatives_exchanges_list`, `fetch_low_priority_data.py` |
| Add/fix | `/global/market_cap_chart` | `fetch_global_market_data.py` (parser + timeout fix) |
| Asset–category | `/coins/{id}` (categories) | `fetch_asset_categories.py` |

All new Analyst-tier calls live in `src/providers/coingecko_analyst.py`; orchestration in the `scripts/fetch_*_data.py` scripts.

---

## 3. New Scripts

| Script | Purpose |
|--------|---------|
| **scripts/fetch_high_priority_data.py** | Trending, categories, markets snapshot, exchange volume history |
| **scripts/fetch_medium_priority_data.py** | All exchanges, derivative exchange details |
| **scripts/fetch_low_priority_data.py** | Categories list, exchange details + tickers, derivatives list |
| **scripts/fetch_asset_categories.py** | Full asset→category mapping (map_category_asset) |
| **scripts/run_data_updates.py** | One entry point: global history → incremental → OHLC → high-priority fetch |

Verification / inspection scripts added: `verify_high_priority_data.py`, `verify_medium_priority_data.py`, `verify_low_priority_data.py`, `verify_category_mappings.py`, `check_category_fetch_progress.py`, `examine_id_system.py`, `list_parquet_files.py` (if updated).

---

## 4. Schema Additions (`src/data_lake/schema.py`)

- **FACT_TRENDING_SEARCHES_SCHEMA** – date, item_type, item_id, item_name, rank, source  
- **FACT_CATEGORY_MARKET_SCHEMA** – date, category_id, category_name, market_cap_*, volume_*, top_3_coins, source  
- **FACT_MARKETS_SNAPSHOT_SCHEMA** – date, asset_id, coingecko_id, symbol, name, price, mcap, volume, ATH/ATL, source  
- **FACT_EXCHANGE_VOLUME_HISTORY_SCHEMA** – date, exchange_id, volume_btc, volume_usd, source  
- **DIM_EXCHANGES_SCHEMA** – exchange_id, exchange_name, country, trust_score, volume, tickers, source  
- **FACT_DERIVATIVE_EXCHANGE_DETAILS_SCHEMA** – date, exchange_id, OI/volume, pair counts, source  
- **DIM_CATEGORIES_SCHEMA** – category_id, category_name, source  
- **FACT_EXCHANGE_TICKERS_SCHEMA** – date, exchange_id, ticker_base/target/pair, last_price_usd, volume_usd, spread, trust_score, source  
- **MAP_CATEGORY_ASSET_SCHEMA** – asset_id, category_id, category_name, source  

All of these are wired into **TABLE_NAMES** for DuckDB/usage.

---

## 5. Data Lake Conventions & ID System

- **DATA_LAKE_ID_SYSTEM_OVERVIEW.md** – Describes `asset_id` (universal bridge), `instrument_id`, `exchange_id`, `category_id`, `coingecko_id`, and how to join.  
- **DATA_LAKE_COINGECKO_REVIEW.md** – Which tables to update vs add from CoinGecko; what to run for freshness.  
- **CATEGORY_MAPPING_EXPLANATION.md** – How `dim_categories` (metadata) and `map_category_asset` (which asset is in which category) work.

New Analyst data is aligned with existing conventions: fact tables use `asset_id` or `exchange_id` where appropriate; mappings use `map_*` and schema names match.

---

## 6. Fixes & Improvements

- **fact_global_market_history**  
  - Was missing (timeout / wrong format).  
  - Parser updated for current API: `market_cap_chart.market_cap` = `[[timestamp_ms, value_usd], ...]`.  
  - Timeout set to 120s; fallback to 365 days if 3650 fails.  
  - Table created with 366 days (2025-01-29 → 2026-01-29).

- **OHLC**  
  - Endpoint corrected to `/coins/{id}/ohlc/range` with `interval`.  
  - 180-day chunking for Analyst limit; backfill script uses price date range.

- **Exchange volume history**  
  - 31-day chunking for `/exchanges/{id}/volume_chart/range` limit.

- **Unicode/safe print**  
  - Safe-print helpers added in fetch/verify scripts to avoid Windows encoding errors.

- **Category list**  
  - `safe_float` (and similar) used so missing/None values in category data don’t crash the pipeline.

---

## 7. MSM v0 – How New Data Is Used

| MSM Feature | New/Updated Data |
|--------------|-------------------|
| **Momentum** | fact_trending_searches (sentiment/trend) |
| **ALT Breadth** | fact_category_market, fact_markets_snapshot (broader coverage) |
| **BTC Dominance** | fact_global_market, fact_global_market_history |
| **Liquidity** | dim_exchanges, fact_exchange_volume_history, fact_exchange_tickers |
| **OI Risk** | fact_derivative_exchange_details |
| **Sector / category** | dim_categories, fact_category_market, map_category_asset |

---

## 8. What You Can Run Regularly

| Goal | Command |
|------|---------|
| Daily snapshots (trending, categories, markets, exchange volume history) | `python scripts/fetch_high_priority_data.py` |
| Exchange rankings + derivative exchange details | `python scripts/fetch_medium_priority_data.py` |
| Categories list + exchange tickers (optional) | `python scripts/fetch_low_priority_data.py` |
| Extend price/mcap/volume to latest | `python scripts/incremental_update.py` |
| Extend OHLC to latest (after incremental) | `python scripts/fetch_analyst_tier_data.py --ohlc` |
| Global market cap history (e.g. 1 year) | `python scripts/fetch_global_market_data.py --history --days 365` |
| All CoinGecko updates in sequence | `python scripts/run_data_updates.py` |
| Funding to latest (Coinglass) | `python scripts/fetch_coinglass_funding.py --incremental` (with API key) |

---

## 9. Documentation Added

- **COINGECKO_ENDPOINTS_INVENTORY.md** – List of endpoints, what’s implemented, priority for MSM.  
- **HIGH_PRIORITY_DATA_FETCH_SUMMARY.md** – What was fetched for high-priority endpoints.  
- **MEDIUM_PRIORITY_DATA_FETCH_SUMMARY.md** – Same for medium priority.  
- **LOW_PRIORITY_DATA_FETCH_SUMMARY.md** – Same for low priority.  
- **DATA_LAKE_ID_SYSTEM_OVERVIEW.md** – ID types and how they link.  
- **DATA_LAKE_COINGECKO_REVIEW.md** – What to update vs add from CoinGecko.  
- **CATEGORY_MAPPING_EXPLANATION.md** – Categories vs asset–category mapping.  
- **DATA_UPDATES_1_AND_2_SUMMARY.md** – Global history add + time-series update steps.  
- **FULL_CATEGORY_FETCH_STATUS.md** – Status of full asset–category fetch.  
- **SINCE_ANALYST_UPGRADE_SUMMARY.md** – This file.

---

## 10. Short Summary

- **10 new data lake tables** from Analyst endpoints (trending, categories, markets snapshot, exchange volume history, dim_exchanges, derivative exchange details, dim_categories, exchange tickers, global market cap history, map_category_asset).  
- **10+ new endpoint integrations** in `coingecko_analyst.py` and fetch scripts.  
- **5 new fetch/orchestration scripts** (high/medium/low priority, asset categories, run_data_updates).  
- **9 new schema definitions** and table names registered.  
- **Global market cap history** fixed and populated (1 year).  
- **ID system and data lake review** documented; category ↔ asset mapping in place (2,711 mappings).  
- **Conventions** kept: asset_id / exchange_id, long-form fact tables, source tracking, and schema names.

All of this was added or done **since the Analyst plan upgrade**.
