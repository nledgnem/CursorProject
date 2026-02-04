# Data Lake vs CoinGecko – Review & Recommendations

## Your Current Data Lake (29 parquet files)

### Dimension tables (6)
| File | Rows | Source | Status |
|------|------|--------|--------|
| dim_asset.parquet | 2,717 | CoinGecko + mapping | OK |
| dim_categories.parquet | 739 | `/coins/categories/list` | OK |
| dim_derivative_exchanges.parquet | 20 | `/derivatives/exchanges` | OK |
| dim_exchanges.parquet | 100 | `/exchanges` | OK |
| dim_instrument.parquet | 605 | Binance | OK |
| dim_new_listings.parquet | 200 | `/coins/list/new` | OK – refresh regularly |

### Fact tables (18)
| File | Date range | Source | Status |
|------|------------|--------|--------|
| fact_price.parquet | 2013-04-28 to 2026-01-05 | market_chart/range | **Update** – extend to latest |
| fact_marketcap.parquet | 2013-04-28 to 2026-01-05 | market_chart/range | **Update** – extend to latest |
| fact_volume.parquet | 2013-12-27 to 2026-01-05 | market_chart/range | **Update** – extend to latest |
| fact_ohlc.parquet | 2013-04-29 to 2026-01-05 | ohlc/range | **Update** – extend to latest |
| fact_funding.parquet | 2023-04-19 to 2026-01-13 | Coinglass | **Update** – extend to latest |
| fact_global_market.parquet | 2026-01-28 | `/global` | OK – refresh daily |
| fact_global_market_history.parquet | — | `/global/market_cap_chart` | **Missing** – add/retry |
| fact_market_breadth.parquet | 2026-01-28 | top_gainers_losers | OK – refresh daily |
| fact_markets_snapshot.parquet | 2026-01-28 | `/coins/markets` | OK – refresh daily |
| fact_trending_searches.parquet | 2026-01-28 | `/search/trending` | OK – refresh daily |
| fact_category_market.parquet | 2026-01-28 | `/coins/categories` | OK – refresh daily |
| fact_exchange_volume.parquet | 2025-10-31 to 2026-01-28 | volume_chart | OK |
| fact_exchange_volume_history.parquet | 2025-10-30 to 2025-12-30 | volume_chart/range | **Update** – extend to latest |
| fact_exchange_tickers.parquet | 2026-01-28 | `/exchanges/{id}` | OK – refresh daily |
| fact_derivative_volume.parquet | 2026-01-28 | `/derivatives` | OK – refresh daily |
| fact_derivative_open_interest.parquet | 2026-01-28 | `/derivatives` | OK – refresh daily |
| fact_derivative_exchange_details.parquet | 2026-01-28 | `/derivatives/exchanges/{id}` | OK – refresh daily |
| fact_open_interest.parquet | 2025-02-14 to 2026-01-13 | Other | OK |

### Mapping tables (3)
| File | Rows | Status |
|------|------|--------|
| map_provider_asset.parquet | 2,717 | OK |
| map_provider_instrument.parquet | 605 | OK |
| map_category_asset.parquet | 2,711 | OK – full fetch done |

---

## 1. Updates (refresh / backfill existing datasets)

### High priority – extend to latest date
- **fact_price**, **fact_marketcap**, **fact_volume** – End at 2026-01-05. Run your usual backfill/pipeline (e.g. `scripts/backfill_historical_data.py` or equivalent) to bring them up to today.
- **fact_ohlc** – Same; extend OHLC backfill to latest date.
- **fact_funding** – Ends 2026-01-13; refresh from your funding source up to today.

### Medium priority – one-off or periodic
- **fact_exchange_volume_history** – Ends 2025-12-30. Run `scripts/fetch_high_priority_data.py` (or the exchange volume range script) again to add more recent days.
- **dim_new_listings** – Re-fetch `/coins/list/new` periodically (e.g. weekly) to keep “new listings” current.

### Daily refresh (already supported by your scripts)
- fact_global_market  
- fact_market_breadth  
- fact_markets_snapshot  
- fact_trending_searches  
- fact_category_market  
- fact_exchange_tickers  
- fact_derivative_volume / fact_derivative_open_interest / fact_derivative_exchange_details  
- dim_new_listings (if you add a schedule)

No new CoinGecko endpoints are required for these updates; use your existing fetch/backfill scripts and run them on a schedule or on demand.

---

## 2. Add – missing or failed CoinGecko dataset

### fact_global_market_history (add or retry)
- **Endpoint:** `GET /global/market_cap_chart` (Analyst).
- **Purpose:** Long history of total crypto market cap (and BTC share) for BTC dominance and regime analysis.
- **Current status:** Documented in your inventory as “timed out – needs retry”. Not present in your current parquet list, so it was likely never written.
- **Action:** Retry the global market cap history fetch (e.g. `scripts/fetch_global_market_data.py` with the history option). If the default request times out, use a shorter `days` (e.g. 365 or 730) and backfill in chunks if needed.
- **Output:** `fact_global_market_history.parquet` (date, market_cap_btc, market_cap_usd, source).

This is the only clearly missing CoinGecko-derived table you’ve already designed; adding it (or retrying until it succeeds) is the main “add” from CoinGecko for your current design.

---

## 3. Optional new datasets from CoinGecko

These are not in your lake yet; add only if they support your MSM/backtest use cases.

| Endpoint | Suggested table | Use case | API cost | Priority |
|----------|------------------|----------|----------|----------|
| `/coins/{id}/tickers` | e.g. fact_coin_tickers | Per-coin CEX/DEX liquidity, spreads | 1 call per coin | Low–medium |
| `/coins/{id}/history` | e.g. fact_coin_history_snapshot | Point-in-time snapshots for backtesting | 1 call per coin/date | Low |
| `/entities/list` + `/{entity}/public_treasury/{coin_id}` | e.g. dim_entities, fact_public_treasury | Institutional/corporate holdings (flow/sentiment) | Multiple | Low |
| `/nfts/markets` | e.g. fact_nft_markets | NFT market sentiment | 1+ | Optional |
| Onchain DEX (GeckoTerminal) | Pools/volume by network | DEX liquidity | Many, complex | Optional |

Recommendation: only consider these after (1) daily updates and (2) `fact_global_market_history` are in place.

---

## 4. Summary – what to do

### Do now
1. **Retry/add** `fact_global_market_history` via `/global/market_cap_chart` (with shorter `days` or chunking if needed).
2. **Update** fact_price, fact_marketcap, fact_volume, fact_ohlc, fact_funding (and optionally fact_exchange_volume_history) to the latest date using your existing backfill/fetch scripts.

### Do regularly
3. **Refresh** daily snapshots (global, breadth, markets, trending, categories, exchange tickers, derivatives, new listings) on a schedule.
4. **Refresh** dim_new_listings and dim_categories periodically (e.g. weekly or when you add new assets).

### Consider later
5. **Optional:** Add `/coins/{id}/tickers` or `/coins/{id}/history` (or public treasury / NFT / onchain) only if you have a clear MSM or research need.

### No change
- Your dimension and mapping tables (dim_asset, dim_categories, dim_exchanges, dim_instrument, dim_new_listings, map_provider_asset, map_provider_instrument, map_category_asset) are in good shape; no new CoinGecko datasets are required for them beyond the refresh of dim_new_listings and dim_categories above.

---

## 5. Quick reference – CoinGecko coverage

| You have | CoinGecko endpoint | Action |
|----------|--------------------|--------|
| fact_price, fact_marketcap, fact_volume | `/coins/{id}/market_chart/range` | Update to latest |
| fact_ohlc | `/coins/{id}/ohlc/range` | Update to latest |
| fact_global_market | `/global` | Keep daily refresh |
| — | `/global/market_cap_chart` | **Add/retry** → fact_global_market_history |
| fact_market_breadth | `/coins/top_gainers_losers` | Keep daily refresh |
| fact_markets_snapshot | `/coins/markets` | Keep daily refresh |
| fact_trending_searches | `/search/trending` | Keep daily refresh |
| fact_category_market | `/coins/categories` | Keep daily refresh |
| fact_exchange_volume + history | `/exchanges/{id}/volume_chart` + range | Update history to latest |
| fact_exchange_tickers | `/exchanges/{id}` | Keep daily refresh |
| dim_exchanges | `/exchanges` | OK |
| dim_categories | `/coins/categories/list` | OK |
| map_category_asset | `/coins/{id}` (categories field) | OK – full fetch done |
| fact_derivative_* | `/derivatives`, `/derivatives/exchanges/{id}` | Keep daily refresh |
| dim_new_listings | `/coins/list/new` | Refresh periodically |

Bottom line: **update** existing time series to the latest date, **add/retry** only `fact_global_market_history` from CoinGecko, and keep the rest on a daily/periodic refresh. No other new CoinGecko datasets are necessary unless you decide to add optional ones (tickers, history, treasury, NFT, onchain) for specific analyses.
