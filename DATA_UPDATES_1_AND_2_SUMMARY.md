# Data Updates 1 & 2 – Summary

## 1. Add: fact_global_market_history – DONE

- **Endpoint:** `GET /global/market_cap_chart`
- **Change:** Parser updated for current API format: `market_cap_chart.market_cap` is `[[timestamp_ms, value_usd], ...]`. Timeout set to 120s; fallback to 365 days if 3650 fails.
- **Result:** `fact_global_market_history.parquet` created with **366 records**, date range **2025-01-29 to 2026-01-29** (1 year).
- **Path:** `data/curated/data_lake/fact_global_market_history.parquet`

To pull 10 years later (if needed):
```bash
python scripts/fetch_global_market_data.py --history --days 3650
```

---

## 2. Update: Extend time series to latest date

### Completed

| Task | Status | Notes |
|-----|--------|------|
| **fact_global_market_history** | Done | Added (see above). |
| **fact_exchange_volume_history** | Done | Re-ran `fetch_high_priority_data.py`. 558 records; daily snapshots + trending/categories/markets refreshed. |
| **fact_trending_searches, fact_category_market, fact_markets_snapshot** | Refreshed | Updated as part of high-priority fetch. |

### In progress (background)

| Task | Status | Notes |
|-----|--------|------|
| **fact_price, fact_marketcap, fact_volume** | Running | `scripts/incremental_update.py` was started in the background. It fetches 2026-01-06 to 2026-01-29 (~3k coins, ~12 min). Check the terminal where it was run; when it finishes, fact_price/marketcap/volume will extend to latest date. |

### After incremental update finishes

| Task | What to run |
|------|-------------|
| **fact_ohlc** | After incremental update has extended `fact_price` to today, run: `python scripts/fetch_analyst_tier_data.py --ohlc` so OHLC backfill extends from 2026-01-06 to the new latest date. |

### fact_funding (Coinglass, not CoinGecko)

- **Source:** Coinglass API.
- **To extend to latest:**  
  `python scripts/fetch_coinglass_funding.py --incremental`  
  (requires Coinglass API key: `--api-key YOUR_KEY`).

---

## Scripts added/updated

1. **`scripts/fetch_global_market_data.py`**
   - `fetch_global_market_cap_chart`: timeout 120s; parses `market_cap_chart.market_cap` as `[[ts, value_usd], ...]`; supports legacy 3-element rows.
   - `save_global_market_cap_history`: optional fallback to 365 days if full range fails.

2. **`scripts/run_data_updates.py`** (new)
   - Runs in order: global market history → incremental update (price/mcap/volume) → OHLC backfill → high-priority fetch (exchange volume history, etc.).
   - Usage: `python scripts/run_data_updates.py`  
   - Flags: `--skip-global-history`, `--skip-incremental`, `--skip-ohlc`, `--skip-exchange-volume-history`, `--global-days`, `--days-back`.

---

## Quick reference

| Goal | Command |
|------|---------|
| Global market cap history only | `python scripts/fetch_global_market_data.py --history --days 365` (or 3650) |
| Extend price/mcap/volume to latest | `python scripts/incremental_update.py` |
| Extend OHLC to latest (after incremental) | `python scripts/fetch_analyst_tier_data.py --ohlc` |
| Exchange volume history + daily snapshots | `python scripts/fetch_high_priority_data.py` |
| Extend funding to latest | `python scripts/fetch_coinglass_funding.py --incremental` (with Coinglass API key) |
| Run all CoinGecko updates | `python scripts/run_data_updates.py` |

---

## Summary

- **1. Add:** fact_global_market_history is in place (366 days, 2025-01-29 to 2026-01-29).
- **2. Update:** Exchange volume history and daily snapshots are refreshed. Price/marketcap/volume incremental update was started in the background; once it finishes, run OHLC backfill. Funding is updated separately via Coinglass with `fetch_coinglass_funding.py --incremental`.
