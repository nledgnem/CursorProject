# Crypto Quant Data Lake — Context Document

**Purpose:** Self-contained reference for any AI session that needs to work with this data lake. Paste the whole file into context at the start of a new chat.

**Last updated:** 2026-04-27 (Drive sync OAuth bug fixed; `*.json` added to sync patterns; `fact_derivative_open_interest` confirmed orphaned and documented — see sections 8, 9, 13).

---

## 1. What this is

Production data lake for a Singapore-based crypto quant operation. Supports two trading strategies (Apathy Bleed + danlongshort) plus a macro regime monitor. Data is ingested from CoinGecko + CoinGlass + exchange APIs, persisted on a Render cloud disk, and backed up nightly to Google Drive.

Owner's focus: **backtestable point-in-time (PiT) research** and **live trade book + risk monitoring** for open positions. Not a market-data firehose — resolution is daily.

---

## 2. Quick start — how to access the data

Two paths depending on where you are.

### From inside the repo (Render shell or local clone)

```python
from repo_paths import data_lake_root
import pandas as pd

root = data_lake_root()  # /data/curated/data_lake on Render; <repo>/data/curated/data_lake locally
df = pd.read_parquet(root / "fact_price.parquet")
```

`data_lake_root()` honors the `RENDER_DATA_LAKE_PATH` env var. **Never hardcode paths** — that's the Rule 1 of the repo.

### From outside the repo (any AI sandbox, Colab, etc.)

Files mirror to a Google Drive folder **"Render Exports"** (id `1J4qy2zH-bo98A2WsA0wq0AkCXtH7sGEj`). Download by file ID via gdown or Drive API:

```python
import gdown
gdown.download(id="1toMleRQ40HFO3fNOH9S_XhgGB3KDhOIk", output="fact_price.parquet")
df = pd.read_parquet("fact_price.parquet")
```

File IDs are stable per file (Drive doesn't change them). See the "Table catalog" below for the current ID list.

---

## 3. Architecture

**Medallion layering:**
- **Bronze** (`fact_*`): raw-ish ingest from source APIs, minimal transform, one row per natural grain. Preserves source column.
- **Silver** (`silver_fact_*`): cleaned, unit-normalized, joined with `dim_*`. Ready for analysis.
- **Gold** (report-specific): downstream aggregates like `msm_timeseries.csv` or `silver_funding_cross_sectional_daily.parquet`.
- **Dim** (`dim_*`): dimension tables for assets, instruments, exchanges, categories.
- **Map** (`map_*`): bridge tables for provider↔asset, category↔asset resolution.

**Storage topology:**
- **Production runtime:** Render persistent disk mounted at `/data`. All writes land under `/data/curated/data_lake/`.
- **Ephemeral container filesystem** at `/opt/render/project/src/` — wiped on every deploy. **Never write runtime state here.**
- **Backup:** nightly Drive sync of `/data/curated/data_lake/*.parquet` + `*.csv` at ~01:24 UTC.

**Pipeline orchestration:**
- `system_heartbeat.py` is the master daemon (foreground process, Render health-checks it).
- Once daily at 00:05 UTC it triggers `run_live_pipeline.py` → Steps 0 (snapshots) → 0.5 (perp listings) → 1 (funding) → 2 (price/mcap) → 3 (macro index) → 3.5 (silver build) → 4 (strategy run).
- After success, `system_heartbeat.py` triggers the Drive export.

---

## 4. Data sources and **tier limitations** ⚠️

**Read this section carefully — current tier limits shape what analysis is actually valid.**

### CoinGecko (Basic tier currently)
- **Historical depth limit: last 2 years only.** Queries for data older than ~730 days return empty/truncated.
- Rate limit: ~30 req/min (Demo tier) — with exponential backoff on 429.
- Used for: daily close price, market cap, spot volume, daily market snapshots (circulating + total supply + rank).

**Historical data caveat:** The lake contains data older than 2 years (e.g. `fact_price` goes back to 2013, `fact_marketcap` to 2013). **That older data was fetched during a brief period when we had Analyst tier access.** During that period the pipeline had issues (exact failure mode unknown) that may have introduced errors. **Treat pre-2024 data as suspect** unless you independently verify.

Concrete implication: **new backfill jobs cannot reproduce pre-2024 data on the current tier.** Any analysis that relies on data before 2024-01 should add an explicit quality check or be restricted to recent history.

### CoinGlass (Hobbyist tier)
- **Rate limit: 30 req/min with 2.2s spacing** between calls (Hobbyist cap).
- Used for: perp funding rates (daily, by exchange+instrument), open interest (Binance-perp universe in `fact_open_interest` as of 2026-04-22; previously BTC-only), aggregated liquidations (Binance-perp universe in `fact_liquidations` as of 2026-04-23).
- **OI history cap**: the `open-interest/aggregated-history` endpoint on Hobbyist tier returns roughly 334 days of OI per symbol (confirmed 2026-04-22 via `--start-date 2024-01-01` backfill that returned data from ~2025-05 onward for most alts). Multi-year OI backtests are not possible on this tier.
- **Liquidations: cross-venue aggregated** across 10 centralized exchanges (Binance, OKX, Bybit, Bitget, HTX, Gate, MEXC, Bitmex, Deribit, Kraken) via `/futures/liquidation/aggregated-history`. Default `exchange_list` is baked into the fetcher; override with `--liquidations-exchange-list` to narrow or widen. **Hyperliquid and Variational are NOT included** — they don't report liquidations to the CoinGlass feed. **History depth:** Hobbyist tier serves 2024-01-01 forward cleanly (verified 2026-04-23 backfill: 593 assets); the ~334-day cap that affects OI does NOT apply to this endpoint.
- **Liquidations zero-pad trim:** the liquidations endpoint pads pre-listing / pre-coverage dates with synthetic `(0, 0)` rows. `scripts/fetch_coinglass_data.py::fetch_liquidation_history` trims leading zero-pad rows per asset at fetch time, so bronze `fact_liquidations` has a first-row-nonzero guarantee. Interior zero days (real quiet days on an active perp) are preserved. The OI fetcher does NOT currently apply the same trim — see section 13.
- **Unit cutover at 2026-01-13:** pre-cutover funding values are decimal fractions (e.g. `0.0001` = 0.01%/8h), post-cutover are in percent (`0.01` = 0.01%/8h). Bronze (`fact_funding`) contains raw mixed-unit values; silver (`silver_fact_funding`) handles the conversion. **Always read from silver for analysis.**
- Endpoints returning 401 on Hobbyist tier: some aggregate exchange-volume and historical-OI endpoints. Silent expectation, not a bug.

### Binance (public REST)
- No rate limit issues at daily cadence.
- Used for: perp listings, onboard dates, panel-level funding/OI (alternative to CoinGlass for coverage checks).

### Hyperliquid + Variational (public APIs)
- No auth, soft rate limits.
- Used for: perp listing snapshots (Step 0.5). One snapshot per day.

---

## 5. Table catalog

**Drive folder:** `Render Exports` (id `1J4qy2zH-bo98A2WsA0wq0AkCXtH7sGEj`). All files below mirror there.

### Bronze fact tables (primary working set)

| Table | Size | Source | Columns (verified) | Grain | Coverage |
|---|---|---|---|---|---|
| `fact_price.parquet` | 14.6 MB | CoinGecko | `asset_id, date, close, source` | daily | 2013-04 → present (pre-2024 from Analyst tier, may have errors) |
| `fact_marketcap.parquet` | 13.8 MB | CoinGecko | `asset_id, date, marketcap, source` | daily | 2013-04 → present (same caveat) |
| `fact_volume.parquet` | 14.5 MB | CoinGecko | `asset_id, date, volume, source` | daily (rolling 24h, NOT calendar-day bar) | 2013-12 → present (same caveat) |
| `fact_funding.parquet` | 1.3 MB | CoinGlass | `asset_id, instrument_id, date, funding_rate, exchange, source` | daily, last 8h obs per day | 2023-04 → present. **Pre-2026-01-13 unit = decimal, post = percent. Read silver instead.** |
| `fact_open_interest.parquet` | 1.43 MB | CoinGlass | `asset_id, date, open_interest_usd, source` | daily (aggregated from 8h close) | Binance-perp universe (~590 altcoins + BTC). BTC from 2025-02-14; most alts from ~2025-05 onward due to CoinGlass Hobbyist tier history cap (~334 days); per-symbol start date varies with listing. Expanded from BTC-only on 2026-04-22. |
| `fact_liquidations.parquet` | 5.88 MB | CoinGlass | `asset_id, date, long_liquidation_usd, short_liquidation_usd, source` | daily (UTC, cross-venue sum) | 593 assets, 2024-01-01 → 2026-04-24, 360,505 rows as of 2026-04-23 backfill. Cross-venue aggregated across 10 centralized perp exchanges (Binance, OKX, Bybit, Bitget, HTX, Gate, MEXC, Bitmex, Deribit, Kraken); **excludes Hyperliquid + Variational**. No ~334-day Hobbyist cap on this endpoint (unlike OI). Leading zero-pad rows trimmed per asset at ingest. |
| `fact_markets_snapshot.parquet` | 1.66 MB | CoinGecko | `asset_id, name, symbol, coingecko_id, date, current_price_usd, market_cap_usd, market_cap_rank, fully_diluted_valuation_usd, total_volume_usd, circulating_supply, total_supply, max_supply, ath_usd, ath_change_percentage, ath_date, atl_usd, atl_change_percentage, atl_date, source` | one snapshot per day, ~2500 coins per snapshot | Started accumulating ~2026-01. Append-only with dedup. |
| `fact_ohlc.parquet` | 358 KB | CoinGecko | _Not directly verified in this session; contains OHLC candles for a smaller universe_ | — | — |
| `fact_derivative_open_interest.parquet` | 341 KB | CoinGecko (different endpoint) | `date, exchange, base_asset, target, open_interest_usd, open_interest_btc, funding_rate, source` | different grain | **QUARANTINE / ORPHANED** — last updated 2026-01-28. Source script (`scripts/fetch_derivative_data.py`) is not wired into production (`system_heartbeat.py` → `live_data_fetcher.py` → `run_live_pipeline.py` does NOT invoke it). Different provider semantics from `fact_open_interest`; do not use for OI analysis. **Use `fact_open_interest.parquet` instead** — the live CoinGlass-sourced OI table, refreshed daily, 590 Binance-perp assets. This file is a stale artifact from an earlier workflow; queue for deletion or revival in a future cleanup. |
| `fact_derivative_exchange_details.parquet` | 4.3 KB | CoinGecko (`/derivatives/exchanges`) | `date, exchange_id, exchange_name, open_interest_btc, trade_volume_24h_btc, number_of_perpetual_pairs, number_of_futures_pairs, number_of_derivatives, source` | one snapshot per day, ~20 derivative exchanges | **QUARANTINE / ORPHANED** — same provenance as `fact_derivative_open_interest`. Verified 2026-04-27: 20 rows total, single snapshot date 2026-01-28. Top 20 centralized derivative exchanges (Binance Futures, Bybit, CoinW, Gate, LBank, etc.) ranked by OI. **Does NOT contain Variational or Hyperliquid** despite earlier external assumption. OI denominated in BTC, not USD. For per-exchange OI breakdowns, no current production source exists — see followup in §13. |

### Silver tables (cleaned, unit-normalized)

| Table | Size | Description |
|---|---|---|
| `silver_fact_price.parquet` | 14.8 MB | Bronze `fact_price` + joined with `dim_asset` metadata. |
| `silver_fact_marketcap.parquet` | 13.7 MB | Bronze `fact_marketcap` cleaned. |
| `silver_fact_funding.parquet` | 1.2 MB | Bronze `fact_funding` with unit cutover resolved → uniform percent/day representation. **Use this, not bronze, for all funding analysis.** |
| `silver_funding_cross_sectional_daily.parquet` | 49 KB | Daily cross-sectional funding stats (mean, median, percentiles, OI-weighted averages). Gold/report layer. |

### Dimension + mapping tables

| Table | Size | Description |
|---|---|---|
| `dim_asset.parquet` | 106 KB | Per-coin identifiers (asset_id, symbol, name, category refs). |
| `dim_instrument.parquet` | 27.5 KB | Perp instruments: instrument_id, exchange, base, quote, asset_id join. |
| `dim_exchanges.parquet` | 35.3 KB | Spot exchange registry. |
| `dim_derivative_exchanges.parquet` | 3.8 KB | Derivatives exchange registry. |
| `dim_categories.parquet` | 12.7 KB | CoinGecko categories (layer-1, defi, memes, etc.). |
| `dim_new_listings.parquet` | 8.3 KB | Recently listed coins with onboard dates. |
| `map_provider_asset.parquet` | 42.8 KB | Provider-specific ticker → canonical asset_id mapping. |
| `map_provider_instrument.parquet` | 15.2 KB | Provider → instrument mapping. |
| `map_category_asset.parquet` | 19.4 KB | Category → asset_ids (many-to-many). |

### Macro / BTC-dominance references

| File | Size | Description |
|---|---|---|
| `btcdom_reconstructed.csv` | 43 KB | **Primary macro signal.** Use column `reconstructed_index_value`. Rule: BTCDOM < 6000 is the sanity bound. |
| `binance_btcdom.csv` | 51 KB | Pristine Binance BTCDOM index — **verification only**, not a modeling feature. |
| `fact_global_market.parquet` | 3.4 KB | Single global market snapshot. |
| `fact_global_market_history.parquet` | 5.6 KB | Global market history. |
| `fact_market_breadth.parquet` | 4.8 KB | Market breadth stats. |
| `fact_category_market.parquet` | 105 KB | Per-category market aggregates. |
| `fact_trending_searches.parquet` | 4.2 KB | CoinGecko trending list (hourly/daily snapshots). |
| `msm_timeseries.csv` | 75 KB | Macro regime monitor gold output — latest run. Consumers must locate newest dir under `reports/msm_funding_v0/` (PiT audit trail). |

### Trading books + runtime state (Apathy Bleed)

| File | Size | Description |
|---|---|---|
| `apathy_bleed_book.csv` | ~2.6 KB | **Source of truth for Apathy trade ledger.** Committed to git. Columns: `trade_id, cohort, ticker, side, entry_date_utc, entry_price_usd, notional_usd, quantity, stop_price_usd, exit_date_target_utc, status, exit_date_utc, exit_price_usd, pnl_usd, pnl_pct, notes`. Statuses: `OPEN`, `CLOSED_MANUAL`, `CLOSED_STOP`, `CLOSED_EXPIRY`. |
| `apathy_alert_log.csv` | ~38 KB | Append-only log of every Telegram alert fired. Gitignored. |
| `apathy_*_state.json` (5 files) | small | Runner state (last-fired times per alert type, etc.). Gitignored. Backed up to Drive as of 2026-04-27 (`*.json` added to sync_patterns). |

**Apathy book semantics:**
- 4 cohorts (C1–C4) formed 2026-04-09. Target exits 40 / 85 / 130 / 175 days later (2026-05-19, -07-03, -08-17, -10-01).
- Each cohort has 4–6 SHORT legs + 1 LONG_BTC hedge row sized to the cohort's total short notional.
- PnL for SHORT: `pnl_pct = (entry - exit) / entry`; `pnl_usd = pnl_pct * notional`. Script writes these at 8/4 decimals with trailing zeros stripped.

### Trading state (danlongshort) — independent strategy

| File | Description |
|---|---|
| `danlongshort_positions.csv` | Manual position ledger (ticker, side, notional_usd, entry_price, entry_date). |
| `danlongshort_alert_log.csv` | Alert audit log. |
| `danlongshort_price_cache.parquet` | 30-day CoinGecko close cache (freshness gate <12h). |
| `danlongshort_*_state.json` | Runner state, bot state. |

⚠️ **As of 2026-04-20**, danlongshort files still live at `/data/*` (outside `/data/curated/data_lake/`) and are therefore **not backed up to Drive**. Same bug class as what Apathy had before today's fix. Fix pending.

### Universe / eligibility / panels

| File | Size | Description |
|---|---|---|
| `universe_eligibility.parquet` | 1.02 MB | Per-coin eligibility flags (point-in-time membership gates). |
| `single_coin_panel.csv` | 5.06 MB | Denormalized per-coin panel for quick ad-hoc analysis. Rebuilt each pipeline run. |
| `stablecoins.csv` | 2.2 KB | Curated stablecoin exclusion list. |

---

## 6. Path resolution conventions

**Rule 1:** Never hardcode `data/curated/data_lake/...`. Always resolve via `repo_paths.data_lake_root()` or `DATA_LAKE_ROOT`.

**Rule 2:** All runtime writes MUST land under `/data/curated/data_lake/` on Render. Paths outside this directory are NOT captured by the nightly Drive sync and will be silently lost on disk wipes.

**Historical bugs fixed 2026-04-20** (for future agents: don't reintroduce these):
1. `configs/perp_listings.yaml` previously set `output.curated_data_lake_dir` to a relative string, resolved against `repo_root` — routed writes to ephemeral container storage on Render. Fix: remove the override so code falls through to `data_lake_root()`.
2. `configs/apathy_alerts.yaml` previously had paths at `/data/apathy_*` (one level above the curated lake). Fix: moved all 7 paths into `/data/curated/data_lake/`.
3. `start_render.sh` seed logic hardcoded to `/data/apathy_bleed_book.csv`. Fix: updated to `/data/curated/data_lake/apathy_bleed_book.csv`.

---

## 7. Update cadence and freshness

| Artifact | Refresh cadence | Expected freshness |
|---|---|---|
| All bronze fact tables | Daily 00:05 UTC via `run_live_pipeline.py` | ≤ 24h on data, Drive sync at ~01:24 UTC makes it visible externally ≤ 25h |
| Silver layer | Same pipeline, Step 3.5 | Same |
| Apathy book | Event-driven (manual closes, expiry triggers) | Live on Render; in Drive within 24h of change |
| Perp listings (`perps_*`, `perp_coverage_*`) | Daily 00:05 UTC via Step 0.5 | Daily snapshot, first write after 2026-04-20 (previously dropped on floor) |
| Trade snapshots (reports) | Hourly during trading windows + daily 08:00 UTC bundle | Live on Render, periodic Drive sync |

**Freshness guardrail:** Before analysis, always check `df['date'].max()` on the specific table you're reading. If it's older than expected, the pipeline likely errored on the most recent run. Render dashboard → Logs tab will show the cause.

---

## 8. Drive backup mechanics

`configs/gdrive_export.yaml` defines what's backed up. Key config:

- `sync_directory: /data/curated/data_lake/` — directory-sync watches this dir only
- `sync_patterns: ["*.parquet", "*.csv", "*.json"]` — covers bronze/silver tables, books/logs/exports, and runner state files. (Updated 2026-04-27.)
- Auth: OAuth refresh token (`GDRIVE_OAUTH_*` env vars). The OAuth consent app is in **production** mode (not testing) so refresh tokens do not expire on the 7-day testing-mode timer. Service account auth was removed; do not reintroduce.
- Target folder: resolved by name "Render Exports" at runtime (the `target_folder_id` in the YAML is stale/cosmetic).

**Implication:** If a file lives outside `/data/curated/data_lake/`, it's not backed up. (As of 2026-04-27, the previous "non-`.csv`/`.parquet` extension" gap is closed for `*.json`; other extensions still excluded.)

---

## 9. Known data quality issues

These are the landmines. Skim before trusting any column blindly.

1. **Pre-2024 fact_price/fact_marketcap/fact_volume**: possibly corrupt (fetched under broken Analyst-tier pipeline). Independent verification required before use.
2. **`fact_volume`** stores **rolling 24h volume at snapshot time**, not calendar-day bar volume. This matters for resampling and for ratio computations.
3. **`fact_funding` unit cutover at 2026-01-13** — raw bronze mixes units. Always use `silver_fact_funding`.
4. **`fact_open_interest` universe** matches `fact_funding` (Binance-perp universe) as of 2026-04-22. Expected gaps: CoinGlass doesn't track OI for some low-liquidity alts, so a minority of symbols in the funding universe may lack OI rows. For strategies executed off Binance perp, this universe is narrower than CoinGlass's full OI coverage — re-derive from source if needed.
5. **`fact_markets_snapshot` supply coverage.** FDV is effectively fully populated (~100% in top-300 by market_cap_rank as of 2026-04-22). `max_supply` is only ~57% populated in top-300 — expected, not a data-quality defect (many coins have no hard cap). Gate logic that needs "fully diluted" should fall back to `total_supply` when `max_supply` is null; `circulating_supply` and `total_supply` are both ~100% in top-300.
6. **`fact_markets_snapshot.circ_ratio` (circulating / total supply)** can exceed 1.0 for some coins due to upstream CoinGecko data errors. Median is ~1.0; flag outliers.
7. **`fact_derivative_open_interest`** has different grain and semantics than `fact_open_interest`. Do NOT join them without understanding both.
8. **`fact_exchange_volume_history`** may return 401 errors on Hobbyist-tier CoinGlass. If the file is very small or its date range is truncated, that's why.

---

## 10. Git vs runtime state

| Category | Location | Tracked? |
|---|---|---|
| Source code, configs, SKILL docs | Repo | ✅ Git |
| `data/curated/data_lake/*.parquet` | Repo (seed) + Render `/data/` (runtime) | ❌ Gitignored — too large |
| `apathy_bleed_book.csv` | Repo (seed) + Render `/data/` (runtime) | ✅ **Intentionally committed** — small, seeds first-boot state |
| `apathy_alert_log.csv` | Render only | ❌ Gitignored |
| `apathy_*_state.json` | Render only | ❌ Gitignored |
| `danlongshort_positions.csv` | Render only (+ empty template in repo root) | ❌ Gitignored runtime; template in repo |
| `perps_*.csv`, `perp_coverage_*.csv` | Render only (generated) | ❌ Gitignored |

On fresh Render deploy, `start_render.sh` seeds `/data/` from the repo snapshot **only if the target doesn't already exist** — existing runtime files are never overwritten. So pushing changes to `apathy_bleed_book.csv` in git does not affect the live Render book.

---

## 11. Rules every analysis should follow

Taken verbatim from `.cursorrules` + `ARCHITECTURE.md`:

1. **Univariate Mandate:** Plot any new series standalone first (histogram/boxplot) before multi-variable charts.
2. **Outlier Inquisition:** Trace any extreme outlier back to bronze.
3. **Conservation of Data:** N-counts must match across stages of analysis.
4. **Look-Ahead Quarantine:** Every engineered feature uses `t-1` data for signal at `t`.
5. **Stale Data Tripwire:** Flag frozen API feeds via max consecutive identical values.
6. **Sensor vs Strategy distinction:** Macro sensors evaluated without execution drag; executable strategies must subtract spread + fees + gas.
7. **Unit Mandate:** Halt and warn if units are missing or unknown. Never assume temporal frequency.
8. **Curated Lake Only:** Never read CSV/parquet from `archive/` or `archive_data/`.
9. **Bounds Checks:** Before regressions/heatmaps, run `.describe()` and assert sane bounds (e.g. BTCDOM < 6000).
10. **UTC Only:** All timestamps strictly UTC-aware. No naive datetime.

---

## 12. AI usage notes (for future sessions)

**Good patterns:**
- Load via `data_lake_root()`, filter to a specific asset_id and date range before any analysis
- When joining tables, check schemas with `df.dtypes` first rather than assuming columns
- When results surprise you, check `fact_funding` unit cutover or 2013–2024 data legitimacy as first hypotheses
- Silver tables are almost always the right layer for analysis; bronze is for ingestion debugging

**Anti-patterns (never do these):**
- Hardcoding `"data/curated/data_lake/..."` as a string
- Writing a new runtime file anywhere other than `/data/curated/data_lake/`
- Computing signals using `t` data at decision time `t` (look-ahead bias)
- Joining `fact_open_interest` (CoinGlass, Binance-perp universe) with `fact_derivative_open_interest` (CoinGecko, different grain and provider) — these are not interchangeable
- Assuming `fact_volume` is a calendar-day bar
- Trusting pre-2024 data without independently verifying against a second source

**Trigger phrases the `our-data` skill responds to:** "Load the price data", "Get funding rates", "What's in the datalake", "Join price with volume", "Download X file".

---

## 13. Known gaps / current work

(as of 2026-04-27)

- **Drive sync OAuth bug FIXED 2026-04-27.** The Google OAuth consent screen for the Drive uploader had been left in "Testing" mode, which expires refresh tokens after 7 days. App is now in production mode, refresh token rotated. Drive sync was silently broken from 2026-04-23 to 2026-04-27 (Mads spotted it). **Followup queued:** add Telegram alerting on `nightly_export.run()` failures so we don't depend on Mads to spot multi-day staleness next time.
- **`fact_derivative_open_interest` and `fact_derivative_exchange_details` confirmed orphaned 2026-04-27.** Source script `scripts/fetch_derivative_data.py` is not invoked by any production pipeline step; both files last refreshed 2026-01-28 (single snapshot in the exchange_details case). Mads-flagged. Documented in `data_dictionary.yaml`. **Open question: do we need per-exchange OI breakdowns?** The original (orphan) workflow attempted this but never made it to production. If yes, two paths: (a) revive `fetch_derivative_data.py` and wire it into Step 0, or (b) build a CoinGlass-based per-exchange OI fetcher (CoinGlass has per-exchange OI on some endpoints; we currently use the cross-exchange aggregated one). For now there's no production source for per-exchange or per-venue (Hyperliquid, Variational) OI breakdowns. Followup: decide direction in a future cleanup.
- **Perp-vs-spot volume split** not ingested anywhere. Planned Tier-3 work for the Apathy Bleed Gate 2.
- **danlongshort paths** still at `/data/*` (not `/data/curated/data_lake/`). Same bug class as Apathy had pre-2026-04-20. Fix pending.
- **`config_loader._p()`** accepts hardcoded absolute paths and resolves relative paths against `REPO_ROOT`, not `data_lake_root()`. Should be refactored to always use `data_lake_root()` for consistency.
- **`--liquidity-gate` on OI**: the funding branch of `fetch_coinglass_data.py` has an optional Top-150 liquidity gate; the OI branch does not honor it. Decision pending on whether OI should apply the same gate (tradability reasoning is the same; leaving OI un-gated keeps it as a raw reference).
- **Hyperliquid + Variational liquidations not covered** (CoinGlass feed limitation). Cross-venue `fact_liquidations` reflects centralized-exchange pressure only. If Hyperliquid execution grows materially for Apathy Bleed, consider direct Hyperliquid liquidation-feed ingestion as a parallel bronze table.
- **`fact_open_interest` may have the same leading-zero-pad behavior** for pre-listing dates that was fixed for `fact_liquidations` on 2026-04-23. The OI fetcher in `scripts/fetch_coinglass_data.py::fetch_oi_history` does not currently trim leading zeros. Audit via `df[df.asset_id=='<late-listing-alt>'].head()` — if the first rows are zero for pre-listing dates, apply the same per-asset leading-zero-trim fix to `fetch_oi_history` and re-backfill.
- **Latent bug FIXED 2026-04-23** in `scripts/fetch_coinglass_data.py`: the fetcher previously read `configs/golden.yaml`'s `start_date` / `end_date` as fallbacks when CLI dates weren't passed, causing silent truncation of manual backfills to `2025-12-31` (the strategy backtest end-date). Replaced with `date.today()` UTC (end) and `today - 730 days` (start) defaults. The production daily pipeline was unaffected because `run_live_pipeline.py` always passes explicit `--end-date today_utc`; only ad-hoc manual backfills were silently truncated.
- **Build `silver_fact_liquidations`** — derived table with opinions applied (e.g. rolling 7d sums, long/short ratio, cross-sectional percentile at each date). Bronze is already clean via per-asset zero-pad trim, so silver here is about derived analytics rather than cleaning.

---

## 14. Reference: File ID quick-lookup (2026-04-20 snapshot)

For `gdown`/Drive API access from external tools. File IDs are stable.

```python
FILE_IDS = {
    # Bronze fact tables
    "fact_price":                          "1toMleRQ40HFO3fNOH9S_XhgGB3KDhOIk",
    "fact_marketcap":                      "1LyWZCU3oWaEuJi3GZo-OyccIygxG2Rll",
    "fact_volume":                         "1n05yi0nvzCbQFHpQ50Zvzgh1lP8QrXA5",
    "fact_funding":                        "1nV6NIJ_ZQ05rwDxCEO4L3kh68dFsmazS",
    "fact_open_interest":                  "187EL5lcFii_Mbbz8TrTW60mtt71S0Xz7",
    "fact_markets_snapshot":               "181h32ykjUUnwAXcmtZ13dpxe8JQzpis5",
    "fact_ohlc":                           "19YPTePz7rNNfrOmlnnn-mvTDCrh73r8w",
    "fact_derivative_open_interest":       "1vQE0Asjeas7S7b9DuOCY-3DkX6ny3q4U",  # QUARANTINE
    "fact_derivative_volume":              "1315-PCVsaVkLl52v5o_nAKG5z2oGTLYT",
    # Silver
    "silver_fact_price":                   "1IaLiu7wg1GHgxhaUwu9t2uTQrZLJdx05",
    "silver_fact_marketcap":               "1PkxcjjV9X82H6bopEJ3TMiT4pco1uB-p",
    "silver_fact_funding":                 "1F4b0967EUW8ne1mff_h5Qwa9J5FSayCc",
    "silver_funding_cross_sectional_daily":"1yaSqkrG-Hlgz_wcme71B7UpCof4CshKW",
    # Dim
    "dim_asset":                           "1VxjPqlyHLCvab2-0hA9_9wt8sVnDydNa",
    "dim_instrument":                      "1Aiug4AUb2v9VZoSRCeBmYYCzR3CJbSDZ",
    "dim_exchanges":                       "1ikwkEsgJjHdeVwVWLJzFrV6dFZ2MNf6J",
    "dim_derivative_exchanges":            "1tIyHLOj2VQsjHON2M5gpyddsF0XOltVS",
    "dim_categories":                      "1eLKHRW6ZAMm_hhfsA1_d3YHFCaP5EYja",
    "dim_new_listings":                    "1Bh3emJuzrbSRyLvFaIFcDDjAro5l3XQQ",
    # Maps
    "map_provider_asset":                  "1xCdkP7cPEufJAEz9Tq10cYedHf4y7OT4",
    "map_provider_instrument":             "1R0iVtDmUXDtn2tHXSutHH_oWpMYWxBHd",
    "map_category_asset":                  "1HOHuYRyNobzP4cegrlz9pj7OuCUUKpwC",
    # Macro / reports
    "btcdom_reconstructed":                "14NAyuNoPNktshxdVEEuiK1foL1a5R7ad",
    "binance_btcdom":                      "1wPasuIceGlFqS4nELkQgFd6EbTOThhQr",
    "msm_timeseries":                      "1sJf2xc9ELw8Rjv_4yF93wk9aUMmjUaFf",
    "fact_global_market":                  "1gWdIMVnCm9rhzvoS4wiiPkTyT9Q-vfBY",
    "fact_global_market_history":          "12skWQ-KLtm7Pl4PIgiz0BXDF3JhXsBAk",
    "fact_market_breadth":                 "19qNgktjECiIYYLA9u6JgFPmTSesAdeZP",
    "fact_category_market":                "1ddiJKiDufYfLTkAuXbKf7HhSVCAkA9Yi",
    "fact_trending_searches":              "1w_ftAylMMCotipX-vbh1mfJkKnoTlRdN",
    # Exchange
    "fact_exchange_volume":                "1oD9eKo2qT1RcYkNCFdbdUg_poVkSflEs",
    "fact_exchange_volume_history":        "10mzIAyfXJj-STTiusY8_Bve34DN-93-_",
    "fact_exchange_tickers":               "1xtv2MCND-N1IFxqUzxgRPzquZAAjbtSg",
    "fact_derivative_exchange_details":    "13p2G3BfgmZkQWWshe35KMWQKTZQ8aWCD",
    # Universe + ops
    "universe_eligibility":                "1BuA8HFeU8bC3bZz4JeBEgtGkRUtd0qnH",
    "single_coin_panel":                   "138qkH2SrQQTF7FZI3VT3XbOOAXZ7l0eh",
    "stablecoins":                         "1PD6pLhtXYezBNlWksk-gvcUVjJ7D6g5q",
    # Trading ledger
    "apathy_bleed_book":                   "1axx9ANQ7VEVsl9e1VOn5BeFKGPnTrvBt",
}
```

If a file ID stops resolving, verify by searching Drive: `parentId = '1J4qy2zH-bo98A2WsA0wq0AkCXtH7sGEj'`.

---

## 15. Pointer to deeper docs

Inside the repo:
- `ARCHITECTURE.md` — canonical architectural reference
- `.cursorrules` — enforcement rules for automated agents
- `data_dictionary.yaml` — column-level documentation. As of 2026-04-27 covers: MSM layers, Apathy book, all bronze `fact_*` tables (`fact_price`, `fact_marketcap`, `fact_volume`, `fact_funding`, `fact_open_interest`, `fact_liquidations`, `fact_markets_snapshot`, `fact_derivative_open_interest` (quarantined), `fact_derivative_exchange_details` (quarantined)), and the `data_sources:` block (CoinGecko / CoinGlass tier caveats).
- `configs/*.yaml` — per-subsystem configuration (gdrive, apathy, perp_listings, etc.)
- `src/data_lake/` — ingestion modules
- `src/providers/` — API client wrappers (CoinGecko, CoinGlass, Binance)

---

**End of context document.** Safe to paste into any LLM chat. For follow-up sessions, start by asking the LLM to read this + the specific table(s) it will touch, then verify schemas with `df.dtypes` before writing code.
