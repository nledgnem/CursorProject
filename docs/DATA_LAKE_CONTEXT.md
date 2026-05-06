# Crypto Quant Data Lake — Context Document

**Purpose:** Self-contained reference for any AI session that needs to work with this data lake. Paste the whole file into context at the start of a new chat.

**Last updated:** 2026-04-28 (verified `silver_funding_cross_sectional_daily` schema and `msm_timeseries` shape against Drive; refreshed Apathy book current-state snapshot; added `fact_liquidations` to §14 FILE_IDS dict; flagged `universe_eligibility.parquet` as stale-despite-daily-touch — see section 13; second-pass review fixed Z_Score_90d window 90→30 + source series env_rate_dec→Environment_APR_daily_pct, split fact_open_interest "OI value" vs "universe filter" phrasing, added universe_eligibility unusual-path note; **third-pass review** updated danlongshort path status from "fix pending" to FIXED with verification source, added §6 "Path-class oddities" mini-section).
**Previously:** 2026-04-27 (Drive sync OAuth bug fixed; `*.json` added to sync patterns; `fact_derivative_open_interest` confirmed orphaned and documented — see sections 8, 9, 13).

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

#### Ingestion universe — `data/perp_allowlist.csv`

The CoinGecko per-coin fetcher (`download_all_coins`) reads its symbol list from `data/perp_allowlist.csv`. This list is **static**, not dynamically rebuilt per pipeline run.

- **Current size:** 977 rows (top 1,000 by mcap, snapshot from 2026-05-05, deduped by symbol with $1M floor). `[DECISION 2026-04-29 — Mads + Dan]`: reduce from prior 2,716-row sticky allowlist to top 1,000 by mcap; below rank ~1,000 the mcap floor is roughly $10–20M which is below practical short-leg liquidity for our strategies. **Status: EXECUTED 2026-05-05.**
- **Refresh cadence: STATIC + quarterly manual refresh.** `[DECISION 2026-05-05 — Mads + Dan]`: keep static rather than rebuild dynamically each pipeline run. The CSV is the source of truth; rebuilds are operator-triggered, not auto.
- **Why static, not dynamic:**
  - **Reproducibility for backtests.** A backtest re-run within a quarter uses the same universe membership it used today — no looking-ahead artifact from coins that entered/exited the top 1,000 between runs.
  - **Simpler mental model.** The CSV is the empirical answer; consumers (`download_all_coins`, `src/danlongshort/portfolio.py`, `src/universe/snapshot.py`, etc.) read it directly without needing to query a derived snapshot.
  - **Avoids gappy data for flickering coins.** Coins oscillating around rank 1,000 mcap would get fetched on some days, skipped on others under a dynamic regime — intermittent series. Static avoids this.
  - **Avoids mid-trade drop-out.** ARIA-class positions where post-trade mcap falls below the cutoff (Apathy Bleed thesis) keep visible-mcap continuity until the next quarterly refresh.
- **Trade-off explicitly accepted:** coins entering top 1,000 between quarterly refreshes are NOT tracked until next refresh; coins that fall out continue to be fetched (small wasted API budget, ~5–10% of daily call count by typical drift). Phase F (dynamic daily rebuild from `fact_markets_snapshot`) was considered and explicitly deferred.
- **Last refresh:** 2026-05-05.
- **Next refresh due:** 2026-08-05 (quarterly).
- **Refresh runbook:** [`docs/runbooks/allowlist_refresh.md`](runbooks/allowlist_refresh.md). System heartbeat fires a Telegram reminder once per day after the due date until the refresh lands.
- **Writer-race defenses (Phase B, 2026-05-04) are in place** preventing reintroduction of duplicate-symbol rows: `expand_allowlist.py` deduplicates + asserts `is_unique` before writing; `download_all_coins` raises `ValueError` if it ever loads an allowlist with duplicate symbols.

#### Markets snapshot — `fact_markets_snapshot.parquet`

Independent of the allowlist. Built daily by `scripts/fetch_high_priority_data.py::fetch_and_save_markets_snapshot` paginating `/coins/markets` (top 2,500 by mcap, 10 pages × 250). Used as the canonical mcap/rank reference (e.g., it was the source-of-truth in the Phase A canonical-slug picker for the writer-race fix). Refreshes every nightly run regardless of allowlist state.

### CoinGlass (Hobbyist tier)
- **Rate limit: 30 req/min with 2.2s spacing** between calls (Hobbyist cap).
- Used for: perp funding rates (daily, by exchange+instrument; default `exchange="Binance"` in the fetcher); open interest (**OI value: cross-exchange aggregated by CoinGlass default — the fetcher passes only `symbol` + `interval`, no `exchange_list`, so the API returns its default cross-venue aggregate. Universe filter: assets with a Binance perp listing — that's how we pick which symbols to ingest, NOT what venues are aggregated into the OI value.** ~590 alts + BTC as of the 2026-04-22 expansion; previously BTC-only); aggregated liquidations (cross-venue across 10 CEXes — see liquidations note below; universe = same Binance-perp listing filter, ~593 assets in `fact_liquidations` as of 2026-04-23).
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
| `fact_open_interest.parquet` | 1.43 MB | CoinGlass | `asset_id, date, open_interest_usd, source` | daily (aggregated from 8h close) | **OI value: cross-exchange aggregated** by CoinGlass default — `scripts/fetch_coinglass_data.py::fetch_oi_history` passes only `symbol` + `interval`, no `exchange_list`. **Universe filter: Binance-perp listing** (~590 altcoins + BTC). BTC from 2025-02-14; most alts from ~2025-05 onward due to CoinGlass Hobbyist tier history cap (~334 days); per-symbol start date varies with listing. Expanded from BTC-only on 2026-04-22. |
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
| `silver_funding_cross_sectional_daily.parquet` | 49 KB | Daily funding aggregates feeding the **General Regime Monitor (3-Factor Gate)**. Schema (verified 2026-04-28 against Drive copy, 1105 rows, 2023-04-19 → 2026-04-27): `date`, `env_rate_dec` (environment funding rate, decimal per 8h), `Environment_APR_daily_pct` (annualized APR as a percent — derived as `env_rate_dec × 3 × 365 × 100`), `Fragmentation_Spread` (cross-sectional dispersion of 8h funding around the environment rate — contagion filter), `Z_Score_90d` (90-day rolling z-score of `Environment_APR_daily_pct` — verified against `majors_alts_monitor/msm_funding_v0/macro_environment.py:97`; window=90 with `min_periods=30`, so the first ~30 rows after a fresh start are NaN. Numerically identical to a z-score on `env_rate_dec` because the two series differ by a positive constant ×1095×100 that cancels out in standardization, but the source-of-truth column is `Environment_APR_daily_pct`). One row per UTC calendar day; single time series, NOT per-asset. **History note:** an earlier silver builder produced large arrays of percentiles, medians and OI-weighted averages per asset; it was rewritten when the 3-Factor Regime Monitor was designed so it now outputs only the precise physical columns the regime monitor consumes. **Exact aggregation method (mean? median? OI-weighted? trimmed?) and universe filters are documented per-column in `data_dictionary.yaml` once the silver builder source is read.** |

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
| `msm_timeseries.csv` | 21.7 KB | Macro Regime Monitor strategy gold output. **Status: strategy paused — last weekly decision is 2024-12-30 (16+ months stale as of 2026-04-28).** 48 weekly rows, period 2024-02-05 → 2024-12-30, 16 columns: `decision_date, next_date, basket_hash, basket_members, n_valid, coverage, F_tk, label_v0_0, label_v0_1, p_v0_0, p_v0_1, r_alts, r_maj_weighted, y, r_btc, r_eth`. Each row is a weekly basket-formation decision over 30 majors/large-caps. **Verified 2026-04-28 from Drive copy:** `label_v0_1` and `p_v0_1` are empty across all 48 rows (stub columns from a `v0_1` model that never shipped); `p_v0_0` is on a 0–100 scale (percent), not 0–1; `coverage = n_valid / 30 × 100`; `y = r_maj_weighted − r_alts` (alpha relative to majors); `basket_hash` holds the alphabetized member list (not a true hash) — used as a deterministic identity for caching; `basket_members` is a separately-ordered list (looks ranked, likely by market cap). **Important context:** the MSM *strategy* is paused but the *funding-environment inputs it consumed* (`Environment_APR_daily_pct`, `Fragmentation_Spread`) are still built fresh daily on Render via the silver pipeline — see `silver_funding_cross_sectional_daily.parquet`. The full PiT audit trail under `reports/msm_funding_v0/` lives on the Render disk only and is not mirrored to Drive. **Column definitions for `F_tk`, `label_v0_*`, `p_v0_*` resolved from the MSM source code in §3.4 work — see `data_dictionary.yaml`.** |

### Trading books + runtime state (Apathy Bleed)

| File | Size | Description |
|---|---|---|
| `apathy_bleed_book.csv` | ~2.6 KB | **Source of truth for Apathy trade ledger.** Committed to git. Columns: `trade_id, cohort, ticker, side, entry_date_utc, entry_price_usd, notional_usd, quantity, stop_price_usd, exit_date_target_utc, status, exit_date_utc, exit_price_usd, pnl_usd, pnl_pct, notes`. Statuses: `OPEN`, `CLOSED_MANUAL`, `CLOSED_STOP`, `CLOSED_EXPIRY`. |
| `apathy_alert_log.csv` | ~38 KB | Append-only log of every Telegram alert fired. Gitignored. |
| `apathy_*_state.json` (5 files) | small | Runner state (last-fired times per alert type, etc.). Gitignored. Backed up to Drive as of 2026-04-27 (`*.json` added to sync_patterns). |

**Apathy book semantics:**
- 4 cohorts (C1–C4) formed 2026-04-09. Target exits 40 / 85 / 130 / 175 days later (2026-05-19, -07-03, -08-17, -10-01).
- Cohort SHORT-leg counts: C1=3, C2=4, C3=4, C4=5 (3–5 SHORTs per cohort) + 1 LONG_BTC hedge row each, hedge sized to that cohort's total short notional.
- PnL for SHORT: `pnl_pct = (entry - exit) / entry`; `pnl_usd = pnl_pct * notional`. Script writes these at 8/4 decimals with trailing zeros stripped. **Verified 2026-04-28 against Drive copy on the three closed trades — formula matches recorded values to 6+ decimals.**

**Current state (snapshot 2026-04-28 from Drive `apathy_bleed_book.csv` — dates fast; refresh on doc updates):** 16 SHORT alt legs across C1–C4 + 4 LONG_BTC hedge rows = 20 trade rows total. 3 CLOSED_MANUAL: ARIA (+$2,157.34, +71.8%, closed same-day 2026-04-09), DEXE (−$1,861.09, −62.6%, closed 2026-04-18), PIEVERSE (−$1,809.17, −60.3%, closed 2026-04-20). 17 still OPEN. **Net realized PnL on closed: −$1,512.92 (≈ −$1,513).** The two losers (DEXE, PIEVERSE) shared a profile of low-float / high-attention coins that pumped after entry — pattern Apathy Bleed wants to exclude via the 5-Gates pre-trade scanner (under development; see `docs/STRATEGIES.md`).

### Trading state (danlongshort) — independent strategy

| File | Description |
|---|---|
| `danlongshort_positions.csv` | Manual position ledger (ticker, side, notional_usd, entry_price, entry_date). |
| `danlongshort_alert_log.csv` | Alert audit log. |
| `danlongshort_price_cache.parquet` | 30-day CoinGecko close cache (freshness gate <12h). |
| `danlongshort_*_state.json` | Runner state, bot state. |

**Path note (FIXED):** danlongshort files now live at `/data/curated/data_lake/` and are backed up by the nightly Drive sync. The path-class bug that affected Apathy pre-2026-04-20 was applied here too; the fix is verified at `configs/danlongshort_alerts.yaml:15-18`. The yaml's `paths:` block now points to `/data/curated/data_lake/danlongshort_*` and the `# NOTE` comment explicitly references the prior bug in past tense.

### Mads Long Short positions log (Drive-only, no pipeline integration)

**Strategy:** Mads's personal long/short book on Variational. Long BTC perp + short basket of altcoins. **Distinct from Apathy Bleed** (different cohort structure, different gate logic, different sizing) and **distinct from danlongshort** (different venue, different ledger). Three separate books, three separate sources of truth.

**Location:** `Mads Portfolio/mads_portfolio_log.md` in Mads's personal Drive (NOT under `Render Exports/`). No corresponding file in the repo or in `/data/curated/data_lake/`.

**Contents (as of 2026-05-01 hand-off from Mads):** account state, current 16 short positions + BTC long hedge, full trade journal for the current round (opened 2026-04-10, 33 trades), closed positions with realized PnL (ARIA −$4,055, DEXE −$3,875), decisions log with reasoning per call, Mads's market philosophy section (track record, framework, anecdotes), beta context (working assumption: blended β = 1.3).

**Maintainer:** Mads, via his own Claude Code sessions. The portfolio log itself is the **source of truth** for the Mads Long Short book — there is no positions CSV for this account anywhere in the lake; no pipeline writes it.

**Boundary — for Dan / Dan's Claude:**

- **Read-only.** Drive Desktop on Dan's PC may surface the file if synced; treat it as reference material only.
- **Render's pipeline does NOT touch `Mads Portfolio/`.** The nightly export's `target_folder_id` resolves to `Render Exports/` (id `1J4qy2zH-bo98A2WsA0wq0AkCXtH7sGEj`); the `Mads Portfolio/` folder is outside that scope by design and must not be added to `configs/gdrive_export.yaml::sources` or to any other writer code path.
- **Don't edit.** If a future session spots a conflict between the portfolio log and Apathy book / lake docs, flag to Mads via chat — don't modify the file. Mads (or his Claude) maintains it.

### Universe / eligibility / panels

| File | Size | Description |
|---|---|---|
| `universe_eligibility.parquet` | 1.02 MB | Per-coin eligibility flags (point-in-time membership gates). **Path note:** lives at `/data/universe_eligibility.parquet` on Render — NOT under `/data/curated/data_lake/`. Reaches Drive only because `configs/gdrive_export.yaml::sources` lists it as an explicit static source. Same path-class oddity as the danlongshort files (see §13). **Stale despite daily file touches** — see §13 for details. |
| `single_coin_panel.csv` | 5.06 MB | Denormalized per-coin panel for quick ad-hoc analysis. Rebuilt each pipeline run. |
| `stablecoins.csv` | 2.2 KB | Curated stablecoin exclusion list. |

---

## 6. Path resolution conventions

**Rule 1:** Never hardcode `data/curated/data_lake/...`. Always resolve via `repo_paths.data_lake_root()` or `DATA_LAKE_ROOT`.

**Rule 2:** All runtime writes MUST land under `/data/curated/data_lake/` on Render. Paths outside this directory are NOT captured by the nightly Drive sync and will be silently lost on disk wipes.

**Path-class oddities — files outside `/data/curated/data_lake/` that still reach Drive.** Four files live one level above the lake but are explicitly named in `configs/gdrive_export.yaml::sources` so they reach Drive via the named-source path rather than the directory sync. **Don't reintroduce this pattern for new files** — write inside the lake and let the directory sync pick them up. The four:

- `/data/universe_eligibility.parquet` (also stale — see §13)
- `/data/single_coin_panel.csv`
- `/data/exports/msm_timeseries.csv`
- `/data/stablecoins.csv`

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

0. **`[RESOLVED 2026-05-05]` Historical incident: writer-race in `download_all_coins`** that produced wrong values (not just stale or missing) in `fact_price` / `fact_marketcap` / `fact_volume` for 139 symbols with bridged variants in the CoinGecko allowlist. Discovered 2026-04-29; root cause was `src/providers/coingecko.py::download_all_coins` keying results by `symbol` while iterating allowlist rows with duplicate symbols (bridged/wrapped/peg variants), causing bridged variants to overwrite canonical-slug data. Pre-fix scale: ETH ~5,000× off, SOL ~700×, DOGE ~120×, SHIB/TRX/AVAX/TON/ZEC/USDT/etc all materially wrong; corruption was *flapping* (different bridged variants winning the iteration race on different runs, ETH timeline showed correct→wrong→correct→wrong phases since 2026-01-28). **Resolution shipped in three phases:** Phase B+C (PR #4, merge `87a4c50`) — fail-fast guard in `download_all_coins` against duplicate symbols + `assert is_unique` in `expand_allowlist.py` + dedupe of `data/perp_allowlist.csv` from 2,997 → 2,716 rows (kept canonical slug per symbol via highest-mcap pick from `fact_markets_snapshot`; dropped USC entirely as not on Binance perps + never picked + sub-$1M mcap; dropped one NaN-symbol row). Phase D (PR #5, merge `c7de283`, executed 2026-05-05) — re-fetched 138 canonical slugs over `[2024-05-10, 2026-05-05]` (start bumped from `2024-05-06` to dodge the `offset_days=-2` padding tripping the 730-day Basic-tier ceiling); upserted all three fact tables atomically (~76K rows dropped, ~74K canonical rows inserted per table). **Verification: scripts/verify_ingestion_integrity.py --mode writer_race confirmed PASS** (Signal 3 ETH/SOL/DOGE in canonical envelopes; Signal 4 BTC/BNB/XRP regression check clean; flap-flop spot-checks at corruption-era dates ETH 2026-02-21/2026-03-06, USDT 2025-09-01, SHIB 2025-08-01 all correctly recovered to canonical mcaps). **Pre-2024-05-10 rows for affected symbols remain as-is** — Basic-tier API depth ceiling prevents re-fetch of older history; pre-2024 era is independently flagged as suspect for the unrelated broken-Analyst-tier-pipeline reason (see §9 entry 1). **Caveat for 9 symbols where two distinct real coins share a ticker** (not just bridged variants of one coin) — the canonical pick was made by highest current mcap as a deterministic-but-imperfect heuristic. Pre-fix value was random per iteration order; post-fix is deterministic regardless of which deterministic choice. Affected: ANT (canonical `autonomi`, alternative `aragon`), AURA (`aura-on-sol` / `aura-finance`), ALPHA (`alpha-fi` / `alpha-finance`), AVA (`concierge-io` / `ava-ai`), ANY (`anyspend` / `anyswap`), ADX (`adex` / `adrena`), AIN (`infinity-ground` / `ai-network`), ACE (`endurance` / `ace-data-cloud-2`), ACT (`act-i-the-ai-prophecy` / `achain` / `acet-token`). Verified to have zero references in any Apathy Bleed cohort (live or backtest) before commit; reversal possible by editing `outputs/writer_race_canonical_slug_mapping.csv` and re-running Phase D against the affected slug. Full audit + numbers: `reports/apathy_universe_cut_audit_2026_04_29.md` §4 + §8 (resolution addendum). Re-run verification anytime via `scripts/verify_ingestion_integrity.py --mode writer_race`.
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

(as of 2026-04-30)

- **`[2026-04-29 → EXECUTED 2026-05-05] CoinGecko ingestion universe reduction (top 1,000 by mcap).`** Phase E shipped: `data/perp_allowlist.csv` reduced from the writer-race-defended 2,716-row allowlist to **977 rows** (top 1,000 by mcap with $1M floor, deduped by symbol). Backup of pre-cut state preserved at `data/perp_allowlist.2716_pre_universe_cut.bak.csv`. Built from the 2026-05-05 `fact_markets_snapshot.parquet` (cached `/coins/markets` output) — bypassed `expand_allowlist.py` since the snapshot is the same data and avoided redundant API calls. **Refresh cadence: STATIC + quarterly** per `[DECISION 2026-05-05 — Mads + Dan]`; next refresh due 2026-08-05. See §4 "Ingestion universe" subsection for the static-vs-dynamic rationale and `docs/runbooks/allowlist_refresh.md` for the refresh procedure. Operational impact: daily `download_all_coins` API calls drop from ~2,716 to ~977 (~64% reduction). All 13 OPEN Apathy Bleed picks remain in the universe; ARIA correctly dropped (closed pick that fell out of top 1,000 between pre-flight audit on 2026-04-16 and execution on 2026-05-05); MATIC replaced by POL (real chain rebrand, not a bug).
- **`[2026-04-29 → RESOLVED 2026-05-05] download_all_coins writer-race.`** Fetcher was keying results by `symbol` while iterating an allowlist with duplicate symbols (bridged/wrapped/peg variants), causing bridged variants to overwrite canonical-slug data. Pre-fix scale: ETH ~5,000× off, SOL ~700×, DOGE ~120×, plus ~135 others affected. Resolved via Phase B+C (PR #4 merge `87a4c50`: fail-fast guard + allowlist dedupe to 2,716 rows) and Phase D (PR #5 merge `c7de283`, executed 2026-05-05: re-fetch of 138 canonical slugs over `[2024-05-10, 2026-05-05]`). All four verification signals PASS; flap-flop spot-checks at corruption-era dates confirm historical re-fetch propagated correctly. Pre-2024-05-10 rows for affected symbols remain as-is (Basic-tier API depth ceiling). Full detail: §9 entry 0 + `reports/apathy_universe_cut_audit_2026_04_29.md` §4 + §8.
- **`[2026-04-29] dim_asset.coingecko_id is a placeholder.`** All 2,717 rows have `coingecko_id == lowercase(symbol)` (e.g. `BTC→"btc"`, `ETH→"eth"`, `ZEC→"zec"`); zero rows have real CoinGecko slugs. Daily ingestion does NOT use this column (reads allowlist directly), so dormant. But corrupts any analysis joining via `dim_asset.coingecko_id`. Documentation drift: `data_dictionary.yaml` `fact_price.asset_id` description (line ~115) still describes it as "Canonical CoinGecko slug … Matches dim_asset.coingecko_id" which is empirically wrong on both halves (asset_id is uppercase symbol, not CoinGecko slug; and the dim_asset column is the placeholder above). **Status: STILL OPEN as of 2026-05-05** — not bundled into the writer-race remediation; separate cleanup task.
- **`[2026-05-05] verify_ingestion_integrity.py log-path coverage.`** The `--mode writer_race` Signal 1 (API call count) and Signal 2 (guard silent) check candidate log paths `/tmp/run_live_pipeline.log`, `/var/log/run_live_pipeline.log`, `/var/log/macro-regime/*.log`. None match Render's actual daemon stdout location, so both signals return INDETERMINATE on Render shell. Not a blocker — Signal 3 (parquet read on affected blue-chips) provides direct empirical evidence stronger than logs. But worth fixing: identify Render's actual log path (probably visible via `find /var/log /tmp /opt/render -name "*.log" -mtime -1` on Render shell), add to `_LOG_CANDIDATE_PATHS` in the verification script. Cleanup item, not urgent.
- **Drive sync OAuth bug FIXED 2026-04-27.** The Google OAuth consent screen for the Drive uploader had been left in "Testing" mode, which expires refresh tokens after 7 days. App is now in production mode, refresh token rotated. Drive sync was silently broken from 2026-04-23 to 2026-04-27 (Mads spotted it). **Followup queued:** add Telegram alerting on `nightly_export.run()` failures so we don't depend on Mads to spot multi-day staleness next time.
- **`fact_derivative_open_interest` and `fact_derivative_exchange_details` confirmed orphaned 2026-04-27.** Source script `scripts/fetch_derivative_data.py` is not invoked by any production pipeline step; both files last refreshed 2026-01-28 (single snapshot in the exchange_details case). Mads-flagged. Documented in `data_dictionary.yaml`. **Open question: do we need per-exchange OI breakdowns?** The original (orphan) workflow attempted this but never made it to production. If yes, two paths: (a) revive `fetch_derivative_data.py` and wire it into Step 0, or (b) build a CoinGlass-based per-exchange OI fetcher (CoinGlass has per-exchange OI on some endpoints; we currently use the cross-exchange aggregated one). For now there's no production source for per-exchange or per-venue (Hyperliquid, Variational) OI breakdowns. Followup: decide direction in a future cleanup.
- **Perp-vs-spot volume split** not ingested anywhere. Planned Tier-3 work for the Apathy Bleed Gate 2.
- **danlongshort paths FIXED** (verified 2026-04-28 against `configs/danlongshort_alerts.yaml:15-18`): all four runtime paths (positions_csv, price_cache_parquet, alert_log_csv, snapshot_state_json) now resolve to `/data/curated/data_lake/`. The `# NOTE` comment in that yaml explicitly calls out the prior `/data/*` bug in past tense. Drive sync therefore captures danlongshort runtime state correctly. (Was previously listed as "fix pending" in this section; that note was stale.)
- **`config_loader._p()`** accepts hardcoded absolute paths and resolves relative paths against `REPO_ROOT`, not `data_lake_root()`. Should be refactored to always use `data_lake_root()` for consistency.
- **`--liquidity-gate` on OI**: the funding branch of `fetch_coinglass_data.py` has an optional Top-150 liquidity gate; the OI branch does not honor it. Decision pending on whether OI should apply the same gate (tradability reasoning is the same; leaving OI un-gated keeps it as a raw reference).
- **Hyperliquid + Variational liquidations not covered** (CoinGlass feed limitation). Cross-venue `fact_liquidations` reflects centralized-exchange pressure only. If Hyperliquid execution grows materially for Apathy Bleed, consider direct Hyperliquid liquidation-feed ingestion as a parallel bronze table.
- **`fact_open_interest` may have the same leading-zero-pad behavior** for pre-listing dates that was fixed for `fact_liquidations` on 2026-04-23. The OI fetcher in `scripts/fetch_coinglass_data.py::fetch_oi_history` does not currently trim leading zeros. Audit via `df[df.asset_id=='<late-listing-alt>'].head()` — if the first rows are zero for pre-listing dates, apply the same per-asset leading-zero-trim fix to `fetch_oi_history` and re-backfill.
- **Latent bug FIXED 2026-04-23** in `scripts/fetch_coinglass_data.py`: the fetcher previously read `configs/golden.yaml`'s `start_date` / `end_date` as fallbacks when CLI dates weren't passed, causing silent truncation of manual backfills to `2025-12-31` (the strategy backtest end-date). Replaced with `date.today()` UTC (end) and `today - 730 days` (start) defaults. The production daily pipeline was unaffected because `run_live_pipeline.py` always passes explicit `--end-date today_utc`; only ad-hoc manual backfills were silently truncated.
- **Build `silver_fact_liquidations`** — derived table with opinions applied (e.g. rolling 7d sums, long/short ratio, cross-sectional percentile at each date). Bronze is already clean via per-asset zero-pad trim, so silver here is about derived analytics rather than cleaning.
- **`universe_eligibility.parquet` is stale despite daily file touches (flagged 2026-04-28).** Drive `modifiedTime` advances every nightly export but `rebalance_date.max()` = `2025-12-01` — 5 months stale. 23 unique monthly rebalance dates from 2024-02-01 to 2025-12-01; 62,491 rows × 26 columns. Some producer is rewriting an unchanged file daily without advancing the rebalance schedule. Investigate: (a) is the monthly rebalance scheduler firing? (b) does any consumer read this file as a "live" gate, and if so are they getting stale eligibility decisions? Tagged `STALE_PIPELINE_WRITES_NO_NEW_DATA` in `data_dictionary.yaml` pending root-cause.
- **`[2026-05-06]` `RENDER_DATA_LAKE_PATH` env var deployment issue affects nightly export pipeline (`silver_fact_price.parquet`, `single_coin_panel.csv`, `universe_eligibility.parquet`, `msm_timeseries.csv`, `stablecoins.csv` to Drive). Required for Cohort Shorts project chat to pull fresh data on cohort scan day. Verify before next cohort scan ~2026-05-24.** See `STRATEGIES.md` §8 #11 for the operational dependency. Pipeline is built but the env var resolution issue must be confirmed fixed; if stale, the cohort scan blocks until the export pipeline is verified healthy.
- **`[2026-05-06] universe_eligibility.parquet staleness — corrected diagnosis.`** Earlier `[2026-04-28]` framing of "stale-pipeline-writes-no-new-data" was wrong for this file. The file is a static seed copied once at boot from in-repo `data/curated/universe_eligibility.parquet` (per `start_render.sh:19-22`, with `[[ ! -f ]]` guard preventing overwrite). No producer is wired into the daily pipeline — `build_universe_snapshots.py` is only invoked from `scripts/run_pipeline.py` and `scripts/run_golden.py`, neither of which is in the `system_heartbeat → run_live_pipeline.py` daily path. The in-repo copy was last regenerated 2026-02-10 (commit `e27666b`) against a config with `end_date: 2025-12-01`, hence `max(rebalance_date)=2025-12-01` since then. Drive `modifiedTime` advancing daily reflects nightly Drive sync re-uploading unchanged content, not a pipeline write. Fix shape: (a) wire `build_universe_snapshots.py` into the daily pipeline (or weekly cron — perp eligibility doesn't need daily refresh), (b) parameterize the producer's `end_date` to be dynamic (e.g., today). Open consumers question: dashboards/, scripts/cohort_*, single_coin_panel.csv builder, etc. need audit before fix is scoped. Severity depends on consumers — if cohort scan reads it as a live gate, P1 for May 24; otherwise P2 cleanup. **Action: investigate consumers in next session; do not bundle with msm_run.py path-mismatch fix.**

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
    "fact_liquidations":                   "1XUkiBdtxaSsUZ_-ppzftVu5Ujwr544Xj",
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

## 16. Documentation conventions

Wired into the nightly Drive sync as of 2026-04-30. Read this section before editing any of the four canonical context docs.

### 16.1 The four canonical context docs

| File (in repo) | Drive filename | Drive file ID | Role |
|---|---|---|---|
| `docs/DATA_LAKE_CONTEXT.md` | `DATA_LAKE_CONTEXT.md` | `1p0OwwSXTa0XSAPtU2SpLuIgEjTGEqcnx` | This file. Operational orientation: tables, paths, freshness, known gaps. |
| `data_dictionary.yaml` | `data_dictionary.yaml` | `1yC0fTcMTB_jfpyzpfYeEpHiDp7Zt842W` | Column-level field definitions. The unit of truth for "what does this column mean / what units / what frequency". |
| `docs/STRATEGIES.md` | `STRATEGIES.md` | `1Nry4d-hRuveRuk2yNqlhoag8lHqMP9J0` | Apathy Bleed strategy spec: thesis, universe, structure, cohorts, gates, decisions log. |
| `docs/BACKTEST.md` | `BACKTEST.md` | `1Vfe7M2eijAEpzcGR7euq8LfJ1_ak6nJ1` | Apathy Bleed backtest record: methodology, cohorts, results, audit notes. |

### 16.2 Single source of truth = the repo

All four files are tracked in git. Edits go: **edit the repo copy → commit → push → merge to main → Render auto-redeploys → next nightly export overwrites the Drive copies**.

- The Drive file IDs above are stable and preserved across nightly syncs (verified 2026-04-30 by manual export trigger; all 4 took the `update` branch, not `create`).
- Drive Desktop on engineer machines (e.g. `G:\My Drive\Render Exports\`) is a **read-only mirror**. Don't edit files there — they get overwritten on the next nightly.
- The nightly export is the **only writer** to Drive for these files. No other code path uploads them.

### 16.3 Render is the only writer

Implementation lives at `configs/gdrive_export.yaml::sources` (4 doc entries with `gdrive.filenames` mappings) and `src/exports/nightly_export.py::run` (relative paths resolved against repo root). The OAuth scope is `drive` (full Drive read/write) — required because the four docs were originally human-authored in Drive, not created by Render's OAuth client; the narrower `drive.file` scope cannot see human-uploaded files.

The fact that Render holds a `drive`-scoped refresh token for Mads's account means Render technically has read/write capability over all of Mads's Drive, not just `Render Exports`. **No code path traverses outside the configured folder**, but the capability exists. If that scope-bounding ever becomes a concern, migrate to a shared drive (drive.file scope works on shared-drive members as if the OAuth client created them).

### 16.4 Drive-side edits are silently overwritten

If you (or anyone) edits the Drive UI copy of one of these four files directly, the next nightly export (~01:24 UTC) will overwrite those edits with whatever's in the repo at that moment. **No warning, no merge, no diff.**

The two migrated docs (`STRATEGIES.md`, `BACKTEST.md`) carry a top-of-file banner spelling this out. The other two (`DATA_LAKE_CONTEXT.md`, `data_dictionary.yaml`) are repo-native and don't need the banner since the convention here is sufficient.

### 16.5 The `[DECISION YYYY-MM-DD]` tagging convention

When a meaningful design / operational decision is made and committed to one of these docs, tag the relevant paragraph or table cell with `[DECISION YYYY-MM-DD]` followed by who made it. Examples already in use:

- `[DECISION 2026-04-29 — Mads + Dan]` — CoinGecko ingestion universe reduction (paused, see §13).
- `[DECISION 2026-04-30 — Mads + Dan]` — OAuth scope expansion `drive.file → drive`.

Two purposes:
1. **Searchability** — `git log -S "[DECISION 2026-04-29]"` surfaces the commit and reasoning.
2. **Audit trail** — when a future-you / future-Claude session asks "why is X this way?", the tag points at the specific decision and who signed off.

Use `[VERIFIED]` for empirically-confirmed claims (the existing `[VERIFIED]` tags throughout `STRATEGIES.md` and `BACKTEST.md` follow this pattern).

### 16.6 Followup-doc convention

The audit memo `reports/apathy_universe_cut_audit_2026_04_29.md` is repo-only (not in the nightly Drive sync). Convention going forward:

- **Long-lived context docs** (the 4 above) → in Drive sync.
- **Audit memos / decision write-ups / one-off investigations** → repo-only under `reports/`.
- If a `reports/` artifact becomes load-bearing for ongoing operations (referenced from `DATA_LAKE_CONTEXT.md` §13 followups, or read by tooling), promote to one of the canonical 4 by either folding the content in or by adding it to the sync sources list (small config change, see commit `abb477a` for the pattern).

### 16.7 Out-of-scope Drive folders — `Mads Portfolio/`

Separate from `Render Exports/` (the folder this section governs), Mads maintains a personal Drive folder `Mads Portfolio/` that is **explicitly outside Render's sync scope**. The canonical artifact there is `Mads Portfolio/mads_portfolio_log.md` — the source of truth for the Mads Long Short book (see §5).

Convention:

- **Render writes only to `Render Exports/`.** The `Mads Portfolio/` folder must not be added to `configs/gdrive_export.yaml::sources`, must not become a target of `upload_or_update_file`, and must not appear in any future writer code path.
- **Dan's Claude treats `Mads Portfolio/` as read-only reference material.** Read it when relevant for context; don't edit; don't suggest auto-syncing it.
- **Cross-doc conflicts are flagged, not fixed.** If a future session reading `mads_portfolio_log.md` spots a contradiction with `STRATEGIES.md` / `data_dictionary.yaml` / `apathy_bleed_book.csv` — surface the conflict to Mads in chat. Mads (or his Claude) reconciles.

This convention also applies to any future Drive folders Mads (or Dan) chooses to keep outside the Render-managed surface.

---

**End of context document.** Safe to paste into any LLM chat. For follow-up sessions, start by asking the LLM to read this + the specific table(s) it will touch, then verify schemas with `df.dtypes` before writing code.
