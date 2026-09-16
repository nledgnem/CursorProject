# Data & System Audit

*2026-09-15 · read-only audit of `C:\Users\Admin\Documents\Cursor` before the Track A / Track B research program.*

**Scope and method**
- **Covered:** the curated lake, the research caches, the strategy/backtest code and governance, and free public APIs.
- **Evidence standard:** every date range comes from file *contents*, not timestamps. Every claim below was checked against files, git, or live API calls.

---

## 0. Read this first — five things that change how research must be done

1. **The local lake has a bronze regression — uncommitted.** On 2026-08-17 something rewrote `fact_price`, `fact_marketcap`, `fact_volume` (plus `dim_asset`, `dim_instrument`, `map_provider_*`, `mapping_validation.json`, and the three wide `*_daily.parquet` files):
   - **Working tree:** `fact_price` has 1,305,534 rows covering 2024-01-07 → 2026-01-05.
   - **Git HEAD (`599e5cb`):** 1,530,793 rows covering 2013-04-28 → 2026-03-30.
   - **Likely cause:** `scripts/convert_to_fact_tables.py` run from the stale `data/raw/*_daily.parquet`.
   - **Not fixed — needs your decision.** Restoring means `git checkout HEAD -- <files>`, which discards the Aug-17 rewrite. Until then, research here uses silver tables and exchange APIs, never local bronze.
2. **The authoritative lake (Google Drive `G:\My Drive\Render Exports`) is not mounted.**
   - **Local snapshot is stale:** it ends 2026-03-30 (silver, funding, OI), with the panel ending 2026-04-04.
   - **Missing locally:** `fact_liquidations` and `msm_timeseries.csv`.
   - **Stray worktree copy:** `.claude/worktrees/cranky-robinson-143c9f/enrichment/_cache/` holds per-asset liquidations, OI and funding to 2026-06-18.
3. **Everything research-grade is daily.** There are no trades, no order book and no intraday bars in the lake. Intraday questions (sessions, funding windows, cross-venue latency) need new data — much of it free (§5).
4. **Funding units are inconsistent across the repo.** Every funding series must be unit-checked before use:
   - `fact_funding` switches from decimal to percent on 2026-01-13.
   - `silver_fact_funding.funding_rate_raw_pct` is actually decimal.
   - The short-squeeze `btc_funding.parquet` looks like percent despite a "decimal" comment.
   - `single_coin_panel` uses the daily *mean*; silver uses the *last* 8h print.
5. **Nothing systematic is live.**
   - Apathy Bleed closed 2026-05-12 (realised −$5,424.66).
   - There are no LL Pro, dantrading or danlongshort realised P&L histories in the repo, so correlation to the "existing book" can only use reconstructed or backtest series.

---

## 1. Repository structure (what matters for research)

| Area | Path | Role |
|---|---|---|
| Live pipeline | `scripts/run_live_pipeline.py`, `system_heartbeat.py`, `start_render.sh` | Render, daily 00:05 UTC. Steps: 0 markets snapshot → 0.5 perp listings → 1 CoinGlass funding/OI/liquidations → 2 CoinGecko price/mcap → 3 BTCDOM (non-fatal) → 3.5 silver → 4 MSM. Nightly Drive export. |
| Lake code | `src/data_lake/`, `scripts/data_ingestion/`, `scripts/fetch_coinglass_data.py` | Ingestion and silver build |
| Strategies | `src/apathy_bleed/`, `src/danlongshort/`, `majors_alts_monitor/` (MSM), `src/rwa_offhours/`, `scripts/dark_event_monitor.py` | See §3 |
| Research | `research/btc_trend_agreement/`, `research/btc_short_squeeze/`, `research/btc_confirmation_lag/`, `research/dvol_regime_overlay/`, `research/fragmentation_spread/` | Self-contained studies with their own caches and helpers; no shared library |
| Docs | `data_dictionary.yaml` (authoritative), `docs/DATA_LAKE_CONTEXT.md`, `docs/STRATEGIES.md`, `docs/BACKTEST.md`, `docs/ARCHITECTURE.md` | Several documented contradictions (§6) |
| Tests | `tests/` (117 collected, 2 collection errors), `majors_alts_monitor/tests/` (5) | No CI |

---

## 2. Data inventory by family

**Conventions**
- **Exchange data:** daily rows are UTC calendar days; the close is known at 23:59:59 UTC.
- **CoinGecko data:** the value on day *t* is the close of *t−1*.
- **"Freely extendable":** verified by live API probe on 2026-09-15.

| Family | What exists | Grain · history | Assets · venues | Gaps / quality | Freely extendable? |
|---|---|---|---|---|---|
| **Spot OHLCV** | `research/btc_confirmation_lag/cache/coinbase_BTCUSD_ohlcv.csv`, `binance_BTCUSDT_ohlcv.csv`; `btc_short_squeeze/data/btc_spot_daily.parquet` (with taker buy); ETH/SOL/HYPE daily; `btc_trend_agreement/cache/binance_alt_panel.parquet` (384 spot pairs, close + quote volume) | daily · 2015/2017 → 2026-09 | BTC full; majors; 384 alts (close only) | Lake has **no true OHLC** except 5 assets; lake close/volume are CoinGecko (volume = rolling 24h) | Yes. Binance 1m spot since 2017; Coinbase |
| **Perp OHLCV** | `btc_short_squeeze/data/alt_panel.parquet` (600k rows, **833 Binance USDT perps incl. delisted**, taker buy quote); `btc_perp_daily.parquet` | daily · 2020-06 → 2026-08-21 | Binance USD-M | `listed_today` flag is wrong (true on 95% of rows) | Yes. `fapi/v1/klines`, any interval |
| **Trades / L1 / L2 book** | none | — | — | Only snapshot spreads: `fact_exchange_tickers` (1 day), Variational bid/ask at 3 sizes (snapshots) | Binance archive: spot trades since 2017, perp aggTrades since 2020, `bookDepth` (depth bands) since 2023-01, `bookTicker` 2023-05 → 2024-03 |
| **Funding** | `silver_fact_funding` (Binance via CoinGlass, ~500 assets, last 8h print, decimal); `BinanceBasisProject/out/data` (541 symbols, **individual prints**, 2025-02 → 2026-02); `btc_funding.parquet` (BTC OI-weighted cross-venue, 2020-03 → 2026-08, **likely percent**); worktree silver funding to 2026-06-18 | daily/8h · 2023-04 → 2026-03 (lake) | Binance only for history | Mixed units (§0.4); no predicted funding; no Bybit/OKX/HL history | Binance `fundingRate` (2019+) and archive (2020+); Bybit (2020+); Hyperliquid `fundingHistory` (2023-05+); OKX only ~3 months; Kraken futures partial |
| **Open interest** | `fact_open_interest` (**BTC only locally**, CoinGlass aggregate); `btc_open_interest.parquet` (BTC, 4-venue aggregate, 2020-02 → 2026-08); worktree `fact_open_interest` (590 assets, 2025-02 → 2026-06); `fact_derivative_open_interest` (96 venues × pairs, **1 day**) | daily | aggregate across venues | No per-venue history; `single_coin_panel.open_interest_usd` filled for only 4 tickers | Binance archive `metrics` (5-min OI, top-trader and global long/short, taker ratio) **since 2020-09**; Binance API only 30 days; Bybit OI 1d since 2020; CoinGlass (key present) |
| **Liquidations** | `btc_liquidations_by_exchange.parquet` (BTC, 8 venues, long/short USD, 2017 → 2026-08); worktree `fact_liquidations` (593 assets, venue-aggregated, 2024-01 → 2026-06) | daily | CoinGlass | **Pre-2020-12-23 zeros are missing data.** Venue coverage is spliced; only Binance/OKX/Bybit/HTX are comparable over 2021–26. Lake `fact_liquidations` absent locally. | No free history: Binance `allForceOrders` removed; the USD-M archive has no liquidation snapshots (COIN-M only, since 2023-06). CoinGlass with the key; or collect the WebSocket `forceOrder` stream going forward. |
| **Basis / futures curve** | none explicit (BTC spot vs perp daily closes can proxy) | — | — | — | Binance `premiumIndexKlines` (perp premium vs index) **since 2020, any interval**; COIN-M quarterly continuous klines since 2020-06; Deribit futures |
| **Options / vol** | `btc_trend_agreement/cache/deribit_btc_dvol_daily.csv` | daily · 2021-03 → 2026-08 | BTC DVOL only | no ETH DVOL, no chains, skew or term structure | Deribit DVOL (BTC, ETH) and historical volatility free; option chains need snapshotting or a vendor |
| **Long/short, taker flow** | `btc_ls_ratio.parquet` (Binance account ratio, 2020-10 → 2026-08); taker buy volume in BTC and alt kline panels | daily | Binance | BTC only for L/S | Binance archive `metrics` (2020-09+) |
| **Cross-venue prices** | Coinbase vs Binance BTC/ETH/SOL daily closes | daily | 2 venues | No synchronized intraday; no Kraken or Hyperliquid prices (except HYPE) | Binance/Coinbase/Kraken 1m klines free; Hyperliquid candles only last 5,000 bars |
| **Listings / delistings** | `dim_instrument` and `data/raw/perp_listings_binance.parquet` (605 Binance perps, onboard dates, **from current exchangeInfo, not point-in-time**, no delist dates); `alt_panel` first/last kline dates (survivorship-free); Hyperliquid/Variational listing snapshots (1 day locally, daily on Render since 2026-04) | event | Binance, HL, Variational | No announcement timestamps; no spot-listing history; delistings only inferable from the last kline | Binance exchangeInfo (current, with `deliveryDate` for settled contracts); announcements need a scraper; HL `meta`/`perpDexs` snapshots going forward |
| **Token supply / unlocks** | `fact_markets_snapshot` circulating/total/max supply (2 days locally; accumulating on Render since 2026-01) | snapshot | CoinGecko | **No historical point-in-time supply, no unlock schedules** | DefiLlama emissions API is paywalled; Tokenomist/TokenUnlocks paid; on-chain reconstruction per token is costly |
| **On-chain / stablecoins** | `data/stablecoins.csv` (static list only) | — | — | none | DefiLlama stablecoins (free, 2017+); chain data needs Dune or nodes |
| **Market structure / contract specs** | `perps_hyperliquid.csv` (max leverage, margin table), `perps_variational.csv` (funding interval, spread tiers), `dim_instrument` | snapshot | HL, Variational, Binance | no history of rule changes | HL `meta`, `perpDexs` (HIP-3 deployers/oracles); Binance exchangeInfo, leverage brackets |
| **Macro / TradFi** | `btc_short_squeeze/data/macro.parquet` (DXY, SPX, NDX, US10Y via yfinance, 2017 → 2026-08); ETF flows (2024-01 → 2026-08) | daily | — | weekend NaNs (~31%) | yfinance / FRED free |
| **Regime / breadth** | `silver_funding_cross_sectional_daily` (Environment_APR, Fragmentation_Spread); `research/dvol_regime_overlay/data/daily_panel.parquet`; `btcdom` files; `alt_breadth_panel` | daily | — | `btcdom_reconstructed` stops 2026-01-29; `universe_eligibility` frozen 2025-12-01 with ETH/SOL mis-mapped | — |

**Implication for the research program**
- **Directly testable today:** BTC-centric and Binance-perp cross-sectional daily studies.
- **Needs a free backfill first:** per-asset funding history, basis, OI/long-short (Binance archive), intraday bars.
- **Needs paid or reconstructed data:** liquidations beyond BTC, unlock schedules, point-in-time supply, announcement timestamps, order books.

---

## 3. Strategies that exist

| Strategy | Signal / structure | Status | Realised record |
|---|---|---|---|
| Apathy Bleed | short top-7 45d alt momentum vs notional BTC long, 60% leg stop, Cold-Flush gate | **Exited 2026-05-12**; post-mortem (worktree only) says structural failure | `apathy_bleed_book.csv` (20 legs) |
| danlongshort | manual beta-neutral book, 30d OLS beta | monitoring runs on Render | repo positions file empty; no P&L |
| MSM v0 funding sensor | weekly Environment_APR / w_risk / Fragmentation gate on long majors / short alts | live sensor | `reports/msm_funding_v0/*/msm_timeseries.csv`, weekly |
| LL Pro trades | discretionary Gold/Blue + enrichment | live, discretionary | **no log in repo** |
| Gerhard SMA / LL Pro trend | long/flat trend with 3-close confirmation | reconstructed only (`research/btc_confirmation_lag/`) | backtest daily returns only |
| Dark events / Variational RWA | flag dark-hours news on equity perps | watching; no trades | trade log empty |
| BTC short-squeeze tranche plan | event study, 11 events | research / discretionary plan | plan PDFs |
| TrendScore, DVOL overlay, Fragmentation taper | research | rejected / monitor only | tables |

---

## 4. Backtesting infrastructure and cost assumptions

| Engine | Good for | Material gaps |
|---|---|---|
| `src/backtest/engine.py` | daily multi-asset L/S from snapshots | no funding, no execution lag, one-sided turnover cost, **252-day annualisation** |
| `majors_alts_monitor/backtest.py` | walk-forward, funding accrual (8h×3), stops | `maker_fee_bps`, `slippage_adv_multiplier` accepted but unused; 252-day vol targeting |
| `scripts/apathy_bleed_backtest.py` | cohort pair backtest with funding | original engine never committed; 75.5% alpha not reproducible; stops fill exactly |
| `research/btc_confirmation_lag/backtest.py` | single-asset, next-open fill, costs, block bootstrap, walk-forward | long/flat BTC only; no funding |
| `research/btc_trend_agreement/strategies.py` | causal daily backtest, look-ahead traps (`verify.py`) | single asset |

**Cost assumptions in use:**
- **Fees and slippage:** 5 bp fee + 5 bp slippage (`configs/golden.yaml`); 60 bp round trip for alt shorts (Apathy).
- **Funding:** 8h × 3 per day.
- **Measured live:**
  - Apathy taker fees ≈ 28 bp on turnover.
  - DEXE stop slippage ≈ 1.7%.
  - Apathy funding −7.6% per 150-day cohort.
  - Variational equity spreads 30–230 bp (`outputs/rwa_cost_floor.md`).
- **Not modelled anywhere:** borrow costs, size-dependent impact.

**Reusable research code**
- **Event sets / declustering, bootstrap null, permutation, matching:** `research/btc_short_squeeze/{eventstudy,controls,stage2_*}.py`.
- **HAC and block bootstrap:** `research/btc_trend_agreement/stats_tools.py`.
- **Walk-forward, AUC/Wilson, sweeps:** `research/btc_confirmation_lag/study.py`.
- **Perp kline and CoinGlass fetchers:** `research/btc_short_squeeze/{fetch_data,alt_fetch}.py`.
- **Spread cost floor:** `scripts/rwa_cost_floor.py`.

**Build decision:** these helpers are duplicated per folder. The program will consolidate them into one shared package (`research/quant_program/qlib/`) and import the existing implementations where they are already correct.

---

## 5. Free data verified by live API probe (2026-09-15)

| Source | Available free | Notes |
|---|---|---|
| Binance USD-M API | funding (2019+), `premiumIndexKlines` basis (2020+), klines any interval, exchangeInfo (897 symbols with onboard/delivery dates) | OI and long/short ratio endpoints: last 30 days only; liquidation history endpoint removed |
| Binance COIN-M | quarterly continuous klines (2020-06+) | futures basis |
| `data.binance.vision` archive | USD-M `metrics` (5-min OI, long/short, taker) **2020-09+**; `bookDepth` 2023-01+; `bookTicker` 2023-05 → 2024-03; aggTrades 2020+; spot trades 2017+; funding and premium monthly | USD-M liquidation snapshots: none |
| Bybit | funding 2020+, OI 1d 2020+ | |
| OKX | funding ~3 months; OI+volume 180 days | collect forward only |
| Hyperliquid | funding 2023-05+; `meta`; `perpDexs` (HIP-3 deployers, oracle updaters) | candles only last 5,000 bars → snapshot forward |
| Kraken Futures | tickers; funding history (recent) | |
| Deribit | DVOL (2021-03+), historical vol | option chains need snapshots |
| DefiLlama | stablecoin supply (2017+) free | **emissions/unlocks paywalled** |
| CoinGlass / CoinGecko | keys present in `.env` | existing fetchers; per-asset liquidations/OI history bounded by plan tier |

---

## 6. Governance and data-quality state

- **Run logging:** `scripts/run_pipeline.py` writes `outputs/runs/<ts>_<confighash>/` with git hash; MSM writes a `run_manifest.json`. Research studies have no run log.
- **Data versioning:** none. The Drive export overwrites in place. 638 parquet files are git-tracked despite docs saying they are ignored (which is how the regression in §0.1 is detectable at all).
- **Tests:** 117 collected; 2 fail to collect without the `G:` drive; root `pytest` picks up `archive/` live-API scripts and crashes; no CI.
  - **Untested:** `apathy_bleed_backtest.py`, danlongshort, `gate_policy`, nightly export, the dark-event monitor.
- **Existing checks:** QC spike thresholds (`qc_curate.py`), freshness checks (MSM 3d, panel 4d), `validate_run.py` invariants, and look-ahead traps (trend agreement only).
- **Documented contradictions:**
  - Step-3 BTCDOM fatality and producer (docs vs code).
  - Gate-ramp description (docs vs `gate_policy.py`).
  - Stale performance claims in `BACKTEST.md`'s final summary.
  - Apathy alert runners still watching a closed book.
  - The Apathy post-mortem exists only in a worktree.

---

## 7. What this means for Track A and Track B

| Question | Feasible now? | Data used / needed |
|---|---|---|
| A: trend confirmation, BTC (+ETH/SOL) | **Yes** | Coinbase/Binance daily OHLCV, DVOL, BTC funding/OI/liquidations/long-short (short-squeeze panel), taker volume, breadth panel, ETF flows. Existing `btc_confirmation_lag` engine. |
| A: short side, multi-horizon agreement, probability model | Yes | same |
| B1 funding dislocations | Yes, after backfill | Binance funding history per perp (free API/archive) + alt perp klines |
| B2 basis | Yes, after backfill | Binance `premiumIndexKlines`, COIN-M quarterlies |
| B3 cross-venue dislocation | **Partial** | daily Coinbase/Binance premium only; intraday needs 1m bars (free) |
| B4 listings | Partial | perp listing effect from first-kline dates (survivorship-free); no announcement timestamps |
| B5 unlocks | **No** | unlock schedules + point-in-time supply missing → data-gap item |
| B6 liquidation / forced flow | Yes (BTC deep; alts 2024-01 → 2026-06) | CoinGlass (key present) |
| B7 OI / price divergence | Yes (BTC 2020+; top perps via Binance archive 2020-09+) | Binance `metrics` |
| B8 combinations | Yes, within B1/B6/B7 coverage | |
| B9 weekend / session | Weekend: yes (daily). Session / funding windows: needs 1h bars | Binance 1h klines (free) |
| B10 contract mechanics | Descriptive only | funding caps, HL oracle/HIP-3 metadata; no rule-change history |
| B11 new products | Framework + forward snapshots | HL `perpDexs`, Binance exchangeInfo, Variational listings |
| Portfolio correlation | Partial | Gerhard/LL daily returns, MSM weekly, majors-alts daily P&L; **no realised LL Pro / danlongshort P&L** |
