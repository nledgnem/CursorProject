# BTC Trend-Agreement Study — how to rerun

Self-contained research module. It does not import from `src/` and does not
write anywhere outside this directory, so it can be rerun or deleted without
touching the rest of the repo.

## Rerun

```bash
cd research/btc_trend_agreement && python run_all.py
```

First run downloads ~4,000 daily candles per asset plus the Deribit DVOL
history and caches them under `cache/` (takes ~30s). Every later run reads the
cache and completes in ~15 seconds. To re-download everything:

```bash
cd research/btc_trend_agreement && python run_all.py --refresh
```

Then re-run the independent checks, which rebuild the headline numbers from the
raw cached candles without importing the analysis modules and run five explicit
look-ahead traps:

```bash
cd research/btc_trend_agreement && python verify.py
```

`verify.py` exits non-zero if any check fails.

Then the 200-day moving-average head-to-head (§9b of the report), which is the
falsification test for whether the three-horizon construction earns its
complexity:

```bash
cd research/btc_trend_agreement && python ma_benchmark.py
```

Finally the broad alt-basket test of the crypto-beta hypothesis (§7b). First run
downloads ~380 Binance symbols (~8 min); later runs use the cached parquet:

```bash
cd research/btc_trend_agreement && python alt_basket.py --sweep
```

## Requirements

`pandas`, `numpy`, `scipy`, `statsmodels`, `matplotlib`, `requests` — all
already in the repo's `requirements.txt`. No API keys: every endpoint used is
public and unauthenticated.

## Layout

| Path | What it is |
|---|---|
| `config.py` | Every parameter in the study. Nothing is fitted to data. |
| `data_io.py` | Venue fetchers + cache + provenance. Timestamp conventions documented at the top. |
| `trend_study.py` | TrendScore construction, conditional forward-return tables, parameter diagnostics. |
| `stats_tools.py` | HAC/Newey-West, non-overlapping subsamples, circular block bootstrap. |
| `strategies.py` | Exposure maps, causal backtest engine, DVOL and drawdown overlays, metrics. |
| `run_all.py` | Orchestrator. Writes all tables and figures; console output is an audit trail. |
| `verify.py` | Independent re-derivation + look-ahead traps. |
| `ma_benchmark.py` | Pre-specified head-to-head vs a single 200-day SMA (tables 25-29, fig11). |
| `alt_basket.py` | Broad point-in-time alt-basket test of the crypto-beta hypothesis (tables 30-34, fig12). |
| `cache/` | Raw downloaded CSVs + `provenance.json`. Safe to delete. |
| `results/tables/` | 38 CSVs, numbered in the order the report uses them. |
| `results/figures/` | 12 PNGs. |
| `btc_trend_agreement_research.md` | The research report. |

## Data sources

| Series | Venue | Instrument | Coverage |
|---|---|---|---|
| BTC (primary) | Coinbase Exchange | `BTC-USD` | 2015-07-20 → present |
| BTC (check) | Binance spot | `BTCUSDT` | 2017-08-17 → present |
| ETH (primary) | Coinbase Exchange | `ETH-USD` | 2016-05-18 → present |
| ETH (check) | Binance spot | `ETHUSDT` | 2017-08-17 → present |
| SOL (primary) | Binance spot | `SOLUSDT` | 2020-08-11 → present |
| SOL (check) | Coinbase Exchange | `SOL-USD` | 2021-06-30 → present |
| BTC DVOL | Deribit | `public/get_volatility_index_data` | 2021-03-24 → present |

All series are **daily UTC closes**. SOL takes Binance as primary purely because
its history is ~10 months longer; the two venues agree to 0.04% median.

## Why not the project data lake

`data/curated/data_lake/fact_price.parquet` carries BTC only from **2024-01-07**
and the local checkout is stale at **2026-01-05** — it cannot support a
2015-present study. It is still used as an independent cross-check, and that
check surfaced a real alignment issue worth knowing about:

> **`fact_price.date` is stamped one day later than the UTC close it
> represents.** `lake[t]` equals the exchange close of `t-1`, consistent with
> CoinGecko `market_chart` 00:00 UTC snapshots. Naively joining the lake to
> exchange data on the same date gives a daily-return correlation of **-0.06**;
> shifting by one day gives **0.9982** with a 0.05% median level difference.

Recorded in `cache/provenance.json` under `lake_crosscheck`.
