# Binance Basis Trade Data Pipeline

Ranks Binance USDT-margined PERPETUAL pairs for basis trades using funding-rate history. Computes funding-only "always-on long spot + short perp" carry metrics across multiple windows and outputs ranked tables, top-20 CSVs, and optional charts.

## Setup

```bash
cd BinanceBasisProject
pip install -r requirements.txt
```

## Usage

```bash
python main.py --days 365 --windows 7 14 30 365 --max-symbols 0 --sleep-ms 300 --out ./out
```

### CLI Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--days` | 365 | Lookback days (max window) |
| `--windows` | 7 14 30 365 | Windows in days |
| `--max-symbols` | 0 | Limit symbols for quick tests; 0 = all |
| `--sleep-ms` | 300 | Delay between API requests (ms) |
| `--out` | ./out | Output directory |
| `--charts` | false | Generate APR distribution and optional per-symbol charts |
| `--chart-symbol` | "" | Symbol for per-symbol time series chart |
| `--w-neg-frac` | 1.0 | Weight for neg_frac in quality score |
| `--w-stdev` | 1.0 | Weight for stdev in quality score |
| `--w-top10-share` | 1.0 | Weight for top10_share in quality score |

### Quick Test (3 symbols)

```bash
python main.py --max-symbols 3 --windows 7 30 --sleep-ms 300
```

## Outputs

- `./out/binance_funding_rankings_<timestamp>.csv` – full rankings for all symbols and windows
- `./out/top20_<window>d.csv` – top 20 by APR and quality for each window
- `./out/data/<symbol>.parquet` – raw funding history per symbol (CSV fallback if parquet fails)
- Console: top 20 by APR and by quality for each window
- With `--charts`: `./out/apr_dist_<window>d.png`, `./out/series_<symbol>.png`

## API

Uses Binance USDT-M Futures REST API (public endpoints only):

- `GET https://fapi.binance.com/fapi/v1/exchangeInfo` – symbol list
- `GET https://fapi.binance.com/fapi/v1/fundingRate` – funding history (paginated)

## Metrics

Per symbol, per window:

- `funding_return` – sum of funding rates (short-perp earns +rate when rate > 0)
- `apr_simple` – `funding_return / window_days * 365`
- `pos_frac`, `neg_frac`, `zero_frac` – fraction of positive/negative/zero prints
- `stdev` – standard deviation of funding rates
- `top10_share` – share of total positive funding from top 10 positive prints
- `max_drawdown` – max drawdown of cumulative funding curve

## Rankings

1. **By APR** – highest `apr_simple` first (pure funding carry)
2. **By Quality** – penalizes `neg_frac`, `stdev`, `top10_share` (event-driven carry); weights configurable via CLI

## Tests

```bash
python -m pytest tests/ -v
```

## Project Structure

```
BinanceBasisProject/
├── main.py          # CLI and pipeline
├── binance.py       # API client (exchangeInfo, fundingRate, pagination, retries)
├── metrics.py       # Funding metrics computation
├── scoring.py       # APR and quality ranking
├── charts.py        # Optional charts
├── requirements.txt
├── README.md
└── tests/
    ├── test_metrics.py
    └── test_scoring.py
```
