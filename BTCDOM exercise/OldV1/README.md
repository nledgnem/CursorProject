# BTCDOM-style index (reconstructed from data lake)

This folder contains a script that reconstructs a **BTCDOM-style index** from the existing parquet data lake using **Binance’s Price Component Info** (constituents, Weight % and Weight Quantity) from the [Binance Futures perpetual index page](https://www.binance.com/en/futures/funding-history/perpetual/index).

## Why did our chart look different from Binance?

1. **Scale** — We default to **base 1000** on the first date, so the index sits around 1000. Binance’s BTCDOM index is in the **~2500–5500** range because they use a different divisor. Use `--match-binance-date` and `--match-binance-value` so the index level matches Binance on a chosen date (e.g. 2026-01-29 and 5000).
2. **Weight (Quantity) vs Weight (%)** — Binance’s index level is based on **Weight (Quantity)** × Last Index Price. We now use quantity weights by default so the *level* is comparable; with weight % only the *shape* is similar.
3. **Basket size** — On some dates we have fewer than 20 constituents (e.g. 15–16) if data is missing, so the basket differs from Binance and can cause extra moves or spikes compared to their chart.
4. **Data source** — We use the data lake’s daily closes; Binance uses their own feed and timing, so small differences in level and timing are expected.

## Data sources (read-only)

- `data/curated/data_lake/fact_price.parquet` — `asset_id`, `date`, `close`, `source` (used for BTC and constituent closes)
- `data/curated/data_lake/fact_marketcap.parquet` — loaded for compatibility (not used for index calculation)
- `data/curated/data_lake/dim_asset.parquet` — loaded for compatibility (not used for constituent set)

## Methodology (Binance Price Component Info)

1. **Constituents and weights**  
   Fixed list of 20 coins and their **Weight (%)** from Binance’s BTCDOM index:  
   ETH (42.82%), XRP (15.11%), BNB (14.82%), SOL (8.64%), TRX (4.67%), DOGE (2.94%), ADA (1.85%), BCH (1.70%), LINK (1.13%), XLM (0.92%), HBAR (0.76%), LTC (0.75%), AVAX (0.71%), ZEC (0.70%), SUI (0.64%), DOT (0.48%), UNI (0.44%), TAO (0.33%), AAVE (0.31%), SKY (0.28%).  
   Component info can be retrieved from: [Binance Futures – Perpetual Index](https://www.binance.com/en/futures/funding-history/perpetual/index).

2. **Last Index Price (per constituent)**  
   For each constituent \(i\) on date \(t\):  
   `Price_i(t) = BTC_close(t) / Constituent_i_close(t)`  
   (BTC and alt closes in USD from `fact_price`; same as “Last Index Price” in Binance’s table.)

3. **Weights**  
   By default we use **Weight (Quantity)** from Binance’s table so the index level matches Binance:  
   raw `S(t) = sum_i (weight_quantity_i * Price_i(t))`.  
   With `--no-quantity-weights` we use Weight (%): raw `S(t) = sum_i (weight_pct_i/100) * Price_i(t)` with renormalization when a constituent is missing.

4. **Index level**  
   Divisor sets the scale. By default: `divisor = S(reference_date) / 1000` so the index is **base 1000** on the first date.  
   To align with **Binance’s chart** (index in the ~2500–5500 range), use  
   `--match-binance-date YYYY-MM-DD --match-binance-value VALUE`  
   so that `divisor = S(match_date) / VALUE` and the index equals VALUE on that date (e.g. use a recent date and the Binance index value from their chart).

**Missing data:** If BTC has no price we drop that date. If a constituent has no price we exclude it for that date; with quantity weights the raw sum uses only available constituents (basket size can vary by date).

## Outputs (all under `BTCDOM exercise/`)

| File | Description |
|------|-------------|
| `btcdom_daily.parquet` | Daily series: `date`, `btcdom_index`, `constituent_count`, `divisor` |
| `btcdom_daily.csv` | Same series in CSV form |
| `btcdom_chart.png` | Plot of date vs `btcdom_index` (full range) |
| `btcdom_chart_2024_07_2026_01.png` | Chart when run with `--start-date 2024-07-01 --end-date 2026-01-31` |

## How to run

From the **repository root**:

```bash
python "BTCDOM exercise/compute_btcdom.py"
```

Optional arguments:

- `--data-path PATH` — Path to data lake directory (default: `data/curated/data_lake`).
- `--output-dir PATH` — Where to write outputs (default: this folder).
- `--top-n N` — Ignored (constituents come from Binance component list).
- `--start-date YYYY-MM-DD` / `--end-date YYYY-MM-DD` — Restrict series and chart to this date range.
- `--reference-date YYYY-MM-DD` — Date for base 1000 (default: first date in series).
- `--match-binance-date YYYY-MM-DD` and `--match-binance-value VALUE` — Set divisor so the index equals VALUE on that date (so the chart scale matches Binance, e.g. ~2500–5500).
- `--no-quantity-weights` — Use weight % instead of weight quantity.
- `--no-verify` — Skip printing verification of BTC/ETH/BNB coverage.

Example: align scale with Binance (index ~5000 on a recent date):
```bash
python "BTCDOM exercise/compute_btcdom.py" --start-date 2024-07-01 --end-date 2026-01-31 --match-binance-date 2026-01-29 --match-binance-value 5000
```

Example with custom reference date:

```bash
python "BTCDOM exercise/compute_btcdom.py" --reference-date 2024-01-01
```

## Requirements

- Python 3.10+
- `polars`
- `matplotlib` (for the chart)
