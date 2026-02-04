# Crypto Backtest Platform

A minimal but robust crypto backtesting engine with point-in-time universe selection and basket rebalancing.

**Requirements**: Python 3.10+ (required for PyArrow 22.0.0)

## Setup

### 1. Create Virtual Environment (Windows)

```powershell
python -m venv venv
venv\Scripts\activate
```

### 2. Install Dependencies

```powershell
pip install -r requirements.txt
```

### 3. Create Data Directories

```powershell
mkdir data\raw
mkdir data\curated
mkdir outputs
```

## Usage

### Step 0: Download Raw Market Data

Downloads price, market cap, and volume data from CoinGecko:

```powershell
python scripts\download_data.py
```

This will:
- Fetch daily data for all coins in `data/perp_allowlist.csv`
- Save to `data/curated/prices_daily.parquet`, `marketcap_daily.parquet`, `volume_daily.parquet`
- Handle rate limits and missing coins gracefully

**Note:** The download script currently saves to `data/curated/` directly. For production use, it should save to `data/raw/` first, then run QC curation.

### Step 0.5: Fetch Binance Perp Listings (Optional)

Fetch Binance perpetual futures listings for point-in-time eligibility checks:

```powershell
python scripts\fetch_binance_perp_listings.py
```

This will:
- Fetch Binance USD-M Futures exchangeInfo
- Extract perpetual USDT contracts with onboard dates
- Save to `data/raw/perp_listings_binance.parquet`
- Generate metadata JSON with date ranges

**Note:** This is optional. If not run, the snapshot builder will fall back to the static `perp_allowlist.csv` file.

### Step 1: QC and Curate Data

Apply quality control rules and curate the raw data:

```powershell
python scripts\qc_curate.py --raw-dir data\raw --out-dir data\curated --outputs-dir outputs
```

This will:
- Read raw parquet files from `data/raw/`
- Apply QC rules: non-negativity checks, outlier detection (return spikes, mcap/volume spikes), duplicate handling
- **Preserve missing values as NA** (gap filling moved to backtest layer)
- Write curated parquet files to `data/curated/`
- Generate `outputs/qc_report.md` (human-readable summary)
- Generate `outputs/repair_log.parquet` (machine-readable audit trail)
- Generate `outputs/run_metadata_qc.json` (run metadata with file hashes)

**QC Rules:**
- Non-negativity: Prices must be > 0; market cap/volume must be >= 0
- Outlier detection: Return spikes > 500%, mcap/volume spikes vs 30d rolling median
- **Gap filling: Disabled by default** - missing values preserved as NA for backtest layer to handle

### Step 2: Build Universe Snapshots

Creates point-in-time snapshots of eligible baskets at each rebalance date:

```powershell
python scripts\build_universe_snapshots.py --config configs\strategy_benchmark.yaml
```

This will:
- Read the strategy config to determine rebalance dates and eligibility rules
- **Use Binance perp listings for point-in-time eligibility** (if `perp_listings_binance.parquet` exists, otherwise falls back to allowlist)
- For each rebalance date, select top N eligible coins based on market cap
- Calculate weights (cap-weighted, sqrt-cap-weighted, or equal-weight-capped)
- Save to `data/curated/universe_snapshots.parquet` (basket snapshots)
- Save to `data/curated/universe_eligibility.parquet` (all candidates with eligibility flags)

### Step 3: Run Backtest

Both `build_universe_snapshots.py` and `run_backtest.py` default to using `data/curated/` but can be configured:

```powershell
python scripts\build_universe_snapshots.py --config configs\strategy_benchmark.yaml --data-dir data\curated
python scripts\run_backtest.py --config configs\strategy_benchmark.yaml --data-dir data\curated
```

The `--data-dir` flag allows you to specify a different data directory (defaults to `data/curated`, falls back to `data/raw` if curated doesn't exist). `run_backtest.py` also supports `--snapshots-path` to specify a custom snapshots file location.

### Run Complete Pipeline (One Command)

Run all steps in sequence:

```powershell
python scripts\run_pipeline.py --config configs\strategy_benchmark.yaml
```

This runs:
1. QC curation (unless `--skip-qc` is used)
2. Binance perp listings fetch (optional, if `--fetch-perp-listings` is used)
3. Universe snapshot building
4. Backtest execution

Options:
- `--skip-qc`: Skip QC step (use existing curated data)
- `--fetch-perp-listings`: Fetch Binance perp listings before building snapshots
- `--perp-listings-output`: Output path for perp listings (default: `data/raw/perp_listings_binance.parquet`)
- `--raw-dir`, `--out-dir`, `--outputs-dir`: QC data directories
- `--qc-config`: Path to QC config YAML
- `--data-dir`: Data directory for snapshots (default: curated)
- `--backtest-data-dir`: Data directory for backtest (can differ from snapshots)
- `--snapshots-path`: Custom snapshots file path

Example with custom paths:
```powershell
python scripts\run_pipeline.py --config configs\strategy_benchmark.yaml --raw-dir data\raw --out-dir data\curated
```

Runs the backtest using the snapshots:

```powershell
python scripts\run_backtest.py --config configs\strategy_benchmark.yaml
```

This will:
- Load universe snapshots and price data
- **Apply gap filling** (if configured in `backtest.gap_fill_mode`)
- **Apply data quality thresholds** (min_history_days, max_missing_frac, etc.)
- Compute daily returns for BTC and the basket
- Apply costs at rebalance dates
- Generate `outputs/backtest_results.csv` and `outputs/report.md`

**Backtest Data Quality Settings** (configure in `configs/strategy_benchmark.yaml` under `backtest:`):
- `gap_fill_mode`: `"none"` (default) or `"1d"` (fill single-day gaps only)
- `min_history_days`: Minimum non-NA days required before eligibility
- `max_missing_frac`: Maximum missing fraction in lookback window
- `max_consecutive_missing_days`: Maximum consecutive missing days
- `basket_coverage_threshold`: Minimum weight coverage to compute basket return (default: 0.90)

## Configuration

Edit `configs/strategy_benchmark.yaml` to customize:

- **Date range**: `start_date`, `end_date`
- **Rebalancing**: `rebalance_frequency` (monthly/quarterly)
- **Universe selection**: `top_n`, `eligibility` rules
- **Weighting**: `weighting` scheme and `max_weight_per_asset`
- **Costs**: `fee_bps`, `slippage_bps`

See the config file for all options.

## Outputs

### Data Files
- `data/raw/*.parquet`: Raw downloaded data (never modified)
- `data/raw/perp_listings_binance.parquet`: Binance perpetual futures listings with onboard dates (optional)
- `data/curated/prices_daily.parquet`: Curated daily close prices
- `data/curated/marketcap_daily.parquet`: Curated daily market caps
- `data/curated/volume_daily.parquet`: Curated daily volumes
- `data/curated/universe_snapshots.parquet`: Basket snapshots (selected top-N with weights)
- `data/curated/universe_eligibility.parquet`: Universe eligibility (all candidates with flags and exclusion reasons)

### QC Outputs
- `outputs/qc_report.md`: Human-readable QC summary (date ranges, missingness, edit counts, top symbols by edits)
- `outputs/repair_log.parquet`: Machine-readable audit log of all QC edits (dataset, symbol, date, action, rule, old_value, new_value)
- `outputs/run_metadata_qc.json`: QC run metadata (config hash, input/output file hashes, repair stats)

### Backtest Results
- `outputs/backtest_results.csv`: Daily performance series
- `outputs/report.md`: Summary metrics (Sharpe, max DD, turnover, etc.)

### Run Metadata
Each script generates a `run_metadata.json` file containing:
- Git commit hash (for reproducibility)
- Config file path and hash
- Data file paths and hashes
- Date ranges
- Row counts
- Filter thresholds (for snapshots)

Metadata files:
- `outputs/run_metadata_qc.json`: QC curation run metadata
- `data/curated/run_metadata_download.json`: Data download run metadata (if download script updated)
- `data/curated/run_metadata_snapshots.json`: Snapshot building run metadata
- `outputs/run_metadata_backtest.json`: Backtest run metadata

## Architecture

See `docs/architecture.md` for:
- System architecture diagram
- Explanation of Universe vs Basket
- Data flow

## Querying Data with DuckDB

The platform includes a DuckDB query interface for inspecting data using SQL:

```powershell
# Run a single SQL query
python scripts\query_duckdb.py --sql "SELECT COUNT(*) FROM universe_snapshots"

# Interactive mode
python scripts\query_duckdb.py

# List available views
python scripts\query_duckdb.py --list-views
```

See `docs/query_examples.md` for 15+ copy/paste SQL queries including:
- Top eligible coins on a rebalance date
- Basket composition changes between rebalances
- **Manager queries**: Exclusions by reason over time, eligible universe size, QC top offenders
- QC edit summaries
- Data coverage analysis
- Price and market cap analysis

## Testing

Run tests:

```powershell
python -m pytest tests/
```

## Key Concepts

- **Universe**: Full dataset (all assets, all dates) stored locally
- **Basket**: Selected subset from universe at a rebalance date, frozen until next rebalance
- **Point-in-Time (PIT)**: Selection uses only data available as-of the rebalance date (no lookahead)

## Troubleshooting

- **429 Rate Limits**: The download script includes exponential backoff. If you hit limits, wait and rerun.
- **Missing Coins**: Coins not in CoinGecko or returning 404 are logged and skipped.
- **Empty Snapshots**: Check eligibility rules in config (min_listing_days, min_mcap, etc.)

## TODO / Future Enhancements

- [x] Binance perp listings for point-in-time eligibility (v0 - Binance only, extensible to Bybit/OKX)
- [ ] Add funding rate carry model
- [ ] Support multiple strategies in parallel
- [ ] Add more weighting schemes
- [ ] Performance attribution analysis
- [ ] Extend perp listings to Bybit/OKX (multi-exchange proxy)

