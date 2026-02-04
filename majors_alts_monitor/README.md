# Majors vs Alts Regime Monitor

Self-contained regime monitor and backtest system for detecting regimes where a **Long BTC/ETH vs Short ALT basket** performs well.

## Key Features

- **Read-only data lake access**: Never writes to `data/` or `research.duckdb`
- **PIT-safe**: Point-in-time correctness throughout
- **Dual-beta neutral**: Long-short construction with BTC/ETH beta neutrality
- **Walk-forward backtest**: Strict walk-forward validation with costs and funding
- **Regime modeling**: Composite score with walk-forward grid search (optional unsupervised)
- **Daily signals**: Generate trading signals for current date

## Installation

```bash
pip install polars numpy scipy scikit-learn statsmodels plotly pyyaml duckdb pyarrow
```

## Usage

### Run Backtest

```bash
python -m majors_alts_monitor.run --start 2023-01-01 --end 2026-01-01 --config majors_alts_monitor/config.yaml
```

### Generate Daily Signals

```bash
python -m majors_alts_monitor.signals --asof 2026-01-08 --config majors_alts_monitor/config.yaml
```

## Configuration

Edit `majors_alts_monitor/config.yaml` to customize:

- **Data paths**: Read-only paths to data lake and DuckDB
- **Universe rules**: Min cap/volume, exclude list, basket size
- **Costs & funding**: Fees, slippage, funding parameters
- **Feature weights**: Default weights for composite score
- **Regime mode**: Composite score or unsupervised (HMM/k-means)
- **Walk-forward**: Train/test window lengths

## Outputs

All outputs are written to directories **outside** the data lake:

- `./reports/majors_alts/`:
  - `regime_timeline.csv`: Daily regime classifications
  - `features_wide.parquet`: Computed features
  - `bt_equity_curve.csv`: Backtest equity curve
  - `bt_daily_pnl.csv`: Daily PnL breakdown
  - `kpis.json`: Performance KPIs (CAGR, Sharpe, Sortino, maxDD, Calmar, etc.)
  - `regime_metrics.csv`: By-regime performance metrics
  - `dashboard.html`: Interactive Plotly dashboard
  - `signals_today.json`: Daily trading signals

- `./artifacts/majors_alts/`:
  - Model parameters, learned weights, logs

- `./cache/`:
  - Ephemeral caches (never in `data/`)

## Architecture

### Data I/O (`data_io.py`)
- Read-only access to data lake parquet files
- Optional DuckDB connection (read-only)
- PIT-safe universe selection
- Symbol normalization (strip PERP, map SATS→1000SATS)

### Features (`features.py`)
- ALT breadth & dispersion
- BTC dominance shift
- Funding skew (if available)
- Liquidity/flow proxies
- Volatility spread
- Cross-sectional momentum

### Dual-Beta Neutral LS (`beta_neutral.py`)
- Ridge regression for beta estimation
- Neutrality solver (minimize BTC/ETH exposure)
- ALT basket construction (PIT-safe)

### Regime Modeling (`regime.py`)
- Composite score: weighted sum of z-scored features
- Walk-forward grid search for optimal weights/thresholds
- Hysteresis bands to reduce churn
- Optional unsupervised methods (HMM/k-means)

### Backtest Engine (`backtest.py`)
- Vectorized daily backtest
- Walk-forward validation
- Costs: maker/taker fees, slippage, funding
- Volatility targeting (optional)

### Outputs (`outputs.py`)
- CSV/Parquet exports
- HTML dashboard (Plotly)
- KPIs and regime metrics

## Testing

```bash
python -m pytest majors_alts_monitor/tests/
```

Tests use synthetic fixtures and never touch the real data lake.

## Read-Only Constraints

**CRITICAL**: This package is designed to be read-only with respect to the data lake:

- ✅ Reads from `data/curated/data_lake/` (parquet files)
- ✅ Reads from `outputs/research.duckdb` (if exists, read-only mode)
- ❌ **NEVER** writes to `data/` or any subdirectory
- ❌ **NEVER** modifies `research.duckdb`
- ✅ Writes only to `./reports/`, `./artifacts/`, `./cache/`

## Acceptance Criteria

✅ One command runs end-to-end without altering the data lake  
✅ Outputs saved only under `./reports/`, `./artifacts/`, `./cache/`  
✅ Regime series + backtest equity curve + KPIs + HTML dashboard produced  
✅ Walk-forward Sharpe/neutrality-error stats reported  
✅ `signals_today.json` prints sane recommendation at current data  
✅ All unit tests pass on synthetic fixtures

## License

Internal use only.
