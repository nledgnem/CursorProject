# File Structure: Regime Monitor and Backtest

## Main Package: `majors_alts_monitor/`

This is the **primary regime monitor and backtest system** that implements the Long BTC/ETH vs Short ALT basket strategy.

### Core Files

#### 1. **Entry Points**

- **`run.py`** - Main CLI script to run full backtest
  - Usage: `python -m majors_alts_monitor.run --start 2024-01-01 --end 2025-12-31`
  - Orchestrates: data loading → feature computation → regime modeling → backtesting → output generation

- **`signals.py`** - Generate daily trading signals (for live monitoring)
  - Usage: `python -m majors_alts_monitor.signals --asof 2026-01-08`
  - Outputs: Current regime, score, recommended positions

- **`__main__.py`** - Package entry point (enables `python -m majors_alts_monitor.run`)

#### 2. **Configuration**

- **`config.yaml`** - Main configuration file
  - Data paths, universe rules, feature weights, regime thresholds, backtest parameters, risk management settings
  - This is the **primary config** used by the strategy

- **`config_baseline.yaml`** - Baseline config (no OI/funding features)
  - Used for comparison testing

#### 3. **Data Layer**

- **`data_io.py`** - Read-only data loader from data lake
  - Loads: prices, marketcap, volume, funding, open interest
  - Uses DuckDB for fast queries
  - **Never writes** to data lake (read-only constraint)

#### 4. **Feature Engineering**

- **`features.py`** - Feature computation library
  - Computes 8 feature groups:
    1. ALT Breadth
    2. BTC Dominance
    3. Funding Skew
    4. Funding Heating
    5. OI Risk
    6. Liquidity
    7. Volatility Spread
    8. Momentum
  - All features are PIT-safe (no lookahead bias)
  - Z-scoring with rolling windows

#### 5. **Regime Modeling**

- **`regime.py`** - Regime classification engine
  - Computes composite score (weighted sum of features)
  - Classifies into 3 or 5 regimes
  - Implements hysteresis and regime persistence
  - High-vol gate (caps score during extreme moves)

#### 6. **Portfolio Construction**

- **`beta_neutral.py`** - Dual-beta neutral portfolio builder
  - Builds ALT basket (top 20 liquid, with filters)
  - Estimates betas (ridge regression)
  - Solves for beta-neutrality
  - Sizes BTC/ETH to offset ALT beta exposure

#### 7. **Backtest Engine**

- **`backtest.py`** - Walk-forward backtest engine
  - Implements the Long BTC/ETH vs Short ALT strategy
  - Manages positions, regime gating, risk management
  - Computes PnL, costs, funding
  - Tracks equity curve, returns, drawdowns

#### 8. **Output Generation**

- **`outputs.py`** - Report generator
  - Creates CSV files (regime timeline, equity curve, daily PnL)
  - Generates KPIs (Sharpe, CAGR, max drawdown, etc.)
  - Creates HTML dashboard (Plotly charts)

#### 9. **Documentation**

- **`README.md`** - Package documentation
- **`IMPLEMENTATION_SUMMARY.md`** - Implementation details

---

## Legacy/Alternative Monitor: `OwnScripts/regime_backtest/`

This is the **previous/legacy regime monitor** (not the main one):

- **`regime_monitor.py`** - Legacy API-based monitor
  - Fetches data from CoinGecko and CoinGlass APIs
  - Computes regime scores (different methodology)
  - Writes to CSV log
  - **No built-in backtest** (regime scoring only)

**Note**: This is NOT the main system. The main system is `majors_alts_monitor/`.

---

## Comparison Scripts (Root Directory)

These scripts compare different configurations or monitors:

- **`compare_monitors_simple.py`** - Compare current vs legacy monitors
- **`compare_baseline_vs_enhanced.py`** - Compare baseline (no OI/funding) vs enhanced
- **`test_basket_sizes.py`** - Test different ALT basket sizes
- **`test_risk_management.py`** - Test different risk management methods
- **`analyze_backtest_results.py`** - Analyze backtest results

---

## Data Fetching Scripts: `scripts/`

- **`fetch_coinglass_data.py`** - Fetch funding rates and Open Interest from CoinGlass API
  - Writes to `data/curated/data_lake/fact_funding.parquet`
  - Writes to `data/curated/data_lake/fact_open_interest.parquet`

---

## Summary

**The main regime monitor and backtest system is:**
- **Package**: `majors_alts_monitor/`
- **Main entry point**: `majors_alts_monitor/run.py`
- **Config**: `majors_alts_monitor/config.yaml`
- **Strategy**: Long BTC/ETH vs Short ALT basket (dual-beta neutral)

**To run a backtest:**
```bash
python -m majors_alts_monitor.run --start 2024-01-01 --end 2025-12-31
```

**To generate signals:**
```bash
python -m majors_alts_monitor.signals --asof 2026-01-08
```

**The legacy monitor (for reference only):**
- `OwnScripts/regime_backtest/regime_monitor.py` (not used in main backtest)
