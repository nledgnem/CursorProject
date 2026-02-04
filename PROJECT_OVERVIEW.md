# Crypto Backtest Platform - Project Overview

**Last Updated:** January 2025  
**Status:** Production-ready backtesting engine with monitor evaluation framework

---

## Executive Summary

This is a **production-grade crypto backtesting platform** for evaluating long-short (LS) basket strategies against BTC. The platform includes:

1. **Data pipeline**: Download → QC → Universe selection → Backtest
2. **Data lake architecture**: Normalized fact/dimension tables with canonical IDs
3. **Monitor evaluation framework**: Tests regime monitors against forward LS returns
4. **Validation framework**: Golden runs + invariant checks for correctness
5. **Point-in-time correctness**: No lookahead bias in universe selection or backtesting

---

## 1. Documentation

### Core Documentation
- **README.md**: Main user guide with setup, usage, and pipeline steps
- **docs/architecture.md**: System architecture, data flow, key concepts
- **DATA_LAKE_FORMAT_EXPLANATION.md**: Detailed explanation of normalized data lake format
- **docs/VALIDATION_FRAMEWORK.md**: Golden run + invariant checks framework
- **DETAILED_EXPLANATION_FOR_MADS.md**: Complete explanation of monitor evaluation process

### Specialized Documentation
- **MONITOR_EVAL_FRAMEWORK.md**: Monitor evaluation framework implementation details
- **INSTRUMENT_ASSET_LINKAGE.md**: How instruments link to assets via `asset_id`
- **docs/query_examples.md**: 15+ SQL query examples for DuckDB
- **docs/snapshot_schema.md**: Universe snapshot schema documentation
- **docs/PARQUET_VS_DATABASE.md**: Comparison of parquet vs database approaches

### Setup/Process Guides
- **GITHUB_SETUP_GUIDE.md**: GitHub repository setup instructions
- **UPDATE_SCRIPTS_GUIDE.md**: Guide for updating scripts
- **PRE_COMMIT_CHECKLIST.md**: Pre-commit validation checklist
- **CLEANUP_MD_FILES.md**: Documentation cleanup notes

---

## 2. Engine Architecture (`src/`)

### Core Modules

#### `src/backtest/`
- **engine.py**: Main backtest engine
  - Computes daily LS returns (BTC - basket)
  - Applies transaction costs on rebalance dates
  - Handles gap filling and data quality thresholds
  - Generates equity curves

#### `src/universe/`
- **snapshot.py**: Universe snapshot builder
  - Point-in-time universe selection
  - Eligibility filtering (perp listings, listing age, mcap, volume)
  - Weight calculation (cap-weighted, sqrt-cap, equal-weight-capped)
  - Top-N selection by market cap

#### `src/data_lake/`
- **schema.py**: Schema definitions for all data lake tables
- **mapping.py**: Provider ID → canonical ID mappings
- **build_duckdb.py**: DuckDB database builder from parquet files
- **validation.py**: Data lake validation logic
- **alignment_audit.py**: Alignment audit tools
- **mapping_validation.py**: Mapping validation tools

#### `src/evaluation/`
- **forward_returns.py**: Computes forward returns (t+1 to t+H, no lookahead)
- **regime_eval.py**: Regime evaluation statistics
  - Bucket stats (per regime: mean, median, std, sharpe_like)
  - Edge stats (regime 1/5 vs all, spread_1_5)
  - Block bootstrap for significance testing

#### `src/monitors/`
- **base.py**: Monitor interface (`MonitorBase` abstract class)
- **existing_monitor.py**: Wrapper for existing regime monitor (loads from CSV)
- **funding_persistence.py**: Stub for future funding-based monitor
- **mcap_relative_value.py**: Stub for future mcap-based monitor

#### `src/providers/`
- **coingecko.py**: CoinGecko API provider for price/mcap/volume data

#### `src/reporting/`
- **metrics.py**: Performance metrics (Sharpe, max DD, turnover, etc.)

#### `src/utils/`
- **data_loader.py**: Data loading utilities
- **metadata.py**: Run metadata generation (hashes, timestamps, etc.)

---

## 3. Scripts (`scripts/`)

### Main Pipeline Scripts
- **run_pipeline.py**: Complete pipeline runner (QC → snapshots → backtest)
- **run_golden.py**: Golden run (deterministic test with fixed config)
- **run_backtest.py**: Standalone backtest execution
- **run_monitor_eval.py**: Monitor evaluation runner
- **run_sensitivity.py**: Sensitivity testing runner

### Data Pipeline Scripts
- **download_data.py**: Downloads price/mcap/volume from CoinGecko
- **qc_curate.py**: Quality control and curation (outlier detection, gap handling)
- **build_universe_snapshots.py**: Builds point-in-time universe snapshots
- **fetch_binance_perp_listings.py**: Fetches Binance perpetual futures listings
- **fetch_coinglass_funding.py**: Fetches funding rates from CoinGlass (stub)

### Data Lake Scripts
- **convert_to_fact_tables.py**: Converts wide-format to normalized data lake format
- **consolidate_to_database.py**: Consolidates data lake into DuckDB
- **add_asset_id_to_instruments.py**: Adds `asset_id` to `dim_instrument` table
- **incremental_update.py**: Incremental data updates

### Validation Scripts
- **validate_run.py**: Validates backtest run (invariants, sanity checks)
- **validate_all_parquet.py**: Validates all parquet files
- **validate_snapshots.py**: Validates universe snapshots
- **validate_mapping.py**: Validates ID mappings
- **validate_canonical_ids.py**: Validates canonical ID consistency

### Utility Scripts
- **query_duckdb.py**: SQL query interface for DuckDB
- **check_data_freshness.py**: Checks data freshness
- **check_instruments_without_asset_id.py**: Finds instruments without asset_id
- **check_legacy_file_dependencies.py**: Checks for legacy file dependencies
- **expand_allowlist.py**: Expands perp allowlist

### Analysis Scripts
- **analyze_losers_rebound.py**: Analyzes losers rebound strategy

---

## 4. Data Structure

### Data Directories

#### `data/raw/`
- **Raw downloaded data** (never modified)
- `prices_daily.parquet`: Wide-format prices (one column per asset)
- `marketcap_daily.parquet`: Wide-format market caps
- `volume_daily.parquet`: Wide-format volumes
- `perp_listings_binance.parquet`: Binance perpetual futures listings
- `run_metadata_download.json`: Download run metadata

#### `data/curated/`
- **Curated wide-format files** (legacy, for backward compatibility)
- `prices_daily.parquet`, `marketcap_daily.parquet`, `volume_daily.parquet`
- `universe_snapshots.parquet`: Basket snapshots (selected top-N with weights)
- `universe_eligibility.parquet`: All candidates with eligibility flags
- `perp_listings_binance_aligned.parquet`: Aligned perp listings (has `instrument_id`)

#### `data/curated/data_lake/`
- **Normalized data lake format** (modern, preferred)
- **Dimension Tables:**
  - `dim_asset.parquet`: 939 assets (asset_id, symbol, name, chain, etc.)
  - `dim_instrument.parquet`: 605 instruments (instrument_id, asset_id, venue, type, etc.)
- **Fact Tables:**
  - `fact_price.parquet`: 478,834 price observations (asset_id, date, close)
  - `fact_marketcap.parquet`: 475,656 marketcap observations
  - `fact_volume.parquet`: 476,969 volume observations
  - `fact_funding.parquet`: 204,361 funding rate observations (asset_id, instrument_id, date, funding_rate)
- **Mapping Tables:**
  - `map_provider_asset.parquet`: CoinGecko ID → asset_id mappings
  - `map_provider_instrument.parquet`: Binance symbol → instrument_id mappings
- **Validation Files:**
  - `canonical_id_validation.json`: ID validation results
  - `mapping_validation.json`: Mapping validation results

#### `data/` (root)
- **Configuration/Reference Files:**
  - `perp_allowlist.csv`: List of perp-eligible coins
  - `blacklist.csv`: Excluded assets (stablecoins, etc.)
  - `stablecoins.csv`: Stablecoin list
  - `wrapped.csv`: Wrapped token list
  - `README.md`: Data directory documentation

### Data Format Evolution

**Legacy (Wide Format):**
- One column per asset (940+ columns)
- Schema changes when assets added
- Hard to join with dimension tables

**Modern (Data Lake Format):**
- Long format (one row per observation)
- Canonical IDs (`asset_id`, `instrument_id`)
- Star schema (dimension + fact + mapping tables)
- Easy joins, SQL-friendly, scales infinitely

---

## 5. Configuration Files (`configs/`)

### Strategy Configs
- **golden.yaml**: Minimal config for golden run (90 days, top 20, monthly)
- **golden_2year.yaml**: 2-year config for monitor evaluation (2024-01-07 to 2025-12-31, top 10, monthly)
- **strategy_benchmark.yaml**: Main benchmark strategy config

### Evaluation Configs
- **monitor_eval.yaml**: Monitor evaluation configuration
  - Horizons: [5, 10, 20, 40, 60] trading days
  - Block bootstrap: block_size=10, n_boot=300
  - Date range filters (optional)

### Config Structure
```yaml
strategy_name: "golden_smoke"
start_date: "2024-01-07"
end_date: "2025-12-31"
rebalance_frequency: "monthly"
top_n: 10
eligibility:
  must_have_perp: true
  min_listing_days: 30
weighting: "sqrt_cap_weighted"
max_weight_per_asset: 0.10
cost_model:
  fee_bps: 5
  slippage_bps: 5
backtest:
  gap_fill_mode: "none"
  basket_coverage_threshold: 0.90
```

---

## 6. Outputs (`outputs/`)

### Backtest Results
- **backtest_results.csv**: Daily performance series
  - Columns: date, r_btc, r_basket, r_ls, cost, r_ls_net, equity_curve
- **rebalance_turnover.csv**: Turnover per rebalance date
- **run_metadata_backtest.json**: Backtest run metadata (hashes, config, date ranges)

### Monitor Evaluation Results
- **monitor_eval/**: Timestamped evaluation runs
  - `regime_bucket_stats.csv`: Statistics per regime bucket
  - `regime_edges.csv`: Edge statistics with bootstrap significance
  - `run_receipt.json`: Full run metadata

### Validation Reports
- **test_validation_*.json**: Various validation test results
- **test_concentration/**: Concentration test results

### Sensitivity Analysis
- **sensitivity/**: Parameter sensitivity tests
  - `top20/`, `top30/`, `top50/`: Different top-N configurations

### Run Artifacts
- **runs/**: 353+ timestamped run artifacts
  - Each run contains: outputs/, configs/, metadata files
  - Includes: qc_report.md, validation_report.md, run_summary.md, report.md

### Research Database
- **research.duckdb**: DuckDB database for querying data lake

### Analysis Outputs
- **losers_rebound_*.csv**: Losers rebound strategy analysis
- **losers_rebound_plot.png**: Visualization
- **baseline_mapping.parquet**: Baseline mapping reference

---

## 7. Tests (`tests/`)

### Core Tests (21 test files)
- **test_backtest_*.py**: Backtest engine tests (costs, gap fill, data quality, smoke)
- **test_universe_snapshot.py**: Universe snapshot building tests
- **test_eligibility_point_in_time.py**: PIT eligibility tests
- **test_exclusion_ordering.py**: Exclusion logic tests
- **test_monitor_eval.py**: Monitor evaluation framework tests
- **test_qc_*.py**: QC curation tests (edge cases, gap fill, spike detection)
- **test_snapshot_sanity.py**: Snapshot sanity checks
- **test_turnover_accounting.py**: Turnover calculation tests
- **test_duckdb_views.py**: DuckDB view tests
- **test_binance_perp_eligibility.py**: Binance perp eligibility tests
- **test_pipeline_modes.py**: Pipeline mode tests
- **test_wrapped_exclusion.py**: Wrapped token exclusion tests
- **test_metadata_return_types.py**: Metadata return type tests

### Test Coverage
- ✅ No lookahead bias verification
- ✅ Invariant checks (weights sum to 1, no stablecoins, etc.)
- ✅ Forward returns calculation (t+1 to t+H)
- ✅ Block bootstrap correctness
- ✅ Data quality thresholds
- ✅ Gap filling logic
- ✅ Cost calculation
- ✅ Turnover accounting

---

## 8. Key Features & Capabilities

### Point-in-Time Correctness
- ✅ Universe selection uses only data available on rebalance date
- ✅ Binance perp listings for PIT eligibility (onboard dates)
- ✅ No lookahead bias in backtest or evaluation

### Data Quality
- ✅ QC curation with outlier detection
- ✅ Gap filling policies (none, 1d)
- ✅ Coverage thresholds (basket_coverage_threshold)
- ✅ Missing data handling (min_history_days, max_missing_frac)

### Validation Framework
- ✅ Golden run (deterministic, hash-stable)
- ✅ 11 core invariants (weights, basket size, exclusions, etc.)
- ✅ Sensitivity tests
- ✅ Manual spot-check queries

### Monitor Evaluation
- ✅ Forward returns (t+1 to t+H, no same-day)
- ✅ Bucket statistics (per regime: mean, median, std, sharpe_like)
- ✅ Edge statistics (regime 1/5 vs all, spread_1_5)
- ✅ Block bootstrap significance testing
- ✅ Configurable horizons and bootstrap parameters

### Data Lake Architecture
- ✅ Canonical IDs (`asset_id`, `instrument_id`)
- ✅ Normalized fact/dimension/mapping tables
- ✅ Instrument-to-asset linkage (381/605 instruments linked)
- ✅ DuckDB integration for SQL queries

### Reproducibility
- ✅ Run metadata with file hashes
- ✅ Config snapshots
- ✅ Git commit tracking
- ✅ Deterministic backtests

---

## 9. Current Status & Progress

### ✅ Completed
1. **Core Backtest Engine**: Fully functional LS basket backtest
2. **Data Pipeline**: Download → QC → Snapshots → Backtest
3. **Data Lake**: Normalized format with canonical IDs
4. **Monitor Evaluation Framework**: Complete with bootstrap significance testing
5. **Validation Framework**: Golden runs + 11 invariants
6. **Point-in-Time Eligibility**: Binance perp listings integration
7. **Instrument-Asset Linkage**: 63% of instruments linked to assets
8. **DuckDB Integration**: SQL query interface
9. **Comprehensive Tests**: 21 test files covering core functionality
10. **Documentation**: Extensive documentation (15+ markdown files)

### ⏳ In Progress / Future
1. **Funding Rate Integration**: CoinGlass integration (stub exists)
2. **Additional Monitors**: Funding persistence, mcap relative value (stubs exist)
3. **Multi-Exchange Perp Listings**: Extend beyond Binance (Bybit, OKX)
4. **Performance Attribution**: Decompose returns by asset/regime
5. **CI/CD Integration**: Automated golden runs on commits

### 📊 Data Coverage
- **Assets**: 939 assets in `dim_asset`
- **Instruments**: 605 instruments in `dim_instrument`
- **Price Data**: 478,834 observations (730 days, 2,718 symbols)
- **Market Cap Data**: 475,656 observations
- **Volume Data**: 476,969 observations
- **Funding Rates**: 204,361 observations
- **Date Range**: 2024-01-07 to 2025-12-31 (2 years)

### 🎯 Recent Work
- Monitor evaluation framework implementation
- Data lake format migration
- Instrument-asset linkage
- Validation framework with golden runs
- Point-in-time eligibility using Binance perp listings
- Comprehensive documentation

---

## 10. Usage Examples

### Run Complete Pipeline
```powershell
python scripts\run_pipeline.py --config configs\golden_2year.yaml
```

### Run Monitor Evaluation
```powershell
python scripts\run_monitor_eval.py \
  --config configs\monitor_eval.yaml \
  --ls-returns outputs\backtest_results.csv \
  --regime OwnScripts\regime_backtest\regime_history.csv
```

### Query Data with DuckDB
```powershell
python scripts\query_duckdb.py --sql "SELECT COUNT(*) FROM universe_snapshots"
```

### Run Golden Run (Validation)
```powershell
python scripts\run_golden.py
```

### Run Tests
```powershell
python -m pytest tests/
```

---

## 11. Technical Stack

### Dependencies (`requirements.txt`)
- **pandas==2.3.3**: Data manipulation
- **numpy==2.3.5**: Numerical computing
- **pyarrow==22.0.0**: Parquet file I/O (requires Python 3.10+)
- **requests==2.32.5**: HTTP requests (CoinGecko API)
- **pyyaml==6.0.3**: YAML config parsing
- **pytest==9.0.2**: Testing framework
- **duckdb>=1.1.3**: SQL query engine

### Data Formats
- **Parquet**: Primary storage format (columnar, compressed)
- **CSV**: Configuration and reference files
- **JSON**: Metadata and run receipts
- **DuckDB**: SQL query interface

### External APIs
- **CoinGecko**: Price, market cap, volume data
- **Binance**: Perpetual futures listings (exchangeInfo)
- **CoinGlass**: Funding rates (stub, not yet integrated)

---

## 12. Project Structure Summary

```
Cursor/
├── src/                    # Core engine modules
│   ├── backtest/          # Backtest engine
│   ├── universe/          # Universe selection
│   ├── data_lake/         # Data lake schema & validation
│   ├── evaluation/        # Monitor evaluation
│   ├── monitors/          # Regime monitors
│   ├── providers/         # Data providers
│   ├── reporting/         # Metrics & reporting
│   └── utils/             # Utilities
├── scripts/                # Executable scripts (25+ files)
├── configs/               # YAML configuration files
├── data/                  # Data storage
│   ├── raw/               # Raw downloaded data
│   ├── curated/           # Curated data (wide format)
│   └── curated/data_lake/ # Normalized data lake
├── outputs/               # Results & artifacts
│   ├── monitor_eval/      # Monitor evaluation results
│   ├── runs/              # Timestamped run artifacts
│   └── sensitivity/       # Sensitivity test results
├── tests/                 # Test suite (21 test files)
├── docs/                  # Documentation
├── OwnScripts/            # External scripts (regime backtest, etc.)
└── venv/                  # Virtual environment
```

---

## 13. Key Design Principles

1. **Point-in-Time Correctness**: Never use future information
2. **Separation of Concerns**: Data → Universe → Backtest → Evaluation
3. **Reproducibility**: Hash-stable outputs, config snapshots, metadata tracking
4. **Extensibility**: Clean interfaces for monitors, providers, strategies
5. **Validation**: Golden runs + invariants catch logic errors
6. **Data Lake First**: Normalized format preferred over wide format
7. **No Lookahead**: Forward returns use t+1 to t+H (explicitly excludes same-day)

---

## 14. Next Steps for AI Collaboration

When explaining this project to an AI, emphasize:

1. **Current State**: Production-ready backtest engine with monitor evaluation framework
2. **Data Architecture**: Dual format (legacy wide + modern data lake)
3. **Key Challenge**: Ensuring point-in-time correctness and no lookahead bias
4. **Recent Work**: Monitor evaluation, data lake migration, validation framework
5. **Future Work**: Funding rate integration, additional monitors, multi-exchange support

**Key Files to Reference:**
- `README.md`: User guide
- `docs/architecture.md`: System architecture
- `DETAILED_EXPLANATION_FOR_MADS.md`: Monitor evaluation explanation
- `DATA_LAKE_FORMAT_EXPLANATION.md`: Data lake format details
- `MONITOR_EVAL_FRAMEWORK.md`: Monitor evaluation implementation

---

**End of Overview**

