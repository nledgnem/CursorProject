# Phase 0, 1, 2 Implementation - COMPLETE ✅

## Summary

All requested features for Phase 0, 1, and 2 have been implemented. The system now supports:
- ✅ Experiment registry with YAML specs
- ✅ Run manifests and catalog system
- ✅ Stability metrics
- ✅ Pure MSM backtest mode (market cap-based, BTC-only, selection-independent)
- ✅ Regime-conditional forward returns evaluation
- ✅ Experiment sweep functionality

## Phase 0 - Quick Audit ✅

1. ✅ **Data Lake Verification**
   - Confirmed `fact_open_interest.parquet` and `fact_funding.parquet` loaded read-only
   - Confirmed no fetch scripts called during backtest

2. ✅ **Renamed Config Fields**
   - `train_window_days` → `lookback_window_days` (clarifies it's burn-in, not training)

## Phase 1 - Backtest Library MVP ✅

3. ✅ **Experiment Registry**
   - Created `experiments/` folder structure:
     - `experiments/msm/` - Market State Monitor experiments
     - `experiments/alpha/` - Coin selection experiments (placeholder)
     - `experiments/exec/` - Execution experiments (placeholder)
   - Example: `experiments/msm/msm_v1_baseline.yaml`

4. ✅ **Run Manifests + Catalog**
   - `runs/<run_id>/manifest.json` - Full config, git commit, data snapshot dates
   - `runs/<run_id>/metrics.json` - Headline stats + stability metrics
   - `runs/<run_id>/regime_timeseries.parquet` - Regime time series
   - `runs/<run_id>/returns.parquet` - Returns time series
   - `catalog/catalog.parquet` - Centralized catalog of all runs

5. ✅ **Stability Metrics**
   - `switches_per_year` - Regime switches per year
   - `avg_regime_duration_days` - Average regime duration
   - `regime_distribution` - % time in each regime
   - `total_switches`, `total_days` - Additional stats

## Phase 2 - Pure MSM Backtest ✅

6. ✅ **Pure MSM Mode**
   - **`build_msm_basket()`** function in `beta_neutral.py`:
     - Market cap-based selection (top N by mcap at time t)
     - Light liquidity sanity check (min_volume_usd, very permissive)
     - No enhanced filters (vol/corr/mom belong to ALPHA)
     - Equal weight or mcap weight options
   - **BTC-only long leg** by default (configurable for BTC+ETH variants)
   - **Fixed major weights** (no beta-neutrality solving for MSM)
   - **Auto regime-conditional forward returns** for MSM experiments

7. ✅ **Regime-Conditional Forward Returns**
   - New module: `majors_alts_monitor/regime_evaluation.py`
   - `evaluate_regime_edges()` - Computes mean(y | regime), hit rate, count, t-stat, p-value
   - Auto-enabled for MSM experiments
   - Optional for other experiments via `with_regime_eval: true`

8. ✅ **MSM Variant Sweeps**
   - New module: `majors_alts_monitor/sweep.py`
   - Sequential execution with progress reporting
   - Per-experiment: k/N, experiment_id, title, timestamps, duration, status
   - Error handling: captures traceback, continues to next (or fail-fast option)
   - End-of-sweep summary: succeeded/failed counts + pointers

## Usage

### Run Single Experiment

```bash
python -m majors_alts_monitor.run \
  --start 2024-01-01 \
  --end 2025-12-31 \
  --experiment experiments/msm/msm_v1_baseline.yaml
```

### Run Sweep (Batch)

```bash
python -m majors_alts_monitor.sweep \
  --glob "experiments/msm/*.yaml" \
  --start 2024-01-01 \
  --end 2025-12-31
```

### View Results

- **Catalog**: `catalog/catalog.parquet` - All runs in one place
- **Run Details**: `runs/<run_id>/manifest.json` - Full experiment spec + config
- **Metrics**: `runs/<run_id>/metrics.json` - Performance + stability metrics
- **Timeseries**: `runs/<run_id>/regime_timeseries.parquet`, `returns.parquet`

## Key Features

### Pure MSM Mode
- **Selection**: Top N by market cap (not volume)
- **Long Leg**: BTC-only (default) or BTC+ETH fixed weights
- **Short Leg**: Top N alts, equal or mcap weighted
- **No Enhanced Filters**: Volatility, correlation, momentum filters disabled
- **Target**: y_{t→t+H} = r_alts_index - r_BTC

### Regime Evaluation
- **Auto for MSM**: Always computed for MSM experiments
- **Optional for Others**: Can enable via `with_regime_eval: true`
- **Outputs**: Mean return, hit rate, count, t-stat, p-value per regime and horizon
- **Horizons**: Configurable (default: [1, 5, 10, 20] days)

### Experiment Tracking
- **No More Archaeology**: Every run logged with full spec + results
- **Catalog**: Easy comparison across experiments
- **Stability Metrics**: Quantify regime persistence and switching frequency

## Files Created/Modified

### New Files:
- `experiments/README.md`
- `experiments/msm/msm_v1_baseline.yaml`
- `majors_alts_monitor/experiment_manager.py`
- `majors_alts_monitor/regime_evaluation.py`
- `majors_alts_monitor/sweep.py`
- `majors_alts_monitor/__main_sweep__.py`

### Modified Files:
- `majors_alts_monitor/config.yaml` (renamed train_window_days)
- `majors_alts_monitor/config_baseline.yaml` (renamed train_window_days)
- `majors_alts_monitor/backtest.py` (renamed train_window_days)
- `majors_alts_monitor/beta_neutral.py` (added build_msm_basket)
- `majors_alts_monitor/run.py` (added experiment support, MSM mode, regime evaluation)
- `majors_alts_monitor/outputs.py` (added experiment manager integration, stability metrics)

## Next Steps (Phase 3 - Optional)

The following are marked as optional but valuable:
- Fix funding aggregation to be OI-weighted
- Implement slippage or remove from config
- Bootstrap significance testing for MSM edges

These can be implemented later as needed.
