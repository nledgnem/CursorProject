# Phase 0, 1, 2 Implementation Progress

## Phase 0 - Quick Audit ✅ COMPLETE

1. ✅ **Data Lake Verification**
   - Confirmed `fact_open_interest.parquet` and `fact_funding.parquet` are loaded read-only via `data_io.py`
   - Confirmed backtest runner never calls `scripts/fetch_coinglass_data.py` (fetch script is offline ingestion only)

2. ✅ **Renamed Misleading Config Fields**
   - Renamed `train_window_days` → `lookback_window_days` in:
     - `config.yaml`
     - `config_baseline.yaml`
     - `backtest.py`
     - `run.py`
   - Updated docstrings to clarify it's a burn-in period, not training

## Phase 1 - Backtest Library MVP ✅ COMPLETE

3. ✅ **Experiment Registry Structure**
   - Created `experiments/` folder with:
     - `experiments/msm/` - Market State Monitor experiments
     - `experiments/alpha/` - Coin selection experiments (placeholder)
     - `experiments/exec/` - Execution experiments (placeholder)
   - Created example experiment spec: `experiments/msm/msm_v1_baseline.yaml`
   - Experiment spec format includes: title, experiment_id, category_path, features, target, state_mapping, backtest

4. ✅ **Run Manifests + Catalog**
   - Created `majors_alts_monitor/experiment_manager.py` with:
     - `ExperimentManager` class
     - `write_manifest()` - writes `runs/<run_id>/manifest.json` with:
       - Fully resolved config
       - Git commit hash
       - Lake snapshot dates used
       - Experiment spec
     - `write_metrics()` - writes `runs/<run_id>/metrics.json`
     - `write_timeseries()` - writes `runs/<run_id>/regime_timeseries.parquet` and `returns.parquet`
     - `update_catalog()` - appends to `catalog/catalog.parquet` with run metadata

5. ✅ **Stability Metrics**
   - Added `compute_stability_metrics()` to `ExperimentManager`:
     - `switches_per_year` - Regime switches per year
     - `avg_regime_duration_days` - Average duration of each regime
     - `regime_distribution` - % time in each regime
     - `total_switches` - Total number of regime switches
     - `total_days` - Total days in backtest
   - Integrated into `OutputGenerator` via `experiment_manager` parameter
   - Metrics written to both `metrics.json` and catalog

6. ✅ **Integration**
   - Updated `run.py` to:
     - Accept `--experiment` flag to load experiment YAML
     - Merge experiment spec into config (experiment overrides)
     - Initialize `ExperimentManager` when experiment spec provided
     - Write manifests, metrics, timeseries, and update catalog after backtest

## Phase 2 - Pure MSM Backtest ⚠️ IN PROGRESS

7. ⏳ **Pure MSM Backtest Mode**
   - **Status**: Need to implement
   - **Required**:
     - Add `build_msm_basket()` function that:
       - Selects top N alts by market cap (not volume)
       - No enhanced filters (vol/corr/mom)
       - Equal weight or mcap weight
       - Fixed exclusions (stables, exchange tokens, wrapped)
     - Modify backtest to support MSM mode
     - Report regime-conditional forward returns: mean(y | regime), hit rate, count, t-stat

8. ⏳ **MSM Variant Sweeps**
   - **Status**: Need to implement
   - **Required**:
     - Create `majors_alts_monitor/sweep.py` module
     - Add `--glob` flag to run multiple experiments
     - Batch processing with progress reporting

## Next Steps

1. **Complete Phase 2.1**: Implement `build_msm_basket()` and pure MSM mode
2. **Complete Phase 2.2**: Implement sweep functionality
3. **Testing**: Test experiment system end-to-end
4. **Documentation**: Update README with experiment usage

## Files Created/Modified

### New Files:
- `experiments/README.md`
- `experiments/msm/msm_v1_baseline.yaml`
- `majors_alts_monitor/experiment_manager.py`

### Modified Files:
- `majors_alts_monitor/config.yaml` (renamed train_window_days)
- `majors_alts_monitor/config_baseline.yaml` (renamed train_window_days)
- `majors_alts_monitor/backtest.py` (renamed train_window_days)
- `majors_alts_monitor/run.py` (added experiment support, experiment manager integration)
- `majors_alts_monitor/outputs.py` (added experiment manager integration, stability metrics)
