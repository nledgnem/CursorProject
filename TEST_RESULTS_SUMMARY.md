# Test Results Summary

## Test Run: MSM Baseline Experiment

**Command:**
```bash
python -m majors_alts_monitor.run \
  --start 2024-01-01 \
  --end 2024-12-31 \
  --experiment experiments/msm/msm_v1_baseline.yaml
```

**Run ID:** `msm_v1_baseline_20260126_092441`

## ✅ What Worked

1. **MSM Mode Detection** ✓
   - Correctly detected `category_path == "msm"`
   - Logged: "MSM mode detected: using market cap-based basket selection"

2. **MSM Basket Building** ✓
   - Used `build_msm_basket()` function
   - Market cap-based selection (top 20 by mcap)
   - Equal weighting
   - Logged: "Built MSM basket for 2024-10-22: 20 assets (top 20 by mcap, equal weighted)"

3. **Manifest Creation** ✓
   - Created: `runs/msm_v1_baseline_20260126_092441/manifest.json`
   - Contains: run_id, experiment_id, title, category_path, git_commit, experiment_spec, resolved_config, data_snapshot_dates

4. **Metrics Creation** ✓
   - Created: `runs/msm_v1_baseline_20260126_092441/metrics.json`
   - Contains: KPIs (cagr, sharpe, max_drawdown, etc.) + stability metrics
   - Stability metrics: switches_per_year=24.95, avg_regime_duration_days=13.6

5. **Regime Evaluation** ✓
   - Created: `runs/msm_v1_baseline_20260126_092441/regime_evaluation.json`
   - Contains: regime-conditional forward returns for horizons [1d, 5d, 10d, 20d]
   - Computes: mean_return, hit_rate, count, t_stat, p_value per regime

6. **Timeseries Files** ✓
   - Created: `runs/msm_v1_baseline_20260126_092441/regime_timeseries.parquet`
   - Created: `runs/msm_v1_baseline_20260126_092441/returns.parquet`

## ⚠️ Issues Found

1. **Catalog Not Created**
   - `catalog/catalog.parquet` was not created
   - The `update_catalog()` call might have failed silently
   - Need to investigate why catalog update didn't happen

2. **Regime Evaluation Error (in shorter test)**
   - When backtest results are empty, regime evaluation fails
   - Fixed with better error handling and validation

## Sample Results

From `metrics.json`:
- **CAGR**: -97.56% (very negative - likely due to short test period or strategy issues)
- **Sharpe**: -16.345 (negative)
- **Max DD**: -59.94%
- **Switches/year**: 24.95 (regime switches ~25 times per year)
- **Avg regime duration**: 13.6 days
- **Regime distribution**: 
  - BALANCED: 84.4%
  - WEAK_RISK_ON_MAJORS: 11.7%
  - WEAK_RISK_ON_ALTS: 3.8%

**Regime Evaluation** (from `regime_evaluation.json`):
- All regimes show negative mean returns for all horizons
- BALANCED regime has most observations (29-40 depending on horizon)
- T-stats are significant (negative) for most regimes

## Next Steps

1. **Fix catalog creation** - Investigate why `update_catalog()` didn't create the catalog file
2. **Test with longer period** - The 2024-01-01 to 2024-12-31 period might be too short or have data issues
3. **Verify MSM basket** - Confirm it's using market cap (not volume) for selection
4. **Test sweep functionality** - Run a batch of experiments
