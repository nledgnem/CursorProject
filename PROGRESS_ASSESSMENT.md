# Progress Assessment: MSM Backtest Library Implementation

**Assessment Date:** 2026-01-26  
**Status:** ~75% Complete

---

## Phase 0 — Quick Audit (0.5 day)

### ✅ 1. Confirm lake tables exist + are used read-only
**Status: COMPLETE**

- ✅ `fact_funding.parquet` is loaded in `data_io.py` (line 147-157)
- ✅ `fact_open_interest.parquet` is loaded in `data_io.py` (line 160-170)
- ✅ All data loading is read-only (DuckDB connection uses `read_only=True`)
- ✅ No writes to `data/` directory

### ✅ 2. Verify no CoinGlass pulls during backtest
**Status: COMPLETE**

- ✅ No `fetch_coinglass` calls found in `majors_alts_monitor/` codebase
- ✅ All funding/OI data comes from data lake parquet files
- ✅ Backtest runner never calls `scripts/fetch_coinglass_data.py`

### ⚠️ 3. Rename misleading config fields
**Status: PARTIAL**

- ✅ `lookback_window_days` is used correctly in `backtest.py` and `config.yaml`
- ❌ **ISSUE:** `regime.py` still uses `train_window_days` parameter (line 354, 368, 420)
  - Function signature: `walk_forward_grid_search(..., train_window_days: int = 252, ...)`
  - Should be renamed to `lookback_window_days` for consistency
  - **Action needed:** Rename parameter in `regime.py`

---

## Phase 1 — "Backtest Library" MVP (1–2 days)

### ✅ 3. Add experiment registry structure
**Status: COMPLETE**

- ✅ `experiments/` folder exists
- ✅ `experiments/msm/` subfolder exists with `msm_v1_baseline.yaml`
- ✅ Experiment YAML structure includes:
  - `title`, `experiment_id`, `category_path`
  - `features` list with IDs + weights
  - `target` definition (long_leg, short_leg)
  - `state_mapping` (thresholds, hysteresis, persistence)
  - `backtest` params

### ✅ 4. Implement run manifests + catalog
**Status: COMPLETE**

- ✅ `experiment_manager.py` implements:
  - `write_manifest()` - fully resolved config + git commit + lake snapshot dates
  - `write_metrics()` - headline stats
  - `write_timeseries()` - regime_timeseries.parquet and returns.parquet
  - `update_catalog()` - appends to `catalog/catalog.parquet`
- ✅ Catalog includes: run_id, title, experiment_id, category_path, features, key knobs, key results, stability stats
- ✅ All outputs go to `runs/<run_id>/` directory

### ✅ 5. Add required stability metrics to outputs
**Status: COMPLETE**

- ✅ `compute_stability_metrics()` in `experiment_manager.py` computes:
  - `switches_per_year` ✅
  - `avg_regime_duration_days` ✅
  - `regime_distribution` (% time in each regime) ✅
- ✅ Metrics stored in `metrics.json` and catalog
- ✅ Metrics included in `regime_metrics.csv` (via `outputs.py`)

---

## Phase 2 — Implement the "Pure MSM" backtest family (1–2 days)

### ✅ 6. Add MSM backtest mode (selection-independent)
**Status: COMPLETE**

- ✅ MSM mode detection: `category_path == "msm"` triggers MSM-specific logic
- ✅ `build_msm_basket()` in `beta_neutral.py` (line 456):
  - Market cap-based selection (top N by mcap at time t)
  - Exclusions (stables, exchange, wrapped) handled automatically
  - Weighting: equal-weight or mcap-weight (configurable)
  - Light liquidity sanity check (min_volume_usd)
  - **No enhanced filters** (vol/corr/mom) - pure MSM
- ✅ Long leg: BTC-only default, optional BTC+ETH fixed weights
- ✅ Target returns: `alts_index - BTC` computed explicitly for MSM
- ✅ Regime-conditional forward returns: auto-computed for MSM (`with_regime_eval: true`)
- ✅ `evaluate_regime_edges()` computes: mean(y | regime), hit rate, count, t-stat

### ✅ 7. Add "MSM variant sweeps"
**Status: COMPLETE**

- ✅ `sweep.py` implements batch execution:
  - `--glob` pattern matching for experiment YAMLs
  - Sequential execution (as recommended)
  - Progress reporting: `[k/N]`, experiment_id, title, timestamps, duration
  - Error handling: captures traceback, continues on failure (configurable `--fail-fast`)
  - End-of-sweep summary: succeeded/failed counts
- ✅ Usage: `python -m majors_alts_monitor.sweep --glob "experiments/msm/*.yaml" --start 2023-01-01 --end 2026-01-01`

---

## Phase 3 — High-impact correctness upgrades (optional, but valuable)

### ❌ 8. Fix funding aggregation to be OI-weighted
**Status: NOT IMPLEMENTED**

- ❌ Current implementation uses simple `mean()` for major funding (line 216 in `features.py`)
- ❌ Current implementation uses `median()` for ALT funding (line 224 in `features.py`)
- ❌ **No OI-weighting** in funding aggregation
- **Action needed:** Implement OI-weighted aggregation:
  - Weight each asset's funding rate by its open interest (or position weight)
  - For major funding: `sum(funding_rate * oi_weight) / sum(oi_weight)`
  - For ALT funding: same approach
  - This impacts both `funding_skew` and `funding_heating` features
  - Also impacts PnL funding costs in backtest

### ✅ 9. Implement slippage
**Status: IMPLEMENTED (basic)**

- ✅ Slippage parameters in config: `slippage_bps`, `slippage_adv_multiplier`
- ✅ Basic slippage model in `backtest.py` (line 658):
  - `slippage_cost = total_turnover * self.slippage_bps`
  - Note: ADV scaling not fully implemented (comment says "would require per-asset volume data")
- ⚠️ **Minor:** ADV scaling multiplier exists but not used (could be enhanced later)

### ❌ 10. Bootstrap significance testing for MSM edges
**Status: NOT IMPLEMENTED**

- ❌ Config has bootstrap settings (`bootstrap.enabled`, `block_size`, `n_boot`) but no implementation
- ❌ `regime_evaluation.py` only computes t-stat and p-value (two-tailed t-test), no bootstrap
- ❌ No block bootstrap function found in codebase
- **Action needed:** Implement block bootstrap:
  - Resample blocks of consecutive days (default: 10 days)
  - Run N iterations (default: 300)
  - Compute p-values and 95% CI for regime edges
  - Add to `evaluate_regime_edges()` function

---

## Summary

### ✅ Completed (7/10 items)
1. ✅ Lake tables read-only verification
2. ✅ No CoinGlass pulls during backtest
3. ✅ Experiment registry structure
4. ✅ Run manifests + catalog
5. ✅ Stability metrics
6. ✅ MSM backtest mode
7. ✅ MSM variant sweeps
9. ✅ Slippage (basic implementation)

### ⚠️ Partial (1/10 items)
3. ⚠️ Config field renaming (mostly done, one remaining in `regime.py`)

### ❌ Not Started (2/10 items)
8. ❌ OI-weighted funding aggregation
10. ❌ Bootstrap significance testing

---

## Remaining Work

### High Priority (Phase 0 & 3)
1. **Rename `train_window_days` → `lookback_window_days` in `regime.py`** (15 min)
   - Function: `walk_forward_grid_search()` 
   - Lines: 354, 368, 420

2. **Implement OI-weighted funding aggregation** (2-3 hours)
   - Modify `_compute_funding_skew()` in `features.py`
   - Modify `_compute_funding_heating()` in `features.py`
   - Use `fact_open_interest.parquet` to weight funding rates
   - Also update backtest funding cost calculation

3. **Implement bootstrap significance testing** (3-4 hours)
   - Add `block_bootstrap()` function to `regime_evaluation.py`
   - Integrate into `evaluate_regime_edges()`
   - Output p-values and 95% CI for regime edges
   - Use config parameters: `block_size`, `n_boot`

### Low Priority (Enhancements)
- Enhance slippage model with ADV scaling (if per-asset volume data available)
- Add parallel sweep execution (if sequential becomes too slow)

---

## Overall Progress: ~75% Complete

**Phase 0:** 90% (1 minor rename needed)  
**Phase 1:** 100% ✅  
**Phase 2:** 100% ✅  
**Phase 3:** 50% (slippage done, OI-weighting + bootstrap needed)

**Estimated time to complete:** 6-8 hours
