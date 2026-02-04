# ChatGPT Feedback - Fixes Implemented

## Summary

All critical issues identified by ChatGPT have been addressed.

---

## ✅ Fixes Implemented

### 1. Deep Merge for Experiment Config ✅

**Issue:** Experiment YAML overrides were incomplete, only patching a few fields.

**Fix:**
- Created `majors_alts_monitor/config_utils.py` with:
  - `deep_merge()` - Recursive dict merge function
  - `apply_msm_config_overrides()` - MSM-specific config overrides that force-disable non-MSM knobs
- Updated `run.py` to use deep merge for regular experiments and MSM-specific overrides for MSM mode
- MSM mode now properly disables `alt_selection` and ignores `neutrality_mode`

**Files Modified:**
- `majors_alts_monitor/config_utils.py` (NEW)
- `majors_alts_monitor/run.py`

---

### 2. Regime Evaluation Target Fixed ✅

**Issue:** Regime evaluation was using strategy PnL (`r_ls_net`) instead of `alts_index - BTC`.

**Fix:**
- In MSM mode, regime evaluation now computes target returns as `r_alts_index - r_BTC`
- Uses `compute_target_returns()` function with actual basket composition at each date
- Reconstructs MSM baskets using `build_msm_basket()` for each evaluation date
- Non-MSM mode still uses strategy returns (gross, not net, for cleaner evaluation)

**Files Modified:**
- `majors_alts_monitor/run.py`

---

### 3. Funding Calculation Fixed ✅

**Issue:** Funding was averaged across all assets, ignoring position weights and signs.

**Fix:**
- Funding is now position-weighted per asset
- Correct sign convention:
  - Short positions: receive funding (negative cost = positive PnL)
  - Long positions: pay funding (positive cost = negative PnL)
- Formula: `funding_cost = sum(w_i * funding_i)` with correct signs

**Files Modified:**
- `majors_alts_monitor/backtest.py` (`_compute_daily_pnl` method)

---

### 4. Gross vs Net Returns Added ✅

**Issue:** Only `r_ls_net` was tracked, no gross returns.

**Fix:**
- Added `r_ls_gross` to backtest results (before costs and funding)
- `r_ls_net` = `r_ls_gross` - `cost` - `funding`
- Both series are now written to timeseries parquet files
- Regime evaluation in non-MSM mode uses `r_ls_gross` for cleaner evaluation

**Files Modified:**
- `majors_alts_monitor/backtest.py`
- `majors_alts_monitor/run.py`

---

### 5. MSM Mode Forces Non-MSM Knobs Disabled ✅

**Issue:** MSM mode could still use non-MSM settings from base config.

**Fix:**
- `apply_msm_config_overrides()` function forces:
  - `alt_selection.enabled = False` (no enhanced filters)
  - `neutrality_mode` ignored (uses fixed major weights)
  - Universe settings properly overridden from experiment spec
- MSM mode now truly uses only market cap-based selection

**Files Modified:**
- `majors_alts_monitor/config_utils.py`
- `majors_alts_monitor/run.py`

---

### 6. Fixed Schedule Rebalancing for MSM ✅

**Issue:** Rebalancing was regime-based (dynamic), not suitable for Pure MSM testing.

**Fix:**
- Added `rebalance_frequency_days` parameter to `BacktestEngine`
- MSM mode uses fixed schedule rebalancing (daily by default, configurable)
- Strategy mode still uses dynamic rebalancing (regime-based)
- Tracks `last_rebalance_date` to enforce schedule

**Files Modified:**
- `majors_alts_monitor/backtest.py`
- `majors_alts_monitor/run.py`

---

## Remaining Considerations

1. **Funding Sign Convention**: Verify with actual funding data that the sign convention matches exchange behavior
2. **Target Returns Computation**: The current implementation reconstructs baskets at each date - this is correct but may be slightly different from actual backtest baskets due to rebalancing timing
3. **Open Interest Table**: ChatGPT noted OI table is missing - this is a data lake issue, not a code issue

---

## Testing Recommendations

1. Run MSM experiment and verify:
   - Config shows `alt_selection.enabled = False`
   - Rebalancing happens on fixed schedule (check logs)
   - Regime evaluation uses `alts_index - BTC` (check `regime_evaluation.json`)
   - Funding is position-weighted (check funding costs in results)

2. Compare MSM vs strategy mode:
   - MSM should have simpler, more deterministic behavior
   - Strategy mode should still use dynamic rebalancing

3. Verify gross vs net returns:
   - `r_ls_gross` should equal `pnl` (before costs)
   - `r_ls_net` should equal `r_ls_gross - cost - funding`

---

## Files Created/Modified

**New Files:**
- `majors_alts_monitor/config_utils.py`

**Modified Files:**
- `majors_alts_monitor/run.py`
- `majors_alts_monitor/backtest.py`
