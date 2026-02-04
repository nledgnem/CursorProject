# ChatGPT Feedback - Fixes Required

## Summary of Issues

1. **Experiment YAML overrides incomplete** - Need deep merge
2. **Regime evaluation uses wrong target** - Should use alts_index - BTC, not strategy PnL
3. **Funding modeling incorrect** - Currently averages, should be position-weighted
4. **Need gross vs net returns** - Separate r_ls_gross and r_ls_net
5. **MSM mode should disable non-MSM knobs** - Force-disable alt_selection, neutrality_mode, etc.
6. **Rebalancing for Pure MSM** - Should be fixed schedule, not regime-based

## Implementation Plan

### Priority 1: Critical Fixes

1. **Deep merge for config** - Implement recursive dict merge
2. **Fix regime evaluation target** - Use compute_target_returns() in MSM mode
3. **Fix funding calculation** - Position-weighted per asset

### Priority 2: Important Improvements

4. **Add gross vs net returns** - Track r_ls_gross separately
5. **Force-disable non-MSM knobs** - In MSM mode, ignore alt_selection, etc.
6. **Fixed rebalancing for MSM** - Daily/weekly schedule instead of regime-based

## Files to Modify

- `majors_alts_monitor/run.py` - Deep merge, MSM mode config, regime evaluation target
- `majors_alts_monitor/backtest.py` - Funding calculation, gross vs net returns, rebalancing
- `majors_alts_monitor/regime_evaluation.py` - May need updates for target computation
