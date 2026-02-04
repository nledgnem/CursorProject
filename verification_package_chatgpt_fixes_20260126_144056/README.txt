ChatGPT Verification Package - Fixes Implementation
============================================================
Generated: 2026-01-26 14:40:56
Package: verification_package_chatgpt_fixes_20260126_144056

This package contains all files needed to verify the 6 fixes implemented
based on ChatGPT's feedback.

STRUCTURE:
----------
specs/
  - msm_v1_baseline.yaml      # MSM experiment spec
  - config.yaml               # Base configuration

code/
  - run.py                    # Main CLI + experiment loading + MSM mode
  - beta_neutral.py           # MSM basket builder (build_msm_basket)
  - features.py               # Feature computation
  - regime.py                 # Regime classification
  - regime_evaluation.py     # Regime-conditional forward returns
  - experiment_manager.py     # Manifests, catalog, stability metrics
  - backtest.py               # Backtest engine (position-weighted funding, gross/net returns)
  - data_io.py                # Data loader + exclusions (_get_stablecoins)
  - config_utils.py           # NEW: Deep merge + MSM config overrides

evidence/
  - manifest.json             # Full experiment spec + resolved config
  - metrics.json              # KPIs + stability metrics + regime_evaluation
  - regime_timeseries.csv     # Regime time series (exported from parquet)
  - returns.csv               # Returns time series (with r_ls_gross, r_ls_net)
  - regime_evaluation.json    # Regime-conditional forward returns (if exists)
  - catalog_row.csv           # Catalog entry for this run

FIXES TO VERIFY:
----------------
1. Deep merge for config - See config_utils.py + run.py (lines ~40-80)
2. Regime evaluation target - See run.py (lines ~331-400), uses alts_index - BTC
3. Funding calculation - See backtest.py _compute_daily_pnl (position-weighted)
4. Gross vs net returns - See backtest.py (r_ls_gross, r_ls_net columns)
5. MSM mode disables non-MSM knobs - See config_utils.py apply_msm_config_overrides()
6. Fixed schedule rebalancing - See backtest.py (rebalance_frequency_days parameter)

KEY VERIFICATION POINTS:
------------------------
- MSM uses top-N mcap (not volume) - see beta_neutral.py:build_msm_basket()
- Long leg default is BTC-only - see msm_v1_baseline.yaml:target.long_leg
- Exclusions applied - see data_io.py:_get_stablecoins() + run.py
- PIT-safe (as-of date filtering) - see beta_neutral.py lines 492-500
- Lake-only backtest - verify backtest.py doesn't import fetch_coinglass_data
- Regime evaluation uses alts_index - BTC - see metrics.json:regime_evaluation
- Manifest completeness - see manifest.json structure
- Catalog entry - see catalog_row.csv

TEST RUN DETAILS:
-----------------
Run ID: msm_v1_baseline_20260126_143518
Date Range: 2024-01-01 to 2025-12-31
Trading Days: 434
Target Returns Computed: 434 (alts_index - BTC)

HOW TO USE:
-----------
1. Drag the entire "verification_package_chatgpt_fixes_20260126_144056" folder into ChatGPT
   OR
2. Select all files in this folder and drag them all at once
   OR
3. Create a zip: Right-click folder -> Send to -> Compressed folder

