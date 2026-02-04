ChatGPT Verification Package
================================

This package contains all files needed to verify Phase 0/1/2 implementation.

STRUCTURE:
----------
specs/
  - msm_v1_baseline.yaml      # MSM experiment spec
  - config.yaml               # Base configuration

code/
  - run.py                    # Main CLI + experiment loading
  - beta_neutral.py           # MSM basket builder (build_msm_basket)
  - features.py               # Feature computation
  - regime.py                 # Regime classification
  - regime_evaluation.py     # Regime-conditional forward returns
  - experiment_manager.py     # Manifests, catalog, stability metrics
  - backtest.py               # Backtest engine (verify lake-only)
  - data_io.py                # Data loader + exclusions (_get_stablecoins)

evidence/
  - manifest.json             # Full experiment spec + config + git commit
  - metrics.json              # KPIs + stability metrics + regime evaluation
  - regime_evaluation.json    # Regime-conditional forward returns
  - regime_timeseries.csv     # Regime time series (exported from parquet)
  - returns.csv               # Returns time series (exported from parquet)
  - catalog_row.csv           # Catalog entry for this run

HOW TO USE:
-----------
1. Drag the entire "verification_package" folder into ChatGPT
   OR
2. Select all files in this folder and drag them all at once
   OR
3. Create a zip file: zip -r verification_package.zip verification_package/

KEY VERIFICATION POINTS:
------------------------
- MSM uses top-N mcap (not volume) - see beta_neutral.py:build_msm_basket()
- Long leg default is BTC-only - see msm_v1_baseline.yaml:target.long_leg
- Exclusions applied - see data_io.py:_get_stablecoins() + run.py
- PIT-safe (as-of date filtering) - see beta_neutral.py lines 492-500
- Lake-only backtest - verify backtest.py doesn't import fetch_coinglass_data
- Regime evaluation computed - see regime_evaluation.json
- Manifest completeness - see manifest.json structure
- Catalog entry - see catalog_row.csv
