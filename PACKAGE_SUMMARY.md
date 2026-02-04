# Verification Package Summary

## Package Created
**Name:** `verification_package_chatgpt_fixes_20260126_144056`

## Contents

### Specs (2 files)
- `msm_v1_baseline.yaml` - MSM experiment specification
- `config.yaml` - Base configuration

### Code (9 files)
- `run.py` - Main CLI + experiment loading + MSM mode + regime evaluation target fix
- `beta_neutral.py` - MSM basket builder (`build_msm_basket`)
- `features.py` - Feature computation
- `regime.py` - Regime classification
- `regime_evaluation.py` - Regime-conditional forward returns
- `experiment_manager.py` - Manifests, catalog, stability metrics
- `backtest.py` - Backtest engine (position-weighted funding, gross/net returns, fixed rebalancing)
- `data_io.py` - Data loader + exclusions
- `config_utils.py` - **NEW:** Deep merge + MSM config overrides

### Evidence (6 files)
- `manifest.json` - Full experiment spec + resolved config (shows alt_selection.enabled = False)
- `metrics.json` - KPIs + stability metrics + **regime_evaluation** (with alts_index - BTC results)
- `regime_timeseries.csv` - 1135 rows (regime time series)
- `returns.csv` - 434 rows (with r_ls_gross, r_ls_net, cost, funding)
- `catalog_row.csv` - Catalog entry for this run
- `regime_evaluation.json` - (if exists separately, otherwise in metrics.json)

## Key Fixes to Verify

1. **Deep merge** - `config_utils.py` + `run.py` lines ~40-80
2. **Regime evaluation target** - `run.py` lines ~331-400 (uses alts_index - BTC)
3. **Funding calculation** - `backtest.py` `_compute_daily_pnl` (position-weighted)
4. **Gross vs net returns** - `backtest.py` (r_ls_gross, r_ls_net columns)
5. **MSM mode disables non-MSM knobs** - `config_utils.py` `apply_msm_config_overrides()`
6. **Fixed schedule rebalancing** - `backtest.py` (rebalance_frequency_days parameter)

## Test Run Details
- **Run ID:** msm_v1_baseline_20260126_143518
- **Date Range:** 2024-01-01 to 2025-12-31
- **Trading Days:** 434
- **Target Returns Computed:** 434 (alts_index - BTC) ✅

## How to Send

1. **Drag entire folder** into ChatGPT
2. **OR select all files** and drag at once
3. **OR create ZIP** and send that

## Naming Convention

Format: `verification_package_<purpose>_<timestamp>`

- Purpose: `chatgpt_fixes` (for this package)
- Timestamp: `YYYYMMDD_HHMMSS` (e.g., 20260126_144056)

Future packages will follow this convention for easy identification.
