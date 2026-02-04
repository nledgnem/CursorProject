# Files to Send to ChatGPT for Verification

## Summary

Send these files to verify Phase 0/1/2 implementation correctness.

---

## 1. Specs and Registries (Source of Truth)

### Experiment Template/Spec
- **`experiments/msm/msm_v1_baseline.yaml`** - The actual MSM experiment spec that was run

### Base Config (for reference)
- **`majors_alts_monitor/config.yaml`** - Base configuration file (shows default settings)

**Note:** There's no separate feature registry YAML - features are defined in code (`majors_alts_monitor/features.py`). The experiment spec references feature IDs directly.

### Exclusions
- Exclusions are handled in `majors_alts_monitor/data_io.py` via `_get_stablecoins()` method
- Additional exclusions can be specified in config: `universe.exclude_assets`
- Exclusions are passed to `build_msm_basket()` in `run.py` (line ~101-103)

---

## 2. Code That Implements the Pipeline

### Experiment Loading + Resolution
- **`majors_alts_monitor/run.py`** - Main CLI entrypoint that:
  - Loads experiment YAML
  - Merges with base config
  - Detects MSM mode
  - Orchestrates the pipeline

### MSM Portfolio Builder (Pure MSM)
- **`majors_alts_monitor/beta_neutral.py`** - Contains:
  - `build_msm_basket()` function (lines ~XXX-YYY) - Market cap-based ALT selection
  - Exclusions logic
  - Weighting (equal or mcap)

### Regime Engine
- **`majors_alts_monitor/features.py`** - Feature computation
- **`majors_alts_monitor/regime.py`** - Regime classification (thresholds, persistence, hysteresis)

### Evaluation Module
- **`majors_alts_monitor/regime_evaluation.py`** - Computes regime-conditional forward returns

### Logging / Library Plumbing
- **`majors_alts_monitor/experiment_manager.py`** - Manifests, catalog, stability metrics
- **`majors_alts_monitor/backtest.py`** - Backtest engine (verify it's lake-only)

### Data Loading (verify lake-only)
- **`majors_alts_monitor/data_io.py`** - Data loader (should be read-only, no fetch calls)

---

## 3. Evidence: One Full Run Output Folder + Catalog Entry

### Complete Run Artifacts
Use run: **`msm_v1_baseline_20260126_092441`**

Send the entire folder contents:
- **`runs/msm_v1_baseline_20260126_092441/manifest.json`** - Full experiment spec + config + git commit + data dates
- **`runs/msm_v1_baseline_20260126_092441/metrics.json`** - KPIs + stability metrics + regime evaluation
- **`runs/msm_v1_baseline_20260126_092441/regime_timeseries.parquet`** - Regime time series (or export to CSV)
- **`runs/msm_v1_baseline_20260126_092441/returns.parquet`** - Returns time series (or export to CSV)
- **`runs/msm_v1_baseline_20260126_092441/regime_evaluation.json`** - Regime-conditional forward returns

### Catalog Entry
- **`catalog/catalog.parquet`** - Export the row for `run_id = "msm_v1_baseline_20260126_092441"` to CSV, or send the entire catalog

---

## Quick File List (Copy-Paste Ready)

```
# Specs
experiments/msm/msm_v1_baseline.yaml
majors_alts_monitor/config.yaml

# Code
majors_alts_monitor/run.py
majors_alts_monitor/beta_neutral.py  (contains build_msm_basket)
majors_alts_monitor/features.py
majors_alts_monitor/regime.py
majors_alts_monitor/regime_evaluation.py
majors_alts_monitor/experiment_manager.py
majors_alts_monitor/backtest.py
majors_alts_monitor/data_io.py  (contains _get_stablecoins for exclusions)

# Evidence (one run - msm_v1_baseline_20260126_092441)
runs/msm_v1_baseline_20260126_092441/manifest.json
runs/msm_v1_baseline_20260126_092441/metrics.json
runs/msm_v1_baseline_20260126_092441/regime_timeseries.parquet
runs/msm_v1_baseline_20260126_092441/returns.parquet
runs/msm_v1_baseline_20260126_092441/regime_evaluation.json
catalog_row_msm_v1_baseline_20260126_092441.csv  (exported catalog row)
```

**Note:** The parquet files can be exported to CSV if needed:
```python
import polars as pl
pl.read_parquet("runs/msm_v1_baseline_20260126_092441/regime_timeseries.parquet").write_csv("regime_timeseries.csv")
pl.read_parquet("runs/msm_v1_baseline_20260126_092441/returns.parquet").write_csv("returns.csv")
```

---

## What ChatGPT Will Verify

- [ ] MSM uses **top-N mcap** membership + exclusions
- [ ] Long leg default is **BTC-only** (variants allowed)
- [ ] Regime-conditioned forward returns computed in MSM mode
- [ ] Full manifest + catalog row produced
- [ ] Backtest runner uses **lake only** (no live CoinGlass)
- [ ] PIT-safe construction (no lookahead)
- [ ] Exclusions are explicit and applied
- [ ] Market cap selection is as-of time t
- [ ] Catalog appends are robust

---

## Additional Notes

1. **No separate feature registry YAML** - Features are defined in `features.py` and referenced by ID in experiment specs
2. **Exclusions** - Handled via:
   - `data_io.py._get_stablecoins()` - Gets stablecoins from `dim_asset.is_stable` flag + blacklist
   - `config.yaml: universe.exclude_assets` - Additional manual exclusions
   - Passed to `build_msm_basket()` in `run.py` (lines 101-103)
3. **Config resolution** - Happens in `run.py` when merging experiment spec with base config (lines 49-80)
4. **Network guard** - Not yet implemented (ChatGPT requested this - should add runtime check)
5. **Catalog** - Currently appends directly; could be made more robust per ChatGPT's suggestion (write individual JSON files, then merge)
6. **PIT-safety** - `build_msm_basket()` filters data to `asof_date` (lines 492-500 in `beta_neutral.py`)
7. **Market cap selection** - Uses latest mcap as-of `asof_date` (lines 503-508 in `beta_neutral.py`)

## Key Code Locations

- **MSM basket building**: `beta_neutral.py` lines 456-560 (`build_msm_basket()`)
- **Exclusions logic**: `data_io.py` lines 80-103 (`_get_stablecoins()`)
- **MSM mode detection**: `run.py` line ~175 (`is_msm_mode = experiment_spec.get("category_path") == "msm"`)
- **Experiment loading**: `run.py` lines 40-80 (loads YAML, merges with config)
- **Catalog update**: `experiment_manager.py` lines 180-248 (`update_catalog()`)
- **Regime evaluation**: `regime_evaluation.py` (entire file)
