#!/usr/bin/env python3
"""Package files for ChatGPT to verify the fixes implemented."""

import shutil
from pathlib import Path
import polars as pl
from datetime import datetime

# Create package directory with timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
package_name = f"verification_package_chatgpt_fixes_{timestamp}"
package_dir = Path(package_name)
package_dir.mkdir(exist_ok=True)

print("=" * 80)
print(f"PACKAGING FILES FOR CHATGPT VERIFICATION (FIXES)")
print("=" * 80)
print(f"Package: {package_name}\n")

# 1. Specs
print("[1/3] Copying specs...")
specs_dir = package_dir / "specs"
specs_dir.mkdir(exist_ok=True)

shutil.copy("experiments/msm/msm_v1_baseline.yaml", specs_dir / "msm_v1_baseline.yaml")
shutil.copy("majors_alts_monitor/config.yaml", specs_dir / "config.yaml")
print(f"  [OK] {specs_dir / 'msm_v1_baseline.yaml'}")
print(f"  [OK] {specs_dir / 'config.yaml'}")

# 2. Code (with fixes)
print("\n[2/3] Copying code files...")
code_dir = package_dir / "code"
code_dir.mkdir(exist_ok=True)

code_files = [
    "majors_alts_monitor/run.py",
    "majors_alts_monitor/beta_neutral.py",
    "majors_alts_monitor/features.py",
    "majors_alts_monitor/regime.py",
    "majors_alts_monitor/regime_evaluation.py",
    "majors_alts_monitor/experiment_manager.py",
    "majors_alts_monitor/backtest.py",
    "majors_alts_monitor/data_io.py",
    "majors_alts_monitor/config_utils.py",  # NEW: Deep merge utility
]

for file_path in code_files:
    src = Path(file_path)
    if src.exists():
        dst = code_dir / src.name
        shutil.copy(src, dst)
        print(f"  [OK] {dst}")
    else:
        print(f"  [MISSING] {file_path}")

# 3. Evidence (latest run with fixes)
print("\n[3/3] Copying run artifacts...")
run_id = "msm_v1_baseline_20260126_143518"  # Latest run with all fixes
evidence_dir = package_dir / "evidence"
evidence_dir.mkdir(exist_ok=True)

# Copy JSON files
json_files = [
    f"runs/{run_id}/manifest.json",
    f"runs/{run_id}/metrics.json",
]

for file_path in json_files:
    src = Path(file_path)
    if src.exists():
        dst = evidence_dir / src.name
        shutil.copy(src, dst)
        print(f"  [OK] {dst}")
    else:
        print(f"  [MISSING] {file_path}")

# Export parquet files to CSV
print("\n  Exporting parquet files to CSV...")
parquet_files = [
    (f"runs/{run_id}/regime_timeseries.parquet", "regime_timeseries.csv"),
    (f"runs/{run_id}/returns.parquet", "returns.csv"),
]

for src_path, csv_name in parquet_files:
    src = Path(src_path)
    if src.exists():
        df = pl.read_parquet(src)
        dst = evidence_dir / csv_name
        df.write_csv(dst)
        print(f"  [OK] {dst} ({len(df)} rows)")
    else:
        print(f"  [MISSING] {src_path}")

# Copy catalog row CSV if it exists
catalog_csv = Path(f"catalog_row_{run_id}.csv")
if not catalog_csv.exists():
    # Try to export from catalog
    catalog_path = Path("catalog/catalog.parquet")
    if catalog_path.exists():
        cat = pl.read_parquet(catalog_path)
        row = cat.filter(pl.col("run_id") == run_id)
        if len(row) > 0:
            catalog_csv = evidence_dir / "catalog_row.csv"
            row.write_csv(catalog_csv)
            print(f"  [OK] {catalog_csv}")

# Copy regime evaluation JSON if it exists
regime_eval_path = Path(f"runs/{run_id}/regime_evaluation.json")
if regime_eval_path.exists():
    shutil.copy(regime_eval_path, evidence_dir / "regime_evaluation.json")
    print(f"  [OK] {evidence_dir / 'regime_evaluation.json'}")

# Create README
readme = package_dir / "README.txt"
with open(readme, "w") as f:
    f.write(f"""ChatGPT Verification Package - Fixes Implementation
============================================================
Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
Package: {package_name}

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
Run ID: {run_id}
Date Range: 2024-01-01 to 2025-12-31
Trading Days: 434
Target Returns Computed: 434 (alts_index - BTC)

HOW TO USE:
-----------
1. Drag the entire "{package_name}" folder into ChatGPT
   OR
2. Select all files in this folder and drag them all at once
   OR
3. Create a zip: Right-click folder -> Send to -> Compressed folder

""")

print(f"\n  [OK] {readme}")

print("\n" + "=" * 80)
print(f"[OK] Package created: {package_dir.absolute()}")
print(f"\nPackage name: {package_name}")
print("\nNext steps:")
print(f"  1. Drag the '{package_name}' folder into ChatGPT")
print("     OR")
print("  2. Select all files in the folder and drag them all at once")
print("     OR")
print("  3. Create a zip: Right-click folder -> Send to -> Compressed folder")
print("\nAll files are ready for upload!")
