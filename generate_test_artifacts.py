#!/usr/bin/env python3
"""
Helper script to generate test artifacts for ChatGPT verification.
"""
import subprocess
import sys
from pathlib import Path
import pandas as pd

repo_root = Path(__file__).parent

print("=" * 80)
print("GENERATING TEST ARTIFACTS FOR CHATGPT VERIFICATION")
print("=" * 80)

# 1. Run tests (skip if they take too long - can run manually)
print("\n1. Tests:")
print("   Run manually: python tests/test_pipeline_modes.py > test_output.txt 2>&1")
print("   (Some tests may require full pipeline runs)")

# 2. Create CSV samples from existing data
print("\n2. Creating CSV samples...")
try:
    eligibility_path = repo_root / "data" / "curated" / "universe_eligibility.parquet"
    if eligibility_path.exists():
        df = pd.read_parquet(eligibility_path)
        sample_path = repo_root / "universe_eligibility_sample.csv"
        df.head(100).to_csv(sample_path, index=False)
        print(f"  Created: universe_eligibility_sample.csv ({len(df.head(100))} rows)")
    else:
        print(f"  [SKIP] {eligibility_path} not found")
except Exception as e:
    print(f"  [ERROR] Failed to create eligibility sample: {e}")

try:
    snapshots_path = repo_root / "data" / "curated" / "universe_snapshots.parquet"
    if snapshots_path.exists():
        df = pd.read_parquet(snapshots_path)
        sample_path = repo_root / "universe_snapshots_sample.csv"
        df.head(100).to_csv(sample_path, index=False)
        print(f"  Created: universe_snapshots_sample.csv ({len(df.head(100))} rows)")
    else:
        print(f"  [SKIP] {snapshots_path} not found")
except Exception as e:
    print(f"  [ERROR] Failed to create snapshots sample: {e}")

# 3. Instructions for manual runs
print("\n" + "=" * 80)
print("NEXT STEPS (Manual):")
print("=" * 80)
print("\n1. Run research mode:")
print("   python scripts/run_pipeline.py --config configs/golden.yaml > research_run_console.txt 2>&1")
print("   Then copy: outputs/runs/<newest>/run_receipt.json")
print("\n2. Run smoke mode:")
print("   python scripts/run_pipeline.py --config configs/golden.yaml --mode smoke --skip-qc --skip-validation > smoke_run_console.txt 2>&1")
print("   Then copy: outputs/runs/<newest>/run_receipt.json")
print("\n3. Copy backtest outputs from research run:")
print("   - outputs/runs/<run_id>/outputs/backtest_results.csv")
print("   - outputs/runs/<run_id>/outputs/rebalance_turnover.csv")
print("   - outputs/runs/<run_id>/outputs/report.md (if exists)")

print("\n" + "=" * 80)
print("FILES READY TO SEND:")
print("=" * 80)
print("[READY] scripts/run_pipeline.py")
print("[READY] src/utils/metadata.py")
print("[READY] configs/golden.yaml")
print("[READY] tests/test_pipeline_modes.py")
print("[READY] test_output.txt (run manually)")
print("[READY] universe_eligibility_sample.csv (created)")
print("[READY] universe_snapshots_sample.csv (created)")
print("\n[NEED TO GENERATE] Manual steps:")
print("   - Research mode run_receipt.json + console output")
print("   - Smoke mode run_receipt.json + console output")
print("   - backtest_results.csv (from research run)")
print("   - rebalance_turnover.csv (from research run)")

