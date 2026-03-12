#!/usr/bin/env python3
"""Quick status check for baseline backtest."""

from pathlib import Path
import json
import time

baseline_dir = Path("reports/majors_alts_baseline")
kpis_path = baseline_dir / "kpis.json"

print("Baseline Backtest Status:")
print("=" * 50)

if kpis_path.exists():
    file_age = (time.time() - kpis_path.stat().st_mtime) / 60
    print("[COMPLETE] Baseline backtest finished!")
    print(f"  Completed: {file_age:.1f} minutes ago")
    
    try:
        with open(kpis_path) as f:
            kpis = json.load(f)
        print("\nResults:")
        print(f"  CAGR: {kpis.get('cagr', 0)*100:.2f}%")
        print(f"  Sharpe: {kpis.get('sharpe', 0):.3f}")
        print(f"  Sortino: {kpis.get('sortino', 0):.3f}")
        print(f"  Max DD: {kpis.get('max_drawdown', 0)*100:.2f}%")
        print("\nRun: python compare_baseline_vs_enhanced.py")
    except Exception as e:
        print(f"  Error loading results: {e}")
else:
    if baseline_dir.exists():
        files = list(baseline_dir.glob("*"))
        if files:
            latest = max(files, key=lambda p: p.stat().st_mtime)
            age = (time.time() - latest.stat().st_mtime) / 60
            print("[RUNNING] Baseline backtest in progress...")
            print(f"  Latest activity: {latest.name}")
            print(f"  Last modified: {age:.1f} minutes ago")
        else:
            print("[WAITING] Baseline backtest starting...")
    else:
        print("[WAITING] Baseline backtest not started yet...")
