#!/usr/bin/env python3
"""Monitor baseline backtest progress."""

import time
from pathlib import Path
import subprocess

print("Monitoring baseline backtest...")
print("=" * 70)

baseline_dir = Path("reports/majors_alts_baseline")
check_interval = 60  # Check every 60 seconds

while True:
    if baseline_dir.exists():
        files = list(baseline_dir.glob("*"))
        if files:
            latest_file = max(files, key=lambda p: p.stat().st_mtime)
            age_seconds = time.time() - latest_file.stat().st_mtime
            
            print(f"\n[{time.strftime('%H:%M:%S')}] Baseline directory exists")
            print(f"  Latest file: {latest_file.name}")
            print(f"  Last modified: {age_seconds:.0f} seconds ago")
            
            # Check if kpis.json exists (indicates completion)
            if (baseline_dir / "kpis.json").exists():
                print("\n[SUCCESS] Baseline backtest completed!")
                print("  Run: python compare_baseline_vs_enhanced.py")
                break
        else:
            print(f"\n[{time.strftime('%H:%M:%S')}] Baseline directory exists but empty")
    else:
        print(f"\n[{time.strftime('%H:%M:%S')}] Waiting for baseline directory...")
    
    print(f"  Checking again in {check_interval} seconds...")
    time.sleep(check_interval)
