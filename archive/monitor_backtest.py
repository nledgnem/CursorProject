#!/usr/bin/env python3
"""Monitor baseline backtest and notify when complete."""

import time
from pathlib import Path
import json
import subprocess

print("=" * 70)
print("MONITORING BASELINE BACKTEST")
print("=" * 70)

baseline_dir = Path("reports/majors_alts_baseline")
check_interval = 60  # Check every 60 seconds
max_wait_hours = 3  # Maximum wait time (3 hours)
start_time = time.time()

print(f"Monitoring baseline backtest completion...")
print(f"  Checking every {check_interval} seconds")
print(f"  Maximum wait: {max_wait_hours} hours")
print(f"  Target file: {baseline_dir / 'kpis.json'}")
print()

iteration = 0

while True:
    iteration += 1
    elapsed = time.time() - start_time
    elapsed_min = elapsed / 60
    
    # Check if completed
    kpis_path = baseline_dir / "kpis.json"
    if kpis_path.exists():
        # Check file age to ensure it's fresh
        file_age = time.time() - kpis_path.stat().st_mtime
        if file_age < 300:  # File modified in last 5 minutes
            print("\n" + "=" * 70)
            print("[SUCCESS] Baseline backtest completed!")
            print("=" * 70)
            
            # Load and display results
            try:
                with open(kpis_path) as f:
                    kpis = json.load(f)
                print("\nBaseline Results:")
                print(f"  CAGR: {kpis.get('cagr', 0)*100:.2f}%")
                print(f"  Sharpe: {kpis.get('sharpe', 0):.3f}")
                print(f"  Sortino: {kpis.get('sortino', 0):.3f}")
                print(f"  Max DD: {kpis.get('max_drawdown', 0)*100:.2f}%")
            except Exception as e:
                print(f"  Could not load KPIs: {e}")
            
            # Run comparison
            print("\n" + "=" * 70)
            print("Running comparison...")
            print("=" * 70)
            try:
                result = subprocess.run(
                    ["python", "compare_baseline_vs_enhanced.py"],
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                print(result.stdout)
                if result.stderr:
                    print("Errors:", result.stderr)
            except Exception as e:
                print(f"Comparison script error: {e}")
            
            print("\n" + "=" * 70)
            print("MONITORING COMPLETE")
            print("=" * 70)
            break
    
    # Check timeout
    if elapsed > max_wait_hours * 3600:
        print(f"\n[TIMEOUT] Maximum wait time ({max_wait_hours} hours) exceeded")
        print("  Baseline backtest may still be running or may have failed")
        break
    
    # Status update
    if iteration % 5 == 0:  # Every 5 minutes
        print(f"[{time.strftime('%H:%M:%S')}] Still waiting... ({elapsed_min:.1f} minutes elapsed)")
        if baseline_dir.exists():
            files = list(baseline_dir.glob("*"))
            if files:
                latest = max(files, key=lambda p: p.stat().st_mtime)
                age = (time.time() - latest.stat().st_mtime) / 60
                print(f"  Latest file: {latest.name} ({age:.1f} minutes ago)")
    
    time.sleep(check_interval)
