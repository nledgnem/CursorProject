#!/usr/bin/env python3
"""Compare backtest results with/without OI and funding features."""

import subprocess
import json
import pandas as pd
from pathlib import Path
from datetime import datetime

print("=" * 70)
print("FEATURE IMPACT COMPARISON")
print("=" * 70)

# Test configurations
configs = [
    {
        "name": "baseline",
        "description": "No OI, no funding",
        "disable_oi": True,
        "disable_funding": True,
    },
    {
        "name": "with_funding",
        "description": "Funding only (no OI)",
        "disable_oi": True,
        "disable_funding": False,
    },
    {
        "name": "with_oi",
        "description": "OI only (no funding)",
        "disable_oi": False,
        "disable_funding": True,
    },
    {
        "name": "with_both",
        "description": "Both OI and funding",
        "disable_oi": False,
        "disable_funding": False,
    },
]

results = []

for config in configs:
    print(f"\n{'='*70}")
    print(f"Running: {config['name']} - {config['description']}")
    print(f"{'='*70}")
    
    # Modify config temporarily
    import yaml
    config_path = Path("majors_alts_monitor/config.yaml")
    with open(config_path) as f:
        yaml_config = yaml.safe_load(f)
    
    # Adjust weights based on what's disabled
    weights = yaml_config["regime"]["composite"]["default_weights"].copy()
    
    if config["disable_funding"]:
        # Remove funding features, redistribute weights
        funding_weight = weights.get("funding_skew", 0) + weights.get("funding_heating", 0)
        del weights["funding_skew"]
        del weights["funding_heating"]
        # Redistribute proportionally
        total_remaining = sum(weights.values())
        for k in weights:
            weights[k] = weights[k] / total_remaining
    
    if config["disable_oi"]:
        # Remove OI, redistribute weights
        oi_weight = weights.get("oi_risk", 0)
        del weights["oi_risk"]
        # Redistribute proportionally
        total_remaining = sum(weights.values())
        for k in weights:
            weights[k] = weights[k] / total_remaining
    
    yaml_config["regime"]["composite"]["default_weights"] = weights
    
    # Save temp config
    temp_config_path = Path(f"majors_alts_monitor/config_{config['name']}.yaml")
    with open(temp_config_path, "w") as f:
        yaml.dump(yaml_config, f)
    
    # Run backtest
    try:
        result = subprocess.run(
            [
                "python", "-m", "majors_alts_monitor.run",
                "--start", "2024-01-01",
                "--end", "2025-12-31",
                "--config", str(temp_config_path)
            ],
            capture_output=True,
            text=True,
            timeout=3600  # 1 hour timeout
        )
        
        # Parse results from output (look for summary stats)
        output = result.stdout + result.stderr
        
        # Extract key metrics (this is a simplified version)
        # In practice, you'd parse the actual output files
        print(f"  Completed: {config['name']}")
        
        results.append({
            "config": config["name"],
            "description": config["description"],
            "status": "completed" if result.returncode == 0 else "failed",
            "output": output[-1000:] if len(output) > 1000 else output,  # Last 1000 chars
        })
        
    except subprocess.TimeoutExpired:
        print(f"  Timeout: {config['name']}")
        results.append({
            "config": config["name"],
            "description": config["description"],
            "status": "timeout",
        })
    except Exception as e:
        print(f"  Error: {config['name']} - {e}")
        results.append({
            "config": config["name"],
            "description": config["description"],
            "status": "error",
            "error": str(e),
        })
    
    # Clean up temp config
    if temp_config_path.exists():
        temp_config_path.unlink()

# Summary
print("\n" + "=" * 70)
print("SUMMARY")
print("=" * 70)
for r in results:
    print(f"  {r['config']:20s} - {r['description']:30s} - {r['status']}")

print("\nNote: Check output files in outputs/ directory for detailed results")
print("Look for files with timestamps matching each run")
