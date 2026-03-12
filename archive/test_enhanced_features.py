"""Test if adding funding heating and OI risk features improves performance."""

import subprocess
import json
import polars as pl
import numpy as np
from pathlib import Path
import yaml

def run_backtest(config_path):
    """Run backtest and return success status."""
    result = subprocess.run(
        ["python", "-m", "majors_alts_monitor.run", 
         "--start", "2024-01-01", "--end", "2025-12-31",
         "--config", config_path],
        capture_output=True,
        text=True
    )
    return result.returncode == 0

def load_results():
    """Load backtest results and compute metrics."""
    bt = pl.read_csv('reports/majors_alts/bt_daily_pnl.csv').sort('date')
    returns = bt['r_ls_net'].to_numpy()
    equity = np.cumprod(1.0 + returns)
    
    running_max = np.maximum.accumulate(equity)
    drawdown = (equity - running_max) / running_max
    
    total_return = equity[-1] / equity[0] - 1.0
    n_days = len(returns)
    cagr = (1.0 + total_return) ** (252.0 / n_days) - 1.0
    
    mean_ret = np.mean(returns)
    std_ret = np.std(returns)
    sharpe = (mean_ret / std_ret * np.sqrt(252)) if std_ret > 0 else 0.0
    
    downside = returns[returns < 0]
    downside_std = np.std(downside) if len(downside) > 0 else 0.0
    sortino = (mean_ret / downside_std * np.sqrt(252)) if downside_std > 0 else 0.0
    
    return {
        "total_return": total_return,
        "cagr": cagr,
        "sharpe": sharpe,
        "sortino": sortino,
        "max_drawdown": np.min(drawdown),
        "hit_rate": np.mean(returns > 0),
        "volatility": std_ret * np.sqrt(252),
        "n_days": n_days,
    }

print("=" * 80)
print("TESTING ENHANCED FEATURES (Funding Heating + OI Risk)")
print("=" * 80)

# Load base config
config_path = Path("majors_alts_monitor/config.yaml")
with open(config_path, 'r') as f:
    config = yaml.safe_load(f)

# Test 1: Current (baseline)
print("\n1. Running baseline (current features)...")
results_baseline = load_results() if Path('reports/majors_alts/bt_daily_pnl.csv').exists() else None
if results_baseline:
    print(f"  Sharpe: {results_baseline['sharpe']:.4f}")
    print(f"  CAGR: {results_baseline['cagr']*100:.2f}%")
    print(f"  Max DD: {results_baseline['max_drawdown']*100:.2f}%")
else:
    print("  Running backtest...")
    success = run_backtest(str(config_path))
    if success:
        results_baseline = load_results()
        print(f"  Sharpe: {results_baseline['sharpe']:.4f}")
        print(f"  CAGR: {results_baseline['cagr']*100:.2f}%")
        print(f"  Max DD: {results_baseline['max_drawdown']*100:.2f}%")
    else:
        print("  ERROR: Backtest failed")
        results_baseline = None

# Test 2: With enhanced features (already updated in config)
print("\n2. Running with enhanced features (funding heating + OI risk)...")
# Config already updated, just run
success = run_backtest(str(config_path))
if success:
    results_enhanced = load_results()
    print(f"  Sharpe: {results_enhanced['sharpe']:.4f}")
    print(f"  CAGR: {results_enhanced['cagr']*100:.2f}%")
    print(f"  Max DD: {results_enhanced['max_drawdown']*100:.2f}%")
    
    # Compare
    if results_baseline:
        print("\n" + "=" * 80)
        print("COMPARISON")
        print("=" * 80)
        print(f"{'Metric':<20} {'Baseline':<20} {'Enhanced':<20} {'Change':<20}")
        print("-" * 80)
        
        for key in ["sharpe", "cagr", "max_drawdown", "sortino", "volatility", "hit_rate"]:
            baseline = results_baseline.get(key, 0.0)
            enhanced = results_enhanced.get(key, 0.0)
            diff = enhanced - baseline
            diff_pct = (diff / abs(baseline) * 100) if baseline != 0 else 0.0
            
            if key in ["sharpe", "sortino", "hit_rate"]:
                print(f"{key:<20} {baseline:<20.4f} {enhanced:<20.4f} {diff:+.4f} ({diff_pct:+.1f}%)")
            elif key == "max_drawdown":
                print(f"{key:<20} {baseline*100:<20.2f}% {enhanced*100:<20.2f}% {diff*100:+.2f}% ({diff_pct:+.1f}%)")
            else:
                print(f"{key:<20} {baseline*100:<20.2f}% {enhanced*100:<20.2f}% {diff*100:+.2f}% ({diff_pct:+.1f}%)")
        
        if results_enhanced.get("sharpe", 0.0) > results_baseline.get("sharpe", 0.0):
            print("\n>>> ENHANCED FEATURES IMPROVED PERFORMANCE <<<")
        else:
            print("\n>>> ENHANCED FEATURES DID NOT IMPROVE PERFORMANCE <<<")
else:
    print("  ERROR: Backtest failed")
