"""Test different ALT basket sizes to see if larger universe helps."""

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

def test_basket_size(basket_size):
    """Test a specific basket size."""
    print(f"\n{'='*80}")
    print(f"Testing Basket Size: {basket_size} ALTs")
    print(f"{'='*80}")
    
    # Load base config
    config_path = Path("majors_alts_monitor/config.yaml")
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Modify basket size
    config["universe"]["basket_size"] = basket_size
    
    # Write temporary config
    temp_config_path = Path(f"majors_alts_monitor/config_basket_{basket_size}.yaml")
    with open(temp_config_path, 'w') as f:
        yaml.dump(config, f)
    
    # Run backtest
    success = run_backtest(str(temp_config_path))
    if not success:
        print(f"  ERROR: Backtest failed")
        temp_config_path.unlink()
        return None
    
    # Load results
    results = load_results()
    print(f"  Total Return: {results['total_return']*100:.2f}%")
    print(f"  CAGR: {results['cagr']*100:.2f}%")
    print(f"  Sharpe: {results['sharpe']:.4f}")
    print(f"  Sortino: {results['sortino']:.4f}")
    print(f"  Max Drawdown: {results['max_drawdown']*100:.2f}%")
    print(f"  Volatility: {results['volatility']*100:.2f}%")
    print(f"  Hit Rate: {results['hit_rate']*100:.2f}%")
    
    # Cleanup
    temp_config_path.unlink()
    
    return results

# Test different basket sizes
basket_sizes = [20, 50, 100, 150, 200, 300]
results_dict = {}

print("=" * 80)
print("TESTING DIFFERENT ALT BASKET SIZES")
print("=" * 80)
print("Testing: 20, 50, 100, 150, 200, 300 ALTs")
print("=" * 80)

for size in basket_sizes:
    results_dict[size] = test_basket_size(size)

# Print summary
print(f"\n{'='*80}")
print("SUMMARY COMPARISON")
print(f"{'='*80}")
print(f"{'Basket Size':<15} {'Sharpe':<10} {'Sortino':<10} {'Max DD':<12} {'CAGR':<10} {'Vol':<10}")
print(f"{'-'*80}")

for size in basket_sizes:
    res = results_dict.get(size)
    if res:
        print(f"{size:<15} {res['sharpe']:<10.4f} {res['sortino']:<10.4f} {res['max_drawdown']*100:<12.2f}% {res['cagr']*100:<10.2f}% {res['volatility']*100:<10.2f}%")

# Find best
if results_dict:
    best_sharpe = max((r for r in results_dict.values() if r), key=lambda x: x['sharpe'])
    best_dd = min((r for r in results_dict.values() if r), key=lambda x: x['max_drawdown'])
    best_cagr = max((r for r in results_dict.values() if r), key=lambda x: x['cagr'])
    
    print(f"\nBest Sharpe: {best_sharpe['sharpe']:.4f}")
    print(f"Best Max Drawdown: {best_dd['max_drawdown']*100:.2f}%")
    print(f"Best CAGR: {best_cagr['cagr']*100:.2f}%")
    
    # Find which basket size had best Sharpe
    for size, res in results_dict.items():
        if res and res['sharpe'] == best_sharpe['sharpe']:
            print(f"\nOPTIMAL BASKET SIZE: {size} ALTs (best Sharpe)")
