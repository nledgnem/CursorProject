"""Test different combinations of risk management methods."""

import subprocess
import json
import polars as pl
import numpy as np
from pathlib import Path
import yaml

def run_backtest(config_path):
    """Run backtest and return results."""
    result = subprocess.run(
        ["python", "-m", "majors_alts_monitor.run", 
         "--start", "2024-01-01", "--end", "2025-12-31",
         "--config", config_path],
        capture_output=True,
        text=True
    )
    return result.returncode == 0

def load_results():
    """Load backtest results."""
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

def test_config(name, config_modifier):
    """Test a specific configuration."""
    print(f"\n{'='*80}")
    print(f"Testing: {name}")
    print(f"{'='*80}")
    
    # Load base config
    config_path = Path("majors_alts_monitor/config.yaml")
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Apply modifications
    config_modifier(config)
    
    # Write temporary config
    temp_config_path = Path(f"majors_alts_monitor/config_{name.lower().replace(' ', '_')}.yaml")
    with open(temp_config_path, 'w') as f:
        yaml.dump(config, f)
    
    # Run backtest
    success = run_backtest(str(temp_config_path))
    if not success:
        print(f"  ERROR: Backtest failed")
        return None
    
    # Load results
    results = load_results()
    print(f"  Total Return: {results['total_return']*100:.2f}%")
    print(f"  CAGR: {results['cagr']*100:.2f}%")
    print(f"  Sharpe: {results['sharpe']:.4f}")
    print(f"  Sortino: {results['sortino']:.4f}")
    print(f"  Max Drawdown: {results['max_drawdown']*100:.2f}%")
    print(f"  Volatility: {results['volatility']*100:.2f}%")
    
    # Cleanup
    temp_config_path.unlink()
    
    return results

# Test different combinations
results_dict = {}

# 1. Baseline (all risk management enabled)
results_dict["All Risk Management"] = test_config(
    "All Risk Management",
    lambda c: None  # Use current config
)

# 2. No risk management
results_dict["No Risk Management"] = test_config(
    "No Risk Management",
    lambda c: c["backtest"]["risk_management"].update({
        "stop_loss": {"enabled": False},
        "volatility_targeting": {"enabled": False},
        "trailing_stop": {"enabled": False},
    })
)

# 3. Stop-loss only
results_dict["Stop-Loss Only"] = test_config(
    "Stop-Loss Only",
    lambda c: c["backtest"]["risk_management"].update({
        "stop_loss": {"enabled": True, "daily_loss_threshold": -0.05},
        "volatility_targeting": {"enabled": False},
        "trailing_stop": {"enabled": False},
    })
)

# 4. Volatility targeting only
results_dict["Volatility Targeting Only"] = test_config(
    "Volatility Targeting Only",
    lambda c: c["backtest"]["risk_management"].update({
        "stop_loss": {"enabled": False},
        "volatility_targeting": {"enabled": True, "target_volatility": 0.20},
        "trailing_stop": {"enabled": False},
    })
)

# 5. Trailing stop only
results_dict["Trailing Stop Only"] = test_config(
    "Trailing Stop Only",
    lambda c: c["backtest"]["risk_management"].update({
        "stop_loss": {"enabled": False},
        "volatility_targeting": {"enabled": False},
        "trailing_stop": {"enabled": True, "drawdown_threshold": -0.15},
    })
)

# 6. Stop-loss + Trailing stop
results_dict["Stop-Loss + Trailing Stop"] = test_config(
    "Stop-Loss + Trailing Stop",
    lambda c: c["backtest"]["risk_management"].update({
        "stop_loss": {"enabled": True, "daily_loss_threshold": -0.05},
        "volatility_targeting": {"enabled": False},
        "trailing_stop": {"enabled": True, "drawdown_threshold": -0.15},
    })
)

# Print summary
print(f"\n{'='*80}")
print("SUMMARY COMPARISON")
print(f"{'='*80}")
print(f"{'Method':<30} {'Sharpe':<10} {'Sortino':<10} {'Max DD':<12} {'CAGR':<10}")
print(f"{'-'*80}")

for name, res in results_dict.items():
    if res:
        print(f"{name:<30} {res['sharpe']:<10.4f} {res['sortino']:<10.4f} {res['max_drawdown']*100:<12.2f}% {res['cagr']*100:<10.2f}%")

# Find best
if results_dict:
    best_sharpe = max((r for r in results_dict.values() if r), key=lambda x: x['sharpe'])
    best_dd = min((r for r in results_dict.values() if r), key=lambda x: x['max_drawdown'])
    
    print(f"\nBest Sharpe: {best_sharpe['sharpe']:.4f}")
    print(f"Best Max Drawdown: {best_dd['max_drawdown']*100:.2f}%")
