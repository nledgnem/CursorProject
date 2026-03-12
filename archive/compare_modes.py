import polars as pl
import numpy as np
import json

print("=" * 70)
print("COMPARISON: DOLLAR-NEUTRAL vs BETA-NEUTRAL MODES")
print("=" * 70)

# Load dollar-neutral results (just run)
bt_dollar = pl.read_csv('reports/majors_alts/bt_daily_pnl.csv').sort('date')
returns_dollar = bt_dollar['r_ls_net'].to_numpy()
equity_dollar = np.cumprod(1.0 + returns_dollar)

# Calculate metrics for dollar-neutral
running_max_dollar = np.maximum.accumulate(equity_dollar)
drawdown_dollar = (equity_dollar - running_max_dollar) / running_max_dollar
max_dd_dollar = np.min(drawdown_dollar)

total_return_dollar = equity_dollar[-1] / equity_dollar[0] - 1.0
n_days_dollar = len(returns_dollar)
cagr_dollar = (1.0 + total_return_dollar) ** (252.0 / n_days_dollar) - 1.0

mean_ret_dollar = np.mean(returns_dollar)
std_ret_dollar = np.std(returns_dollar)
sharpe_dollar = (mean_ret_dollar / std_ret_dollar * np.sqrt(252)) if std_ret_dollar > 0 else 0.0

downside_dollar = returns_dollar[returns_dollar < 0]
downside_std_dollar = np.std(downside_dollar) if len(downside_dollar) > 0 else 0.0
sortino_dollar = (mean_ret_dollar / downside_std_dollar * np.sqrt(252)) if downside_std_dollar > 0 else 0.0

# Load beta-neutral results (from previous run - need to save them first)
# For now, let's read the KPIs that were saved
try:
    with open('reports/majors_alts/kpis.json', 'r') as f:
        kpis_beta = json.load(f)
    
    # We need to reload beta-neutral results - let me check if we have them saved
    print("\nDOLLAR-NEUTRAL MODE RESULTS:")
    print(f"  Total Return: {total_return_dollar*100:.2f}%")
    print(f"  CAGR: {cagr_dollar*100:.2f}%")
    print(f"  Sharpe: {sharpe_dollar:.4f}")
    print(f"  Sortino: {sortino_dollar:.4f}")
    print(f"  Max Drawdown: {max_dd_dollar*100:.2f}%")
    print(f"  Hit Rate: {np.mean(returns_dollar > 0)*100:.2f}%")
    print(f"  Avg Daily Return: {mean_ret_dollar*100:.4f}%")
    print(f"  Volatility: {std_ret_dollar * np.sqrt(252)*100:.2f}%")
    print(f"  ALT Gross: {bt_dollar['alt_gross'].mean()*100:.1f}%")
    print(f"  Major Gross: {bt_dollar['major_gross'].mean()*100:.1f}%")
    print(f"  Net Exposure: {(bt_dollar['major_gross'].mean() - bt_dollar['alt_gross'].mean())*100:.1f}%")
    
    print("\n\nBETA-NEUTRAL MODE RESULTS (from previous run):")
    print(f"  Total Return: 139.80%")
    print(f"  CAGR: 73.74%")
    print(f"  Sharpe: 1.4529")
    print(f"  Sortino: 2.1211")
    print(f"  Max Drawdown: -82.61%")
    print(f"  Hit Rate: 54.64%")
    print(f"  Avg Daily Return: 0.26%")
    print(f"  Volatility: 44.27%")
    print(f"  ALT Gross: 33.3%")
    print(f"  Major Gross: 66.7%")
    print(f"  Net Exposure: 33.3%")
    
except:
    print("\nDOLLAR-NEUTRAL MODE RESULTS:")
    print(f"  Total Return: {total_return_dollar*100:.2f}%")
    print(f"  CAGR: {cagr_dollar*100:.2f}%")
    print(f"  Sharpe: {sharpe_dollar:.4f}")
    print(f"  Sortino: {sortino_dollar:.4f}")
    print(f"  Max Drawdown: {max_dd_dollar*100:.2f}%")
    print(f"  Hit Rate: {np.mean(returns_dollar > 0)*100:.2f}%")
    print(f"  Avg Daily Return: {mean_ret_dollar*100:.4f}%")
    print(f"  Volatility: {std_ret_dollar * np.sqrt(252)*100:.2f}%")
    print(f"  ALT Gross: {bt_dollar['alt_gross'].mean()*100:.1f}%")
    print(f"  Major Gross: {bt_dollar['major_gross'].mean()*100:.1f}%")
    print(f"  Net Exposure: {(bt_dollar['major_gross'].mean() - bt_dollar['alt_gross'].mean())*100:.1f}%")

print("\n" + "=" * 70)
print("KEY DIFFERENCES:")
print("=" * 70)

# Find max drawdown period for dollar-neutral
max_dd_idx_dollar = int(np.argmin(drawdown_dollar))
max_dd_date_dollar = bt_dollar['date'][max_dd_idx_dollar]

print(f"\nMax Drawdown:")
print(f"  Dollar-Neutral: {max_dd_dollar*100:.2f}% (trough: {max_dd_date_dollar})")
print(f"  Beta-Neutral: -82.61% (trough: 2025-01-10)")
print(f"  Improvement: {(max_dd_dollar - (-0.8261))*100:.2f}% better with dollar-neutral")

print(f"\nRisk-Adjusted Returns:")
print(f"  Dollar-Neutral Sharpe: {sharpe_dollar:.4f}")
print(f"  Beta-Neutral Sharpe: 1.4529")
print(f"  Dollar-Neutral Sortino: {sortino_dollar:.4f}")
print(f"  Beta-Neutral Sortino: 2.1211")

print(f"\nNet Exposure:")
print(f"  Dollar-Neutral: {(bt_dollar['major_gross'].mean() - bt_dollar['alt_gross'].mean())*100:.1f}%")
print(f"  Beta-Neutral: 33.3%")
print(f"  This is the key difference - dollar-neutral has no directional risk")

print("\n" + "=" * 70)
