"""Compare different risk management methods."""

import polars as pl
import numpy as np

print("=" * 80)
print("RISK MANAGEMENT METHODS COMPARISON")
print("=" * 80)

# Load current results (all risk management enabled)
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

print("\nCURRENT RESULTS (All Risk Management Enabled):")
print(f"  Total Return: {total_return*100:.2f}%")
print(f"  CAGR: {cagr*100:.2f}%")
print(f"  Sharpe: {sharpe:.4f}")
print(f"  Sortino: {sortino:.4f}")
print(f"  Max Drawdown: {np.min(drawdown)*100:.2f}%")
print(f"  Volatility: {std_ret*np.sqrt(252)*100:.2f}%")
print(f"  Hit Rate: {np.mean(returns > 0)*100:.2f}%")

# Based on test results, show comparison
print("\n" + "=" * 80)
print("COMPARISON (from test_risk_management.py results):")
print("=" * 80)
print(f"{'Method':<35} {'Sharpe':<10} {'Sortino':<10} {'Max DD':<12} {'CAGR':<10}")
print("-" * 80)
print(f"{'All Risk Management':<35} {0.9928:<10.4f} {1.4381:<10.4f} {-91.61:<12.2f}% {39.53:<10.2f}%")
print(f"{'No Risk Management':<35} {1.0695:<10.4f} {1.5597:<10.4f} {-90.99:<12.2f}% {44.36:<10.2f}%")
print(f"{'Stop-Loss Only':<35} {1.1405:<10.4f} {1.6705:<10.4f} {-90.88:<12.2f}% {48.78:<10.2f}%")
print(f"{'Volatility Targeting Only':<35} {1.0777:<10.4f} {1.5530:<10.4f} {-90.95:<12.2f}% {44.87:<10.2f}%")
print(f"{'Trailing Stop Only':<35} {0.9688:<10.4f} {1.4025:<10.4f} {-91.63:<12.2f}% {38.12:<10.2f}%")

print("\n" + "=" * 80)
print("KEY FINDINGS:")
print("=" * 80)
print("1. STOP-LOSS ONLY performs BEST:")
print("   - Highest Sharpe: 1.14")
print("   - Highest Sortino: 1.67")
print("   - Highest CAGR: 48.78%")
print("   - Slightly better drawdown: -90.88%")
print("\n2. ALL RISK MANAGEMENT together is WORSE:")
print("   - Lower Sharpe than stop-loss alone")
print("   - Lower returns")
print("   - Risk management methods may conflict")
print("\n3. DRAWDOWN remains very high across all methods:")
print("   - All methods show ~-91% max drawdown")
print("   - This suggests the drawdown is inherent to the strategy")
print("   - Risk management helps returns/Sharpe but not drawdown")

print("\n" + "=" * 80)
print("RECOMMENDATION:")
print("=" * 80)
print("Use STOP-LOSS ONLY (daily loss threshold: -5%)")
print("This provides the best risk-adjusted returns without over-constraining the strategy.")
print("=" * 80)
