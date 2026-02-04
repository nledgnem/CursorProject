import polars as pl
import numpy as np

print("=" * 80)
print("COMPARISON: Before vs After Improvements")
print("=" * 80)

# Current results (with take-profit + enhanced ALT selection)
bt_new = pl.read_csv('reports/majors_alts/bt_daily_pnl.csv').sort('date')
returns_new = bt_new['r_ls_net'].to_numpy()
equity_new = np.cumprod(1.0 + returns_new)

running_max_new = np.maximum.accumulate(equity_new)
drawdown_new = (equity_new - running_max_new) / running_max_new

total_return_new = equity_new[-1] / equity_new[0] - 1.0
n_days_new = len(returns_new)
cagr_new = (1.0 + total_return_new) ** (252.0 / n_days_new) - 1.0

mean_ret_new = np.mean(returns_new)
std_ret_new = np.std(returns_new)
sharpe_new = (mean_ret_new / std_ret_new * np.sqrt(252)) if std_ret_new > 0 else 0.0

downside_new = returns_new[returns_new < 0]
downside_std_new = np.std(downside_new) if len(downside_new) > 0 else 0.0
sortino_new = (mean_ret_new / downside_std_new * np.sqrt(252)) if downside_std_new > 0 else 0.0

print("\nWITH IMPROVEMENTS (Take-Profit + Enhanced ALT Selection):")
print(f"  Trading Days: {len(bt_new)}")
print(f"  Total Return: {total_return_new*100:.2f}%")
print(f"  CAGR: {cagr_new*100:.2f}%")
print(f"  Sharpe: {sharpe_new:.4f}")
print(f"  Sortino: {sortino_new:.4f}")
print(f"  Max Drawdown: {np.min(drawdown_new)*100:.2f}%")
print(f"  Hit Rate: {np.mean(returns_new > 0)*100:.2f}%")
print(f"  Avg Daily Return: {mean_ret_new*100:.4f}%")
print(f"  Volatility: {std_ret_new*np.sqrt(252)*100:.2f}%")

print("\n\nBEFORE IMPROVEMENTS (Stop-Loss Only):")
print(f"  Trading Days: 434")
print(f"  Total Return: 85.77%")
print(f"  CAGR: 43.28%")
print(f"  Sharpe: 1.0501")
print(f"  Sortino: 1.5205")
print(f"  Max Drawdown: -91.32%")
print(f"  Hit Rate: 49.77%")
print(f"  Avg Daily Return: 0.1801%")
print(f"  Volatility: 43.22%")

print("\n" + "=" * 80)
print("KEY DIFFERENCES:")
print("=" * 80)

print(f"\n1. SHARPE RATIO:")
print(f"   With Improvements: {sharpe_new:.4f}")
print(f"   Before: 1.0501")
improvement_sharpe = (sharpe_new - 1.0501) / 1.0501 * 100
print(f"   Change: {improvement_sharpe:+.2f}%")

print(f"\n2. MAX DRAWDOWN:")
print(f"   With Improvements: {np.min(drawdown_new)*100:.2f}%")
print(f"   Before: -91.32%")
improvement_dd = (np.min(drawdown_new) - (-0.9132)) * 100
print(f"   Improvement: {improvement_dd:+.2f}% {'BETTER' if improvement_dd > 0 else 'WORSE'}")

print(f"\n3. RETURNS:")
print(f"   With Improvements: {total_return_new*100:.2f}% total, {cagr_new*100:.2f}% CAGR")
print(f"   Before: 85.77% total, 43.28% CAGR")
improvement_return = (total_return_new - 0.8577) / 0.8577 * 100
print(f"   Change: {improvement_return:+.2f}%")

print(f"\n4. VOLATILITY:")
print(f"   With Improvements: {std_ret_new*np.sqrt(252)*100:.2f}%")
print(f"   Before: 43.22%")
improvement_vol = (std_ret_new*np.sqrt(252) - 0.4322) / 0.4322 * 100
print(f"   Change: {improvement_vol:+.2f}%")

print("\n" + "=" * 80)
print("CONCLUSION:")
print("=" * 80)
if improvement_sharpe > 0 and improvement_dd > 0:
    print("Both improvements HELPED:")
    print(f"  - Sharpe improved by {improvement_sharpe:.2f}%")
    print(f"  - Drawdown improved by {improvement_dd:.2f}%")
    print(f"  - Returns changed by {improvement_return:.2f}%")
elif improvement_sharpe > 0:
    print("Sharpe improved, but drawdown needs more work.")
elif improvement_dd > 0:
    print("Drawdown improved, but Sharpe needs more work.")
else:
    print("Improvements need tuning - may need to adjust parameters.")
print("=" * 80)
