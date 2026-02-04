import polars as pl
import numpy as np

print("=" * 80)
print("COMPARISON: Less Selective vs More Selective Regime Gating")
print("=" * 80)

# Current results (more selective - only RISK_ON_MAJORS)
bt_selective = pl.read_csv('reports/majors_alts/bt_daily_pnl.csv').sort('date')
returns_selective = bt_selective['r_ls_net'].to_numpy()
equity_selective = np.cumprod(1.0 + returns_selective)

running_max_selective = np.maximum.accumulate(equity_selective)
drawdown_selective = (equity_selective - running_max_selective) / running_max_selective

total_return_selective = equity_selective[-1] / equity_selective[0] - 1.0
n_days_selective = len(returns_selective)
cagr_selective = (1.0 + total_return_selective) ** (252.0 / n_days_selective) - 1.0

mean_ret_selective = np.mean(returns_selective)
std_ret_selective = np.std(returns_selective)
sharpe_selective = (mean_ret_selective / std_ret_selective * np.sqrt(252)) if std_ret_selective > 0 else 0.0

downside_selective = returns_selective[returns_selective < 0]
downside_std_selective = np.std(downside_selective) if len(downside_selective) > 0 else 0.0
sortino_selective = (mean_ret_selective / downside_std_selective * np.sqrt(252)) if downside_std_selective > 0 else 0.0

print("\nMORE SELECTIVE (Only RISK_ON_MAJORS):")
print(f"  Trading Days: {len(bt_selective)}")
print(f"  Total Return: {total_return_selective*100:.2f}%")
print(f"  CAGR: {cagr_selective*100:.2f}%")
print(f"  Sharpe: {sharpe_selective:.4f}")
print(f"  Sortino: {sortino_selective:.4f}")
print(f"  Max Drawdown: {np.min(drawdown_selective)*100:.2f}%")
print(f"  Hit Rate: {np.mean(returns_selective > 0)*100:.2f}%")
print(f"  Avg Daily Return: {mean_ret_selective*100:.4f}%")
print(f"  Volatility: {std_ret_selective*np.sqrt(252)*100:.2f}%")

# Check regime distribution
regime_counts = bt_selective.group_by('regime').agg(pl.len().alias('count'))
print(f"\n  Regime Distribution:")
for row in regime_counts.iter_rows(named=True):
    print(f"    {row['regime']}: {row['count']} days ({row['count']/len(bt_selective)*100:.1f}%)")

print("\n\nLESS SELECTIVE (RISK_ON_MAJORS + BALANCED) - Previous Results:")
print(f"  Trading Days: ~403")
print(f"  Total Return: 139.80%")
print(f"  CAGR: 73.74%")
print(f"  Sharpe: 1.4529")
print(f"  Sortino: 2.1211")
print(f"  Max Drawdown: -82.61%")
print(f"  Hit Rate: 54.64%")
print(f"  Avg Daily Return: 0.26%")
print(f"  Volatility: 44.27%")

print("\n" + "=" * 80)
print("KEY DIFFERENCES:")
print("=" * 80)

print(f"\n1. TRADING FREQUENCY:")
print(f"   More Selective: {len(bt_selective)} days")
print(f"   Less Selective: ~403 days")
print(f"   Reduction: {((403 - len(bt_selective)) / 403 * 100):.1f}% fewer trading days")

print(f"\n2. MAX DRAWDOWN:")
print(f"   More Selective: {np.min(drawdown_selective)*100:.2f}%")
print(f"   Less Selective: -82.61%")
improvement = (np.min(drawdown_selective) - (-0.8261)) * 100
print(f"   Improvement: {improvement:.2f}% {'BETTER' if improvement > 0 else 'WORSE'}")

print(f"\n3. RETURNS:")
print(f"   More Selective: {total_return_selective*100:.2f}% total, {cagr_selective*100:.2f}% CAGR")
print(f"   Less Selective: 139.80% total, 73.74% CAGR")
print(f"   Trade-off: {'Higher' if total_return_selective > 1.398 else 'Lower'} returns but {'better' if improvement > 0 else 'worse'} drawdown")

print(f"\n4. RISK-ADJUSTED:")
print(f"   More Selective Sharpe: {sharpe_selective:.4f}")
print(f"   Less Selective Sharpe: 1.4529")
print(f"   More Selective Sortino: {sortino_selective:.4f}")
print(f"   Less Selective Sortino: 2.1211")

print("\n" + "=" * 80)
print("CONCLUSION:")
print("=" * 80)
if improvement > 0:
    print("Being more selective (only RISK_ON_MAJORS) REDUCES drawdown")
    print(f"by {improvement:.2f}%, but may reduce returns and trading frequency.")
else:
    print("Being more selective does NOT reduce drawdown significantly.")
    print("The drawdown is inherent to the strategy (short ALTs).")
print("=" * 80)
