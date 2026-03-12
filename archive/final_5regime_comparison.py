import polars as pl
import numpy as np

print("=" * 80)
print("FINAL COMPARISON: 3 Regimes vs 5 Regimes (Trading in STRONG + WEAK)")
print("=" * 80)

# Current results (5 regimes - STRONG + WEAK RISK_ON_MAJORS)
bt_5reg = pl.read_csv('reports/majors_alts/bt_daily_pnl.csv').sort('date')
returns_5reg = bt_5reg['r_ls_net'].to_numpy()
equity_5reg = np.cumprod(1.0 + returns_5reg)

running_max_5reg = np.maximum.accumulate(equity_5reg)
drawdown_5reg = (equity_5reg - running_max_5reg) / running_max_5reg

total_return_5reg = equity_5reg[-1] / equity_5reg[0] - 1.0
n_days_5reg = len(returns_5reg)
cagr_5reg = (1.0 + total_return_5reg) ** (252.0 / n_days_5reg) - 1.0

mean_ret_5reg = np.mean(returns_5reg)
std_ret_5reg = np.std(returns_5reg)
sharpe_5reg = (mean_ret_5reg / std_ret_5reg * np.sqrt(252)) if std_ret_5reg > 0 else 0.0

downside_5reg = returns_5reg[returns_5reg < 0]
downside_std_5reg = np.std(downside_5reg) if len(downside_5reg) > 0 else 0.0
sortino_5reg = (mean_ret_5reg / downside_std_5reg * np.sqrt(252)) if downside_std_5reg > 0 else 0.0

print("\n5 REGIMES (STRONG + WEAK RISK_ON_MAJORS, exit on BALANCED):")
print(f"  Trading Days: {len(bt_5reg)}")
print(f"  Total Return: {total_return_5reg*100:.2f}%")
print(f"  CAGR: {cagr_5reg*100:.2f}%")
print(f"  Sharpe: {sharpe_5reg:.4f}")
print(f"  Sortino: {sortino_5reg:.4f}")
print(f"  Max Drawdown: {np.min(drawdown_5reg)*100:.2f}%")
print(f"  Hit Rate: {np.mean(returns_5reg > 0)*100:.2f}%")
print(f"  Avg Daily Return: {mean_ret_5reg*100:.4f}%")
print(f"  Volatility: {std_ret_5reg*np.sqrt(252)*100:.2f}%")

# Check regime distribution
regime_counts = bt_5reg.group_by('regime').agg(pl.len().alias('count'))
print(f"\n  Regime Distribution:")
for row in regime_counts.iter_rows(named=True):
    print(f"    {row['regime']}: {row['count']} days ({row['count']/len(bt_5reg)*100:.1f}%)")

# Days with actual positions
days_with_positions = bt_5reg.filter(pl.col('total_gross') > 0.01)
print(f"\n  Days with positions (gross > 1%): {len(days_with_positions)}")
if len(days_with_positions) > 0:
    regime_positions = days_with_positions.group_by('regime').agg(pl.len().alias('count'))
    print("    Regime distribution on days with positions:")
    for row in regime_positions.iter_rows(named=True):
        print(f"      {row['regime']}: {row['count']} days")
    
    # Check average returns by regime
    print("\n    Average returns by regime (on days with positions):")
    for regime in ['STRONG_RISK_ON_MAJORS', 'WEAK_RISK_ON_MAJORS', 'RISK_ON_MAJORS']:
        regime_days = days_with_positions.filter(pl.col('regime') == regime)
        if len(regime_days) > 0:
            avg_ret = regime_days['r_ls_net'].mean()
            print(f"      {regime}: {avg_ret*100:.4f}% ({len(regime_days)} days)")

print("\n\n3 REGIMES (Only RISK_ON_MAJORS) - Previous Results:")
print(f"  Trading Days: 434")
print(f"  Total Return: 123.44%")
print(f"  CAGR: 59.49%")
print(f"  Sharpe: 1.3031")
print(f"  Sortino: 1.9419")
print(f"  Max Drawdown: -89.16%")
print(f"  Hit Rate: 49.31%")
print(f"  Avg Daily Return: 0.2222%")
print(f"  Volatility: 42.97%")
print(f"  Days with positions: ~39")

print("\n" + "=" * 80)
print("KEY DIFFERENCES:")
print("=" * 80)

print(f"\n1. TRADING FREQUENCY:")
print(f"   5 Regimes: {len(bt_5reg)} days total, {len(days_with_positions)} with positions")
print(f"   3 Regimes: 434 days total, ~39 with positions")
print(f"   Change: {len(days_with_positions) - 39} more/fewer position days")

print(f"\n2. MAX DRAWDOWN:")
print(f"   5 Regimes: {np.min(drawdown_5reg)*100:.2f}%")
print(f"   3 Regimes: -89.16%")
improvement = (np.min(drawdown_5reg) - (-0.8916)) * 100
print(f"   Improvement: {improvement:.2f}% {'BETTER' if improvement > 0 else 'WORSE'}")

print(f"\n3. RETURNS:")
print(f"   5 Regimes: {total_return_5reg*100:.2f}% total, {cagr_5reg*100:.2f}% CAGR")
print(f"   3 Regimes: 123.44% total, 59.49% CAGR")
print(f"   Trade-off: {'Higher' if total_return_5reg > 1.2344 else 'Lower'} returns")

print(f"\n4. RISK-ADJUSTED:")
print(f"   5 Regimes Sharpe: {sharpe_5reg:.4f}")
print(f"   3 Regimes Sharpe: 1.3031")
print(f"   5 Regimes Sortino: {sortino_5reg:.4f}")
print(f"   3 Regimes Sortino: 1.9419")

print("\n" + "=" * 80)
print("CONCLUSION:")
print("=" * 80)
if improvement > 0:
    print(f"5 regimes with earlier exits (BALANCED) REDUCES drawdown by {improvement:.2f}%")
    print("by exiting positions earlier when regime weakens.")
else:
    print("5 regimes does NOT reduce drawdown significantly.")
    print("The earlier exit signal (BALANCED) helps, but losses still occur on exit days.")
print("=" * 80)
