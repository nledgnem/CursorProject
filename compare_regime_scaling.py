import polars as pl
import numpy as np

print("=" * 80)
print("COMPARISON: With vs Without Regime-Based Position Sizing")
print("=" * 80)

# Current results (with regime scaling: STRONG=100%, WEAK=60%)
bt_scaled = pl.read_csv('reports/majors_alts/bt_daily_pnl.csv').sort('date')
returns_scaled = bt_scaled['r_ls_net'].to_numpy()
equity_scaled = np.cumprod(1.0 + returns_scaled)

running_max_scaled = np.maximum.accumulate(equity_scaled)
drawdown_scaled = (equity_scaled - running_max_scaled) / running_max_scaled

total_return_scaled = equity_scaled[-1] / equity_scaled[0] - 1.0
n_days_scaled = len(returns_scaled)
cagr_scaled = (1.0 + total_return_scaled) ** (252.0 / n_days_scaled) - 1.0

mean_ret_scaled = np.mean(returns_scaled)
std_ret_scaled = np.std(returns_scaled)
sharpe_scaled = (mean_ret_scaled / std_ret_scaled * np.sqrt(252)) if std_ret_scaled > 0 else 0.0

downside_scaled = returns_scaled[returns_scaled < 0]
downside_std_scaled = np.std(downside_scaled) if len(downside_scaled) > 0 else 0.0
sortino_scaled = (mean_ret_scaled / downside_std_scaled * np.sqrt(252)) if downside_std_scaled > 0 else 0.0

print("\nWITH REGIME-BASED POSITION SIZING (STRONG=100%, WEAK=60%):")
print(f"  Trading Days: {len(bt_scaled)}")
print(f"  Total Return: {total_return_scaled*100:.2f}%")
print(f"  CAGR: {cagr_scaled*100:.2f}%")
print(f"  Sharpe: {sharpe_scaled:.4f}")
print(f"  Sortino: {sortino_scaled:.4f}")
print(f"  Max Drawdown: {np.min(drawdown_scaled)*100:.2f}%")
print(f"  Hit Rate: {np.mean(returns_scaled > 0)*100:.2f}%")
print(f"  Avg Daily Return: {mean_ret_scaled*100:.4f}%")
print(f"  Volatility: {std_ret_scaled*np.sqrt(252)*100:.2f}%")

# Check regime distribution and position sizes
regime_counts = bt_scaled.group_by('regime').agg(pl.len().alias('count'))
print(f"\n  Regime Distribution:")
for row in regime_counts.iter_rows(named=True):
    print(f"    {row['regime']}: {row['count']} days ({row['count']/len(bt_scaled)*100:.1f}%)")

# Days with actual positions
days_with_positions = bt_scaled.filter(pl.col('total_gross') > 0.01)
print(f"\n  Days with positions (gross > 1%): {len(days_with_positions)}")
if len(days_with_positions) > 0:
    regime_positions = days_with_positions.group_by('regime').agg([
        pl.len().alias('count'),
        pl.col('total_gross').mean().alias('avg_gross'),
    ])
    print("    Regime distribution and average gross exposure:")
    for row in regime_positions.iter_rows(named=True):
        print(f"      {row['regime']}: {row['count']} days, avg gross={row['avg_gross']*100:.1f}%")
    
    # Check average returns by regime
    print("\n    Average returns by regime (on days with positions):")
    for regime in ['STRONG_RISK_ON_MAJORS', 'WEAK_RISK_ON_MAJORS', 'RISK_ON_MAJORS']:
        regime_days = days_with_positions.filter(pl.col('regime') == regime)
        if len(regime_days) > 0:
            avg_ret = regime_days['r_ls_net'].mean()
            avg_gross = regime_days['total_gross'].mean()
            print(f"      {regime}: {avg_ret*100:.4f}% return, {avg_gross*100:.1f}% gross ({len(regime_days)} days)")

print("\n\nWITHOUT REGIME-BASED POSITION SIZING (Previous Results):")
print(f"  Trading Days: 434")
print(f"  Total Return: 118.97%")
print(f"  CAGR: 57.63%")
print(f"  Sharpe: 1.2573")
print(f"  Sortino: 1.8539")
print(f"  Max Drawdown: -90.82%")
print(f"  Hit Rate: 49.77%")
print(f"  Avg Daily Return: 0.2193%")
print(f"  Volatility: 43.95%")
print(f"  Days with positions: 48")

print("\n" + "=" * 80)
print("KEY DIFFERENCES:")
print("=" * 80)

print(f"\n1. MAX DRAWDOWN:")
print(f"   With Scaling: {np.min(drawdown_scaled)*100:.2f}%")
print(f"   Without Scaling: -90.82%")
improvement = (np.min(drawdown_scaled) - (-0.9082)) * 100
print(f"   Improvement: {improvement:.2f}% {'BETTER' if improvement > 0 else 'WORSE'}")

print(f"\n2. RETURNS:")
print(f"   With Scaling: {total_return_scaled*100:.2f}% total, {cagr_scaled*100:.2f}% CAGR")
print(f"   Without Scaling: 118.97% total, 57.63% CAGR")
print(f"   Trade-off: {'Higher' if total_return_scaled > 1.1897 else 'Lower'} returns")

print(f"\n3. RISK-ADJUSTED:")
print(f"   With Scaling Sharpe: {sharpe_scaled:.4f}")
print(f"   Without Scaling Sharpe: 1.2573")
print(f"   With Scaling Sortino: {sortino_scaled:.4f}")
print(f"   Without Scaling Sortino: 1.8539")

print(f"\n4. POSITION SIZING:")
if len(days_with_positions) > 0:
    avg_gross_scaled = days_with_positions['total_gross'].mean()
    print(f"   With Scaling: {avg_gross_scaled*100:.1f}% average gross exposure")
    print(f"   Without Scaling: ~11.1% average gross exposure")
    print(f"   Reduction: {(1.0 - avg_gross_scaled / 0.111) * 100:.1f}% smaller positions")

print("\n" + "=" * 80)
print("CONCLUSION:")
print("=" * 80)
if improvement > 0:
    print(f"Regime-based position sizing REDUCES drawdown by {improvement:.2f}%")
    print("by reducing position sizes in WEAK regimes (60% vs 100%).")
    print("This reduces risk exposure during uncertain periods.")
else:
    print("Regime-based position sizing does NOT significantly reduce drawdown.")
    print("Smaller positions in WEAK regimes help, but losses still occur.")
print("=" * 80)
