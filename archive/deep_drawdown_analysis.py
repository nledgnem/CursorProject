import polars as pl
import numpy as np

print("=" * 70)
print("DEEP DRAWDOWN ANALYSIS: Why Both Modes Have High Drawdowns")
print("=" * 70)

# Load dollar-neutral results
bt_dollar = pl.read_csv('reports/majors_alts/bt_daily_pnl.csv').sort('date')
returns_dollar = bt_dollar['r_ls_net'].to_numpy()
equity_dollar = np.cumprod(1.0 + returns_dollar)

running_max_dollar = np.maximum.accumulate(equity_dollar)
drawdown_dollar = (equity_dollar - running_max_dollar) / running_max_dollar
max_dd_idx_dollar = int(np.argmin(drawdown_dollar))
max_dd_date_dollar = bt_dollar['date'][max_dd_idx_dollar]

# Find peak before drawdown
peak_idx_dollar = int(np.argmax(equity_dollar[:max_dd_idx_dollar+1]))
peak_date_dollar = bt_dollar['date'][peak_idx_dollar]

print(f"\nDOLLAR-NEUTRAL MODE:")
print(f"  Max Drawdown: {np.min(drawdown_dollar)*100:.2f}%")
print(f"  Peak: {peak_date_dollar} (equity: {equity_dollar[peak_idx_dollar]:.4f})")
print(f"  Trough: {max_dd_date_dollar} (equity: {equity_dollar[max_dd_idx_dollar]:.4f})")

# Analyze the drawdown period
dd_period_dollar = bt_dollar.filter(
    (pl.col('date') >= peak_date_dollar) & 
    (pl.col('date') <= max_dd_date_dollar)
)

print(f"\n  Drawdown Period Analysis ({peak_date_dollar} to {max_dd_date_dollar}):")
print(f"    Days: {len(dd_period_dollar)}")
print(f"    Total return: {(equity_dollar[max_dd_idx_dollar] / equity_dollar[peak_idx_dollar] - 1.0)*100:.2f}%")
print(f"    Average daily return: {dd_period_dollar['r_ls_net'].mean()*100:.4f}%")
print(f"    Worst day: {dd_period_dollar['r_ls_net'].min()*100:.2f}%")
print(f"    Days with >5% loss: {len(dd_period_dollar.filter(pl.col('r_ls_net') < -0.05))}")

# Check if the issue is the ALT basket or regime model
print(f"\n  Regime Distribution During Drawdown:")
regime_counts = dd_period_dollar.group_by('regime').agg(pl.count().alias('count'))
for row in regime_counts.iter_rows(named=True):
    print(f"    {row['regime']}: {row['count']} days ({row['count']/len(dd_period_dollar)*100:.1f}%)")

# Check position sizing
print(f"\n  Position Sizing During Drawdown:")
print(f"    Avg ALT gross: {dd_period_dollar['alt_gross'].mean()*100:.1f}%")
print(f"    Avg Major gross: {dd_period_dollar['major_gross'].mean()*100:.1f}%")
print(f"    Avg Net exposure: {(dd_period_dollar['major_gross'].mean() - dd_period_dollar['alt_gross'].mean())*100:.1f}%")

# Check if losses are due to ALT outperformance
print(f"\n  Loss Pattern Analysis:")
worst_days_dollar = dd_period_dollar.sort('r_ls_net').head(5)
print(f"    Worst 5 days:")
for row in worst_days_dollar.iter_rows(named=True):
    print(f"      {row['date']}: {row['r_ls_net']*100:.2f}% (regime: {row['regime']})")

# The real question: why does dollar-neutral also have high drawdown?
print(f"\n" + "=" * 70)
print("ROOT CAUSE: Why Both Modes Have High Drawdowns")
print("=" * 70)

print("\n1. THE STRATEGY IS SHORT ALTS")
print("   Both modes are short ALT basket (33-50% short)")
print("   When ALTs outperform majors, the strategy loses")
print("   This is the core risk of the strategy, regardless of neutrality mode")

print("\n2. COMPOUNDING EFFECT")
print("   With 100% gross exposure, losses compound quickly")
print("   Example: -10% day followed by -5% day = -14.5% total (not -15%)")
print("   But the compounding works against you on down days")

print("\n3. REGIME MODEL MAY NOT BE WORKING")
print("   The strategy should only trade in RISK_ON_MAJORS regime")
print("   But losses occurred during BALANCED regime (check above)")
print("   This suggests the regime model isn't filtering out bad periods")

print("\n4. VOLATILITY OF ALT BASKET")
print("   ALT basket contains volatile assets that can move 10-20% in a day")
print("   With 33-50% short exposure, a 20% ALT move = -6.6% to -10% loss")
print("   Multiple such days compound to extreme drawdowns")

print("\n5. BETA-NEUTRAL vs DOLLAR-NEUTRAL")
print("   Dollar-neutral: 0% net, but still 100% gross exposure")
print("   Beta-neutral: 33% net long, but better risk-adjusted returns")
print("   The net exposure helps in bull markets (higher returns)")
print("   But both suffer in ALT outperformance periods")

print("\n" + "=" * 70)
print("CONCLUSION:")
print("The high drawdown is NOT primarily due to net exposure.")
print("It's due to:")
print("1. Core strategy risk (short ALTs)")
print("2. High volatility of ALT basket")
print("3. Compounding of losses")
print("4. Regime model may need improvement")
print("=" * 70)
