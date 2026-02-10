import polars as pl
import numpy as np

# Load backtest results
bt = pl.read_csv('reports/majors_alts/bt_daily_pnl.csv').sort('date')

print("=== ROOT CAUSE ANALYSIS ===\n")

# Issue 1: Major turnover is always 0
print("1. MAJOR TURNOVER ISSUE:")
print(f"   Average major turnover: {bt['major_turnover'].mean():.6f}")
print(f"   Max major turnover: {bt['major_turnover'].max():.6f}")
print(f"   Non-zero major turnover days: {bt.filter(pl.col('major_turnover') > 0).height}")
if bt['major_turnover'].max() == 0:
    print("   PROBLEM: Majors are never being rebalanced!")
else:
    print(f"   Majors ARE being rebalanced (max: {bt['major_turnover'].max():.6f})")
print()

# Issue 2: PnL vs r_ls_net mismatch
print("2. PNL vs RETURN MISMATCH:")
mismatch = bt.filter((pl.col('pnl') - pl.col('r_ls_net')).abs() > 0.01)
print(f"   Days where |PnL - r_ls_net| > 0.01: {len(mismatch)} / {len(bt)} ({len(mismatch)/len(bt)*100:.1f}%)")
print(f"   Formula: r_ls_net = pnl - cost - funding")
print(f"   Example day:")
example = mismatch.head(1).to_dicts()[0]
print(f"     Date: {example['date']}")
print(f"     PnL: {example['pnl']:.6f}")
print(f"     Cost: {example['cost']:.6f}")
print(f"     Funding: {example['funding']:.6f}")
print(f"     r_ls_net: {example['r_ls_net']:.6f}")
print(f"     Expected: {example['pnl'] - example['cost'] - example['funding']:.6f}")
if abs(calc - worst_day['r_ls_net']) > 0.0001:
    print("   PROBLEM: The formula doesn't match!")
else:
    print("   Formula is correct")
print()

# Issue 3: Extreme returns
print("3. EXTREME RETURNS:")
extreme = bt.filter(pl.col('r_ls_net').abs() > 0.10)
print(f"   Days with |return| > 10%: {len(extreme)}")
print(f"   Worst return: {bt['r_ls_net'].min():.4f} ({bt['r_ls_net'].min()*100:.2f}%)")
print(f"   This suggests position sizing is wrong - returns should be much smaller\n")

# Issue 4: Check if weights are being used correctly
print("4. POSITION SIZING HYPOTHESIS:")
print("   The PnL calculation uses weights directly:")
print("     pnl = sum(-weight_alt * ret_alt) + sum(weight_major * ret_major)")
print("   If weights sum to 1.0 for alts and 1.0 for majors, that's 200% gross exposure")
print("   A -10% move in alts with 100% short exposure = -10% return")
print("   But if alts move +20% and majors move +10%, we get:")
print("     pnl = -1.0 * 0.20 + 1.0 * 0.10 = -0.10 = -10%")
print("   This is correct IF weights are normalized to sum to 1.0 total")
print("   But if ALT weights sum to 1.0 AND major weights sum to 1.0, we have 200% exposure\n")

# Check the actual calculation
print("5. ACTUAL CALCULATION CHECK:")
worst_day = bt.sort('r_ls_net').head(1).to_dicts()[0]
print(f"   Worst day: {worst_day['date']}")
print(f"   PnL: {worst_day['pnl']:.6f}")
print(f"   r_ls_net: {worst_day['r_ls_net']:.6f}")
print(f"   If r_ls_net = pnl - cost - funding:")
calc = worst_day['pnl'] - worst_day['cost'] - worst_day['funding']
print(f"     Calculated: {calc:.6f}")
print(f"     Actual: {worst_day['r_ls_net']:.6f}")
print(f"     Difference: {abs(calc - worst_day['r_ls_net']):.6f}")
if abs(calc - worst_day['r_ls_net']) < 0.0001:
    print("   ✓ Formula is correct")
else:
    print("   ⚠️  Formula doesn't match - there's a bug in the calculation\n")

# Check equity curve
returns = bt['r_ls_net'].to_numpy()
equity = np.cumprod(1.0 + returns)
print(f"\n6. EQUITY CURVE:")
print(f"   Starting equity: {equity[0]:.4f} (should be 1.0)")
print(f"   Final equity: {equity[-1]:.4f}")
print(f"   Max equity: {equity.max():.4f}")
print(f"   Min equity: {equity.min():.4f}")
if abs(equity[0] - 1.0) > 0.01:
    print(f"   PROBLEM: Equity doesn't start at 1.0!")
    print(f"   First return: {returns[0]:.4f}")
    print(f"   If we start at 1.0: 1.0 * (1 + {returns[0]:.4f}) = {1.0 + returns[0]:.4f}")
else:
    print("   Equity starts correctly at 1.0")
