import polars as pl

bt = pl.read_csv('reports/majors_alts/bt_daily_pnl.csv').sort('date')

# Check if scaling is actually working
print("Checking regime-based position scaling...")
print("=" * 80)

# Days with positions
days_with_positions = bt.filter(pl.col('total_gross') > 0.01)

# Group by regime and check average gross exposure
regime_stats = days_with_positions.group_by('regime').agg([
    pl.len().alias('count'),
    pl.col('total_gross').mean().alias('avg_gross'),
    pl.col('total_gross').min().alias('min_gross'),
    pl.col('total_gross').max().alias('max_gross'),
    pl.col('r_ls_net').mean().alias('avg_return'),
])

print("\nRegime Statistics (days with positions):")
for row in regime_stats.iter_rows(named=True):
    print(f"\n{row['regime']}:")
    print(f"  Count: {row['count']} days")
    print(f"  Avg Gross: {row['avg_gross']*100:.1f}%")
    print(f"  Min Gross: {row['min_gross']*100:.1f}%")
    print(f"  Max Gross: {row['max_gross']*100:.1f}%")
    print(f"  Avg Return: {row['avg_return']*100:.4f}%")

# Check if WEAK_RISK_ON_MAJORS has smaller positions than STRONG
weak_days = days_with_positions.filter(pl.col('regime') == 'WEAK_RISK_ON_MAJORS')
strong_days = days_with_positions.filter(pl.col('regime') == 'STRONG_RISK_ON_MAJORS')

if len(weak_days) > 0:
    weak_avg = weak_days['total_gross'].mean()
    print(f"\nWEAK_RISK_ON_MAJORS average gross: {weak_avg*100:.1f}%")
    
if len(strong_days) > 0:
    strong_avg = strong_days['total_gross'].mean()
    print(f"STRONG_RISK_ON_MAJORS average gross: {strong_avg*100:.1f}%")
    if len(weak_days) > 0:
        ratio = weak_avg / strong_avg if strong_avg > 0 else 1.0
        print(f"Ratio (WEAK/STRONG): {ratio:.2f} (expected ~0.6)")

print("\n" + "=" * 80)
print("If all regimes show 100% gross, the scaling is NOT being applied correctly.")
print("The solver might be scaling everything back up to meet neutrality constraints.")
print("=" * 80)
