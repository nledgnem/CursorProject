import polars as pl

bt = pl.read_csv('reports/majors_alts/bt_daily_pnl.csv')

print("Major gross analysis:")
print(f"  Mean: {bt['major_gross'].mean():.3f}")
print(f"  Min: {bt['major_gross'].min():.3f}")
print(f"  Max: {bt['major_gross'].max():.3f}")
print(f"  All values: {sorted(bt['major_gross'].unique().to_list())}")

print("\nSample rows:")
for row in bt.head(3).iter_rows(named=True):
    print(f"  {row['date']}: ALT={row['alt_gross']:.3f}, Major={row['major_gross']:.3f}, Total={row['total_gross']:.3f}")

print("\nIssue: Major gross should be 0.5 (50%), but it's showing as 1.0 (100%)")
print("This suggests the calculation is summing absolute values incorrectly, or weights are doubled")
