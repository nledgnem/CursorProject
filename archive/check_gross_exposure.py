import polars as pl

bt = pl.read_csv('reports/majors_alts/bt_daily_pnl.csv')

print("Gross exposure stats:")
print(f"  ALT gross: mean={bt['alt_gross'].mean():.3f}, max={bt['alt_gross'].max():.3f}")
print(f"  Major gross: mean={bt['major_gross'].mean():.3f}, max={bt['major_gross'].max():.3f}")
print(f"  Total gross: mean={bt['total_gross'].mean():.3f}, max={bt['total_gross'].max():.3f}")

worst = bt.sort('r_ls_net').head(1).to_dicts()[0]
print(f"\nWorst day ({worst['date']}):")
print(f"  ALT gross: {worst['alt_gross']:.3f}")
print(f"  Major gross: {worst['major_gross']:.3f}")
print(f"  Total gross: {worst['total_gross']:.3f}")
print(f"  Return: {worst['r_ls_net']:.4f}")

# Check if gross exposure is the issue
high_gross = bt.filter(pl.col('total_gross') > 1.2)
print(f"\nDays with gross > 120%: {len(high_gross)}")
if len(high_gross) > 0:
    print("  These days have excessive gross exposure!")
