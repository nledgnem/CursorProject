import polars as pl
import json

# Check backtest results
bt = pl.read_csv('reports/majors_alts/bt_daily_pnl.csv')
print(f'Backtest results: {len(bt)} days')
if len(bt) > 0:
    print(f'Date range: {bt["date"].min()} to {bt["date"].max()}')
    print(f'Columns: {bt.columns}')

# Check KPIs
with open('reports/majors_alts/kpis.json') as f:
    kpis = json.load(f)
print(f'\nKPIs:')
for k, v in kpis.items():
    if isinstance(v, float):
        print(f'  {k}: {v:.4f}')
    else:
        print(f'  {k}: {v}')

# Check regime timeline
regime = pl.read_csv('reports/majors_alts/regime_timeline.csv')
print(f'\nRegime timeline: {len(regime)} days')
print(f'Regime distribution:')
regime_dist = regime.group_by('regime').agg(pl.len().alias('count')).sort('regime')
for row in regime_dist.iter_rows(named=True):
    print(f'  {row["regime"]}: {row["count"]} days')
