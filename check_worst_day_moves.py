import polars as pl
from datetime import date

# Load prices
prices = pl.read_parquet('data/curated/data_lake/fact_price.parquet')

# Worst day: 2024-11-07
worst_date = date(2024, 11, 7)
prev_date = date(2024, 11, 6)

# Get prices
prices_prev = prices.filter(pl.col('date') == pl.date(prev_date.year, prev_date.month, prev_date.day))
prices_curr = prices.filter(pl.col('date') == pl.date(worst_date.year, worst_date.month, worst_date.day))

# Join to compute returns
returns = (
    prices_prev.select(['asset_id', 'close']).rename({'close': 'close_prev'})
    .join(
        prices_curr.select(['asset_id', 'close']).rename({'close': 'close_curr'}),
        on='asset_id', how='inner'
    )
    .with_columns([
        ((pl.col('close_curr') / pl.col('close_prev')) - 1.0).alias('ret')
    ])
    .sort('ret', descending=True)
)

print(f"Price moves on {worst_date}:")
print(f"\nTop 10 ALT movers (likely in basket):")
top_alts = returns.filter(~pl.col('asset_id').is_in(['BTC', 'ETH'])).head(10)
for row in top_alts.iter_rows(named=True):
    print(f"  {row['asset_id']}: {row['ret']*100:.2f}%")

print(f"\nBottom 10 ALT movers:")
bottom_alts = returns.filter(~pl.col('asset_id').is_in(['BTC', 'ETH'])).tail(10)
for row in bottom_alts.iter_rows(named=True):
    print(f"  {row['asset_id']}: {row['ret']*100:.2f}%")

print(f"\nMajor moves:")
majors = returns.filter(pl.col('asset_id').is_in(['BTC', 'ETH']))
for row in majors.iter_rows(named=True):
    print(f"  {row['asset_id']}: {row['ret']*100:.2f}%")

# Calculate average ALT return (cap-weighted would be better, but simple avg for now)
alt_returns = returns.filter(~pl.col('asset_id').is_in(['BTC', 'ETH']))
avg_alt_ret = alt_returns['ret'].mean()
print(f"\nAverage ALT return: {avg_alt_ret*100:.2f}%")
print(f"Median ALT return: {alt_returns['ret'].median()*100:.2f}%")

# If we're short 50% alts and they move +X%, we lose X% * 0.5
# If average ALT moved +40%, and we're short 50%, we lose 20% - matches!
if avg_alt_ret > 0.30:
    print(f"\n*** EXTREME MARKET MOVE DETECTED ***")
    print(f"Average ALT moved {avg_alt_ret*100:.1f}%")
    print(f"With 50% short exposure, this = {avg_alt_ret*0.5*100:.1f}% loss")
    print(f"This explains the -20% daily return!")
