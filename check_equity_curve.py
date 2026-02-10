import polars as pl
import numpy as np

bt = pl.read_csv('reports/majors_alts/bt_daily_pnl.csv').sort('date')
returns = bt['r_ls_net'].to_numpy()
equity = np.cumprod(1.0 + returns)

# Find periods of significant decline
running_max = np.maximum.accumulate(equity)
drawdown = (equity - running_max) / running_max

# Find all drawdowns > 20%
large_dds = []
in_dd = False
dd_start = None
dd_start_idx = None

for i, dd in enumerate(drawdown):
    if dd < -0.20:  # > 20% drawdown
        if not in_dd:
            in_dd = True
            dd_start = bt['date'][i]
            dd_start_idx = i
    else:
        if in_dd:
            in_dd = False
            dd_end = bt['date'][i-1]
            dd_end_idx = i-1
            min_dd = np.min(drawdown[dd_start_idx:dd_end_idx+1])
            large_dds.append({
                'start': dd_start,
                'end': dd_end,
                'days': dd_end_idx - dd_start_idx + 1,
                'max_dd': min_dd,
                'start_equity': equity[dd_start_idx],
                'trough_equity': equity[np.argmin(drawdown[dd_start_idx:dd_end_idx+1]) + dd_start_idx],
            })

print("Large Drawdowns (>20%):")
for i, dd_info in enumerate(large_dds, 1):
    print(f"\n{i}. {dd_info['start']} to {dd_info['end']}")
    print(f"   Max DD: {dd_info['max_dd']*100:.2f}%")
    print(f"   Duration: {dd_info['days']} days")
    print(f"   Start Equity: {dd_info['start_equity']:.4f}")
    print(f"   Trough Equity: {dd_info['trough_equity']:.4f}")

# Check if the issue is the net long exposure in beta-neutral mode
print(f"\n\nNet Exposure Analysis:")
print(f"Average net exposure: {(bt['major_gross'].mean() - bt['alt_gross'].mean())*100:.1f}%")
print(f"Max net exposure: {(bt['major_gross'].max() - bt['alt_gross'].min())*100:.1f}%")

# Check if losses correlate with net exposure
print(f"\nCorrelation Analysis:")
net_exposure = bt['major_gross'] - bt['alt_gross']
correlation = bt.select([
    pl.corr('r_ls_net', net_exposure).alias('corr_net_exp_returns')
]).item()
print(f"  Correlation (net exposure vs returns): {correlation:.4f}")

# Check worst periods by net exposure
high_net_exp = bt.filter(net_exposure > 0.4)  # > 40% net long
if len(high_net_exp) > 0:
    print(f"\n  Days with >40% net long exposure: {len(high_net_exp)}")
    print(f"  Average return on those days: {high_net_exp['r_ls_net'].mean()*100:.4f}%")
