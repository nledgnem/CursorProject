# MSM v0 - Funding-only Module

A standalone module for majors vs alts monitoring using funding rate features only.

## Overview

MSM v0 implements a weekly decision cadence with:
- **Long leg**: BTC/ETH 70/30 benchmark
- **Short leg**: Top 30 eligible ALTs (by market cap)
- **Feature**: 7-day mean funding rate basket
- **Labels**: 5 percentile bins (Red, Orange, Yellow, YellowGreen, Green)
- **Target**: `y = r_alts - r_maj` (ALT basket return minus major benchmark return)

## Decision Cadence

**Weekly decisions anchored to Monday 00:00 UTC**

Convention: Monday 00:00 UTC means decision at the start of Monday UTC week.
- Decision dates are the start of each Monday UTC week
- Feature window: 7 calendar days prior to decision date (exclusive of decision date)
- Returns window: From decision date to next decision date

Example:
- Decision date: 2024-01-08 (Monday 00:00 UTC)
- Feature window: 2024-01-01 to 2024-01-07 (7 days prior)
- Next decision: 2024-01-15 (next Monday)
- Returns window: 2024-01-08 to 2024-01-15

## Universe Selection

Top N=30 eligible ALTs ranked by market cap at decision time, with exclusions:
- BTC, ETH (majors)
- Stablecoins (from `dim_asset.is_stable` or hardcoded list)
- Exchange tokens (hardcoded: BNB, FTT, HT, OKB, KCS, GT, MX, CRO, LEO, VGX)
- Wrapped tokens (hardcoded: WBTC, WETH, etc.)
- Liquid staking tokens (hardcoded: stETH, rETH, cbETH, wstETH, stSOL, rBTC)
- Bridge/pegged assets (hardcoded: renBTC, tBTC, etc.)

## Feature Computation

For each ALT in the top 30 basket:
1. Compute 7-day mean funding rate over the 7 calendar days prior to decision date
2. Aggregate multiple instruments per asset by taking mean per day, then mean over days

Basket feature `F_tk` = mean of per-coin 7d funding means across valid ALTs

**Coverage rule**: Require >=60% of the 30 assets to have valid 7d funding. If coverage < 60%, skip that week entirely (no feature/label/return row).

## Labeling

Two labeling modes:

### v0.0 FULL_SAMPLE
- Percentiles computed vs all weeks in the dataset
- Exploratory mode (allows look-ahead)
- Use for exploratory analysis only

### v0.1 ROLLING_PAST_52W
- Percentiles computed vs prior 52 valid weekly observations only
- PIT-safe (point-in-time safe)
- If <52 prior weeks available, label = NA

**Bin mapping**:
- Red: 0-20th percentile
- Orange: 20-40th percentile
- Yellow: 40-60th percentile
- YellowGreen: 60-80th percentile
- Green: 80-100th percentile

## Returns

**Costs OFF** (no fees, slippage, or funding costs):
- `r_alts`: Equal-weight return of top 30 ALT basket from t_k to t_{k+1}
- `r_btc`: BTC return over same window
- `r_eth`: ETH return over same window
- `r_maj_70_30`: 0.7*r_BTC + 0.3*r_ETH
- `y`: r_alts - r_maj_70_30 (target)

## Outputs

All outputs written to `reports/msm_funding_v0/<run_id>/`:

1. **msm_timeseries.csv**: Weekly time series with:
   - decision_date, next_date
   - basket_hash, basket_members, n_valid, coverage
   - F_tk (feature)
   - label_v0_0, label_v0_1
   - r_alts, r_btc, r_eth, r_maj_70_30, y

2. **summary_by_label_v0_0.csv**: Summary statistics by label (v0.0):
   - count, mean_y, median_y, std_y, hit_rate(y<0)

3. **summary_by_label_v0_1.csv**: Summary statistics by label (v0.1):
   - count, mean_y, median_y, std_y, hit_rate(y<0)

4. **run_manifest.json**: Full configuration and metadata

## Usage

### CLI Entrypoint

```bash
python -m majors_alts_monitor.msm_funding_v0.msm_run \
    --start-date 2024-01-01 \
    --end-date 2024-12-31 \
    --config majors_alts_monitor/msm_funding_v0/msm_config.yaml \
    --run-id my_run_20240126
```

### Data Sanity Check Only

```bash
python -m majors_alts_monitor.msm_funding_v0.msm_run \
    --start-date 2024-01-01 \
    --end-date 2024-12-31 \
    --sanity-check-only
```

### Python API

```python
from majors_alts_monitor.msm_funding_v0.msm_run import run_msm_v0
from pathlib import Path
from datetime import date

run_msm_v0(
    config_path=Path("majors_alts_monitor/msm_funding_v0/msm_config.yaml"),
    start_date=date(2024, 1, 1),
    end_date=date(2024, 12, 31),
    run_id="my_run_20240126",
)
```

## Configuration

See `msm_config.yaml` for full configuration options:
- Data paths
- Decision cadence (anchor day, hour, minute)
- Universe rules (basket size, exclusions, min market cap)
- Feature computation (lookback days, min coverage)
- Labeling (bin ranges, window sizes)
- Output paths

## Dependencies

- polars
- pandas
- pyyaml
- Existing data lake structure (fact_price, fact_marketcap, fact_funding, dim_asset)
