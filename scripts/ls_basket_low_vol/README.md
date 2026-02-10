# LS Basket Low-Vol Pipeline

Dollar-neutral long/short basket construction that **minimizes realized volatility** and **controls tail risk**, using daily data from the data lake.

## Requirements

- Python 3.10+
- Dependencies: `pandas`, `numpy`, `scikit-learn`, `scipy` (and optionally `cvxpy` for full QP)
- Data: `data/curated/data_lake/` (fact_price, fact_volume, fact_marketcap, dim_asset)

## Quick Start

```bash
# From repo root
python run_ls_basket_low_vol.py --quick
```

## Full Run

```bash
python run_ls_basket_low_vol.py
# Or with custom config:
python run_ls_basket_low_vol.py --config path/to/config.json
```

## Methods

- **Method A (Global Min-Variance QP)**: Minimize portfolio variance with Ledoit–Wolf covariance, CVaR penalty, turnover penalty. Dollar-neutral, gross exposure cap, per-asset and liquidity constraints. Optional PCA factor exposure limit.
- **Method B (Cluster-Matched Pairs)**: Cluster by return correlations, create matched long/short sub-baskets within each cluster. Within-cluster and overall dollar neutrality.

## Outputs

Written to `outputs/ls_basket_low_vol/`:

- `runs/` — weight snapshots, daily PnL, summary CSVs
- `reports/` — summary.md, diagnostic plots
- `configs/` — params_used.json
- `outputs/run_metadata_ls_low_vol.json` — run metadata

## Configuration

See `config_default.json`. Key parameters:

- `start_date`, `end_date` — backtest period (default from strategy_benchmark.yaml)
- `universe_qc.min_mcap_usd`, `min_volume_usd_14d_avg` — universe filters
- `method_a`: G, max_w_abs, alpha_cvar, beta_turnover
- `method_b`: K (clusters), cluster_budget
- `constraints`: max_avg_turnover, min_long_short_corr

## Data Notes

- **fact_volume**: Assumed USD volume or `price × volume` for ADV. CoinGecko volume is often USD.
- **fact_funding**: Available for carry; not used in this pipeline by default.
- Missing fields: Proceed with conservative defaults as documented in outputs.
