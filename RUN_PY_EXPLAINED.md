# run.py — What It Is and How It Works

## 1. What is run.py?

**run.py** is the main CLI entrypoint for the majors vs alts regime monitor. It:

- Loads **base config** (`config.yaml`) and optionally an **experiment spec** (YAML).
- Runs a full pipeline: **data load → features → regime modeling → backtest → outputs → regime evaluation**.
- Supports two modes: **regular** (volume-based basket, beta/dollar neutrality) and **MSM** (market-cap basket, fixed major weights, alts vs BTC evaluation).

It does **not** change the data lake; it only reads from it and writes outputs to `./reports/`, `./artifacts/`, and `./runs/`.

---

## 2. CLI Arguments

| Argument | Required | Default | Meaning |
|----------|----------|---------|---------|
| `--start` | Yes | — | Start date (YYYY-MM-DD) |
| `--end` | Yes | — | End date (YYYY-MM-DD) |
| `--config` | No | `majors_alts_monitor/config.yaml` | Base config file path |
| `--experiment` | No | — | Experiment YAML path (overrides/extends config) |

**Example:**

```bash
python -m majors_alts_monitor.run --start 2024-01-01 --end 2026-01-01 --config majors_alts_monitor/config.yaml --experiment experiments/msm/msm_v1_baseline.yaml
```

---

## 3. Config and Experiment Spec Resolution

### 3.1 Base Config

1. Load `config.yaml` (data paths, universe, features, regime, costs, backtest, outputs).
2. Parse `--start` and `--end` into `date` objects.

### 3.2 Experiment Spec (if `--experiment` provided)

1. Load the experiment YAML.
2. Detect **MSM mode**: `category_path == "msm"`.
3. **If MSM mode:**
   - Call `apply_msm_config_overrides(config, experiment_spec)`.
   - This overrides universe (basket size, min_volume from `target.short_leg`), **disables** alt_selection, merges `state_mapping` into regime config, and merges `backtest`.
4. **If regular mode:**
   - Extract `backtest` and `state_mapping` from the spec.
   - Map `state_mapping` → `config["regime"]["composite"]` (thresholds, n_regimes).
   - Deep-merge into config.

Result: a single **merged config** used for the rest of the run.

---

## 4. What a Run Does (Step by Step)

### Step 1: Data Load

- Initialize `ReadOnlyDataLoader` with `data_lake_dir`, optional `duckdb_path`, optional `universe_snapshots_path`.
- Call `load_dataset(start, end)` → returns dict: `price`, `marketcap`, `volume`, `funding`, `open_interest`, `universe_snapshots`.
- Require at least `price`, `marketcap`, `volume`. `funding` and `open_interest` are optional.
- Get stablecoins for exclusion; combine with `exclude_assets` from config.

### Step 2: Feature Computation

- Initialize `FeatureLibrary(burn_in_days, lookback_days)`.
- Call `compute_features(prices, marketcap, volume, funding, open_interest, majors, exclude_assets)`.
- Produces a wide DataFrame with raw features and z-scored variants (e.g. `z_alt_breadth_pct_up`, `z_btc_dominance`, etc.).

### Step 3: Regime Modeling

- Initialize `RegimeModel` from `config["regime"]` (mode, default_weights, thresholds, hysteresis, n_regimes).
- Call `regime_model.compute_composite_score(features, prices=prices)`.
- Produces `regime_series`: DataFrame with `date`, `score`, `regime` (e.g. STRONG_RISK_ON_ALTS, BALANCED, WEAK_RISK_ON_MAJORS).

### Step 4: Basket and Neutrality Functions

Two different setups depending on mode:

**MSM mode:**

- `build_alt_basket` → `beta_neutral.build_msm_basket(prices, marketcap, volume, asof_date, n, min_mcap_usd, min_volume_usd, exclude_assets, weighting)` — top N alts by market cap, equal or mcap weighted.
- `solve_neutrality` → fixed major weights from `target.long_leg.weights` (e.g. BTC 1.0 or 70/30 BTC/ETH); scale alts to 50% gross (short), majors to 50% gross (long).

**Regular mode:**

- `build_alt_basket` → `beta_neutral.build_alt_basket` — volume-based, optional enhanced filters (volatility, correlation, momentum).
- `solve_neutrality` → `beta_neutral.solve_neutrality` — beta-neutral or dollar-neutral, minimize BTC/ETH exposure.

### Step 5: Backtest

- Initialize `BacktestEngine` (fees, slippage, funding, vol_target, risk_management, rebalance_frequency).
- In MSM mode: `rebalance_frequency_days` from spec (default 1 = daily).
- Call `backtest_engine.run_backtest(prices, marketcap, volume, funding, features, regime_series, build_alt_basket, estimate_beta, solve_neutrality, start_date, end_date, walk_forward, lookback_window_days, test_window_days)`.
- Produces `backtest_results`: DataFrame with `date`, `pnl`, `cost`, `funding`, `r_ls_gross`, `r_ls_net`, `alt_turnover`, etc.

### Step 6: Experiment Manager (if experiment spec provided)

- Generate `run_id` from `experiment_id`.
- Create run directory under `runs/<run_id>/`.
- Write `manifest.json` (experiment_spec, resolved_config, data_snapshot_dates).

### Step 7: Output Generation

- Initialize `OutputGenerator(reports_dir, artifacts_dir, experiment_manager)`.
- Call `generate_outputs(regime_series, features, backtest_results, start_date, end_date)`.
- Writes: `regime_timeline.csv`, `features_wide.parquet`, `bt_equity_curve.csv`, `bt_daily_pnl.csv`, `kpis.json`, `regime_metrics.csv`, `dashboard.html` (if plotly available).

### Step 8: Regime Evaluation (MSM mode or if `with_regime_eval` in spec)

**MSM mode:**

- Compute **target returns** = alts index return − BTC return (1-day forward) for each date in regime_series.
- Use `target.short_leg` and `target.long_leg` for basket definition.
- Call `evaluate_regime_edges(target_returns, regime_series, horizons_days, bootstrap)`.
- Produces regime-conditional means, hit rates, t-stats, edge stats (best/worst regime, spread).

**Regular mode:**

- Use strategy returns (`r_ls_gross` or `r_ls_net`) as target.
- Same evaluation flow.

- Print formatted results; if experiment manager active, save `regime_evaluation.json` under run directory.

### Step 9: Experiment Artifacts (if experiment manager active)

- Merge KPIs and stability metrics.
- Write `metrics.json` (including regime_evaluation if computed).
- Write `regime_timeseries.parquet` and returns to run directory.
- Update experiment catalog.

### Step 10: Cleanup

- Call `data_loader.close()` in a `finally` block.

---

## 5. Key Components and Dependencies

| Component | Source | Role |
|-----------|--------|------|
| **ReadOnlyDataLoader** | `data_io.py` | Load price, marketcap, volume, funding, OI from data lake |
| **FeatureLibrary** | `features.py` | Compute raw + z-scored features |
| **DualBetaNeutralLS** | `beta_neutral.py` | Build ALT basket, estimate betas, solve neutrality |
| **RegimeModel** | `regime.py` | Composite score → regime classification |
| **BacktestEngine** | `backtest.py` | Walk-forward backtest with costs/funding |
| **OutputGenerator** | `outputs.py` | CSV, Parquet, JSON, HTML dashboard |
| **ExperimentManager** | `experiment_manager.py` | Run directory, manifest, metrics, catalog |
| **evaluate_regime_edges** | `regime_evaluation.py` | Regime-conditional forward return stats |
| **apply_msm_config_overrides** | `config_utils.py` | MSM-specific config overrides |

---

## 6. MSM vs Regular Mode Summary

| Aspect | MSM Mode | Regular Mode |
|--------|----------|--------------|
| **Trigger** | `category_path: "msm"` in experiment spec | Any other experiment or no spec |
| **ALT basket** | Top N by market cap (from `target.short_leg`) | Volume-based, optional filters |
| **Major weights** | Fixed (e.g. BTC 1.0 or 70/30 BTC/ETH) | Beta/dollar neutrality solver |
| **Regime evaluation target** | alts index − BTC | Strategy PnL (r_ls_gross / r_ls_net) |
| **alt_selection** | Disabled | Can be enabled (vol/corr/mom filters) |
| **Rebalance** | Fixed schedule (e.g. daily) | Regime-based (trade only in RISK_ON_MAJORS) |

---

## 7. Outputs

### Reports (default: `./reports/majors_alts/`)

| File | Content |
|------|---------|
| `regime_timeline.csv` | date, score, regime |
| `features_wide.parquet` | All computed features |
| `bt_equity_curve.csv` | Cumulative equity |
| `bt_daily_pnl.csv` | Daily PnL, costs, funding, returns |
| `kpis.json` | CAGR, Sharpe, Sortino, maxDD, Calmar, hit_rate, turnover |
| `regime_metrics.csv` | Performance by regime |
| `dashboard.html` | Interactive Plotly dashboard (if available) |

### Experiment Run (if `--experiment` provided: `./runs/<run_id>/`)

| File | Content |
|------|---------|
| `manifest.json` | experiment_spec, resolved_config, data_snapshot_dates |
| `metrics.json` | KPIs, stability, regime_evaluation |
| `regime_timeseries.parquet` | Regime series |
| `returns.parquet` | Gross/net returns |
| `regime_evaluation.json` | Regime-conditional stats (if computed) |

---

## 8. Short Summary

**run.py** is the main orchestration script for the majors vs alts regime monitor. It:

1. Loads base config and optionally merges an experiment spec (MSM or regular).
2. Loads data from the data lake (read-only).
3. Computes features and regime series.
4. Runs a walk-forward backtest using either MSM basket (market-cap, fixed majors) or regular basket (volume-based, beta-neutral).
5. Generates reports and, if an experiment spec is provided, writes run artifacts and regime evaluation.
6. Keeps the data lake read-only; all outputs go to reports, artifacts, and runs.

For MSM experiments, use `--experiment experiments/msm/msm_v1_baseline.yaml` to get fixed-basket, alts-vs-BTC regime evaluation.
