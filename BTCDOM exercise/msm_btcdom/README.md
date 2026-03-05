# MSM + BTCDOM: gating and funding vs return analysis

Standalone analysis under **BTCDOM exercise**. Joins BTCDOM to MSM v0 timeseries (read-only from `reports/`), optionally gates by BTCDOM threshold, and runs correlation analysis plus strategy exploration. **No changes** to `majors_alts_monitor/` or `reports/`.

## Inputs

- **MSM timeseries**: e.g. `reports/msm_funding_v0/<run_id>/msm_timeseries.csv` (columns: `decision_date`, `F_tk`, `y`, …).
- **BTCDOM recon**: `../recon.csv` from [btcdom_recon](../btcdom_recon) (columns: `timestamp`, `btcdom_recon`, `n_constituents_used`, …).

## Usage

Run from the **repository root** (paths are relative to current working directory). MSM v0 is unchanged: run it as usual to produce `reports/msm_funding_v0/<run_id>/msm_timeseries.csv`, then use this folder.

1. **Join BTCDOM to MSM timeseries** (required first step):

   ```bash
   python "BTCDOM exercise/msm_btcdom/join_btcdom.py" --timeseries-csv "reports/msm_funding_v0/msm_v0_2024_02_to_2026_01/msm_timeseries.csv" --out-dir "BTCDOM exercise/msm_btcdom/out"
   ```

   Optional: `--recon-csv` (default: recon.csv under BTCDOM exercise). Optional gating: add `--gate --mode above --threshold 4000` (or `--mode below`, or `--mode between --low 3500 --high 5500`) to also write `msm_timeseries_gated.csv` and print counts.

2. **Correlation analysis** (overall and by BTCDOM regime):

   ```bash
   python "BTCDOM exercise/msm_btcdom/correlation_analysis.py" --timeseries-csv "BTCDOM exercise/msm_btcdom/out/msm_timeseries_with_btcdom.csv" --out-dir "BTCDOM exercise/msm_btcdom/out"
   ```

   Produces `funding_return_correlation_report.txt`, `.json`, and plots (`funding_vs_return_scatter.png`, `funding_return_rolling_corr.png`). Use `--format txt` or `--format json` or `--no-plot` as needed; `--regime-quantiles 4` (default) for quartiles.

3. **Exploration notebook**: open `funding_btcdom_exploration.ipynb`, set `DATA_PATH` to `out/msm_timeseries_with_btcdom.csv` (or full path), run all. Explores funding–return relationship, BTCDOM regimes, and implementable strategies:
   - **Gate**: only trade when BTCDOM above/below/between thresholds; compare cum return and hit rate vs always-on.
   - **Size by BTCDOM**: scale exposure by a function of BTCDOM (e.g. zero above 5k, full below 4k).
   - **Label + BTCDOM**: only trade when funding label is Green and BTCDOM in a chosen band.

## Config

`config.yaml`: default paths (`recon_path`, `timeseries_path`, `out_dir`) and optional gating defaults. CLI arguments override config.

## Files

| File | Purpose |
|------|--------|
| `join_btcdom.py` | Join recon.csv to MSM timeseries by asof(decision_date); optional gate filter. |
| `correlation_analysis.py` | Correlation(F_tk, y) overall and by BTCDOM regime; report + plots. |
| `funding_btcdom_exploration.ipynb` | Exploratory notebook: strength, strategies, findings. |
| `config.yaml` | Default paths and gate options. |
| `out/` | Joined CSV, gated CSV (if requested), correlation report, plots. |
