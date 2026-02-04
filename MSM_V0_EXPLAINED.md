# MSM v0 — What It Is and What We Did

## 1. What is MSM v0?

**MSM v0** is a small, **funding-only** analysis module that:

- Runs **weekly** (every Monday 00:00 UTC).
- Uses a **funding “thermometer”** (average funding across top alts) as the only feature.
- Compares **alt performance vs a BTC/ETH 70/30 benchmark** and labels weeks by how “hot” funding was (Red = low, Green = high).

It does **not** trade; it produces a **timeseries and summary stats** so you can see whether funding levels line up with alt outperformance/underperformance.

---

## 2. What We Built (The Module)

We implemented a **standalone module** on top of your existing data lake:

| File | Role |
|------|------|
| **msm_run.py** | CLI entrypoint: `--start-date`, `--end-date`, `--run-id`, `--sanity-check-only` |
| **msm_config.yaml** | All parameters: Monday anchor, 70/30 majors, top 30 alts, 7d funding, 60% coverage, 5 bins, v0.0/v0.1 modes |
| **msm_data.py** | Read-only loader: reads `fact_price`, `fact_marketcap`, `fact_funding`, `dim_asset` from the data lake |
| **msm_universe.py** | Picks top 30 ALTs by market cap at each Monday; excludes BTC, ETH, stablecoins, exchange/wrapped/liquid-staking/bridge tokens |
| **msm_feature.py** | For each week: 7-day mean funding per alt → basket mean **F_tk**; skips week if &lt;60% of basket has valid 7d funding |
| **msm_label.py** | Maps F_tk to 5 percentile bins (Red/Orange/Yellow/YellowGreen/Green). v0.0 = full sample; v0.1 = rolling 52 weeks (NA until 52 valid weeks) |
| **msm_returns.py** | As-of pricing: r_alts (equal-weight top 30), r_maj = 0.7×r_BTC + 0.3×r_ETH, **y = r_alts − r_maj**; skips if price coverage &lt;60% |
| **msm_outputs.py** | Writes CSVs and `run_manifest.json` under `reports/msm_funding_v0/<run_id>/` |

So: **we did not change your data lake**; we only **read** it and produced the MSM v0 outputs.

---

## 3. What a Run Does (Step by Step)

For each **Monday** in `[start_date, end_date]`:

1. **Universe**  
   Rank all eligible assets by market cap at that Monday; take top 30 (excluding majors and the exclusion list). If we can’t get 30, the week is skipped (`skipped_no_universe`).

2. **Feature**  
   For each of those 30 alts, compute the **7-day mean funding** over the 7 calendar days *before* that Monday.  
   - Basket feature **F_tk** = mean of those per-coin 7d means (only over alts with valid data).  
   - If fewer than 60% of the 30 have valid 7d funding, the week is skipped (`skipped_funding_coverage`).

3. **Returns**  
   Using **as-of** prices (latest available on or before the date):  
   - **r_alts** = equal-weight return of the 30 alts from this Monday to next Monday.  
   - **r_maj** = 0.7×r_BTC + 0.3×r_ETH over the same window.  
   - **y** = r_alts − r_maj (alt outperformance vs benchmark).  
   If price coverage for alts or majors is too low, the week is skipped (`skipped_price_coverage` or `skipped_returns_computation`).

4. **Labels**  
   - **v0.0 (FULL_SAMPLE)**  
     Percentiles of F_tk are computed over **all** weeks in the run. Each week gets a bin (Red … Green) and a percentile **p_v0_0**.  
   - **v0.1 (ROLLING_PAST_52W)**  
     Percentiles are computed over the **prior 52 valid weeks** only. If there are fewer than 52 prior valid weeks, **label_v0_1** and **p_v0_1** are NA. After 52 weeks, every week gets a v0.1 label.

5. **Output**  
   One row per valid week in `msm_timeseries.csv`; summaries by label in `summary_by_label_v0_0.csv` and `summary_by_label_v0_1.csv`; config and metadata in `run_manifest.json`.

So in one sentence: **we did** a weekly pipeline that (1) selects top 30 alts, (2) computes 7d mean funding and F_tk, (3) computes y = r_alts − r_maj with as-of prices, (4) labels by funding percentiles (v0.0 and v0.1), and (5) writes the tables and manifest.

---

## 4. What the Results Mean

### 4.1 Run we care about: `msm_v0_2024_02_to_2026_01`

- **102 decision dates** (Mondays from 2024-02-05 to 2026-01-12).  
- **102 valid weeks** (0% rejection).  
- So every Monday in that range had enough funding data, enough price data, and a valid universe.

### 4.2 Main output: `msm_timeseries.csv`

Each row = one Monday (decision date) and the next Monday (next_date).

| Column | Meaning |
|--------|--------|
| **decision_date** | Monday we “decide” (observe F_tk and assign label). |
| **next_date** | Next Monday (end of the return window). |
| **basket_hash / basket_members** | Which 30 alts were in the basket (order can differ). |
| **n_valid** | How many of the 30 had valid 7d funding (used to compute F_tk). |
| **coverage** | Same as n_valid/30 in % (e.g. 90 = 27/30). |
| **F_tk** | Basket 7d mean funding (the “thermometer” value). |
| **label_v0_0** | Red / Orange / Yellow / YellowGreen / Green (from full-sample percentiles). |
| **label_v0_1** | Same 5 labels but from rolling 52-week percentiles; **NA** for the first 52 valid weeks. |
| **p_v0_0, p_v0_1** | Percentile rank of F_tk (0–100) in full sample and in rolling 52w. |
| **r_alts** | Equal-weight weekly return of the 30 alts. |
| **r_btc, r_eth** | Weekly returns of BTC and ETH. |
| **r_maj_weighted** | 0.7×r_btc + 0.3×r_eth (the benchmark return). |
| **y** | **r_alts − r_maj_weighted** (alts vs 70/30 benchmark). Positive = alts beat the benchmark that week. |

So **results** here = one row per week with: funding feature (F_tk), two labeling schemes (v0.0 and v0.1), and the outcome **y** (what we’d use for backtests or strategy ideas).

### 4.3 Summaries: `summary_by_label_v0_0.csv` and `summary_by_label_v0_1.csv`

Each file has one row per label (Red, Orange, Yellow, YellowGreen, Green):

| Column | Meaning |
|--------|--------|
| **label** | Red / Orange / Yellow / YellowGreen / Green. |
| **count** | Number of weeks with that label. |
| **mean_y** | Average **y** (r_alts − r_maj) when that label was observed. |
| **median_y** | Median **y** for that label. |
| **std_y** | Standard deviation of **y** for that label. |
| **hit_rate** | Fraction of those weeks where **y &lt; 0** (alts underperformed the benchmark). So *low* hit_rate = alts tended to *outperform* when that label was on. |

Interpretation idea:

- If **Red** (low funding) has **low hit_rate** (alts often beat the benchmark), that’s “low funding → alts outperformed.”
- If **Green** (high funding) has **high hit_rate** (alts often underperformed), that’s “high funding → alts underperformed.”

Your current numbers (v0.0):

- Red: mean_y ≈ 0.0003, hit_rate ≈ 38% → slight outperformance when funding was low.
- Yellow: mean_y ≈ −0.0215, hit_rate 65% → underperformance when funding was mid.
- Green: mean_y ≈ −0.013, hit_rate 62% → slight underperformance when funding was high.

So **what we did** in terms of “results” is: we produced these tables and the manifest; the **interpretation** is that you look at how **y** and **hit_rate** vary by label (and possibly over time).

### 4.4 v0.0 vs v0.1

- **v0.0**  
  Uses **all** 102 weeks to define percentiles. Good for exploration; not for “realistic” backtest (uses future info).  
  → **102 weeks** get a label.

- **v0.1**  
  Uses only the **previous 52 valid weeks** to define percentiles. No look-ahead; first 52 weeks get **NA**.  
  → In your run, **50 weeks** get a v0.1 label (from 2025-02-03 onward).

So for “what we did”: we produced **both** labeling modes; v0.1 is the one to use for any forward-looking or realistic evaluation.

### 4.5 `run_manifest.json`

- **config**  
  Full MSM v0 config (dates, universe, feature, label, returns, outputs).

- **metadata**  
  - `n_weeks` / `n_decision_dates`  
  - `rejection_counts`: skipped_no_universe, skipped_funding_coverage, skipped_price_coverage, skipped_returns_computation  
  - `rejection_rate`  
  - **sanity_check**: date ranges and coverage for funding, marketcap, price, and top-N assets.

So the **results** also include this manifest for reproducibility and for knowing exactly what data range and rules were used.

---

## 5. Short Summary

- **What MSM v0 is:** A weekly, funding-only monitor: one feature (7d mean funding across top 30 alts), two labeling modes (full-sample vs rolling 52w), and a single outcome **y** = alts vs BTC/ETH 70/30.
- **What we did:**  
  - Implemented the module (run, config, data, universe, feature, label, returns, outputs).  
  - Ran it on your lake from 2024-02-01 to 2026-01-13.  
  - Got 102/102 valid weeks, 102 v0.0 labels and 50 v0.1 labels, and wrote `msm_timeseries.csv`, the two summary-by-label CSVs, and `run_manifest.json`.
- **How to use the results:**  
  - Use **msm_timeseries.csv** for time series of F_tk, labels, and **y**.  
  - Use **summary_by_label_v0_0.csv** and **summary_by_label_v0_1.csv** to see mean/median/std of **y** and **hit_rate** by funding bin.  
  - Prefer **v0.1** for any assessment that should avoid look-ahead; use **v0.0** for exploratory checks.

If you want, we can next add a one-page “how to run it again” (exact command + how to point at a different date range or run_id).
