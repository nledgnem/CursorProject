# ADR 003 — Remove BTCDOM_Trend from the MRF gate

- **Date**: 2026-08-17
- **Status**: Accepted
- **Supersedes**: the BTCDOM condition in the Macro Regime Filter gate (not the field itself)
- **Related**: [ADR 001](001-data-lake-on-render-disk.md), [ADR 004](004-use-official-binance-btcdom-index.md), `docs/runbooks/btcdom_macro_index.md`

## Context

The Macro Regime Filter gate was:

```
is_mrf_active = (funding_regime == "Q2: Weak") AND (BTCDOM_Trend == "Rising")
```

It gates a **long-majors / short-alts** book. `BTCDOM_Trend` is derived as
`btcd_index_decision > sma_30` from `btcdom_reconstructed.csv`.

Three independent problems surfaced.

### 1. The premise inverts out of sample

A dominance measure was built independently (BTC ÷ top-50 point-in-time Binance
alt basket) and validated against the **real** Binance BTCDOM perp at
**+0.81 daily-change correlation**. It runs from 2019 — four years longer than
the production series — which is what made the test possible at all.

Conditioning the forward 20-day **BTC-minus-alts** return on "dominance above
its 30d SMA":

| Era | on | off | spread | HAC *t* |
|---|---|---|---|---|
| Full sample | +2.15% | −0.85% | +2.99pp | 1.68 |
| Train (pre-2022) | +1.65% | −5.65% | **+7.31pp** | 2.05 (p=0.04) |
| **OOS (2022+)** | +2.37% | +4.00% | **−1.63pp** | −1.37 |

Since 2022 the gate's premise has held with the **wrong sign** — "dominance
rising" was followed by alts *outperforming* BTC. The inversion is not itself
statistically significant (p=0.17), but the in-sample effect that justified the
gate does not survive, and the point estimate opposes the gate's logic.

### 2. It was not independent information

"BTC dominance is rising" and "long-majors/short-alts is working" are near
restatements of each other — the gate is momentum on the gated book's own P&L.
A plain trailing 30-day relative-momentum flag gives the **same reading on
84.9% of days** and produces the same spreads (+6.40pp train, −1.94pp OOS).

Trend-following your own equity curve is a legitimate technique, but it should
be adopted deliberately and justified as such, not carried as a macro input.

### 3. The production series was never long enough to detect either problem

`btcdom_reconstructed.csv` starts 2024-07-04 — under two years. Neither the
inversion nor the redundancy is visible in that window.

### Operational trigger

As of 2026-08-17 the daily Telegram snapshot read
`Regime: Q4: High | Unknown | GATE:ON` — `BTCDOM_Trend` was NULL in production,
so the gate had been **un-evaluable**, and one of the three regime components
had been dark. Investigating that is what prompted the analysis above.

Separately noted while investigating, and **not addressed here**: the repo-seed
copy of `btcdom_reconstructed.csv` (frozen at 2026-01-29, the old `TARGET_END`)
has 82 of 574 days with flat repeated values and **−0.007** daily-change
correlation with the real Binance BTCDOM series. The runbook states the repo
copy is a first-boot seed and not data, so this may not reflect the Render
copy — but it is worth verifying there.

> **Followed up in [ADR 004](004-use-official-binance-btcdom-index.md):** the
> Render copy has the same defect. The reconstruction is the real Binance index
> lagged one day (corr +0.76 at k=+1, +0.003 same-day), caused by the
> `fact_price.date` offset. It has been replaced by the official index.

## Decision

**Remove `BTCDOM_Trend` from the MRF gate.** The gate becomes:

```
is_mrf_active = (funding_regime == "Q2: Weak")
```

`pd.NA` when `funding_regime` is missing — the null channel from the
2026-02..07 silent-null incident is preserved.

**Keep `BTCDOM_Trend` as a context field.** `btcd_index_decision`, `sma_30`,
`BTCDOM_Trend` and `btcdom_7d_ret` are still computed and still written to
`msm_timeseries.csv` and `macro_features`. They are displayed, not consumed.

**Drop the parameter rather than ignore it.** `compute_mrf_gate` now takes one
argument. A silently-ignored `trend` argument would be the same class of defect
this module exists to prevent: every caller must be migrated consciously, and
an un-migrated two-argument call fails loudly with `TypeError`.

## Consequences

- The MRF gate is evaluable whenever funding data exists. A dark BTCDOM feed can
  no longer make it un-evaluable — which is the state production was in.
- Backtested `y_gated` / `y_filtered` series change. Charts produced by
  `scripts/generate_equity_curve_comparison.py` and
  `scripts/generate_underwater_chart.py` will differ from previously published
  versions. This is intended; they now reflect the funding-only gate.
- The gate is less selective, so the book will be deployed more often. That is a
  real exposure change, not just a code change.
- The BTCDOM guards were also rebalanced; see the second addendum.
## Addendum — the display layer, fixed in the same change

The gate removal alone would have left a second defect live, so it was fixed
here too.

### What was wrong

`_regime_label` rendered the gate with `bool(gate)`. In Python
**`bool(float("nan")) is True`**, so an *un-evaluable* gate displayed as
`GATE:ON` — a risk-on gate shown as open precisely when it could not be
evaluated at all.

Worse, the same logical NULL takes a different shape depending on the read path:

| Read path | Value | Old rendering |
|---|---|---|
| SQLite (`prev`) | `None` | `GATE:OFF` |
| pandas/CSV (new row) | `NaN` | `GATE:ON` |

`ingest_latest_master_csv` compares `prev` (SQLite) against the new row (CSV
frame), so identical data rendered as a *change* and fired
`MACRO REGIME CHANGE DETECTED`. The 2026-08-17 alert pair
(`Q4: High | Unknown | GATE:OFF -> Q4: High | Unknown | GATE:ON`, with both
other components byte-identical) is exactly this.

`funding_regime` had the same defect via `str()`: `str(None) == "None"` versus
`str(nan) == "nan"`. Now that funding is the *sole* gate input, a phantom change
there would matter more than before.

### What was done

- Added `is_missing`, `gate_label` and `format_regime_label` to
  `src/macro_regime/btcdom_trend.py`.
- **The gate now renders three states**: `GATE:ON` / `GATE:OFF` /
  `GATE:UNKNOWN`. An un-evaluable gate is neither open nor closed, and the
  nullable-gate design exists precisely to preserve that distinction.
- **Killed the duplication.** `_regime_label` existed verbatim in *both*
  `src/apathy_bleed/macro_snapshot.py` and `scripts/live/live_data_fetcher.py`
  — which is how one rendering bug shipped in two places at once. Both are now
  thin delegates to the canonical renderer.
- `msm_run.py`'s terminal status was already `pd.notna`-guarded, so it never had
  the `bool(nan)` defect — but it collapsed unknown into
  `"INACTIVE - HOLD 100% CASH"`, issuing a position *directive* from a value it
  did not have. It now reads
  `"UNKNOWN - GATE COULD NOT BE EVALUATED, DO NOT TRADE ON THIS"`.

### Consequences

- Phantom regime-change alerts stop: a given logical NULL now produces a
  byte-identical label regardless of read path, so an alert fires only when the
  regime actually changed.
- `GATE:UNKNOWN` is a **new string** in Telegram alerts and logs. Anything
  parsing those two-state labels needs to handle it.
- 27 new tests in `tests/test_btcdom_trend_null_safety.py` pin all of it,
  including the exact 2026-08-17 phantom-alert shape and a test that both call
  sites agree with the canonical renderer.

## Alternatives considered

- **Repair the reconstruction and keep the gate.** Rejected: even computed
  correctly from a validated dominance series, the premise does not hold out of
  sample. Fixing the data would not fix the signal.
- **Replace the reconstruction with the real `binance_btcdom.csv`** (610 rows,
  zero flat days, clean). Rejected for gating for the same reason; it remains the
  better source if the field is ever wanted for display or longer history.
- **Keep the gate but invert it.** Rejected: fitting a sign to an insignificant
  out-of-sample point estimate is exactly the overfitting this analysis exists to
  avoid.
- **Accept and ignore a `trend` argument** for call-site compatibility. Rejected
  as a silent-degradation pattern.

## Evidence

- `research/btc_trend_agreement/btcdom_value.py` — the test, rerunnable
- `research/btc_trend_agreement/results/tables/35_btcdom_proxy_validation.csv` — proxy validation
- `research/btc_trend_agreement/results/tables/36_btcdom_trend_value_add.csv` — the value-add table above
- `research/btc_trend_agreement/results/tables/37_btcdom_autocorr_diagnostics.csv` — redundancy diagnostics
- `research/btc_trend_agreement/results/figures/fig13_btcdom_value.png`
- `tests/test_btcdom_trend_null_safety.py` — pins the new gate contract


---

## Second addendum — guard severity, and the freshness mismatch

Two follow-ons, implemented after the gate change.

### Problem 1: a display-only field could halt the trading pipeline

Three tripwires still treated BTCDOM as critical: a missing-file `SystemExit`, a
3-day-stale `SystemExit` (both in `msm_run.py`), and `run_gold_layer_audit`
raising on `btcdom_7d_ret` holes. When any fired:

```
SystemExit -> run_live_pipeline.py exits non-zero
  -> heartbeat Telegram: "Strategy will not advance"
  -> _save_last_pipeline_success_date() never runs
  -> catch-up retries every 15 min, fails identically
  -> macro_features gets no new decision_date
```

So a stale *context* field stopped `Environment_APR`, `w_risk`,
`funding_regime` and `Fragmentation_Spread` — every input that actually drives
the gate — from updating. The severity was inverted: the field that mattered
least held hostage the ones that mattered most. Before ADR 003 this was correct,
because stale BTCDOM meant a wrong trading decision. That justification is gone.

**Principle applied:** *a tripwire's severity should match the blast radius of
what it guards.* Wrong decision → fatal. Wrong display → degrade and shout.

- `BTCDOM_IS_DECISION_INPUT = False` in `msm_run.py`. A named constant, not
  deleted code: it documents the reasoning and makes reverting one line.
- Missing / malformed / stale now log a WARNING and set `btcdom_status`
  (`ok` / `missing` / `malformed` / `stale:Nd` / `degraded`) instead of exiting.
  Columns are left absent, so `compute_btcdom_trend` yields NA and the field
  renders `Unknown` — which is only safe because the first addendum made
  absence visible end to end.
- `run_gold_layer_audit(df, *, btcdom_is_critical=False) -> list[str]`. BTCDOM
  checks are collected and returned; everything decision-relevant
  (`F_tk_apr` density and unit bounds, `y`, temporal desync, weekday
  consistency, empty frame) still raises. The incident regression tests pass
  `btcdom_is_critical=True`, so the strict path stays covered.

**Degrading must not mean going quiet.** Invisible degradation is exactly how
the 2026-02..07 incident survived six months, so:

- `btcdom_status` is persisted to the Gold layer and to `macro_features`
- the daily Telegram status appends a `⚠️ BTCDOM context feed: stale:34d` line
  **only when unhealthy** — daily nag when broken, no noise when fine
- `run_gold_layer_audit`'s warnings are logged by the caller
- a test (`test_degrading_btcdom_never_goes_silent`) pins that a degraded feed
  can never return zero warnings

`btcdom_status` is deliberately **not** part of the regime label: the label is
compared against `prev` to decide whether to fire a change alert, and a value
ticking `stale:33d` → `stale:34d` nightly would fire a phantom change every day.

Adding the column required a migration, since `_upsert_dataframe` builds its
INSERT list from the dataframe. `_ensure_columns` now reconciles
`macro_features` against the Gold layer on every run (idempotent
`ALTER TABLE ADD COLUMN`), which is safer than a one-off script nobody
remembers to run.

### Problem 2: the freshness guard and the merge disagreed

The guard tolerated **3 days** of drift; the merge was exact-date:

```python
df = df.merge(trend_df, on="decision_date", how="left")
```

So an index one day behind passed every check and still produced a NULL trend on
the newest row — and `run_gold_layer_audit` tolerates 2 trailing nulls, so it
didn't fire either. This is the most likely explanation for the live
`Q4: High | Unknown | GATE:ON` snapshot on 2026-08-17: not a dead feed, just two
rules disagreeing about what "fresh enough" means.

Now `pd.merge_asof(..., direction="backward", tolerance=3 days)`, so both rules
use the same tolerance. Carrying a ≤3-day-old dominance **level** onto a weekly
decision row is honest.

Deliberately **not** applied to `btcdom_7d_ret`: as-of matching the ends of a
return window would relabel an 8- or 9-day move as a 7-day return. That merge
stays exact, and a missing end stays NaN.

### Consequences

- A dark BTCDOM feed no longer stops the strategy from advancing. It produces a
  daily warning line and `Unknown` in the trend field.
- Historical `btcd_index_decision` / `sma_30` / `BTCDOM_Trend` values change
  where the index previously had gaps: rows that were NULL may now carry a
  ≤3-day-old level. Backfilled outputs will differ from prior published ones.
- `macro_features` gains a `btcdom_status` column on first run after deploy.
- Anything parsing the daily Telegram message must tolerate the new optional
  `⚠️ BTCDOM context feed:` line.