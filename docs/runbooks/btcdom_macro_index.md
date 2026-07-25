# Runbook: BTCDOM macro index (`btcdom_reconstructed.csv`)

Owner: macro/regime pipeline · Created 2026-07-22 after the silent-null incident below.

## What this file is

`data/curated/data_lake/btcdom_reconstructed.csv` — the reconstructed BTCDOM
**index level** (dimensionless index points, base ≈ 2448 at 2024-07-04). It is
**not** a dominance percentage; do not compare it directly to "BTC dominance is
56.3%". Documented sanity bound: `reconstructed_index_value < 6000`.

- **Producer:** `scripts/data_ingestion/btcdom_backfill.py` = **Step 3 (fatal)** of `run_live_pipeline.py`
- **Consumer:** `majors_alts_monitor/msm_funding_v0/msm_run.py` (Step 4), which derives
  `btcd_index_decision`, `sma_30`, `BTCDOM_Trend`, `btcdom_7d_ret`
- **Canonical copy:** the Render persistent disk. The repo copy is a **first-boot seed**
  (`repo_paths.data_lake_root()` resolution order, last resort) and will drift from
  Render. Do not treat the repo copy as data.

---

## The 2026-02-02 → 2026-07-21 incident

**Symptom.** `BTCDOM_Trend` in `msm_timeseries.csv` read `"Falling"` on 26 consecutive
weekly rows while BTC dominance actually rose (~54% → ~56.3%). `btcd_index_decision`,
`sma_30` and `btcdom_7d_ret` were all null on those rows; `BTCDOM_Trend` was never null.

**Chain of causes.**

1. `btcdom_backfill.py` had `TARGET_END = date(2026, 1, 29)`, a hardcoded constant.
   Added 2026-03-05 (`b17103f`) as a *research* bound, with a comment explaining it was
   temporary. Promoted into the live pipeline 2026-03-12 (`81e840e`) — and **the comment
   was deleted in the same commit while the constant was kept**.
2. The script rewrote the whole CSV every night, so its **mtime advanced daily** while
   its terminal date never moved. Existence checks and freshness-by-timestamp both passed.
3. `msm_run.py` left-merged on `decision_date`; every date after 2026-01-29 became NaN.
4. The trend was `np.where(index > sma, "Rising", "Falling")`. `NaN > NaN` is `False`,
   so **missing data rendered as "Falling"** — a direction, with no null channel.
5. `run_gold_layer_audit` did `df.dropna(subset=["btcdom_7d_ret"])` **before** asserting
   density, so it could only fire at 100% null. At 25% null it passed every night.
6. The fabricated value propagated to `macro_state.db`, the regime logs, and the
   **apathy_bleed 08:00 UTC Telegram snapshot** (~170 sends).

**Root cause was NOT lost data.** `fact_price` BTC coverage ran well past the cutoff
throughout. The producer was discarding it.

### A second, independent path bug (found 2026-07-22 while fixing the first)

The BTCDOM block in `msm_run.py` **rebound** `data_lake_dir` to
`Path(config["data"]["data_lake_dir"])` — the `msm_config.yaml` key that the same file
marks, 450 lines earlier, as *"deprecated and IGNORED"* because it is the relative
deploy-snapshot path `./data/curated/data_lake`.

Consequence on Render: **Step 3 wrote the index to the persistent disk while Step 4 read
the repo seed**, which only changes on deploy.

Verified — `btcd_index_decision` in the Drive `msm_timeseries.csv` matches the **repo**
copy to 6 decimal places on every date, and differs from the Render-disk copy by ~2%:

| decision_date | msm_timeseries | repo seed | Render disk (Drive) |
|---|---|---|---|
| 2025-12-22 | 4257.386745 | **4257.386745** | 4344.268492 |
| 2026-01-19 | 4109.356047 | **4109.356047** | 4199.579353 |
| 2026-01-26 | 4394.358227 | **4394.358227** | 4504.299499 |

**Fixing `TARGET_END` alone would not have fixed the incident** — the extended index would
have landed on a disk Step 4 never read. Both fixes are required.

This also corrects an earlier reading of the evidence. The observation that
`btcd_index_decision` was identical across three months of daily Render runs looked like
proof that the reconstruction is deterministic. It was not: Step 4 was reading a **static
committed file**. That observation carries no information about determinism.

**Note on the null dates.** The nulls begin 2026-02-02 but the constant only entered the
live tree on 2026-03-12. Nothing was deleted retroactively — the MSM pipeline was itself
lagging at 2026-01-19 until mid-March, so February rows were **never** computed correctly.
Verified: the Drive copy and the first commit of the CSV both end 2026-01-29.

---

## Guards now in place

| Guard | Location | Fires when |
|---|---|---|
| Coverage staleness | `btcdom_backfill.py` (`MAX_COVERAGE_LAG_DAYS`, default 3) | `fact_price` BTC coverage lags today — Step 2 likely failed |
| Index staleness | `msm_run.py` (`BTCDOM_FRESHNESS_THRESHOLD_DAYS`, default 3) | the CSV's max date lags today — Step 3 froze or failed |
| Nullable trend | `src/macro_regime/btcdom_trend.py` | never — it *cannot* emit a direction from a null |
| Bounded-tail rule | `data_quality_gate.py` (`MAX_TRAILING_INCOMPLETE_ROWS`, default 2) | interior nulls, or a null tail longer than the live window |

All four are fatal. A stale macro index now halts the run rather than producing a
confident wrong column.

---

## Diagnosing "the pipeline halted on BTCDOM"

```bash
tail -3 data/curated/data_lake/btcdom_reconstructed.csv
```

- **Terminal date is months old, file mtime is today** → the classic freeze. Check for a
  reintroduced absolute end date, or an unintended `BTCDOM_TARGET_END` env var on Render.
- **Terminal date tracks `fact_price` but both are old** → upstream problem. Step 2
  (CoinGecko price ingest) is the place to look, not this script.

```bash
python -c "import pandas as pd; d=pd.read_parquet('data/curated/data_lake/fact_price.parquet'); print(d[d.asset_id=='BTC']['date'].max())"
```

## Re-running the reconstruction

```bash
python scripts/data_ingestion/btcdom_backfill.py
```

Writes `btcdom_reconstructed.csv` **and** `btcdom_state.db` (repo root, tracked in git).
There is no env override for the state DB path, so a research run will dirty the repo —
use a throwaway worktree, or back up and restore `btcdom_state.db`.

Env overrides (research only, never set in production):

| Variable | Default | Purpose |
|---|---|---|
| `BTCDOM_TARGET_END` | unset | ISO date cap on the window |
| `BTCDOM_TAIL_MARGIN_DAYS` | `0` | drop N trailing days to dodge tail-end data drops |
| `BTCDOM_MAX_COVERAGE_LAG_DAYS` | `3` | fail if BTC price coverage lags today by more |
| `DATA_LAKE_ROOT` | unset | isolate the CSV output (does **not** move the state DB) |

## Validating a rebuild

There is no prior-correct version of 2026-02-02 → present to diff against — those rows
were never computed. Use these three instead; none is proof, all three passing is a
reasonable bar:

1. **Seam continuity (strongest).** The first newly-computed value must join the last
   frozen value without a step — a divisor-based reconstruction carries its divisor
   across. Use **2026-01-27** as the reference point, not the terminal row: the last two
   rows of the frozen file are identical (a duplicate that has not been explained).
2. **Sanity bound.** `reconstructed_index_value < 6000` on every row.
3. **Directional agreement.** Public BTC dominance fell to ~54% in early July 2026 and
   recovered to ~56.3%. BTCDOM is a futures index, not the dominance percentage, so this
   is correlation not equality — but a reconstruction that disagrees on direction over a
   move that size is wrong.

## After a rebuild — do not forget the backfill

Fixing forward does not repair history. The 26 fabricated `"Falling"` rows were UPSERTed
into `macro_state.db` keyed on `decision_date` and are still there until corrected.
Any Drive-side historical snapshot of `msm_timeseries.csv` carries them too.

## Second bug, exposed by the fix: NaN reconstruction crash (2026-07-24)

The first successful run on the fixed code un-froze the window and extended the
reconstruction into the 2026-01-29 → present range **for the first time in six
months**. It immediately hit a pre-existing latent bug in `index_calculator.py`
(untouched by the freshness fix):

```
File "index_calculator.py", line 242, in _apply_segment
    p_clamped = max(lb, min(ub, p_raw))
decimal.InvalidOperation
```

**Mechanism** — same class as the original incident, one layer down:
`_build_rebalance_params` read a **NaN `close`** for a top-20 constituent on a
rebalance date. The `d()` helper maps `None → Decimal("0")` but a float `NaN`
became `Decimal("NaN")`, which poisoned `rebalance_prices = btc/NaN = NaN`, hence
NaN clamp bounds. `Decimal` comparisons against NaN **raise** (unlike float NaN,
which compares False) — so the reconstruction crashed rather than silently
emitting a NaN index. Had it not crashed, the NaN would have poisoned the
segment's `numerator`/`divisor` and turned the whole rebalance segment's index to
NaN. This was dormant only because the frozen window never reached a basket with a
gappy price.

**Fix** (`index_calculator.py`):
- New `_to_decimal_opt()` returns `None` for NaN/inf/None/unparseable; a NaN can
  no longer become a usable `Decimal`.
- `_build_rebalance_params` now pulls a candidate buffer (`head(40)`) and selects
  the `TARGET_BASKET_N` (20) largest-cap assets **that have a finite, positive
  price**, refilling around gaps. Fails loud (`ValueError`) if fewer than
  `MIN_BASKET_N` (15) valid constituents exist — a degenerate basket halts rather
  than reconstructs.
- BTC close is NaN-guarded at ingest; a missing rebalance-date BTC price still
  raises, a missing segment-day price produces a gap (never a NaN row).

**Seam verified**: over the 547-row historical window (2024-07-04 → 2026-01-01),
new code and old code produce byte-identical output (max abs diff `0.0`). The fix
only changes dates that previously crashed. Regression coverage:
`tests/test_btcdom_reconstruction_nan_price.py`.

**Still needs Render-side validation**: the specific asset(s)/date(s) with the NaN
price live only in the Render `fact_price` (the window past the local seed's
2026-01-05 end), so the exact gap was not reproduced locally. Confirm on Render
with a read-only query, and seam-check the extended index at 2026-01-27 after the
next run.

## Two BTCDOM implementations — do not confuse them

There are now two reconstructions in the repo:

- **Production (authoritative):** `scripts/data_ingestion/btcdom_backfill.py` — pipeline **Step 3**, writes `btcdom_reconstructed.csv`, consumed by `msm_run.py` (Step 4). This is the one hardened in the 2026-07-24 incident: freshness tripwire, nullable trend, NaN-safe clamp, bounded-tail DQ gate, seam-validated.
- **Research (not wired in):** `btcdom/btcdom_lake_native.py` — a "lake-native" reconstruction from the LL Pro analysis work. Useful for exploration; **not** part of any pipeline.

**If the research one is ever promoted to replace Step 3, it inherits every guard the production one earned** — freshness tripwire, nullable/NaN-safe trend, bounded-tail gate, and a seam re-validation against the 2026-01-29 boundary. Swapping in a fresh reconstruction without those reintroduces the exact failure class this runbook exists for (silent fabrication on stale/gappy input). Until then, `btcdom_backfill.py` is the source of truth.

## Known open issues

- **The reconstruction is not value-stable across runs, and this is now UNEXPLAINED.**
  Three observations of `2026-01-29`: 4256.450127 (commit `b17103f`, 2026-03-05),
  4274.436719 (commit `6277bd4`, 2026-03-28), 4355.088554 (Render disk via Drive,
  2026-07-21) — a ~2.3% upward drift on a date whose inputs stopped changing months ago.
  Repo and Render-disk copies differ on 574 of 575 rows, back to 2024-07-05.
  The apparent stability of the Render lineage was an artifact of the path bug above
  (Step 4 read a static committed file), so it is **not** evidence of determinism.
  Candidates: the reconstruction is non-deterministic given identical inputs, or it is
  deterministic but `fact_price`/`fact_marketcap` are revised underneath it.
  **The separating test:** run `btcdom_backfill.py` twice against frozen inputs and diff
  the output. Identical output rules out non-determinism and leaves input revision.
  Worth doing before trusting the rebuilt series — note the state-DB caveat above.
- `index_calculator.py` uses `sort_values(...).head(20)` with pandas' default unstable
  quicksort. Exact float marketcap ties are effectively impossible, so this is a latent
  nit rather than a live cause.
