# Reviewer prompt (round 2)

## Addendum for round 2 (read first)

This bundle is the repository **after** a first external review.

**Start with these files:**
- `REVIEW_RESPONSE.md`: each earlier finding, whether it was fixed, and what changed.
- `track_a/robustness.py` and `results/tables/track_a/robustness_*.csv`: the entry-vs-exit split, windows ending
  2025-12-31 or before the Aug-2026 episode, transition-segment bootstrap, and leave-one-year/cycle-out.
- `track_b/controls.py`: gross-return controls, joint bootstrap, balance diagnostics.
- `track_b/studies.py`: hedge costs and funding, missing-data handling, t / wild-cluster p.
- `track_b/run.py`: BH family without controls.
- `track_b/scorecards.py`: rule-based verdicts.
- `track_b/panel.py`: the `interval_change` construction (previously missing).
- `qlib/stats.py`.
- `btc_confirmation_lag/backtest.py`: `pnl_from_target` (previously missing).

**Verify the fixes rather than re-deriving them.** Try to break them. In particular:
1. **Direction attribution:** is `directional()` in `track_a/robustness.py` a faithful "entries only / exits only"
   variant for long/flat?
2. **Controls:** are the joint-bootstrap CIs in `track_b/controls.py` correct when signal and control events overlap
   on the same asset-days?
3. **Verdict rule:** is the balance threshold (|SMD| ≤ 0.5) reasonable? Does the verdict rule contain any
   researcher degrees of freedom that would flip a verdict?
4. **Data change:** the CoinGlass universe changed between v1 and v2 (36 → 53 assets). Does anything in
   `REVIEW_RESPONSE.md` over-interpret v1-vs-v2 differences?

Everything below is the original round-1 prompt, unchanged.

---

You are a skeptical senior crypto quant reviewer. Your objective is to falsify the conclusions in this repository,
not summarize them or improve the backtests.

Treat every headline result as false until the code and evidence establish otherwise.

Do not perform new parameter searches and do not optimize any new thresholds.

For every issue, cite the exact file and line(s), and state which category it falls into:
- HARD CODE BUG
- LOOK-AHEAD / DATA LEAKAGE
- STATISTICAL INFERENCE PROBLEM
- MODEL-SELECTION / RESEARCHER-DEGREES-OF-FREEDOM PROBLEM
- COST / EXECUTION PROBLEM
- INTERPRETATION / ATTRIBUTION ERROR
- MINOR / NON-DECISION-CHANGING

Then state whether fixing it could change the recommendation.

## 1. BTC Gerhard fast-track

Try to disprove the recommendation.

### A. Is the reported walk-forward OOS genuinely independent?

Distinguish:
- model-level walk-forward OOS;
- genuinely untouched researcher OOS.

The research was performed after the 2026 data existed and was motivated partly by an August-2026 event. Determine
how much of the conclusion depends on 2022–2026, and specifically on 2026.

Re-run the exact frozen candidate rule ending 2025-12-31. Do not select new parameters.

### B. Effective sample size

The strategy has roughly 68 BTC transition events and about 25 round trips.

Assess whether the 60-day daily block bootstrap produces over-confident Sharpe-difference CIs, because the
economically independent observations are trend transitions or episodes rather than days.

Recalculate uncertainty using:
- episode / transition bootstrap;
- leave-one-year-out;
- if sensible, leave-one-major-regime/cycle-out.

Do not optimize anything.

### C. Entry versus exit attribution

The current fast rule appears to accelerate BOTH transitions into long exposure and transitions out of long
exposure. This is critical.

Run four frozen variants:
1. current 3-close;
2. fast upside entries only, normal downside exits;
3. normal upside entries, fast downside exits only;
4. fast in both directions.

Use the existing rule and threshold only.

Determine exactly how much of the reported improvement comes from entering rallies earlier versus exiting declines
earlier. Check whether the existing `economics_summary` already shows materially different `to_long` and
`to_short` effects.

### D. Threshold identification

The M5 grid starts at 0.5, and BTC selected 0.5 in every fold.

Determine whether this is genuine threshold stability or simply a boundary solution. Compare M5 with the
already-defined M1 one-close model.

Do NOT search below 0.5.

If the evidence only says "faster is better" rather than "0.5σ is special", say so explicitly.

### E. Remove the motivating event

Show results with the August-2026 episode removed, and with all of 2026 removed. This is a robustness test, not a
parameter search.

## 2. Track B price controls

Audit `track_b/controls.py` line by line. In particular, verify:

### A. Direction and sign accounting

The control uses net returns that already include transaction costs.

Check whether multiplying a long net control return by −1 for a short signal incorrectly turns transaction costs
into a positive return. Derive the formula explicitly.

If it is wrong, fix it and recompute every short-signal control comparison.

### B. Matching quality

The controls currently appear to match mainly on ret_z buckets. Assess whether signal and control observations
should also be conditioned or matched on:
- asset or asset characteristics;
- time / year / regime;
- volatility;
- ADV / liquidity;
- BTC beta.

Do not add dimensions automatically. First quantify whether the current signal and control populations materially
differ.

### C. Control uncertainty

Check whether the bootstrap treats the matched-control bucket mean as fixed. If so, determine whether the reported
excess CIs understate uncertainty.

Use a joint clustered bootstrap, or an equivalent regression / matched design, that propagates uncertainty from both
the signal and control samples.

### D. Coverage

Several signals have only 30–70% control coverage. Ensure that statements about "beating the price control" apply
only to the matched subset and are not generalised to all events.

## 3. BH / inference

Recompute the BH correction independently from the raw OOS p-values. Check:
- the number of hypotheses;
- whether controls should be included in the family;
- whether the carry variant is included;
- handling of NaNs;
- directional vs two-sided hypotheses.

Then audit the underlying p-values. The code appears to convert cluster t-statistics using the standard normal CDF.
Determine whether that is appropriate with only about 6–33 monthly clusters.

Recompute with an appropriate small-cluster method:
- a t distribution with cluster df, as a basic check;
- a wild-cluster bootstrap, if the infrastructure permits.

Do not loosen the significance threshold. State whether "nothing survives BH" changes.

## 4. Costs / entry timing / missing data

Audit every execution assumption. Check specifically:
- signal timestamp versus bar timestamp;
- next-open entry;
- off-by-one horizon errors;
- funding accrual window;
- entry and exit fees;
- slippage;
- turnover costs;
- BTC hedge transaction costs;
- BTC hedge funding / financing assumptions;
- missing prices;
- delistings;
- listing-day execution;
- stale / frozen candles.

Inspect `track_b/studies.py` carefully. Determine whether BTC-hedged returns pay transaction costs for BOTH the alpha
leg and the BTC hedge.

Check whether `np.nan_to_num` on return arrays can silently turn missing prices into 0% returns. Any missing return
that affects a held position must be handled explicitly, not silently replaced.

## 5. C1 look-ahead audit

C1 is the strongest Track B candidate and therefore deserves the most skepticism.

Trace `interval_change` all the way back to the raw funding observations. The historical funding interval is
reportedly inferred from funding-print spacing.

Prove that `interval_change` at signal time t can be known using ONLY information available by t. If determining the
shortened interval requires observing the next funding timestamp, C1 contains look-ahead and must be invalidated.

Also verify:
- funding cap data;
- funding sign;
- funding accrual after entry;
- interval timestamp;
- event timestamp;
- entry timestamp.

If required source code is missing, mark C1 UNVERIFIABLE rather than accepting the report's assertion.

## 6. Classification review

Review every WATCH and REJECT. Distinguish between:
- REJECT AS TRADE;
- NO INCREMENTAL STRUCTURAL ALPHA;
- INSUFFICIENT EVIDENCE;
- WORTH FORWARD TESTING.

Do not let a high AQS override contradictory evidence.

Pay particular attention to C1, Q4, O1, F1, F2, O4, CB1, X4, Q2, Q3, and P1/P2.

Flag any inconsistencies between the scorecard table, the narrative conclusions and `RESEARCH_BACKLOG.md`.

## 7. Missing dependencies

Do not trust self-reported statements such as "integrity checks passed". Trace the actual code.

If any required dependency is missing, including:
- `backtest.py` / `pnl_from_target`;
- feature construction;
- panel construction;
- `qlib.stats`;
- scoring / evidence-gate code;
- `interval_change` construction;

state exactly which conclusion cannot be verified. Do not infer correctness from documentation.

## 8. Final output

Do NOT summarize the research. Return:

**A. Critical errors.** Concrete file:line findings that could change a decision.

**B. Material methodology problems.** Issues that weaken confidence but may not reverse the sign.

**C. BTC Gerhard verdict.** Choose one, and explain what evidence would change it:
- KEEP 3-CLOSE
- PAPER FAST-TRACK
- PRODUCTION FAST-TRACK
- UNVERIFIABLE

**D. Track B verdict table.** For every signal, choose one:
- KEEP WATCH
- DROP
- REJECT AS TRADE
- FORWARD TEST ONLY
- UNVERIFIABLE

**E. Next tests.** At most 5, ranked by expected information value. No new parameter searches.

**F. Drop list.** Research branches that should receive no more historical optimization.

Be adversarial. If a conclusion survives the attack, say why.
