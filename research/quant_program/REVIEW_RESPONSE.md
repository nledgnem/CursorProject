# Response to External Review (2026-09-15)

For each claim: whether it is fair, what was verified in code, what was fixed, and what changed. Numbers are from
the v2 reruns.

**Overall:** the review is fair and valuable. Six of its seven code and statistics findings were real bugs or real
weaknesses. The seventh, C1 look-ahead, was a reasonable flag on missing code but turned out clean. The conclusions
move in the direction the reviewer expected:
- the BTC fast-track is downgraded to paper-only;
- C1 no longer "beats the control" (the control can't test it);
- nothing survives multiple testing, now under three BH variants.

## Point-by-point

| # | Review claim | Fair? | Verified | Fix / test | What changed |
|---|---|---|---|---|---|
| 1 | BTC gain doesn't isolate earlier entries from earlier exits | **Yes; the most important point** | `machine.py:60-74` applies the schedule to every transition. `economics_summary.csv`: to_long +1.75% [−0.38, +4.18], to_short +2.00% [+0.38, +4.14] | `track_a/robustness.py`: frozen fast/0.5σ rules for up-only, down-only and both | BTC ΔSharpe: both +0.23; **exits only +0.15**; entries only +0.09 (+0.05 without Aug-2026). Faster exits **hurt** ETH (−0.15, every leave-out negative) and SOL |
| 2 | 0.5σ is a boundary solution | **Yes** | `THRESH_GRID` starts at 0.5; M1 +0.232 > M5 +0.214 | None needed; no search below 0.5 | Wording changed to "faster was better on BTC since 2022", not "0.5σ is special" |
| 3 | Not researcher OOS; test ending 2025-12-31 | **Yes** | The question was posed after the 2026 data existed | Frozen rules on 2019-2025 and 2019-2026-07 | +0.19 without 2026; +0.20 without the Aug-2026 episode. The real weakness is 2019–21 (−0.05) |
| 4 | Daily block bootstrap is overconfident | **Fair concern; mostly not borne out** | 51 transition segments since 2019 | Segment bootstrap, leave-one-year-out, leave-one-cycle-out | Segment CI [+0.04, +0.60] vs block [+0.05, +0.48]. All 8 year and 5 cycle leave-outs positive (min +0.15). Largest segment 32% of gain |
| 5 | Short-control sign bug (costs flipped into profit) | **Yes, a real bug** | `controls.py:43` negated a net return | Compare gross returns; unit test `test_controls_short_signal_uses_gross_not_net` | Q3 excess −4.8% [−7.7, −0.3] → −4.0% [−7.8, +1.0]. The v1 "short-after-liquidation" structural claim is withdrawn |
| 6a | Control mean treated as fixed in the bootstrap | **Yes** | `controls.py` v1 | Joint month-cluster bootstrap | C1 excess CI [+0.1, +35.4] → [−0.2, +33.9]. Overlapping samples (O1, O4) get *narrower* CIs, because month shocks cancel; the reviewer assumed wider |
| 6b | Matching only on ret_z | **Yes; mattered more than expected** | Balance diagnostics added | SMDs reported; verdict rule requires balance | CoinGlass signals are well balanced. **C1, funding and basis are not** (log-ADV SMD −1.0 to −1.6), so the control can neither confirm nor refute them |
| 7 | BTC hedge costs omitted | **Yes, and BTC hedge funding too** | `studies.py:201, 268-277` | Hedge pays \|β\|×20bp and BTC perp funding | Net −0.2 to −0.5pp; O1 portfolio Sharpe 0.79 → 0.35; F1 +1.2% → +0.7% |
| 8 | `nan_to_num` books missing returns as 0% | **Yes** | `studies.py:274-275` | Skip or truncate; counts logged; unit test | Small in practice: at most 8 skipped and 19 truncated per signal, none in CoinGlass signals |
| 9a | BH arithmetic | **Correct, as the reviewer found** | Recomputed | — | — |
| 9b | Normal vs t for cluster t-stats | **Yes** | `studies.py:222` | `stats.cluster_p` (t, k−1 df) and `stats.wild_cluster_p`; unit tests | Min positive-signal BH p: 0.14 (t), 0.13 (wild), 0.175 (directional). The reviewer's ~0.26 was computed on v1 data |
| 9c | Controls shouldn't be in the BH family | **Yes** | — | Excluded (family 23) | — |
| 10 | C1 `interval_change` may use future prints | **Fair to flag** (`panel.py` wasn't in the zip); **not a problem** | `panel.py:71` `gap_h = ts.diff()` (gap to the previous print); `:134` lagged median; day-t prints settle before the close; entry t+1 open | None | C1 stays eligible for forward testing |
| 11 | Classifications too score-driven | **Mostly yes** | — | Rule-based verdicts (`scorecards.py::verdict`) cap the recommendation and drive the monitor; unit test | O1, Q4, Q1, X3 → NO INCREMENTAL. F1, F2, O4, X4 → REJECT AS TRADE. CB1, CB2, X1 → INSUFFICIENT. C1 → FORWARD TEST ONLY |

## Where I differ slightly from the review

- **Q4.**
  - *Review:* keep WATCH "narrowly".
  - *Verdict rule:* NO INCREMENTAL STRUCTURAL ALPHA, because the balanced control shows only +0.7% [−0.5, +1.9].
  - *Where we agree:* it is worth forward data collection, as the benchmark any liquidation signal must beat.
- **Joint bootstrap direction.** Resampling both samples does not always widen the CI. When signal and control events
  share months (and often the same asset-days), the paired resampling tightens it. That is correct, not optimistic.
- **Effective-sample concern.** It is legitimate, but the transition-level inference barely moved the lower bound.
  The case for paper-only rests on the attribution (point 1), the 2019–21 null, and research-period OOS, not on the
  bootstrap.

## Issues the review did not raise (found during the fixes)

1. **Data changed mid-review.** The backfill fix replaced empty TradFi bases in the CoinGlass top-60 with real
   crypto, so CoinGlass signals now cover 53–55 assets instead of 36–37.
   - v1 → v2 comparisons for Q, O and X signals mix code and data changes.
   - Q4 held on the larger cross-section: +1.98% → +1.96%.
2. **Carry-trade regression, introduced by fix 7 and caught before publishing.** Carry briefly charged the BTC hedge
   cost although it isn't BTC-hedged. Now uses the alt legs only; result unchanged at −0.62% per event.
3. **Two-sided BH counts the significantly negative carry trade as a "discovery".** This lowers other signals'
   adjusted p. The directional BH (0.175) is the cleaner headline.

## Verdicts requested by the review

**BTC Gerhard: PAPER FAST-TRACK.**
- Two frozen books (1-close, 0.5σ), with entry and exit legs logged separately.
- 3-close stays live.
- **Would upgrade:** the real-fill replay agrees, and a forward sample of 12 or more transitions keeps both legs
  non-negative.
- **Would drop:** the faster-exit leg turns negative forward, or the 2019–21-style null returns.

**Track B:**

| Verdict | Signals |
|---|---|
| FORWARD TEST ONLY | C1 |
| KEEP WATCH (research only) | X2, L1 |
| DROP as structural alpha / keep as benchmark | Q4, O1, Q1, X3 |
| REJECT AS TRADE | F1, F2, F3, F4, F1c, P1, P2, O2, O3, O4, Q2, Q3, X4 |
| INSUFFICIENT EVIDENCE | CB1, CB2, X1 |

**Next tests (≤ 5, no parameter search):**
1. Real-fill replay of BTC P1/P2 with entry and exit legs separated (P0-3).
2. Contract-spec snapshots, plus a C1 shadow log with median-based review (P0-2, P0-4).
3. Price-only control on the full eligible universe, so C1, funding and basis become testable (data plumbing).
4. Paired portfolio bootstrap, Q4 vs U1 (P1-6).
5. Liquidation event stream before any revisit of Q-family causality (P0-1).

**Drop list:**
- BTC threshold grids;
- more OI / funding / liquidation threshold combinations on daily data;
- daily basis-fade variants;
- hour-of-day mining;
- ML confirmation models before the entry/exit question is settled forward.
