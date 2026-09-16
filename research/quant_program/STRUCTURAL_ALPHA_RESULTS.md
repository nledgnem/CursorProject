# Structural Alpha Results — Track B (v2, after external review)

*Research program, 2026-09-15. Survivorship-free Binance USD-M perp universe, daily bars, 2019-09 → 2026-09-14.
In-sample < 2024-01-01 ≤ out-of-sample. Personal research; nothing here places orders.*

v2 supersedes v1 (same date). The fixes and what they changed are in §2 and `REVIEW_RESPONSE.md`.

## 1. Bottom line

1. **Nothing survives multiple-testing control, under any variant.** Family: 23 alpha hypotheses, controls excluded.

   | BH variant | Lowest adjusted p for a *positive* signal |
   |---|---:|
   | Two-sided p, t distribution | 0.14 |
   | Two-sided p, wild-cluster bootstrap | 0.13 |
   | Directional (one-sided) p | 0.175 |

   The only adjusted p ≈ 0 is the carry trade, and it is significantly *negative*.
2. **C1 (funding interval shortened) is the only candidate left for forward testing.**
   - **Event mean:** +15.6% per event OOS, CI [+4.4%, +29.8%], after all costs.
   - **Portfolio:** OOS Sharpe 1.36.
   - **But it's outlier-driven:** the median event is −0.4%, and the top 5 of 113 events make up 81% of P&L.
   - **The price control can't judge it:** C1 fires on far less liquid alts than the controls (log-ADV SMD −1.41).
3. **Q4 and O1 are momentum-conditioned, not structural.**
   - Q4 still earns +1.96% per event OOS [+0.17%, +3.48%] on a larger 53-asset universe.
   - Against a like-for-like price-only control (balanced on ADV, beta and year), its excess is +0.7% [−0.5%, +1.9%].
   - O1's excess is +0.4% [−0.2%, +0.8%].
4. **Contrarian funding, basis fades, the delta-neutral carry trade, and O2–O4 are rejected as trades.**
5. **The v1 claim that "shorting after liquidations loses beyond the price effect" does not survive the control fix.**
   - Q3's excess moved from −4.8% [−7.7%, −0.3%] to −4.0% [−7.8%, +1.0%].
   - The shorts are still bad absolute trades (Q2 −4.8% [−8.9%, −1.2%]), but nothing incremental is established.
   - The pre-registered "short-liquidation continuation" test (backlog P1-3) remains a forward hypothesis only.
6. **Diversification is unchanged.** Every event portfolio has |ρ| ≤ 0.13 to BTC, the Gerhard 3-close book and the
   majors/alts L/S book.

## 2. What changed from v1

**Fixes applied** (triggered by external review; all have unit tests; 30 tests pass):

| # | Issue | Fix | Effect |
|---|---|---|---|
| 1 | Short-signal price controls flipped the control's **net** return, turning its costs into profit | Compare gross returns (hedged price + funding) | Short-signal excess shifted about 0.7–1.5pp; Q3 lost significance |
| 2 | Control bucket means held fixed in the bootstrap | Joint month-cluster bootstrap of signal and control | Wider CIs for non-overlapping samples. *Narrower* for overlapping ones (O1, O4), where month shocks cancel |
| 3 | Control comparability unmeasured | Balance diagnostics (standardised mean differences, SMD) | Controls are comparable only for CoinGlass-universe signals (§5) |
| 4 | BTC hedge leg paid no costs and no funding | Hedge pays \|β\| × 20bp round trip plus BTC perp funding | Net means fell about 0.2–0.5pp (F1 +1.2% → +0.7%; O1 portfolio Sharpe 0.79 → 0.35) |
| 5 | `np.nan_to_num` booked missing prices as 0% | Skip events with a missing entry bar; truncate at the last valid close | At most 8 skipped and 19 truncated per signal; none for CoinGlass signals (`portfolio_data_gaps.csv`) |
| 6 | p-values from a normal distribution despite 6–33 clusters | t distribution with (clusters − 1) df, plus wild-cluster bootstrap | p rises slightly |
| 7 | Controls included in the BH family | Excluded | Family size 25 → 23 |
| 8 | AQS could outrank contradictory evidence | Rule-based research verdicts cap the recommendation and drive the watchlist | See §3 |

**Unplanned change in the data.** The stage-4 backfill fix replaced empty TradFi bases with real crypto, so the
CoinGlass universe grew from 36–37 to 53–55 assets.
- CoinGlass-based signals (Q, O, X, U) therefore changed in both data and code.
- Q4's OOS mean is essentially unchanged on the larger cross-section (+1.98% → +1.96%), a modest same-period,
  new-asset replication.
- Non-CoinGlass signals (F, P, L1, C1, CB) have identical event sets, so their changes come only from the fixes.

**C1 look-ahead audit:** clean.
- `panel.py:71` builds `gap_h` as the gap to the *previous* funding print.
- `panel.py:134` compares it with a lagged 14-day median.
- Every day-t print settles before the day-t close, and entry is the t+1 open.

## 3. Ranked results with research verdicts

**Definitions**
- Net = per event at the primary horizon, BTC-hedged (hedge costs and funding included), after costs.
- The control test uses 5-day gross returns on the matched subset. `cov` = share of events matched to a control.
  "Balanced" = |SMD| ≤ 0.5 on log ADV, beta and year.

**Verdict rules** (`track_b/scorecards.py::verdict`), applied in order:

| Verdict | Condition |
|---|---|
| INSUFFICIENT EVIDENCE | Fewer than 30 OOS events |
| REJECT AS TRADE | OOS mean ≤ 0, or portfolio Sharpe ≤ 0 both IS and OOS |
| NO INCREMENTAL STRUCTURAL ALPHA | Matched and balanced control, excess CI not above 0 |
| FORWARD TEST ONLY | Net CI above 0 |
| WATCH | Otherwise |

| Signal | Verdict | AQS | OOS n | OOS net mean [95% CI] | p (t) | BH p | Excess vs price control | Top-5 share | OOS port. Sharpe |
|---|---|---:|---:|---|---:|---:|---|---:|---:|
| **C1** funding interval shortened | **FORWARD TEST ONLY** | 72.4 | 113 | +15.6% [+4.4, +29.8] | 0.025 | 0.14 | +15.2% [−0.2, +33.9]; cov 69%; **not comparable** (ADV SMD −1.41) | 81% | 1.36 |
| **Q4** short-liq + OI rebuild → long | NO INCREMENTAL | 79.0 | 1088 | +1.96% [+0.17, +3.48] | 0.031 | 0.14 | +0.7% [−0.5, +1.9]; cov 84%; balanced | 29% | 1.11 |
| **O1** price↑ OI↑ → long | NO INCREMENTAL | 66.6 | 2291 | +1.03% [−0.26, +2.22] | 0.14 | 0.40 | +0.4% [−0.2, +0.8]; cov 99%; balanced | 34% | 0.35 |
| Q1 long-liq flush + OI reset → long | NO INCREMENTAL | 58.2 | 844 | +0.91% [−1.12, +3.15] | 0.42 | 0.64 | +1.7% [−0.3, +3.6]; cov 87%; balanced | 61% | 0.73 |
| X3 capitulation rebound | NO INCREMENTAL | 58.6 | 287 | +0.24% [−2.04, +2.90] | 0.85 | 0.87 | +1.4% [−0.6, +3.6]; cov 82%; balanced | >100% | 0.72 |
| X2 short-squeeze continuation | WATCH | 62.2 | 215 | +0.95% [−1.20, +3.33] | 0.41 | 0.64 | +4.7% [+0.1, +11.9], but on only 36% of events | >100% | 0.68 |
| L1 new perp listing → short | WATCH (research only) | 50.6 | 414 | +0.57% [−6.22, +6.96] | 0.87 | 0.87 | no control (listings) | — | −0.02; **median +13.0%, OOS portfolio drawdown −71%** |
| CB2 Coinbase premium low → short BTC | INSUFFICIENT | 52.0 | 27 | +1.20% | 0.26 | 0.49 | — | >100% | 0.65 |
| CB1 Coinbase premium high → long BTC | INSUFFICIENT | 51.6 | 9 | −0.30% | 0.81 | 0.87 | — | — | −0.23 |
| X1 crowded long reversal | INSUFFICIENT | 35.2 | 21 | −29.4% [−64.9, −6.3] | 0.067 | 0.22 | — | — | −1.80 |
| F2 extreme negative funding → long | REJECT AS TRADE | 61.0 | 1465 | +0.44% [−1.47, +2.16] | 0.63 | 0.76 | not comparable (ADV SMD −1.58) | >100% | −0.53 (IS −0.47) |
| F1 extreme positive funding → short | REJECT AS TRADE | 49.2 | 629 | +0.68% [−2.53, +2.90] | 0.62 | 0.76 | not comparable | 87% | −0.36 (IS −1.07) |
| X4 funding despite reversal + OI | REJECT AS TRADE | 42.2 | 43 | −1.71% [−10.0, +4.3] | 0.66 | 0.76 | — | — | 0.16 |
| O4 price↓ OI↓ | REJECT AS TRADE | 40.4 | 2024 | −0.53% | 0.46 | 0.66 | +0.2% [−0.2, +0.7] | — | −0.98 |
| F4 funding > 100% annualised → short | REJECT AS TRADE | 39.0 | 294 | −4.87% [−13.5, +2.2] | 0.21 | 0.44 | not comparable | — | −0.41 |
| Q3 short squeeze + OI reset → short | REJECT AS TRADE | 38.4 | 152 | −3.78% [−6.60, −0.13] | 0.065 | 0.22 | −4.0% [−7.8, +1.0] | — | −1.13 |
| Q2 long-liq + OI rebuild → short | REJECT AS TRADE | 38.0 | 340 | −4.83% [−8.86, −1.19] | 0.015 | 0.13 | −3.1% [−8.3, +1.7] | — | −2.35 |
| F3 funding despite reversal → short | REJECT AS TRADE | 37.0 | 1734 | −1.17% | 0.18 | 0.42 | −0.3% | — | −0.43 |
| F1c delta-neutral carry | REJECT AS TRADE | 37.0 | 629 | −0.77% [−0.96, −0.58] | <0.001 | <0.001 (negative) | — | — | — |
| O2 price↑ OI↓ | REJECT AS TRADE | 35.2 | 52 | −0.56% | 0.60 | 0.76 | — | — | −0.18 |
| P1 premium z high → short | REJECT AS TRADE | 33.0 | 3416 | −1.12% [−2.30, +0.52] | 0.18 | 0.42 | +1.5% [+0.2, +3.7] on 37% of events; not comparable | — | −0.44 |
| P2 premium z low → long | REJECT AS TRADE | 33.0 | 6637 | −0.49% | 0.37 | 0.64 | not comparable | — | −1.73 |
| O3 price↓ OI↑ → short | REJECT AS TRADE | 29.4 | 104 | −1.92% [−3.38, −0.37] | 0.017 | 0.13 | −1.1% [−3.0, +0.6] | — | −1.19 |
| *U1 control: price ≥ +1σ → long* | CONTROL | — | 3790 | +0.23% [−0.81, +1.29] | 0.68 | — | — | — | −0.37 |
| *U2 control: price ≤ −1σ → long* | CONTROL | — | 3803 | −0.76% [−1.55, +0.17] | 0.12 | — | — | — | −1.39 |

**Machine-readable versions** (`results/tables/track_b/`):
- `signal_results.csv` — includes `OOS_p`, `OOS_p_wild`, `OOS_p_normal` and their `_bh` columns
- `control_matched_excess.csv` — includes SMDs and fixed-control CIs
- `scorecards.csv` / `scorecards.md` — includes verdict and gates
- `portfolio_stats.csv`, `portfolio_data_gaps.csv`, `correlations.csv`

## 4. Findings by family

### Funding mechanics (C1): FORWARD TEST ONLY
- **Mechanism:** Binance shortens the funding interval when funding hits the cap. 117 of 136 events have extreme
  negative funding, so the trade goes long and collects the capped rate.
- **Payoff shape:** carry plus a squeeze lottery. The OOS median event is −0.4%; the mean excluding the top 5 is
  +3.0%.
- **Capacity:** median ADV $15m. Alt leg costs 100bp; the median all-in round trip including the hedge is 106bp.
- **Why the control can't settle it:** a like-for-like control would need illiquid alts at extreme funding, which
  *is* the signal.
- **Verdict:** forward shadow log only (backlog P0-2 spec snapshots, P0-4 shadow log), with median and ex-top-5
  statistics as primary. Not tradeable at size.

### Liquidations (Q1–Q4) and OI/price (O1–O4): momentum/reversal conditioning
- **Balance is good.** In the CoinGlass universe, signal and control populations match (|SMD| ≤ 0.28 on ADV, beta and
  year), so the control test is informative.
- **Q4:** excess +0.7% [−0.5%, +1.9%].
- **O1:** excess +0.4% [−0.2%, +0.8%]. The OI filter adds at most a fraction of a percent to "buy what went up".
- **Portfolios:** the Q4 portfolio (Sharpe 1.11) beats the U1 control portfolio (−0.37). The two differ in event count
  and turnover, and the gap is not bootstrapped (backlog P1-6).
- **Shorts after liquidations (Q2, Q3):** losing trades, but no longer significantly worse than the price effect.
- **Keep** Q4 and O1 as benchmarks for any future liquidation or OI signal.

### Funding / basis (F1–F4, P1, P2, F1c): REJECT AS TRADE
- **Portfolios:** every contrarian funding and basis portfolio has negative Sharpe in both IS and OOS.
- **Carry (F1c):**

  | Component | Mean per event |
  |---|---:|
  | Funding received | +0.85% |
  | Premium convergence | +0.05% |
  | Costs | −1.52% |
  | **Net** | **−0.62%** (OOS −0.77% [−0.96%, −0.58%]) |

- **The controls are not comparable for this family** (ADV SMD −1.0 to −1.6). P1's matched-subset excess (+1.5%)
  covers only 37% of events and is not evidence of structure.

### Listings (L1): WATCH as research only
- The OOS median event is +13.0% (the short wins), but the OOS portfolio drew down −71%. Short convexity.
- Needs announcement timestamps, borrow availability and a pre-registered tail cap (P1-2).

### Interactions (X1–X4)
- **X2:** positive excess on a 36% matched subset, with the top 5 events > 100% of P&L. WATCH, no claim.
- **X4:** negative on the larger universe. Rejected.
- **X1:** 21 OOS events. Insufficient.

### Cross-venue (CB1/CB2): INSUFFICIENT EVIDENCE
- 9 and 27 OOS events; the portfolios are BTC beta (ρ +0.26 and −0.36).

### Intraday and weekend
Unchanged from v1:
- No persistent hour-of-day profile (IS–OOS correlation of hourly means −0.33 to +0.17).
- 21:00 UTC is positive in both periods for BTC (≈ +4bp), below fees.
- No day-of-week effect.

## 5. Control comparability (balance diagnostics, OOS)

| Signal group | log ADV SMD | Beta SMD | Year SMD | Control informative? |
|---|---|---|---|---|
| Q1, Q4, O1, O4, X3 (CoinGlass universe) | −0.0 to +0.1 | −0.0 to +0.24 | −0.18 to +0.28 | **Yes** |
| O2, O3 | −0.83, −0.63 | −0.52, −0.60 | +0.57, +0.37 | Partly (small, less liquid names) |
| C1, F1–F4, P1, P2 | −1.0 to −1.6 | varies | varies | **No:** the controls are top-55 CoinGlass assets |

A price-only control built on the full eligible universe (not only CoinGlass assets) would make funding, basis and C1
testable. It is a data-plumbing task, not a new hypothesis.

## 6. Correlations (daily, 2021+)

- Every event portfolio has |ρ| ≤ 0.13 to BTC and to Gerhard 3-close, and ≤ 0.10 to majors/alts L/S.
- CB1/CB2 are the exception, at ±0.3 to BTC.
- Duplicated bets:

  | Cluster | Members | Correlation to control |
  |---|---|---:|
  | Up-move momentum | U1, O1, Q4 | 0.7–0.9 |
  | Down-move reversal | U2, O4, Q1, X3 | 0.6–0.8 |

## 7. Monitor

- `python -m qlib.monitor` now takes its action from the research verdict.
- REJECT AS TRADE, INSUFFICIENT EVIDENCE and NO INCREMENTAL STRUCTURAL ALPHA all map to IGNORE.
- On 2026-09-14 all 24 hits were IGNORE (C1, X2 and L1 did not fire).
- The monitor never places orders.

## 8. Governance

- **Run manifest** is in `runs/`.
- **Checks passed:** duplicates, dtype, time sort, frozen tails.
- **`universe_changes`** gives an informational warning (2019–20, when only a handful of names were eligible).
- **Tests:** 30 unit tests pass.

## 9. Reproduce

```bash
python -m track_b.run
```
```bash
python -m track_b.controls
```
```bash
python -m track_b.scorecards
```
```bash
python -m track_b.charts
```
```bash
python -m qlib.monitor
```
