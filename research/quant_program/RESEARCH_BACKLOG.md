# Research Backlog

*2026-09-15. Ordered by **expected research value (ERV)**.*

**ERV** = P(effect is real and tradeable) × value if real × (1 / effort), each scored 1–5 → product 1–125.

- **Value:** net P&L potential × capacity × diversification against the trend book.
- **Effort:** engineering plus data cost (5 = trivial).

**Priorities**
- **P0:** do now, cheap and time-sensitive (data that cannot be recovered later, or decisions that block everything
  else).
- **P1:** next 4–8 weeks.
- **P2:** opportunistic.
- **P3:** only if P0–P2 produce something.

**Rules for every item**
- Write the hypothesis, primary horizon and decision rule before touching the data.
- Test against the price-only controls.
- BH-adjust within the batch.
- A negative result closes the item.

## P0 — do now

| ID | Item | Why now | P | V | E | ERV | Done when |
|---|---|---|---:|---:|---:|---:|---|
| P0-1 | **Liquidation event collector** (Binance `forceOrder` WS, Bybit liquidation WS) → append-only parquet with timestamps | Intraday liquidation history cannot be recreated for free. Every liquidation idea (Q-family, X2, the continuation hypothesis) is blocked by daily CoinGlass aggregates with no intraday ordering | 3 | 4 | 4 | 48 | Collector running 30 days with a gap monitor; daily reconciliation against CoinGlass totals |
| P0-2 | **Contract-spec snapshotter**: daily `fundingInfo` (interval, cap/floor) plus `exchangeInfo` for all perps, point-in-time | C1 is the only control-beating effect, but its intervals are inferred from print spacing and caps are unknown historically. Snapshots also give listing/delisting and TradFi flags | 3 | 3 | 5 | 45 | 30 days of snapshots; C1 events reconciled against actual interval changes |
| P0-3 | **Replay faster BTC Gerhard confirmation and M4 staging on the actual signal/fill log, and start two frozen paper books** (P1 1-close, P2 0.5σ fast-track). **Log entry and exit decisions separately** | After external review, confidence is LOW–MEDIUM: 62% of the backtest gain is faster *exits*, the gain is ≈ 0 over 2019–21, and faster exits hurt ETH/SOL (`TREND_CONFIRMATION_RESEARCH.md` §8a). Only forward data is untouched out-of-sample | 3 | 3 | 4 | 36 | Replay documented; paper books running; review after 12 BTC transitions or 18 months, with the entry and exit legs judged separately |
| P0-4 | **Daily shadow log** of `qlib.monitor` hits (hypothetical next-open fills, recorded before outcomes) | The only source of genuinely untouched OOS evidence. Costs nothing; every week of delay is lost sample | 3 | 3 | 5 | 45 | Scheduled daily run; 90-day review of C1, Q4, O1 and P1-3 vs controls |
| P0-5 | **Lake bronze regression and Drive mount** (user decision: `git checkout HEAD -- <files>` or re-export) | Production and research data integrity; Drive is the authoritative lake and is not mounted | — | — | 5 | blocker | Lake files match the Drive export; `DATA_AND_SYSTEM_AUDIT.md` critical issues cleared |

## P1 — next 4–8 weeks

| ID | Item | Hypothesis / purpose | P | V | E | ERV |
|---|---|---|---:|---:|---:|---:|
| P1-1 | **Binance archive positioning metrics** (stage 5: top-trader L/S, taker buy/sell, OI; 20 perps) | Pre-register 3 hypotheses *before* opening the data: (a) extreme taker-buy imbalance + OI up → continuation; (b) top-trader L/S divergence from retail → fade retail; (c) OI reset after a >20% OI drawdown → long. Must beat price controls | 2 | 3 | 4 | 24 |
| P1-2 | **Listings with announcement timestamps and tail control** | L1: median +13%, win 67%, drawdown −71%. Test a basket short with a per-name cap and a stop fixed in advance (one value, no grid). Needs announcement times (Binance CMS API) and short availability | 2 | 4 | 3 | 24 |
| P1-3 | **Pooled "short-liquidation continuation" test** (pre-registered now): after a short-liquidation z ≥ 2 day, go long for 5d, BTC-hedged, vs U1 matched control | Q3 (−4.8% vs control, CI < 0), Q4 (+0.8%) and X2 (+2.2%) all point the same way, but the pattern was found post hoc. Evaluate on shadow-log data (P0-4) and on intraday timing once P0-1 has data; never re-run on 2024–26 to "confirm" | 2 | 3 | 4 | 24 |
| P1-4 | **Point-in-time token unlock / supply schedules** (build vs buy; see `DATA_GAP_ANALYSIS.md`) | The unlock short is the strongest structural prior not yet testable; the data gate is the whole problem | 3 | 4 | 2 | 24 |
| P1-5 | **C1 deep-dive after P0-2**: funding-cap proximity as a continuous feature, capacity at 1% ADV, exit rule fixed in advance | Determine whether the payoff is carry (scalable) or squeeze lottery (not). Report median-based and ex-top-5 statistics as primary | 2 | 3 | 4 | 24 |
| P1-6 | **Bootstrap test of portfolio Sharpe for Q4/O1 vs U1 control** (same construction, paired blocks) | The portfolio gap (1.20 / 0.79 vs −0.03) is the only remaining argument that OI/liquidations add value; test it properly or drop it | 2 | 2 | 5 | 20 |

## P2 — opportunistic

| ID | Item | Note | P | V | E | ERV |
|---|---|---|---:|---:|---:|---:|
| P2-1 | Binance **TradFi perps** (162 contracts: equities, commodities, indices, premarket) | New product: off-hours price discovery for single stocks. Overlaps with the Variational dark-events work. Separate universe and cost model; never mix into crypto studies | 2 | 3 | 3 | 18 |
| P2-2 | **Cross-venue funding dispersion** (Binance vs Bybit vs Hyperliquid; 37–60 coins already cached) | Venue-constrained arbitrage capital; Hyperliquid retail flow. Delta-neutral, so needs a realistic transfer/margin cost model | 2 | 3 | 3 | 18 |
| P2-3 | **21:00 UTC hour** as execution timing for trend entries | Only hour positive in both periods (BTC +4bp). Below fees as alpha; test as an order-timing preference in the P0-3 replay | 2 | 1 | 5 | 10 |
| P2-4 | Coinbase premium on **1h bars with USDT/USD FX correction** | Daily version is BTC beta with 9–27 OOS events | 1 | 2 | 4 | 8 |
| P2-5 | Carry (F1c) restricted to majors with **maker execution** | Funding is real (0.58% median) but four legs at alt costs kill it; viable only with total costs below ≈ 90bp | 1 | 3 | 3 | 9 |
| P2-6 | ~~Hygiene: exclude TradFi symbols from backfill `active_liquid`~~ **DONE 2026-09-15**: the first stage-5 run spent ~9 of 20 slots on XAU, SOXL, CL, SKHY, SNDK, SNXX, MU, BZ; filter added and stages 4–5 re-run | Pipeline correctness | — | — | 5 | done |

## P3 — only if something above works

| ID | Item | Condition to start |
|---|---|---|
| P3-1 | Non-linear / ML confirmation or signal models | Only after an interpretable baseline (M6 logistic, OOS AUC 0.705) is beaten OOS after costs with a pre-registered spec |
| P3-2 | Multi-signal combination book | At least two signals with BH p < 0.10 and above-control excess on shadow data |
| P3-3 | Sub-daily trend confirmation (4h closes) | Only if P0-3 shows the daily fast-track holds up on real fills |

## Done — external review fixes (2026-09-15)

| Fix | Where |
|---|---|
| Entry-vs-exit attribution and frozen-rule robustness: no 2026, no Aug-2026 episode, transition-segment bootstrap, leave-one-year-out and leave-one-cycle-out | `track_a/robustness.py` |
| Price-control comparison uses gross returns (v1 turned costs into profit for short signals); joint month-cluster bootstrap over signal and control; balance diagnostics | `track_b/controls.py` |
| BTC hedge leg pays its own round trip and BTC funding | `track_b/studies.py::evaluate`, `event_portfolio` |
| Missing prices while held truncate or skip a position instead of booking 0% (counts in `portfolio_data_gaps.csv`) | `track_b/studies.py::event_portfolio` |
| p-values: t distribution with (clusters−1) df and wild-cluster bootstrap; BH family excludes the price controls | `qlib/stats.py`, `track_b/run.py` |
| Rule-based research verdicts (REJECT AS TRADE / INSUFFICIENT EVIDENCE / NO INCREMENTAL STRUCTURAL ALPHA / FORWARD TEST ONLY / WATCH) override AQS on the watchlist | `track_b/scorecards.py`, `qlib/monitor.py` |
| C1 `interval_change` audited: built from the gap to the *previous* funding print, all day-t prints settle before the day-t close, entry is the t+1 open. **No look-ahead** | `track_b/panel.py:71,134` |

## Closed — do not revisit without new data

| Item | Result |
|---|---|
| Contrarian funding (F1, F3, F4) | Negative or unstable IS/OOS; crowded side wins over 5d |
| Basis fade (P1, P2) | Negative IS and OOS |
| Delta-neutral alt carry at taker costs | −0.62% per event, t −7.6 |
| Day-of-week effects | Not significant in-sample; OOS pattern absent IS |
| Hour-of-day profile (except 21:00) | IS–OOS correlation ≈ 0 |
| Generalising the BTC fast-track to ETH, SOL or LL | ETH walk-forward negative; LL first colour closes already confirm 96–100% |
| "Don't chase large day-1 moves" as a general rule | No general effect (BTC z ≥ 3 first closes +6.3% fwd10) |
