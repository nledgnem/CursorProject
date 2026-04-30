# STRATEGIES.md — Apathy Bleed

> **⚠️ This file is the single source of truth in the repo (`docs/STRATEGIES.md`).** Edits made directly in the Drive copy will be overwritten on the next nightly sync from Render. To update: edit the repo copy, commit, push to main. Render's nightly export will propagate to Drive (preserving file ID), and Drive Desktop will sync to your PC.

**Last updated:** 2026-04-29 (second update same day) — refreshed §6 realized outcomes (DEXE and PIEVERSE closed at the 60% stop level) and added new entries to §7 / §8 reflecting first losses; **`[DECISION 2026-04-29 — Mads + Dan]`** CoinGecko ingestion universe reduction from top ~3,000 → top 1,000 by mcap added to §2 universe note (status: PENDING EXECUTION, full context in DATA_LAKE_CONTEXT.md §4 and `data_dictionary.yaml::data_sources.coingecko.ingestion_universe`). Original document dated 2026-04-09; sections 1, 3–5 substantively unchanged.

## 1. Thesis

Altcoin hype cycles are structurally finite. When a mid/small-cap altcoin experiences a violent spot pump (often narrative-driven — AI, meme, RWA), retail liquidity eventually exhausts and the coin bleeds back toward (or below) its pre-pump BTC-relative valuation over the following 3–6 months. Apathy Bleed monetizes this decay by shorting a basket of recent top outperformers against a BTC long hedge. The alpha source is the asymmetry between the speed of hype (weeks) and the duration of decay (months). The strategy performs best in "Golden Pocket" regimes (Environment APR 5–15%) where leverage is elevated and subsequent altcoin decay is most violent and reliable. It is a mean-reversion strategy on the relative performance of altcoins vs BTC, not a directional crypto bet. `[VERIFIED — backtested across 12 clean cohorts with 100% win rate on the pair trade, all costs in]`

---

## 2. Universe and Structure

### Long Leg
- **Instrument:** BTC perpetual future `[VERIFIED]`
- **Sizing:** Notional-matched to total short notional (1:1 ratio) `[VERIFIED]`
- **Venue:** Variational (primary), Hyperliquid (secondary) `[VERIFIED]`
- **Beta-hedged alternative tested:** BTC-only beta-hedged (size BTC to basket's rolling 30d beta) improved alpha from 75% → 92% but doubled worst drawdown from -30% to -64%. BTC+ETH two-factor hedge (ETH 0–30%) degraded Sharpe from 3.31 → 1.71–1.97. **Current decision: notional match.** `[VERIFIED]`
- **Mads' view:** He believes ETH should eventually be part of the hedge leg since alts correlate more with ETH, but acknowledged the backtest period had BTC dominance. Open to revisiting. `[VERIFIED]`

### Short Leg
- **Instruments:** Altcoin perpetual futures, equal-weighted per leg `[VERIFIED]`
- **Per-leg notional:** $3,000 in current live deployment `[VERIFIED]`
- **Sizing rationale:** Small scale for forward validation; purpose is experience, not profit `[VERIFIED]`

### Universe Exclusions (hardcoded)
Three explicit lists plus two algorithmic filters `[VERIFIED]`:
- **Derived tokens (14):** WBTC, STETH, CBETH, RETH, WBETH, WETH, BTCB, RBTC, MBTC, LBTC, HBTC, TBTC, SXP, SUSD
- **Exchange + mega-cap (34):** BTC, ETH, SOL, BNB, XRP, ADA, DOT, TRX, LTC, BCH, OKB, KCS, HT, GT, MX, BGB, LEO, CRO, FTT, VGX, WRX, COCOS, DYDX, GMX, CET, WOO, KNC, CRV, AERO, CAKE, RAY, JUP, UNI, SUSHI
- **Stablecoins (20):** USDT, USDC, DAI, FDUSD, TUSD, USDP, PYUSD, USDE, FRAX, LUSD, BUSD, UST, USDS, USDD, EUSD, EURC, EURT, USTC, PAXG, XAUT
- **Junk filter:** Ticker length ≤ 2 characters → excluded
- **Junk filter:** Ticker contains any numeric digit → excluded
- After all filters: 200 panel tickers → 161 eligible `[VERIFIED]`
- **Upstream universe note:** the panel itself is built from the CoinGecko ingestion universe — currently 2,997 sticky-allowlist coins, decided on 2026-04-29 to reduce to top 1,000 by mcap (PENDING EXECUTION). The "200 panel tickers" count is post-strategy-filter and post-allowlist; after the cut, expect this to drop materially (probably ~150–180 panel tickers, ~120–150 eligible — verify after the allowlist refresh). Full context in `DATA_LAKE_CONTEXT.md` §4 and `data_dictionary.yaml::data_sources.coingecko.ingestion_universe`.

### Cohort Design
- **Why cohorts:** Fixed formation windows create a clean snapshot of "who pumped most," preventing signal drift. The 45-day window acts as a natural noise filter — only coins with sustained momentum make the cut. `[VERIFIED]`
- **Formation window:** 45 calendar days (trailing lookback) `[VERIFIED — swept 30, 45, 60, 75, 90, 120, 150 days; 45d consistently top-ranked]`
- **Execution lag:** 0 days (immediate) `[VERIFIED — lag tested at 0, 7, 14, 15, 30, 60 days; 0d won decisively]`
- **Holding period:** 150 days in backtest baseline `[VERIFIED]`. Mads preferred 180 days for clean 4-cohort rotation (180 ÷ 45 = 4). Backtested: 180d comparable Sharpe but ~17% worse worst-case drawdown. 135d (3 × 45) was tightest on risk. **Current live deployment uses variable hold-days per cohort due to seeded entry.** `[VERIFIED]`
- **Cohort cadence:** Every 45 days, non-overlapping formation windows `[VERIFIED]`
- **Steady-state book:** ~3–4 overlapping cohorts, ~15–20 positions if picking 3–5 per cohort `[VERIFIED]`
- **No position doubling:** If a coin appears in consecutive cohorts' top 7, skip it in the newer one. Original exit date stands, no extensions. `[VERIFIED — Mads initially considered extending, then changed his mind]`

---

## 3. Entry Rules — Current State

### Signal Generation
- **Method:** Hybrid — systematic scanner + discretionary overlay `[VERIFIED]`
- **Scanner:** Ranks all eligible altcoins by 45-day relative return vs BTC. Outputs Top 7 for discretionary review. Shows 6 return numbers per coin: formation, post-formation, and total in both USD and vs-BTC terms. `[VERIFIED]`
- **Discretionary selection:** Mads and Dan review the Top 7 and pick 3–7 per cohort. Selection criteria include narrative risk, perp liquidity, sector concentration, post-formation return direction, and formation return magnitude. `[VERIFIED]`
- **Who decides:** Mads has final authority on coin selection. Dan executes. `[VERIFIED]`

### Gates
1. **Pre-Listing Bias / Perp Check:** Coin must have `is_perp_active == 1` on the execution day in panel data, AND must have a listed perp on Variational or Hyperliquid (cross-referenced via `perp_coverage_summary.csv`). `[VERIFIED]`
2. **Cold Flush Macro Gate:** If `Environment_APR < 2.0%` (in percentage points), no new positions entered. Only 1 trigger in sample period (2025-03-10, APR=0.57), no cohort was affected due to MS snapping. `[VERIFIED]` Gate is wired but effectively untested with real cold-period data. `[BELIEF — insufficient cold periods in sample to validate empirically]`
3. **October 2025 Anomaly Quarantine:** Any cohort whose lifecycle touches Oct 2025 is quarantined from clean metrics. `[VERIFIED — backtest-only gate, not relevant for live trading going forward]`

### Venue Filter
- Variational covers most candidates (21 of 28 TOP7 across seeded cohorts)
- Hyperliquid covers 9 of 28
- Several coins only available on Variational, making it the primary execution venue `[VERIFIED]`

---

## 4. Exit Rules

### Stop-Loss
- **Level:** 60% adverse excursion per individual short leg (if shorted at $1.00, stop at $1.60) `[VERIFIED]`
- **Implementation:** Set manually as stop-loss orders on Variational UI at entry time `[VERIFIED]`
- **Why 60% and not 50%:** Swept 20%–150% + no-stop. 60% dominated (avg Sharpe 2.57 across setups). 50% triggered too many false stop-outs — coins that briefly spiked +55% then collapsed. The extra 10 cents of breathing room materially improved returns. `[VERIFIED]`
- **Override:** Mads/Dan can manually close before stop if discretionary judgment warrants (see ARIA close below). `[VERIFIED]`
- **Telegram alerts:** Warning at 45% adverse, Critical at 55%, Stop Hit at 60% — checked hourly. `[VERIFIED]`

### Time Exit
- Each position has a hard exit date = entry_date + hold_days (varies by seeded cohort) `[VERIFIED]`
- No extensions — Mads explicitly decided against this `[VERIFIED]`
- Telegram reminders at 7, 3, and 1 day before expiry `[VERIFIED]`

### Manual Override
- Mads/Dan can close any position at any time via Variational UI `[VERIFIED]`
- Must run `apathy_close_trade.py` to update the book CSV after manual close `[VERIFIED]`

### No partial-take or scaling rules defined `[VERIFIED — not discussed]`

---

## 5. Live Operations

### Scripts Running on Render
- **`system_heartbeat.py`** — foreground process, runs live pipeline at 00:05 UTC daily, hosts Streamlit dashboard `[VERIFIED]`
- **`scripts/apathy_alert_runner.py`** — background watchdog loop in `start_render.sh`, auto-restarts on crash `[VERIFIED]`
- **`scripts/danlongshort_alert_runner.py`** — separate strategy, also in watchdog loop `[VERIFIED — from recent_chats]`

### Alert Types and Cadences
- **~00:05 UTC daily:** Macro regime status (APR, fragmentation, gate status) — from `live_data_fetcher.py` `[VERIFIED]`
- **08:00 UTC daily:** Apathy Bleed portfolio snapshot (positions, mark prices from Variational public API, unrealized PnL, nearest expiry, nearest stop, regime) + exit date reminders + scanner cohort reminders `[VERIFIED]`
- **Hourly:** Stop proximity checks — only sends Telegram if a position breaches 45%/55%/60% thresholds. Dedup: one alert per position per UTC hour per tier; tier crossing fires immediately. `[VERIFIED]`
- **Scanner reminder:** Fires at 40, 43, 45 days since last cohort entry `[VERIFIED]`

### State Files (on Render persistent disk `/data/`)
- `/data/apathy_bleed_book.csv` — trade book, append-only, source of truth for all positions `[VERIFIED]`
- `/data/apathy_alert_log.csv` — audit trail of all alerts sent `[VERIFIED]`
- `/data/apathy_stop_proximity_state.json` — dedup state for stop alerts `[VERIFIED]`
- `/data/apathy_daily_bundle_state.json` — dedup for daily 08:00 bundle `[VERIFIED]`
- `/data/apathy_daily_snapshot_state.json` — dedup for portfolio snapshot `[VERIFIED]`

### Position Monitoring Workaround
- Variational has NO position API (trading API still in development). Portfolio snapshot is reconstructed from: book CSV (entry prices, notional) + Variational public read-only API mark prices (`GET /metadata/stats`, no auth). `[VERIFIED]`
- If a position is closed on Variational UI but not logged via `apathy_close_trade.py`, the bot will keep tracking a phantom position. Operational discipline required. `[VERIFIED]`

### Book Sync: Local → Render
- On deploy, `start_render.sh` does an append-only `trade_id` merge: rows in repo snapshot that don't exist in `/data` are appended. Never overwrites or deletes existing `/data` rows. `[VERIFIED]`

### Config
- `configs/apathy_alerts.yaml` — all thresholds, paths, schedule knobs `[VERIFIED]`
- `configs/perp_listings.yaml` — venue API endpoints, ticker normalization rules `[VERIFIED]`

---

## 6. Realized Outcomes So Far

### Closed Positions

| Ticker | Cohort | Entry Price | Exit Price | PnL % | PnL USD | Exit Date | Reason |
|---|---|---|---|---|---|---|---|
| ARIA | C4 | $0.7483 | $0.2109 | **+71.8%** | **+$2,157** | 2026-04-09 | Manual close — dumped within hours of entry, took profit |
| DEXE | C4 | $8.0697 | $13.1237 | **−62.6%** | **−$1,861** | 2026-04-18 | Manual close at/marginally past +60% stop level — coin pumped post-entry |
| PIEVERSE | C2 | $0.5087 | $0.8156 | **−60.3%** | **−$1,809** | 2026-04-20 | Manual close at +60% stop level — coin pumped post-entry |

`[VERIFIED — PnL math reproduced from book to 8 decimals: pnl_pct = (entry − exit) / entry; pnl_usd = pnl_pct × notional. Net realized: −$1,512.92.]`

### Replacements
- SIGN entered as ARIA replacement in C4 at $0.03214, $2,995 notional, stop at $0.05142 `[VERIFIED]`
- **No replacements were entered for DEXE or PIEVERSE.** Their cohorts (C4, C2) carry one fewer short leg each going forward. `[VERIFIED]`

### Open Positions (as of 2026-04-29)
- **13 short legs** across 4 cohorts:
  - C1: 3 (ZEC, DASH, ZEN)
  - C2: 3 (ICNT, CHZ, FARTCOIN) — PIEVERSE closed
  - C3: 4 (KITE, AXS, MORPHO, STABLE)
  - C4: 3 (ONT, TAO, SIGN) — ARIA closed and replaced; DEXE closed without replacement
- **4 BTC long hedges** (one per cohort, notional unchanged from formation)
- **Total open short notional:** ~$39,150
- **Total BTC long notional:** ~$45,128 (unchanged from formation; **BTC hedges were not reduced when shorts closed early**)
- **Resulting BTC bias:** ~+$6,000 net long BTC across the book — over-hedged after closes. See §8 for the open question on hedge rebalancing policy.
- **Deposit:** $30,000 (~2.6× effective leverage on remaining open shorts)
- **Net realized PnL:** **−$1,513** (ARIA +$2,157, DEXE −$1,861, PIEVERSE −$1,809) `[VERIFIED]`

### Cohort Exit Dates
- C1 (ZEC, DASH, ZEN): May 19, 2026 — 40 days from entry
- C2 (ICNT, CHZ, FARTCOIN): Jul 3, 2026 — 85 days [PIEVERSE closed early on 2026-04-20]
- C3 (KITE, AXS, MORPHO, STABLE): Aug 17, 2026 — 130 days
- C4 (ONT, TAO, SIGN): Oct 1, 2026 — 175 days [ARIA closed 2026-04-09 and replaced by SIGN; DEXE closed early on 2026-04-18]

---

## 7. Lessons Learned

### What Worked
1. **ARIA validated the thesis spectacularly.** +614% formation return → dumped 72% within hours of entry. This is exactly the "retail hype exhaustion" the strategy bets on. The formation return magnitude was the highest in the basket and produced the fastest/largest profit. `[VERIFIED]`

2. **Discretionary overlay added value immediately.** Mads closed ARIA manually at +71.8% rather than waiting 175 days. The backtest holds to expiry, but live discretion to take extreme profits early is sensible risk management. `[VERIFIED]`

3. **The 60% stop level was correct.** At entry, all 15 positions were within 3% of entry price. The stop prices set on Variational UI all matched entry × 1.60. No false stop-out drama on day one. `[VERIFIED]`

4. **`[Update 2026-04-29]` The 60% stop level fired and held, twice.** DEXE (entry 2026-04-09, closed 2026-04-18 at −62.6%, 9 days held) and PIEVERSE (entry 2026-04-09, closed 2026-04-20 at −60.3%, 11 days held) both hit the +60% adverse threshold. Both were closed manually at or marginally past the stop — Mads used the stop level as a guide for manual closure rather than waiting for automated execution. The empirical validation of the 60% level (chosen via backtest sweep across 20%–150%) carries through to live operation. Without the stop discipline, both positions would have continued bleeding: DEXE briefly traded above $14 (+74%), PIEVERSE above $0.85 (+67%). `[VERIFIED]`

### What Failed / Surprised Us
1. **Variational's UPnL display is misleading.** The UI shows leveraged returns, not price returns. ICNT showed -55.76% UPnL which caused initial alarm, but the actual adverse price move was only +2.8%. This caused a false "URGENT" flag in our analysis. **Lesson: always calculate from entry price, never trust the venue's percentage display on a leveraged account.** `[VERIFIED]`

2. **Venue coverage is a binding constraint.** 7 of 28 TOP7 candidates had no perp on either venue. The strategy's theoretical optimal basket cannot be fully replicated in practice. Variational covers most (~75%), Hyperliquid is sparse for small-caps. **Lesson: what you can backtest is different from what you can trade.** `[VERIFIED]`

3. **The rolling entry model significantly underperformed.** Weekly rolling entry (add new top-5 coins as they appear, cap at 20–25 positions) produced Sharpe 0.92 vs 3.31 for fixed cohorts. 36% stop-out rate vs near-zero. The fixed cohort's "limitation" of only entering every 45 days is actually its edge — it forces patience and filters noise. `[VERIFIED]`

4. **`[Update 2026-04-29]` Two consecutive losses at the stop within 11 days.** Live tally so far: 1 win, 2 losses at the 60% stop, all from the 2026-04-09 entry window. Per-leg loss rate is materially higher than backtest implied for this entry window (backtest reported 100% win rate on the pair across 12 clean cohorts). The pair-trade — short basket vs BTC long — has not yet had time to validate; the BTC hedge offsets some of the per-leg loss, and the cohorts are still mid-hold (earliest expiry 2026-05-19). Multiple non-mutually-exclusive explanations: (a) start-date sensitivity (backtest Sharpe varied 1.76–3.17 across 6 entry-date offsets, mean 2.51), (b) residual basket beta of −0.37 against an unfavorable BTC tape during the hold, (c) noise from small live n=3, (d) genuine regime change vs backtest period. **Decision: continue running, re-evaluate cleanly after C1 cohort completes 2026-05-19.** `[VERIFIED on data; explanations are [BELIEF]]`

### What We Suspect But Can't Prove
1. **The backtest Sharpe of 3.31 may be high.** Start-date sensitivity test showed Sharpe ranging from 1.76 to 3.17 across 6 different offsets (mean 2.51). The month-start alignment produced an above-average result. True expected Sharpe is probably 2.0–3.0, still excellent. `[VERIFIED]`

2. **Funding costs may worsen in different regimes.** Backtest showed -1% to -2.5% per cohort (~5–8% of gross alpha). But this was in a relatively benign funding environment. During extreme hype periods, funding on short alt perps could spike much higher. `[BELIEF]`

---

## 8. Open Design Questions

1. **Holding period: 150 vs 180 vs 135 days.** Mads initially committed to 180d for clean 4-cohort rotation. Backtest showed 180d comparable on Sharpe but -47.7% worst DD vs -30.5% for 150d. 135d tightest on risk. **Status: [OPEN — live deployment uses variable hold-days due to seeding, decision deferred until first cohort cycle completes]**

2. **BTC+ETH hedge leg.** Mads believes alts correlate more with ETH and wants ETH in the hedge. Backtest with ETH 0–30% degraded returns due to rebalancing cost and ETH underperformance in sample. Mads acknowledged this is regime-dependent. **Status: [OPEN — using BTC-only for now, revisit when live experience accumulated]**

3. **Beta-hedged BTC sizing.** BTC-only beta-hedged (size BTC to basket's rolling 30d beta) showed Sharpe 3.80 and 92% alpha but -64% worst drawdown. Smoothed version: 3.26 Sharpe, -43% DD. Better alpha, worse tail. **Status: [OPEN — Phase 2 upgrade after live validation of notional match]**

4. **Panel data freshness for the scanner.** The signal scanner runs on `single_coin_panel.csv`. When Dan runs it for new cohort discovery, the data must be current. A daily panel generation pipeline was discussed and partially built (`src/exports/panel_generation.py`). **Status: [OPEN — needs verification that panel generation runs reliably on Render before next cohort scan]**

5. **Variational position API.** Currently no programmatic position monitoring. Book CSV is the workaround. When Variational launches their trading/portfolio API, the monitoring layer should be upgraded. Dan has signed up for the waitlist. **Status: [OPEN — blocked on Variational]**

6. **Cold Flush gate validation.** Only 1 weekly observation below 2.0% APR in the entire 2-year dataset. The gate is wired but effectively untested. **Status: [OPEN — keep as safety rail, will gain data over time]**

7. **Replaced position policy.** When ARIA was closed early, SIGN was entered as a replacement. No formal rule exists for when/whether to replace early closes. DEXE and PIEVERSE were NOT replaced after closing. **Status: [OPEN — discretionary for now; the divergent treatment of ARIA vs DEXE/PIEVERSE suggests an implicit rule (replace winners, don't replace stop-outs) but it isn't formalized]**

8. **Multiple strategy accounts.** Apathy Bleed runs on its own Variational settlement pool. Mads wants separate accounts per strategy. Hyperliquid supports subaccounts. **Status: [VERIFIED — architecture supports this, Apathy Bleed is isolated]**

9. **`[Added 2026-04-29]` Hedge rebalancing on early closes.** Each cohort's LONG_BTC hedge was sized at formation to match the cohort's total short notional. When a short closes early — whether profitably (ARIA) or via stop (DEXE, PIEVERSE) — the cohort's BTC hedge is not reduced. Result: book is currently over-hedged on BTC by ~$6,000 (open shorts ~$39K, BTC longs ~$45K). Net effect is a small directional long-BTC bias on the residual book. **Status: [OPEN — need explicit policy. Options: (a) leave hedges static, accept residual bias as small; (b) trim BTC hedge proportionally on each early close; (c) re-hedge to current open-short notional only on cohort expiry. Discuss with Mads.]**

10. **`[Added 2026-04-29]` Post-cut audit — universe reduction impact on historical picks.** Pre-flight check pending before executing the top-1000 cut: were the 16 live Apathy picks (cohort 2026-04-09) AND the 12 backtest cohorts' picks all ranked ≤1000 by mcap on their entry dates? If even one pick was ranked >1000, that's evidence the strategy uses long-tail coins and the cut would systematically exclude future similar picks. **Status: [OPEN — Claude Code investigation queued before allowlist re-run; if any pick was >1000, halt and surface to Dan/Mads.]**

---

## Universe-cut cross-table NaN risk (operational note, not a strategy question)

When the CoinGecko allowlist drops to top 1,000, Binance-perp listings outside top 1,000 by mcap will still be in `fact_funding` / `fact_open_interest` / `fact_liquidations` (CoinGlass universe is independent — Binance-perp filter, ~590 coins). Joins from those CoinGlass tables to `fact_marketcap` / `fact_volume` will silently produce NaN for any Binance-perp coin outside top 1,000. Audit count of affected `asset_id`s post-deploy. If >5% of CoinGlass universe falls outside the new CoinGecko universe, escalate.

---

## 9. What's NOT the Strategy

1. **Apathy Bleed is NOT auto-executing.** The scanner generates candidates; Mads picks. Early versions of Claude jumped to "here's the trade to put on TODAY" — that's wrong. The signal is systematic, the execution is discretionary.

2. **The 4.53 Sharpe is NOT the tradeable number.** That was an Event Study Trajectory Sharpe (cross-sectional average of cohorts aligned to Day 0). It measures signal quality, not portfolio returns. The tradeable Continuous Sharpe is ~2.0–3.3 depending on start date. Both Claude and Gemini contributed to clarifying this.

3. **The `is_perp_active` flag in panel data does NOT mean the perp is available on your venue.** It's a Binance-centric flag. The actual venue filter requires cross-referencing against Hyperliquid and Variational perp listings via `perp_coverage_summary.csv`.

4. **Notional matching is NOT beta-neutral.** The backtest basket had average beta ~1.4 to BTC. A $1:$1 notional match leaves residual net-short-crypto exposure (~-0.37 beta). This was tested and acknowledged but accepted for simplicity. If precise beta neutrality matters, size BTC long to ~1.4x the short notional.

5. **The backtest does NOT include stop-loss slippage.** All stops assume exit at exactly the 60% level. Real altcoin perps can gap 5–10% beyond the trigger. Operational stop at 55% was suggested but not implemented. The DEXE close on 2026-04-18 actually came in marginally PAST 60% (exit $13.1237 vs stop $12.91152, ~1.7% slippage on the trigger), confirming this concern in live data.

6. **Apathy Bleed is NOT the same as `danlongshort`.** `danlongshort` is a separate beta-neutral long/short strategy with its own CSV, alert runner, and Telegram prefix. They share Render infrastructure but have no code or state overlap. Do not conflate them.

7. **Do NOT write code or run analysis before getting explicit go-ahead.** Mads established this as a hard workflow rule. Always: (1) clarify what you plan to do, (2) identify failure modes, (3) assess if it's the right analysis, (4) wait for approval. This is stored in Claude's memory edits.

8. **`[Added 2026-04-29]` Apathy Bleed does NOT have a "5-Gates exclusion scanner."** A framework with that name was discussed in earlier exploratory chats (covering supply ratio, perp/spot vol, OI/mcap, funding regime, and vol/mcap trajectory) and never became part of the actual strategy. The only gates that exist in production are the three listed in §3: perp/venue check, Cold Flush APR gate, and the Oct 2025 quarantine. If you see "5-Gates" referenced in any other context document about Apathy Bleed, it is stale.

---

## Summary

**The most important thing the receiving session needs to know:** Apathy Bleed is live with real money ($30K deposit, ~$84K total notional [$39K open shorts + $45K BTC longs], 13 short legs + 4 BTC hedges on Variational). The monitoring infrastructure is deployed on Render. The book CSV at `/data/apathy_bleed_book.csv` is the single source of truth. Three positions have been closed: ARIA +71.8% (replaced with SIGN), DEXE −62.6% (no replacement, hit 60% stop), PIEVERSE −60.3% (no replacement, hit 60% stop). Net realized: **−$1,513**. The 60% stop level fired correctly on both losses. Open question: hedge rebalancing policy on early closes (book is currently over-hedged ~$6K on BTC). Next cohort scan is due ~May 24, 2026; first full cohort completes 2026-05-19. Do not make changes to the live pipeline without Dan's explicit approval and Mads' sign-off on any coin selection.
