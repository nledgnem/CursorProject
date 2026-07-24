# LL Pro Trades — Strategy Reference

Discretionary, market-neutral relative-value book driven by LL Pro Discovery signals + data-lake enrichment. Traded on Variational. This is the **LLPROtrades** book — distinct from `dantrading` (separate larger book) and from the systematic strategies in the repo (Apathy Bleed, danlongshort, MSM).

This doc is the durable reference / session-handoff for the strategy. Companion docs: [[DATA_LAKE_NOTES]] (lake quirks + file IDs), `enrichment/build_enrichment.py` (the enrichment module), `ll_public_extracts.md` (the LL Pro framework corpus). Section references below (e.g. "Section 6") point at `ll_public_extracts.md`.

---

## 1. What it is / isn't

- **Is:** take pasted LL Pro Discovery rows (Gold/Blue state + level/pattern columns), enrich each with the data lake (volume, mc, funding, OI, age, perp availability, and derived vs-BTC / liquidation / beta reads), and decide **long / short / ignore**. Then hold a roughly beta-neutral relative-value book and manage it.
- **Isn't:** a reimplementation of the Larsson Line indicator. LL Pro is the source of truth for state and signals; we do not rebuild it (scope-drift guard, `ll_public_extracts.md` §"Scope reminder"). The narrow exception is spot-checking a specific signal against independent data (Rule 2).

## 2. The two halves

**Signal — from LL Pro Discovery (user pastes a screenshot):**
- Gold / Blue / None on **both USD and BTC pairs** (the $ and ₿ columns).
- Fresh-flip vs steady-state ("flipped gold today" = white dot on the most recent closed candle, vs "is gold").
- Column 3 Levels (Breakout / Bounce / Breakdown / Rejection) — AI-driven, treat as investigate-not-act (Rule 2).
- Column 4 Patterns (Bull/Bear pattern icon) — ML-driven, rare (~0.1% base rate), high-conviction when present.

**Enrichment — from the data lake:**
- Static columns (`enrichment/build_enrichment.py`): `latest_close_usd/btc`, `mc`, `mc_tier`, `vol_30d_avg_usd`, `funding_rate_latest/_7d_avg`, `oi_latest`, `oi_7d_change_pct`, `age_days`, `perp_binance/_hyperliquid/_variational`.
- Derived live (per-ticker lookups): **vs-BTC return (7d / 30d)**, **liquidation long:short skew (7d)**, **OI 7d change**, **30d beta vs BTC**.

## 3. Decision method — the filter stack

For each ticker, stack these. Any one can veto.

| Check | Signal | Rule |
|---|---|---|
| **Cross-pair** (USD vs BTC state) | Conviction | Both aligned = strong. USD Gold + BTC None/Blue (or vice-versa) = weak → usually ignore (Section 2, 4). |
| **vs-BTC return** (7d/30d) | Relative strength — the core edge | Long must be **outperforming** BTC; short must be **underperforming**. A Discovery "Gold" that is flat/positive vs BTC is a screen artifact of BTC moving, not alpha. |
| **Liquidation skew** (long:short, 7d) | Capitulation detection | Extreme long-flush (>8:1, seen up to 300:1) = the move already happened → **late, skip.** *Biggest recurring lesson.* |
| **OI 7d change** | Fragility vs exhaustion | Building OI = fuel for continuation. Drained OI (−25%+) = unwind done, thesis stale. |
| **Funding** | Crowding / carry | Negative funding on a long = shorts pay you (bullish). Very negative on a short = shorts already crowded (skip). |
| **Perp availability** | Tradeability | Gating: ~80% of the universe is spot-only. No perp on Binance/HL/Variational → can't express (esp. kills short candidates). |
| **Age / mc / volume** | Sizeability | Filter micro-caps, fresh listings (<~90d), sub-$1M/day volume. |

**Verdict:** long / short / ignore.

### Hard discipline overlays (these override a "clean" screen signal)
- **No longs in Blue** — Rule 1, no exceptions.
- **Skip parabolas** — broken-parabola overlay (Section 5). Entering a vertical +100–200% move off a base is the worst R:R. *Lessons paid in PnL: WLD (−25%), plus TAIKO / ORDI / USELESS avoided.*
- **Skip capitulated shorts** — extreme long-flush = move already spent; squeeze risk high. *ADA, PEPE, CHZ, STX, VIRTUAL, SKYAI, MANTA, etc. all screened as shorts and all failed this.*
- **Skip cross-pair conflicts** — screener fires on USD-pair state; when BTC pair disagrees, trust the vs-BTC data.
- **Skip untradeable** — tokenized stocks (MSTRX, CRCLon, GOOGLX, TSLAX…), stablecoins showing "Gold" mechanically, BTC wrappers echoing BTC's state.

## 4. Portfolio construction

- **Relative-value / market-neutral:** long outperformers, short underperformers, hedge residual with a BTC position.
- **Beta-managed:** track 30d beta-weighted net exposure vs BTC; keep near zero.
  - Caveat: betas are regime-dependent and **spike toward 1.0+ in correlated sell-offs**. "Neutral today" underestimates true short-in-a-crash exposure. Treat computed net beta as a lower bound on downside.
  - The cleanest neutral adjustment is **BTC itself** (β≈1.0, deep liquidity) or **trimming the highest-beta position**, NOT adding a fresh alt short (that adds idiosyncratic risk for a systematic problem).
- **Sizing discipline:** small on chase/parabolic/thin names; full on clean large-cap relative-strength; ~1/3 typical size on shorts (squeeze risk + countertrend to crypto's long-run drift).
- **Stops:** set on Variational so they self-execute. Exit a long when **both** vs-BTC windows go negative (thesis broken). Take profit on winners whose relative strength has decayed even if USD price still holds.

## 5. Regime awareness (the layer added mid-campaign)

The edge depends on **alt dispersion**, which comes and goes:
- **Bear leg** → shorts work (made money short SOL/ETH).
- **Recovery** → close shorts, alts lead, longs work (NEAR/INJ/JTO/HYPE/UNI).
- **Chop / BTC-leads** → no clean setups on either side; correct action is **sit out**. Forcing trades into a dead tape is the main way this book gives back gains.

Quantified two ways (both agree in the July-2026 chop):
- **Alt breadth** = % of perp-tradeable alts beating BTC over 30d. ~18–21% currently = bottom-quartile, BTC-leads.
- **BTC dominance** (lake-native, `btcdom/btcdom_lake_native.py`) = BTC mc / total mc, stables+wrapped excluded. Rising = BTC-leads.

Backtest of the "long BTC / short alt basket" (BTC-dominance) trade, conditioned on breadth:
- Works best in the **middle** (breadth 21–45%): ~+3% / 20–30d, ~60–66% win rate.
- **Loses** at breadth <14% (alts maximally beaten down → snap-back, the index-level version of the capitulation trap).
- Regime is persistent ~1–2 weeks then mean-reverts (corr breadth_t vs t+5d = +0.72; t+30d = −0.23). Not a multi-month trend to ride.
- Caveats: small effective sample (~25 independent 30d windows), survivorship bias (favorable direction), no tx costs modeled, 2-year window = one macro regime. **Research finding, not a sized trade yet.**

Framework backing: Section 6 (BTC↔Alts) — alt-outperformance via BTC-pair Gold is expected primarily when **BTC's own state is None/grey**, not when BTC is Gold.

## 6. Operational spine

- **Data source:** the Drive-synced lake at `G:\My Drive\Render Exports\`, refreshed nightly by Render (~01:55 UTC). Read parquet directly — faster and immune to the MCP 10 MB download cap.
- **Staleness discipline:** always confirm `silver_fact_price` max date before trusting a read. Early lesson: a stale local mirror gave wrong analysis for ~6 weeks before it was caught. Drive sync fixes the whole class.
- **Asset-ID gotchas:** BTC is `asset_id="BTC"` (not "bitcoin"); `fact_markets_snapshot` uses `"BITCOIN"` for canonical Bitcoin while silver uses `"BTC"`; `"None"` string is a CSV NA trap. See [[DATA_LAKE_NOTES]].
- **Travel-readiness:** keep the book lean (few positions), stops that execute without you online, and run the same analysis from a laptop via Drive sync + Claude.

## 7. What actually worked (honest post-hoc)

- **The discipline is the strategy.** Winners weren't clever picks — they were UNI/NEAR/INJ/JTO when clearly outperforming BTC, and shorts when coins were clearly bleeding vs BTC. The value-add was consistently **rejecting** the capitulated shorts, parabolic longs, and cross-pair-conflicted signals the screener kept surfacing.
- **Beta drift is the silent killer.** Repeatedly, trimming alt longs while leaving a fixed BTC short quietly turned a neutral book into a directional short that bled as BTC rallied. Re-check net beta after every set of closes.
- **The screener over-fires in trending-BTC regimes.** Both long and short screens fill with structurally-failed candidates during chop. A no-trade day is a valid, common, correct outcome.

## 8. Known limitations

- Hyperliquid funding is **not** in the lake (its perp JSON has no funding field); only Variational funding is parseable from JSON, Binance from `silver_fact_funding`. HL-only coins have NaN funding.
- `fact_open_interest` is CoinGlass aggregate across venues (not per-venue); OI can be non-NaN for coins with no Binance/HL/Variational perp.
- Enrichment mixes as-of dates: price/mc/funding at lake-max, perp-coverage flags at their own snapshot date. Row carries multiple as-of timestamps.
- Regime metrics (breadth, dominance) are 2-year, single-macro-cycle. The BTC-dominance trade is a research finding, not validated across a full cycle.
