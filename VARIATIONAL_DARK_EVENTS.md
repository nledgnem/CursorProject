# VARIATIONAL_DARK_EVENTS

**Trade type:** other (structured event trial)
**Status:** watching
**Owner:** Mads (portfolio owner) · **Effort lead:** off-hours RWA event trial
**Created:** 2026-07-02

---

## Thesis
High-impact (>4% expected) news that breaks **while a Variational RWA equity name is
genuinely dark** (no venue forming a price) leaves **residual edge after the perp's own
overnight repricing (OLP)**, capturable at small size. We are testing whether that
residual edge is real and repeatable — by trading it manually and logging outcomes, not
by backtest (no equity data). This is a **manual trial for experience + a log**, not a
validated systematic strategy. Statistical validation is explicitly **not** expected at
this sample size.

## Catalysts
- **Own-name dark-window events:** 8-Ks, M&A, guidance changes on the traded name.
- **Asian peer earnings surprises** (the catalyst is a *different* company): SK Hynix,
  Samsung, TSMC → mapped to US names via `configs/rwa_peer_map_candidate.csv`
  (Hynix/Samsung → MU, NVDA; TSMC → NVDA, AMD). Sign can be state-dependent (a
  capacity-expansion "beat" can be **bearish**) — human judgment decides.
- Only **screamers** (events we *think* imply **>4%** moves) qualify. No scoring model;
  the tool only flags dark-window **timing**.

## Invalidators
- **INV-COST — RESOLVED.** A name's realised move must beat its size-clip cost floor.
  Measured in `outputs/rwa_cost_floor.md` (round-trip ≈ size_100k spread). **HIMS is
  excluded** (233 bps @100k → needs >2.3% just to break even); the monitor drops any
  `FAIL_TOO_WIDE` name automatically. size_100k spread is the operative floor.
- **INV-DARK — CONDITIONAL.** Trade only when the name was dark at the event's break
  time. Dark calendar is DST-aware, with the hardcoded NYSE holiday/half-day list AND CME
  Globex single-stock-futures sessions (`src/rwa_offhours/dark_calendar.py`). **The
  condition:** from the **CME SSF launch 2026-07-27**, covered megacaps (NVDA, TSLA, MU,
  META, GOOGL verified; full 55-list pending) are **lit on weekdays** (Globex ~23h) →
  their weekday dark window disappears. Only **weekends/holidays** stay dark for covered
  names. Foreign names (TSM, NVO, NOK) are rarely dark anyway → excluded from peer
  read-through. So INV-DARK holds only for: (a) weekends/holidays (all names), and (b)
  weekday dark for non-CME-covered names.
- **INV-EDGE — OPEN.** Does residual edge survive OLP repricing? **Resolved by the trade
  log at the review gate, NOT by backtest.** (The offline read-through study that would
  have measured this is PARKED — see below.)

## Entry
Manual, only after verification of: (1) the flag, (2) the **real** break timestamp
(first-seen-on-web ≠ broke-at — discard if unclean), and (3) a **live firm quote at
click** on Variational. Indicative quotes are never treated as executable.

## Sizing — flag the asymmetry
**OUTSIDE the crypto macro-regime gate.** This is event-driven **synthetic equity**, not
crypto beta, so it does **not** inherit the crypto book's sizing/risk rules. **Fixed flat
trial notional per trade set by Mads; no conviction sizing during the trial.** Needs its
own risk caps.

## Compliance
**CLEARED** by the owner (no trading restrictions or privileged access in other roles).
The PA-dealing/MNPI gate is resolved for this trial. Re-check if roles/holdings change.

## Data & method (trial phase)
- **Free sources only** (Yahoo/Stooq daily, exchange filings). No data spend. Accepted
  limitation.
- **Cadence:** once daily ~08:00 SGT. Weekend events surface Monday — accepted.
- **Flow:** AI news scan (own-name screamers + peer earnings) → curated
  `inputs/dark_events_<date>.json` with clean `broke_at_utc` → `scripts/dark_event_monitor.py`
  → flat flags (`outputs/dark_event_flags.{csv,md}`). Flags are **clues for manual
  verification, not signals.**

## Review gate
After **10 trades OR 2 quarters, whichever first** → decide **scale / kill / continue**.
Trade log: `VARIATIONAL_DARK_EVENTS_trade_log.csv` (append-only; one row per trade, with
a written rationale recorded **before** entry).

---

## Running log
- **2026-07-02** — Idea file created. Scope set to manual trial (this decision). Compliance
  cleared; no data spend. Monitor stripped to a dark-window timing flagger (no scoring, no
  PDF). INV-COST resolved (HIMS excluded; NYSE holidays hardcoded). INV-EDGE open, to be
  resolved by this trade log at the review gate. Read-through study PARKED. Status:
  **watching** — awaiting the first qualifying dark-window screamer.
- **2026-07-02** — CME SSF launch 2026-07-27 removes weekday dark window for covered
  megacaps. Thesis narrows to (a) weekends/holidays for all names, (b) weekday dark for
  non-CME-covered names only. Liquidity ramp of new CME contracts unknown — dark calendar
  treats covered names as lit from launch date regardless (conservative). INV-DARK is now
  **CONDITIONAL** (see Invalidators). Dark calendar updated with Globex sessions
  (effective 2026-07-27) + `cme_ssf_covered` / `eu_offvenue_coverage` config columns.
- **2026-07-02** — `cme_ssf_covered` RESOLVED from CME's official fact card (May-2026).
  **Covered (12, lit weekdays from launch):** AMD, GOOGL, INTC, LLY, META, MSFT, MU, NFLX,
  NVDA, PLTR, QCOM, TSLA. **Uncovered (17):** the rest. **Weekday-dark hunting ground from
  2026-07-27** = uncovered + US (non-foreign) + cost-floor PASS/MARGINAL: **AAOI, ARM,
  CBRS, COIN, CRCL, LITE, MRVL, MSTR, NBIS, RKLB, SNDK, USAR**. COIN/MSTR are
  crypto-news-sensitive (owner's domain); HOOD is uncovered but cost-floor
  INSUFFICIENT_DATA (thin, revisit). CAVEATS: fact card is the May version (6/30 release
  named SpaceX, absent from the table) -> **RE-VERIFY the final list at/after launch**;
  coverage != liquidity -> **log real CME volumes at re-verify** (a dead contract forms no
  price). Eurex per-name check still open (low priority; only the 07:00-09:00 UTC shoulder).
- **2026-07-02** — Eurex shoulder check **CLOSED, no impact.** Eurex US SSFs are
  block/off-book (majority via Trade Entry Services, settle off NYSE/NASDAQ open), not
  continuous on-screen price formation, and the roster skews European; the 12 non-megacap
  survivors are very unlikely to have liquid Eurex contracts. 07:00-09:00 UTC shoulder
  stays dark for the survivor set; INV-DARK unchanged. (Segment-level finding; spot-check
  an individual Eurex contract page only if trading a specific name.)
- **2026-07-02** — Pre-wired the **CME liquidity gate** for the 2026-07-27 re-verify:
  added config columns `cme_ssf_liquid` (Y/blank=lit, N=still-dark) and
  `cme_ssf_adv_contracts`. Blank today -> zero behaviour change; a covered name later
  marked `cme_ssf_liquid=N` (dead launch volume) returns to the weekday-dark hunting
  ground (coverage != liquidity). **Launch division of labor:** owner returns the confirmed
  55-name list + per-name early CME ADV; Claude Code flips changed `cme_ssf_covered`, fills
  `cme_ssf_liquid`/`cme_ssf_adv_contracts`, regenerates, and re-cross-checks the cost
  floor. Sole remaining blocker before any live trade: **Mads's sizing.**
- **2026-07-08** — First qualifying dark-window screamer found: Samsung's Q2 prelim
  guidance (released ~09:00 KST / 2026-07-07T00:00Z, before KOSPI open) triggered a
  memory-sector selloff; MU fell ~7% in the overnight/premarket dark window on
  profit-taking/margin-sustainability concerns despite the earnings beat. Peer-mapped
  Samsung → MU per config; flagged `dark_at_break=Y`. SNDK also fell ~7% on the same
  news but has no Samsung peer-map entry (only MU/NVDA are mapped) so was correctly not
  flagged — worth revisiting whether SNDK/WDC belong in the peer map given they moved in
  lockstep with MU. NVDA barely moved (dominated by a separate Kyber-delay/denial story,
  <2%) so the Samsung→NVDA non-mapping looks right. **Still a CLUE, not verified** — needs
  manual confirmation of the real break timestamp and a live firm quote before any trade;
  no trade taken by the monitor itself. STEP 0.5 not run (no `UNVERIFIED` rows remained).
- **2026-07-09** — Scan: no new qualifying screamers. DeepSeek own-AI-chip report (Reuters,
  2026-07-07) and the Samsung Q2-guidance-driven memory selloff (MU/SNDK ~-5-7% since
  2026-07-06/07) are the dominant stories in the window but are 2+ days stale relative to
  today's prior-US-close cutoff and already covered by the 2026-07-08 log entry — not
  re-flagged. No fresh own-name 8-K/M&A/guidance items and no new Hynix/Samsung/TSMC
  surprises since prior close. `inputs/dark_events_2026-07-09.json` = []. 0 events
  evaluated, 0 flags, 0 discarded. STEP 0.5 not run (no `UNVERIFIED` rows remained).
- **2026-07-10** — Scan: no qualifying screamers. ARM (+11.16%) and MRVL (+6.64%) rallied
  hard during the 2026-07-09 cash session on sector-wide AI-chip analyst price-target
  hikes (RBC/UBS/Cantor on MRVL) and "anticipatory buying ahead of earnings" — but no
  single discrete news item with a clean `broke_at_utc` (accumulated momentum, not a
  screamer catalyst), and the move happened intraday/lit, not in a dark window — discarded
  per Step 2 timestamp hygiene. TSMC news was a Citi analyst upgrade ahead of TSMC's own
  July-16 earnings (not TSMC's own surprise). Samsung/SK Hynix news (Q2 guidance selloff,
  SK Hynix Nasdaq ADR listing 2026-07-10) is a continuation of the 2026-07-07/08 story,
  already logged, not re-flagged. `inputs/dark_events_2026-07-10.json` = []. 0 events
  evaluated, 0 flags, 0 discarded. STEP 0.5 not run (no `UNVERIFIED` rows remained).
- **2026-07-11** (Saturday, weekend dark window since Friday 2026-07-10 US close) — Scan:
  no qualifying screamers. SK Hynix's record $26.5B Nasdaq ADR listing (priced 2026-07-10,
  ADRs +15% vs offer) and CRCL's 14% jump on OCC national-trust-bank approval both broke
  during Friday's lit US cash session, not in a dark window, and are continuations of
  already-logged/known stories — not re-flagged. ARM's 11% move and Meta's leaked AI-capex
  memo lack a single clean discrete `broke_at_utc` (accumulated momentum / stale multi-day
  stories) — discarded per Step 2 timestamp hygiene. No fresh own-name 8-K/M&A/guidance
  items and no new Hynix/Samsung/TSMC surprises since Friday's close.
  `inputs/dark_events_2026-07-11.json` = []. 0 events evaluated, 0 flags, 0 discarded.
  STEP 0.5 not run (no `UNVERIFIED` rows remained).
- **2026-07-12** (Sunday, weekend dark window since Friday 2026-07-10 US close) — Scan: no
  qualifying screamers. Checked own-name 8-Ks/M&A/guidance for the 29 names and fresh
  Hynix/Samsung/TSMC surprises: nothing new since Friday's close — Samsung Q2-guidance
  selloff and SK Hynix's Nasdaq ADR listing/CRCL OCC approval are all continuations of the
  2026-07-07/10 stories already logged, not re-flagged. MSTR's ~3,588 BTC sale (to fund
  preferred dividends) is a capital-management story, not a >4%-screamer with a clean
  dark-window break. Tesla/Intel/SpaceX/xAI "Terafab" consortium news is stale (announced
  Mar/Apr 2026), not fresh. No new Asian peer earnings (KRX/TWSE closed Sunday).
  `inputs/dark_events_2026-07-12.json` = []. 0 events evaluated, 0 flags, 0 discarded.
  STEP 0.5 not run (no `UNVERIFIED` rows remained).
- **2026-07-13** (Monday, prior US close = Friday 2026-07-10) — Scan: no qualifying
  screamers. Checked own-name 8-Ks/M&A/guidance for the 29 names and fresh
  Hynix/Samsung/TSMC surprises: Rocket Lab's $8B Iridium Communications acquisition is
  real but dated 2026-06-28/29 (definitive agreement), stale relative to Friday's close —
  not re-flagged. Samsung's Q2 prelim guidance/KOSPI selloff and SK Hynix's Nasdaq ADR
  listing remain continuations of the 2026-07-07/10 stories already logged. TSMC's own
  earnings are not until 2026-07-16 (no surprise yet); SK Hynix's earnings not until
  2026-07-22. No new own-name items for AMD, QCOM, META, LLY, PLTR, NFLX, INTC, TSLA, MU,
  GOOGL, MSFT, NVDA, or the uncovered names (ARM, MRVL, NBIS, CRCL, COIN, HOOD, MSTR,
  RKLB, etc.) since Friday's close. `inputs/dark_events_2026-07-13.json` = []. 0 events
  evaluated, 0 flags, 0 discarded. STEP 0.5 not run (no `UNVERIFIED` rows remained).
- **2026-07-14** (Tuesday, prior US close = Monday 2026-07-13) — Scan: no qualifying
  screamers. Dominant story since Monday's close is the US-Iran military escalation
  around the Strait of Hormuz (US strikes 2026-07-11/12, blockade of Iranian shipping
  announced 2026-07-13) — this is a macro/geopolitical shock, not an own-name 8-K/M&A/
  guidance event or an Asian peer earnings surprise, so it falls outside this monitor's
  two catalyst categories regardless of its market impact; not logged as an event.
  SK Hynix's record -15.4% Seoul session (worst day since 2008) was triggered by a KIS
  broker note published before Seoul's open on Monday 2026-07-13 — but that break time
  predates this scan's window (prior US close = Monday's close) and was also confounded
  with Nasdaq-ADR-listing profit-taking and the Iran/oil shock rather than a clean single
  earnings-surprise catalyst; discarded as stale/unclean per Step 2. Today's Seoul session
  (SK Hynix -4.7%→flat, Samsung +4.3%) is continued macro-driven chop, not a discrete
  screamer with a clean `broke_at_utc`. No fresh own-name 8-K/M&A/guidance items found for
  any of the 29 names since Monday's close (checked NVDA, TSLA, META, AMD, MU, INTC, QCOM,
  PLTR, NFLX, GOOGL, MSFT, LLY, and the uncovered names). TSMC's own earnings are not until
  2026-07-16 (no surprise yet); SK Hynix's earnings not until 2026-07-22.
  `inputs/dark_events_2026-07-14.json` = []. 0 events evaluated, 0 flags, 0 discarded.
  STEP 0.5 not run (no `UNVERIFIED` rows remained).
- **2026-07-15** (Wednesday, prior US close = Tuesday 2026-07-14) — Scan: no qualifying
  screamers. TSMC's own Q2 earnings are not until tomorrow, 2026-07-16 (no surprise yet).
  KOSPI rebounded +0.5% to ~6,840 Tuesday after Monday's -8.95% Iran/oil-shock-driven rout;
  Samsung and SK Hynix stabilizing/bouncing (no fresh discrete earnings surprise from
  either — continuation of the 2026-07-13 macro/geopolitical shock and the 2026-07-07/10
  Q2-guidance/Nasdaq-listing stories already logged). MU +4.24% on 2026-07-14 was a lit
  regular-session move (sector sentiment recovery), not a dark-window event, and not tied
  to a single clean own-name catalyst — discarded per Step 2. No fresh own-name 8-K/M&A/
  guidance items for any of the 29 names since Tuesday's close (checked NVDA, TSLA, META,
  AMD, MU, INTC, QCOM, PLTR, NFLX, GOOGL, MSFT, LLY, and the uncovered names incl. USAR,
  CRCL, COIN, MSTR, ARM, MRVL, RKLB — nothing dated 2026-07-14/15). SK Hynix's own earnings
  not until 2026-07-22. `inputs/dark_events_2026-07-15.json` = []. 0 events evaluated, 0
  flags, 0 discarded. STEP 0.5 not run (no `UNVERIFIED` rows remained). Copied
  `outputs/dark_event_flags.md` to G:\My Drive\Variational After-Hours Monitor\
  dark_event_flags_2026-07-15.md.
- **2026-07-16** (Thursday, prior US close = Wednesday 2026-07-15) — Scan: no qualifying
  screamers. Dominant story is Micron -8.2% on CXMT (Chinese DRAM maker) China-competition
  fears (Apple reportedly testing CXMT chips), dragging Intel/AMD/Marvell/SanDisk lower —
  but this broke during Wednesday's lit afternoon regular session, not a dark window;
  discarded per Step 2. Samsung (-5.19%) and SK Hynix (-7.59%) fell in Thursday's Seoul
  premarket purely in sympathy with the US chip rout (read-through of the MU/CXMT story),
  not a fresh Samsung/Hynix-specific earnings or guidance surprise — does not qualify as a
  peer catalyst per this monitor's definition. TSMC's own Q2 earnings are scheduled 2pm
  Taipei time today, after this scan's cutoff — no surprise data available yet (results to
  check tomorrow, 2026-07-17). No fresh own-name 8-K/M&A/guidance items for NVDA, TSLA,
  META, AMD, MU, INTC, QCOM, PLTR, NFLX, GOOGL, MSFT, LLY, or the uncovered names (ARM,
  MRVL, RKLB, CRCL, COIN, MSTR, USAR, NBIS, HOOD) since Wednesday's close. Rocket Lab's
  Iridium acquisition remains stale (announced late June). SK Hynix's own earnings not
  until 2026-07-22. `inputs/dark_events_2026-07-16.json` = []. 0 events evaluated, 0 flags,
  0 discarded. STEP 0.5 not run (no `UNVERIFIED` rows remained). Copied
  `outputs/dark_event_flags.md` to G:\My Drive\Variational After-Hours Monitor\
  dark_event_flags_2026-07-16.md.
- **2026-07-17** (Friday, prior US close = Thursday 2026-07-16) — Scan: 1 event evaluated,
  0 flags. NFLX's Q2 2026 earnings (slight EPS beat, revenue miss, weak FCF, soft Q3 guide)
  released 2026-07-16T20:01:00Z (4:01pm ET, right after the 16:00 ET primary close), stock
  -8.58% in after-hours — genuine screamer, clean timestamp, but the release fell inside
  NYSE extended-hours trading (4:00pm-8:00pm ET is a lit venue per the dark calendar), so
  `dark_at_break=N`; the move already happened lit, not dark — correctly not flagged.
  TSMC's own Q2 earnings call (06:00 UTC 07-16, during TWSE trading hours) beat estimates
  but raised FY capex guidance to $60-64B (from $52-56B), spooking chip stocks broadly;
  peer-mapped AMD (-3.61%) and NVDA (-2.30%) both stayed below the 4% screamer bar so were
  excluded before even reaching the dark-window check. MU (-4.60%, >4%) is not peer-mapped
  to TSMC in the config (only Samsung/Hynix map to MU) so does not qualify as a peer
  catalyst regardless of magnitude — worth another look at whether MU/TSMC deserves a peer
  row given today's clean sympathy move, similar to the SNDK/Samsung question flagged
  2026-07-08. TSM itself (-4.6%) is excluded per the standing foreign-name policy (TWSE was
  open live at the 06:00 UTC break). Samsung/SK Hynix's +8%/+13% Wednesday rally was a
  macro/CPI-driven sector rebound, not a company-specific surprise. No fresh own-name
  8-K/M&A/guidance items for TSLA, META, AMD, MU, INTC, QCOM, PLTR, GOOGL, MSFT, LLY, ARM,
  MRVL, RKLB, CRCL, COIN, MSTR, USAR, NBIS, or HOOD since Wednesday's close. STEP 0.5 not
  run (no `UNVERIFIED` rows remained). Copied `outputs/dark_event_flags.md` to
  G:\My Drive\Variational After-Hours Monitor\dark_event_flags_2026-07-17.md.
- **2026-07-18** (Saturday, weekend dark window since Friday 2026-07-17 US close) — Scan:
  0 events evaluated, 0 flags. Checked own-name 8-K/M&A/guidance for all 29 names: nothing
  fresh since Friday's close — MU's only recent 8-K is a routine dividend announcement
  (stale), TSLA's next scheduled event is Q2 earnings 2026-07-22 (not yet occurred). NBIS
  fell ~14% intraday Friday on its asset-light data-center partnership disclosure (thin on
  partner/capex detail), but this moved during Friday's lit regular session with
  conflicting same-day intraday narratives (also +8% same day on a debt-deal headline) and
  no single clean `broke_at_utc` — discarded per Step 2 timestamp hygiene; also a
  continuation of NBIS's month-long neocloud-derating selloff, not a fresh discrete
  catalyst. No Asian peer earnings since Friday's close: KRX/TWSE closed for the weekend,
  so no fresh Hynix/Samsung/TSMC surprises; TSMC's own 2026-07-16 earnings and the Samsung
  Q2-guidance/SK-Hynix-listing stories remain prior continuations already logged
  2026-07-07 through 2026-07-17. No new items for the 12 CME-covered names or the
  uncovered hunting ground (ARM, MRVL, RKLB, CRCL, COIN, MSTR, USAR, HOOD, LITE, AAOI,
  CBRS). `inputs/dark_events_2026-07-18.json` = []. STEP 0.5 not run (no `UNVERIFIED` rows
  remained). Copied `outputs/dark_event_flags.md` to G:\My Drive\Variational After-Hours
  Monitor\dark_event_flags_2026-07-18.md.
- **2026-07-19** (Sunday, weekend dark window since Friday 2026-07-17 US close) — Scan:
  0 events evaluated, 0 flags. Checked own-name 8-K/M&A/guidance for all 29 names: no
  weekend filings found for NVDA, TSLA, MU, META, AMD, QCOM, INTC, PLTR, NFLX, GOOGL,
  MSFT, LLY, or the uncovered names (ARM, MRVL, RKLB, CRCL, COIN, MSTR, USAR, NBIS, HOOD,
  LITE, AAOI, CBRS, SNDK). Friday's Moonshot Kimi K3 (2.8T-parameter Chinese open-weight
  model) semiconductor selloff — Phlx Semi Index -5.7%, TSM -7%, S&P 500 -1.01% — broke and
  was fully priced during Friday's lit US/Asian cash sessions on 2026-07-17; stale relative
  to this scan's prior-close cutoff, not re-flagged (TSM also excluded per standing
  foreign-name policy). No fresh Hynix/Samsung/TSMC peer surprises since Friday's close:
  KRX/TWSE closed for the weekend. Note: SK Hynix's Q2 earnings are now confirmed for
  2026-07-29 (later than the 07-22 date referenced in the 2026-07-13 log entry) — no
  guidance issued yet. Samsung Q2-guidance and SK Hynix Nasdaq-listing stories remain prior
  continuations already logged 2026-07-07 through 07-18. `inputs/dark_events_2026-07-19.json`
  = []. STEP 0.5 not run (no `UNVERIFIED` rows remained). Copied `outputs/dark_event_flags.md`
  to G:\My Drive\Variational After-Hours Monitor\dark_event_flags_2026-07-19.md.
- **2026-07-20** (Monday, prior US close = Friday 2026-07-17) — Scan: 0 events evaluated,
  0 flags. No fresh own-name 8-K/M&A/guidance items for any of the 29 names since Friday's
  close: TSLA's Q2 earnings not until 2026-07-22 (no surprise yet); MU briefly surpassed
  Meta/Tesla market cap Thursday 2026-07-16 but on no discrete catalyst (stale, already a
  continuation of the memory-sector rally); PHLX Semiconductor Index sitting in a broad
  bear-market de-rating (-20.2% from its 2026-06-22 peak) is sector-wide, not a single-name
  event. No fresh Hynix/Samsung/TSMC surprises since Friday's close — KRX/TWSE closed for
  the weekend; SK Hynix's Nasdaq-listing/selloff and Samsung's Q2-guidance selloff remain
  prior continuations already logged 2026-07-07 through 07-19; TSMC's 2026-07-16 earnings
  already logged. Checked the uncovered hunting ground (ARM, MRVL, RKLB, CRCL, COIN, MSTR,
  USAR, NBIS, HOOD, LITE, AAOI, CBRS, SNDK): RKLB's >12% drop was 2026-07-16 (Iridium-deal
  related, stale, lit session); Coinbase/MicroStrategy/Circle weekend commentary was
  general stablecoin/BTC market color (Armstrong's "digital gold" remarks, Phong Le on
  bank competition), not a discrete >4% screamer with a clean break timestamp — discarded.
  `inputs/dark_events_2026-07-20.json` = []. STEP 0.5 not run (no `UNVERIFIED` rows
  remained). Copied `outputs/dark_event_flags.md` to G:\My Drive\Variational After-Hours
  Monitor\dark_event_flags_2026-07-20.md.
- **2026-07-21** (Tuesday, prior US close = Monday 2026-07-20) — Scan: 0 events evaluated,
  0 flags. No own-name 8-K/M&A/guidance items for any of the 29 names since Monday's close;
  TSLA and GOOGL earnings not until 2026-07-22 (no surprise data yet). Monday's KOSPI
  session was a volatile whipsaw (-4.46% intraday plunge, recovered to close -0.54%) driven
  by macro/geopolitical factors (US-Iran conflict escalation, AI-capex demand jitters ahead
  of this week's hyperscaler earnings) rather than a discrete Samsung/Hynix-specific
  earnings or guidance surprise, and happened during Monday's lit KRX session anyway — does
  not qualify as a peer catalyst per this monitor's definition. SK Hynix's own earnings
  confirmed 2026-07-29 (no guidance issued yet); no fresh Hynix/Samsung/TSMC surprises since
  Monday's close. Checked CME-covered names and the uncovered hunting ground (ARM, MRVL,
  RKLB, CRCL, COIN, MSTR, USAR, NBIS, HOOD, LITE, AAOI, CBRS, SNDK) — nothing new.
  `inputs/dark_events_2026-07-21.json` = []. STEP 0.5 not run (no `UNVERIFIED` rows
  remained). Copied `outputs/dark_event_flags.md` to G:\My Drive\Variational After-Hours
  Monitor\dark_event_flags_2026-07-21.md.
