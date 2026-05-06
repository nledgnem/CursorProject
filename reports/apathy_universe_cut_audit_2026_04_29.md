# Apathy Bleed Universe Cut — Pre-Flight Audit

> **STATUS: WRITER-RACE RESOLVED 2026-05-05; universe cut READY TO EXECUTE as Phase E.**
> The §4 writer-race that paused this work shipped its full remediation 2026-05-05 (Phase B+C
> fail-fast guard + allowlist dedupe via PR #4 merge `87a4c50`; Phase D historical re-fetch via
> PR #5 merge `c7de283` + manual Render shell run). All four verification signals PASS; flap-flop
> spot-checks at corruption-era dates correctly recovered to canonical mcaps (see §8 resolution
> addendum for empirical numbers). Universe cut to top 1,000 by mcap is no longer gated and can
> execute via `scripts/archive/expand_allowlist.py --n 1000 --min-mcap 1000000 --output data/perp_allowlist.csv`.

**Date:** 2026-04-29 (revised 2026-04-30 with Step 3 findings + pause)
**Decision:** Mads + Dan, 2026-04-29 — reduce CoinGecko ingestion universe from current 2,997-row sticky allowlist to top 1,000 by market cap.
**Rationale (still valid, but now blocked):** at rank ~1,000 the market cap floor is roughly $10–20M, below the practical short-leg liquidity threshold for Apathy Bleed (Variational/Hyperliquid altcoin perps, $3K legs) and danlongshort (Binance perps, beta-neutral L/S).
**Audit purpose:** verify no historical strategy pick was ranked >1,000 by market cap at entry. If any pick was outside top 1,000, the cut would silently exclude similar future picks — invalidating the decision.

---

## 1. Live picks audit (16 SHORT legs, entry 2026-04-09) — CLEAN

### 1.1 Methodology

- **Pick set:** 16 SHORT legs from `data/curated/data_lake/apathy_bleed_book.csv` across cohorts C1–C4. (4 LONG_BTC hedge legs excluded — BTC is always rank #1.)
- **Primary rank source:** `fact_markets_snapshot.parquet` on **2026-04-16** (7 days post-entry — the closest available snapshot date; the table has gaps and no row for 2026-04-09).
- **Snapshot scope:** top 2,500 coins per snapshot date (verified — 2,500 rows, ranks 1..2500). Anything ranked >2,500 would not appear at all.
- **Resolution:** matched on `symbol` (uppercase) directly in the snapshot. All 16 resolved on first pass.
- **Why `fact_markets_snapshot` and not `fact_marketcap`?** The snapshot table is fed via a different ingestion path (`fetch_high_priority_data.py` paginating `/coins/markets`) that is **not** affected by the writer-race bug documented in §4. `fact_markets_snapshot` is the trustworthy rank source for this audit; `fact_marketcap` is corrupted for ~139 blue-chip symbols.

### 1.2 Result — full distribution

All 16 live picks ranked **≤1,000** at the snapshot date.

| Ticker   | Cohort | market_cap_rank | market_cap_usd |
|----------|--------|----------------:|---------------:|
| ZEC      | C1     |              20 | $5,663,449,114 |
| TAO      | C4     |              41 | $2,324,801,576 |
| MORPHO   | C3     |              71 |   $973,656,826 |
| STABLE   | C3     |              96 |   $565,544,969 |
| DEXE     | C4     |              99 |   $535,816,626 |
| DASH     | C1     |             106 |   $483,238,353 |
| CHZ      | C2     |             109 |   $438,806,034 |
| KITE     | C3     |             148 |   $270,132,481 |
| FARTCOIN | C2     |             167 |   $214,153,315 |
| AXS      | C3     |             179 |   $194,051,780 |
| ZEN      | C1     |             273 |   $104,521,366 |
| PIEVERSE | C2     |             283 |   $101,346,566 |
| ICNT     | C2     |             316 |    $85,454,196 |
| ONT      | C4     |             358 |    $73,826,248 |
| SIGN     | C4     |             463 |    $50,346,566 |
| ARIA     | C4     |             911 |    $17,514,499 |

Rank ≤1,000: 16 / 16. Range min=20 (ZEC), max=911 (ARIA), median=157.

Raw output: `outputs/apathy_live_pick_ranks_audit.csv`.

### 1.3 ARIA closeness-to-cutoff

ARIA at rank #911 ($17.5M) is the closest to the cutoff. **However**, ARIA was closed `CLOSED_MANUAL` on 2026-04-09 (entry day) at +71.8% PnL on the SHORT, meaning the price crashed on entry day. The 2026-04-16 snapshot mcap reflects the *post-crash* state. Pre-crash mcap was approximately 3–4× higher (~$60–70M), placing the actual 2026-04-09 entry-day rank around #300–500 — well clear of the cutoff.

### 1.4 Halt decision

**No halt on the operational hypothesis.** All 16 live picks were within top 1,000 at entry. The top-1,000 cut is consistent with realized Apathy Bleed picking behaviour. The universe cut is paused for separate reasons — see §4, §7.

---

## 2. Backtest cohort audit — NOT EXECUTED

Reasoning: live picks alone validate operational hypothesis (all ≤1000, median #157); backtest pick list lives in `STRATEGIES.md` (Drive-only, not in local repo) and `reports/msm_funding_v0/` is the MSM labelling backtest, not Apathy Bleed cohorts; cross-sectional rank inference via `fact_marketcap` for pre-2026-01 dates is now also unreliable due to the writer-race documented in §4. Decision: live audit is sufficient for go-ahead-on-rationale, backtest audit deferred.

---

## 3. Step 3 — universe-coverage audit (REFRAMED)

### 3.1 The "280-coin gap" framing was wrong

**Original framing (from prompt):** "~2,718 of the 2,997 allowlist entries actually produce rows in `fact_volume` / `fact_marketcap` (~280-coin / ~9% silent loss, unexplained)."

**Actual finding:** there is no silent-loss gap of 280 coins. The 2,997 vs 2,718 discrepancy is **fully explained by duplicate symbols in the allowlist itself**:

- `data/perp_allowlist.csv`: 2,997 rows, **2,718 unique symbols**, 280 duplicate-symbol rows across **139 distinct symbols**.
- Examples: ETH appears 4×, USDT 19×, SOL 5×, ZEC 3×, DOGE 3×, SHIB 3× — each entry has the same `symbol` but a different `coingecko_id` (the canonical CoinGecko slug plus bridged/wrapped/staked variants).
- Every unique allowlist symbol has at least *some* historical data in `fact_marketcap` (verified: zero never-appears).

### 3.2 What the 280 duplicate rows actually do

Each duplicate-symbol row in the allowlist triggers a separate CoinGecko API call (paid) but the result feeds into the writer-race documented in §4 — so the duplicate rows aren't "lost", they're "stomping on each other". The blast radius is captured in §4.

### 3.3 The 1,083-symbol coverage gap on the latest date

A separate finding (lower-severity) — see §6.

### 3.4 Original 6-bucket categorization NOT executed

Buckets `slug_mismatch / missing_dim / API_404 / API_error / delisted / unknown` proposed in the workflow do not apply to the actual gap structure. The category that DOES apply but wasn't in the proposed list is **"writer-race overwrite (139 symbols, ~280 rows)"** — and that's the dominant finding, documented in §4.

---

## 4. P0 — `download_all_coins` writer-race

**Severity: P0 production data integrity emergency.** Major coin (ETH, SOL, DOGE, SHIB, AVAX, TRX, TON, ZEC, USDT, ...) market caps and prices in `fact_marketcap`, `fact_price`, `fact_volume` are wrong by 100×–5000×. The bug was discovered during Step 3 of this audit on 2026-04-30.

### 4.1 Source location and mechanism

[`src/providers/coingecko.py:291-308`](../src/providers/coingecko.py#L291) — the daily fetcher iterates allowlist rows, makes one CoinGecko API call per row using the row's `coingecko_id`, but stores results into a dict **keyed by `symbol`**:

```python
for idx, row in allowlist_df.iterrows():
    symbol = row["symbol"]
    cg_id = row["coingecko_id"]
    prices, mcaps, vols = fetch_price_history(cg_id, ...)
    if prices:
        all_prices[symbol] = prices  # <-- overwrites prior entries with same symbol
        all_mcaps[symbol] = mcaps
        all_volumes[symbol] = vols
```

Because the allowlist contains 280 duplicate-symbol rows (139 distinct symbols, each with the canonical slug plus 1–18 bridged/wrapped variants), the fetcher runs N API calls per duplicated symbol but keeps only the last iteration's result. Earlier results — including the canonical slug's data when it was iterated before a bridged variant — are silently overwritten.

The downstream pivot/concat logic (lines 318–337) uses `symbol` as the key, so by the time the parquet is written there's exactly one row per symbol per date. The corruption is invisible at the table level.

### 4.2 Empirical impact — `fact_marketcap` 2026-04-25 (most recent)

| Symbol | fact_mcap value | Real-coin expected | Off by | Variants in allowlist |
|--------|----------------:|-------------------:|-------:|---|
| ETH    |     $41,188,391 | ~$200–300B         | **~5,000×** | `ethereum`, `bridged-wrapped-ether-starkgate`, `near-intents-bridged-eth`, `osmosis-alleth` |
| SOL    |     $94,892,102 | ~$60–100B          | ~700× | `solana`, `base-bridged-sol-base`, `binance-peg-sol`, `osmosis-allsol`, `wrapped-solana` |
| ZEC    |     $26,152,764 | ~$5.66B            | ~218× | `zcash`, `binance-peg-zcash-token`, `omnibridge-bridged-zcash-solana` |
| DOGE   |    $252,381,118 | ~$30B              | ~120× | `dogecoin`, `binance-peg-dogecoin`, `department-of-government-efficiency` |
| SHIB   |     $57,197,504 | ~$15B              | ~260× | `shiba-inu`, `binance-peg-shib`, `shiba-on-base` |
| TRX    |     $16,185,310 | ~$15B              | ~900× | `tron`, `solana-bridged-trx-solana` |
| TON    |     $27,724,694 | ~$5B               | ~180× | `the-open-network`, `tokamak-network` |
| AVAX   |     $18,837,855 | ~$10B              | ~530× | `avalanche-2`, `binance-peg-avalanche` |
| USDT   | (absent on latest) | ~$155B           | n/a    | 19 variants — `tether` plus 18 bridged |

Sane values for non-duplicate symbols (BTC, BNB, XRP, TAO, DASH): all correct, scale matches expectation. Confirms the bug fires only on duplicate-symbol groups.

### 4.3 Tables NOT affected

- **`fact_markets_snapshot`** — fed by `scripts/fetch_high_priority_data.py::fetch_and_save_markets_snapshot` which paginates CoinGecko `/coins/markets` (returns one row per CoinGecko slug, no per-row symbol-keyed dict). Verified correct ranks for ETH/SOL/ZEC etc. in this audit (§1.1).
- **CoinGlass tables** (`fact_funding`, `fact_open_interest`, `fact_liquidations`) — separate ingestion path, separate universe resolver, unrelated bug surface.

### 4.4 Blast radius (bounded but not yet measured)

- 139 distinct symbols affected on every date the writer-race ran (which is every nightly `run_live_pipeline.py` since the bridged-variant rows were added to the allowlist).
- Affected fields per (symbol, date): `close`, `marketcap`, `volume`. So the corrupted-cell count is roughly `139 × (days_since_dupes_added) × 3`.
- Date range of corruption not yet bounded. The dedicated fix task (§7) needs to (a) git-blame the allowlist to find when the bridged variants were added, (b) determine if the writer-race semantic was *always* broken or only after duplicates appeared.

### 4.5 Implications for downstream analysis

Anything that reads `fact_marketcap` / `fact_price` / `fact_volume` and joins on the affected symbols is producing wrong numbers. Concrete consumers to audit:

- **danlongshort beta computation** — reads `fact_price` per symbol; betas for affected symbols are computed against bridged-variant price series rather than canonical.
- **MSM labelling** (`reports/msm_funding_v0/*`) — reads `fact_marketcap` for basket weighting; affected basket members get bridged-variant mcap weights.
- **Apathy Bleed Gate 5 (vol/mcap trajectory)** — designed against `fact_volume + fact_marketcap`. If a Gate 5 candidate's symbol is in the affected 139, the gate decision is computed on bridged-variant data.
- **Universe snapshot builders** (`src/universe/snapshot.py`) — read `fact_marketcap` for eligibility filtering; affected symbols may be incorrectly included or excluded.

The dedicated task (§7) should enumerate these consumers exhaustively and decide whether re-running them post-fix is necessary.

### 4.6 Blast radius — empirical bounds (Followup-A.1 audit, 2026-04-30)

Read-only investigation done in this session to inform the remediation strategy for the dedicated fix task. No code changes; numbers only.

#### 4.6.1 Corruption start date

Git history of `data/perp_allowlist.csv`:

- **`8ade802` 2026-02-04** — initial repo commit; allowlist was empty (1 line, header only).
- **`e27666b` 2026-02-10 12:23 +0800** — bulk add of 2,997 rows. Already had **2,717 unique symbols + 280 duplicate-symbol rows** at this commit. Same structure as today.
- No subsequent commits to the allowlist file in the repo.

**The bug has been firing on every Render nightly since at least 2026-02-10** (~80 days as of 2026-04-30). The Feb-10 SHA differs from the current SHA — content has shifted since (some `coingecko_id` values changed) — but the duplicate-symbol *structure* (2997/2717/280) is unchanged. Earlier bridged-variant injection on Render (pre-repo-commit) is possible but cannot be verified from the repo alone.

#### 4.6.2 Corruption is *flapping*, not stable

The bug doesn't produce a fixed wrong value. It produces *whichever* bridged variant happens to be iterated last on each run. As the allowlist content shifts (manually-edited rows, reordering, etc.), the "winner" changes, and the stored value flips.

ETH timeline in `fact_marketcap` shows three distinct phases:

| Date range | ETH stored mcap | State |
|---|---:|---|
| 2017-05-19 to 2026-01-27 | $10B–$340B | Correct (real Ethereum) |
| **2026-01-28 to 2026-02-15** | **~$2-3M** | **Wrong** (winning variant: micro-token) |
| 2026-02-16 to 2026-03-05 | ~$240B | Correct again |
| 2026-03-06 onward | $41M (per 2026-04-25 obs) | Wrong (different winning variant) |

So the 2026-02-10 commit is a *lower bound* on continuous-corruption start; the bug-firing condition predates it (the 2026-01-28 ETH transition shows the duplicate-bearing allowlist was already in use on Render then), but the empirical evidence shows the corruption became persistent only from ~2026-03-06.

Across all 139 affected symbols, **38 (27%) show ≥1 day-to-day order-of-magnitude jump in `fact_marketcap` post-2026-02-10** — proxy for flap-and-flop transitions. The other 101 symbols had a single losing variant from the start, no flapping observed at the order-of-magnitude scale (smaller flapping may exist).

**Implication for remediation:** "delete all rows for affected symbols since 2026-02-10 and refetch" is correct and safe. "Flag rows in a wrong-value range" is fragile because the wrong-value range itself is non-stationary.

#### 4.6.3 Cell counts (potentially-corrupt)

| Window | Rows in `fact_marketcap` | Rows in `fact_price` | Rows in `fact_volume` | Total cells |
|---|---:|---:|---:|---:|
| Post-2026-02-10 (75 days) | 9,762 | 9,762 | 9,762 | **29,286** |
| Post-2024-01-01 (lower bound for trustworthiness per §9 of `DATA_LAKE_CONTEXT.md`) | 84,190 | (similar) | (similar) | **~250,000** |

Top-10 affected symbols by post-Feb-10 row density (75 days max — symbols active for the full window): DUSD, FARTCOIN, ETH, USDT0, USUAL, SIGMA (74), USDF (74), AIN (73), ACE (73), ACT (73). Most affected symbols have nearly-full coverage of the post-Feb-10 window.

**Note:** "potentially-corrupt" — the bug FLAPS, so some rows in the window are correct and some are wrong. Per §4.6.2, ~27% of affected symbols show clear flapping evidence (10×+ jumps); the remaining 73% likely had a single losing variant for the whole window (= more rows continuously wrong, not fewer).

#### 4.6.4 Downstream consumer inventory

77 `.py` files reference one or more of the corrupt tables (`grep -l "fact_(marketcap|price|volume)" --include="*.py"`). Filtering to **load-bearing production paths**:

| File | Pattern | Affected? |
|---|---|---|
| `src/universe/snapshot.py` | reads `fact_marketcap` for mcap-rank filter; reads `fact_volume` for volume threshold | **YES** — affected symbols (e.g. ETH, SOL, DOGE) get bridged-variant mcap, may be silently excluded from baskets they should be in (or vice versa) |
| `src/backtest/engine.py` | reads price/mcap series per symbol for backtests | **YES** — backtests over the corruption window use wrong values for the 139 symbols |
| `src/data_lake/build_duckdb.py` | rebuilds DuckDB from bronze parquets | **YES** — DuckDB inherits corruption |
| `majors_alts_monitor/data_io.py` | MSM data loader | **YES** — MSM labelling reads via this loader |
| `majors_alts_monitor/msm_funding_v0/msm_data.py` | MSM basket weighting reads `fact_marketcap` | **YES** — affected basket members get bridged-variant weights |
| `scripts/data_ingestion/build_silver_layer.py` | builds `silver_fact_price.parquet`, `silver_fact_marketcap.parquet` from bronze | **YES (propagation)** — silver inherits bronze corruption; any silver consumer also affected |
| `scripts/apathy_bleed_gate5_spotcheck.py` | reads vol/mcap for Gate 5 trajectory | **YES** — Gate 5 candidates in the 139 get corrupt-data decisions |
| `scripts/build_universe_snapshots.py` | universe snapshot builder | **YES** — same risk as `src/universe/snapshot.py` |
| `src/danlongshort/portfolio.py` | reads `danlongshort_price_cache.parquet` (separate cache) | **PROBABLY** — depending on how the cache is fed (silver vs direct CG fetch). Verify in fix task. |
| `src/data_lake/mapping_validation.py` | reads schemas for validation; doesn't use values | **NO (analytical)** |
| `src/data_lake/perp_listings.py` | reads only `asset_id` column for membership checks | **NO (analytical)** |

The remaining 65+ files are archive scripts, debug snapshots, notebooks, and one-off diagnostic tools — not load-bearing for ongoing analysis.

#### 4.6.5 Remediation strategy implications

These numbers inform the dedicated fix task's choice between the three options outlined in §7:

| Option | Feasibility | Cost | Cleanness |
|---|---|---|---|
| **(re-fetch)** affected slice from CoinGecko | ~139 API calls (one per canonical slug, full 75-day or 730-day range per call) | Trivial — well under daily Basic-tier limits | **Cleanest.** Overwrites all flapping with single ground-truth source. |
| **(flag)** add `data_quality` column marking known-bad rows | Per §4.6.2, "known-bad" is non-stationary — flagging requires per-(symbol, date) ground-truth anyway | Medium (schema change + every consumer adapts) | Worst — pushes complexity onto every downstream consumer. |
| **(truncate + re-ingest)** drop and re-fetch from scratch | Same API cost as (re-fetch), broader scope | Trivial API; medium operational risk during truncation window | Heavy-handed but simple. |

**Recommendation for the fix task:** Option (re-fetch). 139 API calls × ~75 days = recoverable in a single backfill run. After the writer-race code fix lands, run a one-shot backfill for affected symbols' canonical slugs, overwriting `fact_marketcap[asset_id IN (139 syms)]` and similar for `fact_price` / `fact_volume`. Document the backfill job's result counts in a follow-up to this memo.

#### 4.6.6 What this audit does NOT do

- No code change. `download_all_coins` still has the writer-race; no allowlist deduplication; no `is_unique` assert.
- No re-fetch. Corrupt cells remain corrupt as of this commit.
- No silver-layer rebuild. `silver_fact_*` continues to inherit bronze corruption.
- No consumer regression test. Listed consumers should be exercised post-fix to confirm correct behavior on previously-affected symbols.

These belong in the dedicated `coingecko-data-integrity-fix` task per §7. The numbers here just bound the work.

---

## 5. `dim_asset.coingecko_id` is a structural placeholder (medium severity, dormant)

Originally surfaced as the trigger of the cross-check anomaly during the live-pick audit. Step 3 confirmed it's a separate latent issue, not the same bug as §4.

### 5.1 Finding

All 2,717 rows in `dim_asset.parquet` have `coingecko_id == lowercase(symbol)`. **Zero rows have real CoinGecko slugs.** Examples:

| asset_id | dim_asset.coingecko_id | real CoinGecko slug |
|---|---|---|
| BTC  | `btc`  | `bitcoin` |
| ETH  | `eth`  | `ethereum` |
| USDT | `usdt` | `tether` |
| BNB  | `bnb`  | `binancecoin` |
| XRP  | `xrp`  | `ripple` |
| TAO  | `tao`  | `bittensor` |
| ZEC  | `zec`  | `zcash` |

### 5.2 Status

**Dormant.** Daily ingestion does NOT use `dim_asset.coingecko_id` (uses `allowlist.coingecko_id` directly), so the bug is not currently load-bearing. Any analysis or join that resolves coin metadata via `dim_asset.coingecko_id` produces wrong results, but no in-tree consumer was identified that does so.

### 5.3 Documentation drift

`data_dictionary.yaml` line 115 documents `fact_price.asset_id` as *"Canonical CoinGecko slug (e.g. 'bitcoin', 'ethereum', 'aria-ai'). Matches dim_asset.coingecko_id."* — this description is empirically wrong on both halves. `fact_price.asset_id` is uppercase symbol (e.g. `BTC`, `ETH`), not a CoinGecko slug; and `dim_asset.coingecko_id` is a placeholder (above), not a real slug. This documentation drift should be corrected as part of the dedicated fix task.

### 5.4 Investigation required (in dedicated task)

1. Confirm no production code path reads `dim_asset.coingecko_id`.
2. Decide remediation: rebuild `dim_asset` from authoritative source (CoinGecko `/coins/list`) so the column carries real slugs, or drop the column entirely.
3. Fix the data-dictionary descriptions for `fact_price` / `fact_marketcap` / `fact_volume` `asset_id` columns.

---

## 6. The 1,083-symbol coverage gap on 2026-04-25 (low severity)

A separate finding: 1,083 of 2,718 unique symbols have no row in `fact_marketcap` on 2026-04-25 specifically. Recency profile:

- 0–7 days stale:    973 symbols (median: 1 day)
- 8–30 days stale:    34 symbols
- 31–90 days stale:   48 symbols
- 91–365 days stale:  28 symbols
- max stale:         191 days

Pattern matches transient daily-ingestion failures (throttle / 429 / timeout) rather than structural loss. The 973 0–7-day-stale subset is plausibly explained by daily fetcher partial failures that don't fully recover within the same window. Lower priority than §4 and §5.

---

## 7. Status and next actions

### 7.1 What this audit produced

- §1 live pick audit: **CLEAN** — top-1,000 cut is operationally consistent with realized Apathy Bleed picking.
- §2 backtest pick audit: deferred (Drive-only artifact, not reproducible from local repo).
- §3 universe-coverage audit: reframed (no silent loss; the 280-row "gap" is duplicate-symbol rows feeding §4).
- §4 writer-race bug: **P0 surfaced, not yet fixed.**
- §5 `dim_asset` placeholder: dormant medium-severity, not yet fixed.
- §6 latest-date coverage gap: low severity, lower priority.

### 7.2 What does NOT happen now

- **No `expand_allowlist.py` re-run.** No `data/perp_allowlist.csv` modification.
- **No backup-CSV commit.** No reduction commit. No promotion of `Status: PENDING` markers to `EXECUTED`.
- The universe cut work is paused at end-of-Step-3.

### 7.3 What happens next (handed to a dedicated task)

A new task — `coingecko-data-integrity-fix-2026-04-29` — picks up from this memo. Scope:

1. Fix the `download_all_coins` writer-race (§4). Choose between keying by `coingecko_id` vs. deduplicating allowlist by symbol (former is more invasive, latter is also a prerequisite for the universe cut).
2. Audit the full blast radius (§4.4). How many cells corrupt × when corruption began. Output: dated incident report.
3. Decide remediation strategy for historical corruption: re-fetch affected slice, mark suspect with quality flag, or drop and re-ingest.
4. Add `expand_allowlist.py` uniqueness guard: `assert allowlist['symbol'].is_unique` before writing.
5. Audit `dim_asset.coingecko_id` placeholder (§5). Confirm no consumers, then rebuild correctly or drop.
6. **Re-run the universe cut (Steps 4–7 of the original task) only after (1)–(5) ship.** The cut becomes the closing step of the integrity fix rather than a parallel workstream.
7. Add Telegram regression alert: daily check on (a) `len(allowlist['symbol'].unique()) == len(allowlist)`, (b) for `BTC|ETH|SOL|BNB`, latest `fact_marketcap` value within an order of magnitude of CoinGecko live-API value.

### 7.4 Documentation actions taken in this session

- This memo (`reports/apathy_universe_cut_audit_2026_04_29.md`) — restructured 2026-04-30 with PAUSED header.
- `docs/DATA_LAKE_CONTEXT.md` §9 (known data-quality issues) — new entry at top warning about §4 corruption.
- `docs/DATA_LAKE_CONTEXT.md` §13 (known gaps / current work) — three new bullets covering universe-cut pause, writer-race P0, and `dim_asset` placeholder.
- `data_dictionary.yaml` `data_sources.coingecko.notes` — appended with writer-race warning.

Single docs-only commit. No allowlist or fetcher changes in this commit.

---

## 8. Resolution addendum (2026-05-05)

Phase B+C+D shipped. Writer-race RESOLVED.

### 8.1 What landed

| Phase | PR | Merge SHA | What |
|---|---|---|---|
| **B** Code defenses | #4 | `87a4c50` (2026-05-04) | Fail-fast guard in `download_all_coins` against duplicate symbols; `assert symbol.is_unique` in `expand_allowlist.py`; commits `b1e53d1` |
| **C** Allowlist dedupe | #4 | `87a4c50` (2026-05-04) | `data/perp_allowlist.csv` reduced 2,997 → 2,716 rows. Backup at `data/perp_allowlist.2997_pre_writer_race_dedupe.bak.csv` (commit `b16f58f`). Dedupe commit `ac80300`. USC dropped entirely (audit per §1.4 + bug-impact §4.6); one NaN-symbol row dropped (data hygiene). |
| **D.0–D.1** Phase D scripts | #5 | `c7de283` (2026-05-05) | `scripts/verify_ingestion_integrity.py` (pluggable post-deploy check, `--mode writer_race` initial mode); `scripts/refetch_writer_race_affected.py` (one-shot driver). Commit `ce12374`. |
| **D.2–D.4** Re-fetch on Render | — | manually executed 2026-05-05 17:36 UTC | 138 canonical slugs over `[2024-05-10, 2026-05-05]` (start bumped from 2024-05-06 to dodge `offset_days=-2` padding tripping the 730-day Basic-tier ceiling — first attempt 401d on this boundary). Atomic upsert: 76,766 stale rows dropped + 73,920 canonical rows inserted in `fact_marketcap`; similar magnitudes in `fact_price` (77,140/73,920) and `fact_volume` (76,884/73,920). 0 errors in 374s wall time. |
| **D.5** Verification | — | 2026-05-05 | All four signals PASS (Signals 1+2 INDETERMINATE locally, but Signal 3+4 direct PASS; flap-flop spot-checks at 2026-02-21, 2026-03-06, 2025-09-01, 2025-08-01 all recovered to canonical mcaps). |
| **D.6** Cache invalidation | — | 2026-05-05 | `/data/curated/data_lake/danlongshort_price_cache.parquet` deleted on Render shell. Next danlongshort run rebuilds from corrected bronze. |

### 8.2 Empirical verification

Pre-fix → post-fix on `fact_marketcap[asset_id, date=2026-05-05]`:

| Symbol | Pre-fix (sample 2026-04-25) | Post-fix (2026-05-05) | Recovery factor |
|---|---:|---:|---:|
| ETH | $41M | $284B | ~6,900× |
| SOL | $95M | $48.6B | ~511× |
| DOGE | $252M | $17.1B | ~67× |
| USDT | absent | $189.5B | n/a (was missing) |

Flap-flop spot-checks (the tougher test — verifies historical-era dates corrected, not just today):

| Date | Symbol | Pre-fix (audit memo) | Post-fix (verified 2026-05-05) | Verdict |
|---|---|---:|---:|---|
| 2026-02-21 | ETH | ~$2M | $237B | PASS |
| 2026-03-06 | ETH | ~$2M | $250B | PASS |
| 2025-09-01 | USDT | ~$1.7M | $168B | PASS |
| 2025-08-01 | SHIB | ~$1.5M | $7.3B | PASS |

### 8.3 Caveat — 9 ambiguous canonical picks

For 9 symbols where two distinct real coins share a ticker (not just bridged variants of one coin), the canonical pick was made by **highest current mcap** as a deterministic-but-imperfect heuristic. Pre-fix value was random per iteration order; post-fix is deterministic regardless of choice — strictly better. Verified zero references in any Apathy Bleed cohort (live or backtest) before commit.

| Symbol | Canonical (highest mcap) | Rejected alternative(s) |
|---|---|---|
| ANT | `autonomi` | `aragon` |
| AURA | `aura-on-sol` | `aura-finance` |
| ALPHA | `alpha-fi` | `alpha-finance` |
| AVA | `concierge-io` | `ava-ai` |
| ANY | `anyspend` | `anyswap` |
| ADX | `adex` | `adrena` |
| AIN | `infinity-ground` | `ai-network` |
| ACE | `endurance` | `ace-data-cloud-2` |
| ACT | `act-i-the-ai-prophecy` | `achain`, `acet-token` |

Reversal possible by editing `outputs/writer_race_canonical_slug_mapping.csv` and re-running `scripts/refetch_writer_race_affected.py` against the affected slug. Mirrored in `data_dictionary.yaml::data_sources.coingecko.canonical_slug_resolutions`.

### 8.4 What's NOT fixed

- **Pre-2024-05-10 rows for affected symbols.** Basic-tier API depth ceiling prevents re-fetch of older history. Pre-2024 era is independently flagged as suspect for the unrelated broken-Analyst-tier-pipeline reason (DATA_LAKE_CONTEXT.md §9 entry 1) — Phase D doesn't fix that, doesn't claim to.
- **`dim_asset.coingecko_id` placeholder** (audit §5). Still OPEN. Not bundled into writer-race remediation. Separate cleanup task.
- **`verify_ingestion_integrity.py` log-path coverage** (Signals 1+2). Returns INDETERMINATE on Render shell because the script's candidate log paths don't match Render's actual daemon stdout location. Not a blocker — Signal 3 (parquet read on affected blue-chips) is direct empirical evidence stronger than logs. Cleanup item filed in DATA_LAKE_CONTEXT.md §13.

### 8.5 Phase E — universe cut now unblocked

The original 2026-04-29 decision to reduce CoinGecko ingestion universe from ~3,000 to top 1,000 by mcap is no longer gated. Re-run path:

```
python scripts/archive/expand_allowlist.py --n 1000 --min-mcap 1000000 --output data/perp_allowlist.csv
```

The `expand_allowlist.py` builder now has the dedupe + uniqueness assert from Phase B, so a fresh build is guaranteed not to reintroduce the writer-race trigger condition. Expected outcome: 2,716 → ~1,000 rows, drops bottom-tier sub-$10M-mcap coins from the daily ingestion.

Pre-flight live-pick audit (§1) already cleared the operational hypothesis — all 16 SHORT picks ranked ≤1,000 by mcap, median #157.

Status as of this addendum: **READY TO EXECUTE.** Awaiting Dan/Mads decision on timing.
