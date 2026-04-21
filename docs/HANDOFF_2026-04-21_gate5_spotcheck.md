# Session Handoff — Apathy Bleed Gate 5 Spot-Check

**Date:** 2026-04-21
**Previous session:** Path-resolution audit + DATA_LAKE_CONTEXT.md + initial Gate 5 scoping (claude.ai chat)
**Next:** Run the spot-check, interpret, decide whether Gate 5 is worth formalizing
**Working environment:** This session is intended to run in **Claude Code** against the local repo. The spot-check script reads from the local data lake (`data/curated/data_lake/`) — no Render shell access needed for this step.

---

## 1. What Apathy Bleed is (1-minute version)

Long BTC / Short alt basket pair trade. Four cohorts (C1–C4) formed 2026-04-09, entered as baskets of 4–6 short legs each plus a matching LONG_BTC hedge sized to the cohort's total short notional. Cohorts exit at staggered target dates: C1 at 40 days, C2 at 85, C3 at 130, C4 at 175.

Three of the 16 short legs have closed so far. The other 13 are still OPEN:

| Ticker   | Cohort | Status         | PnL      | Return  | Note                                    |
|----------|--------|----------------|----------|---------|-----------------------------------------|
| ARIA     | C4     | CLOSED_MANUAL  | +$2,157  | +71.8%  | Closed same-day (2026-04-09)            |
| DEXE     | C4     | CLOSED_MANUAL  | −$1,861  | −62.6%  | Closed 2026-04-18, pumped into the face |
| PIEVERSE | C2     | CLOSED_MANUAL  | −$1,809  | −60.3%  | Closed 2026-04-20, also pumped          |

Net realized PnL: −$1,513.

**DEXE and PIEVERSE look like manipulated tokens that pumped after entry**, killing the short. ARIA was closed immediately when the setup was obviously wrong. The losers share a profile: coins where the "apathy is setting in" thesis was wrong because attention was actually arriving, not leaving.

The scanner's job is to reject these kinds of candidates at entry.

---

## 2. The 5 proposed exclusion gates

All designed to fire BEFORE taking a new short position, rejecting the candidate if any gate fails.

| # | Gate                                    | Data needed                          | Tier |
|---|-----------------------------------------|--------------------------------------|------|
| 1 | Circulating / total supply ratio        | fact_markets_snapshot (partial)      | 2    |
| 2 | Derivatives / spot volume ratio         | Not ingested                         | 3    |
| 3 | OI / market cap                         | fact_open_interest (BTC-only)        | 2    |
| 4 | Funding rate regime                     | silver_fact_funding                  | **1 — ready** |
| 5 | Volume / market cap trajectory          | fact_volume + fact_marketcap         | **1 — ready** |

**Gate 4 (funding)** — confirmed design: reject if funding has been DEEPLY NEGATIVE recently (crowded-short — too late to add another short). Threshold TBD.

**Gate 5 (vol/mcap) — the focus of this session.** Design:
- Reject if the 7-day trailing mean of vol/mcap is trending UP over the last 14 days (attention is arriving, not leaving)
- Also reject if the 21-day coefficient of variation of daily vol/mcap is high (unstable attention regime — could flip back on)

The intuition: this strategy is named "Apathy Bleed" because it shorts coins where attention is fading. The losers (DEXE, PIEVERSE) are cases where attention came back. We want to catch that at entry.

---

## 3. The immediate task — spot-check, not full audit

Before building a proper audit or a backtest, a 10-minute eye-test: **do the three closed tickers look distinguishable on vol/mcap trajectory?**

If yes → Gate 5 design has signal, worth formalizing into an audit.
If no → Gate 5 as designed doesn't work, rethink before building infrastructure.

### 3.1 Script to run

Already committed at `scripts/apathy_bleed_gate5_spotcheck.py`. From repo root:

```bash
python scripts/apathy_bleed_gate5_spotcheck.py
```

Outputs:
- Resolved asset_ids (ARIA/DEXE/PIEVERSE → CoinGecko slugs)
- Per-ticker summary stats (level, 7d-mean, 14d-slope, 21d-CV) at as_of = 2026-04-09
- Raw 30-day vol/mcap series (wide table, date × 3 tickers)
- Optional PNG `apathy_bleed_gate5_spotcheck.png` if matplotlib is installed

### 3.2 Things that might go wrong, in order of likelihood

1. **Symbol-to-asset_id resolution fails for one or more tickers.** The script prints `WARN:` lines if dim_asset doesn't have a match on the upper-case symbol. Likely offenders: ARIA (could be `aria-ai` or `aria-protocol`), PIEVERSE (new-ish token). If this happens, use `map_provider_asset.parquet` to resolve manually — it has provider-native tickers joined to canonical asset_ids.

2. **Insufficient history for one or more tickers.** The script warns if < 21 days of data. PIEVERSE especially may be a newly-listed token with limited CoinGecko history. Work with whatever raw data exists even if formal stats aren't computable.

3. **pandas version or API mismatch.** The script was drafted for modern pandas but not tested end-to-end; paste any traceback.

### 3.3 Interpretation

Look at the 7d-mean, 14d-slope, and 21d-CV columns across the three tickers. The hypothesis predicts:
- ARIA (winner) should have a NEGATIVE or near-zero 14d slope (attention leaving) and LOW 21d CV (stable regime)
- DEXE and PIEVERSE (losers) should have POSITIVE 14d slope (attention arriving) OR HIGH 21d CV (jumpy regime)

If ARIA looks like the losers on these stats, the gate is dead — don't invest more effort without rethinking.

If there's a visible separation, the gate is alive — proceed to formalize.

---

## 4. If Gate 5 is alive — formalization plan

Graduate the spot-check into a committed audit script.

**Target path:** `scripts/apathy_bleed_gate5_volmcap_audit.py`

**Output layout:**
```
/data/curated/data_lake/audits/
  apathy_bleed/
    gate5_volmcap/
      2026-04-09/                                               # run_id = as_of_date
        apathy_bleed_gate5_volmcap_stats_2026-04-09.parquet
        apathy_bleed_gate5_volmcap_meta_2026-04-09.json
```

**Why the filename embeds the as-of date:** The Drive uploader (`src/exports/gdrive_uploader.py` via `src/exports/nightly_export.py`) flattens subdirectory paths when naming Drive files. If we wrote `stats.parquet` in both `2026-04-09/` and `2026-04-22/` dirs, the second would overwrite the first on Drive, destroying the PiT audit trail in the backup. Embed the date in the filename.

**Output schema (one row per eligible ticker):**
- `asset_id` (CoinGecko slug)
- `symbol`
- `as_of_date` (UTC)
- `vol_mcap_ratio_last` (ratio on as_of_date, raw)
- `vol_mcap_ratio_7d_mean`
- `vol_mcap_ratio_14d_slope` (Δratio/day)
- `vol_mcap_ratio_21d_cv` (dimensionless)
- `n_obs_used`
- `in_apathy_book` (bool)
- `apathy_outcome` (`CLOSED_WIN` | `CLOSED_LOSS` | `OPEN` | null)
- `market_cap_rank` (from fact_markets_snapshot if available)
- `marketcap_usd` (on as_of_date)

**Universe:** Top 300 by market cap rank on the as-of date (use `fact_markets_snapshot` if available for the date; else top 300 by mcap value from `fact_marketcap`). Exclude stablecoins (see `stablecoins.csv` in the lake). Require ≥ 30 consecutive days of both vol+mcap ending at `as_of_date`.

**CLI:** `python scripts/apathy_bleed_gate5_volmcap_audit.py [--as-of YYYY-MM-DD]` (default: latest date with both vol and mcap data).

---

## 5. Repo conventions you MUST read before writing analytical code

(non-optional; enforced by `.cursorrules`)

1. `docs/DATA_LAKE_CONTEXT.md` — **the** data-model primer. Read first. Covers tier limits (CoinGecko Basic = only 2024-onwards trustworthy; CoinGlass Hobbyist = 2.2s rate limit), schemas, known data-quality landmines, Drive file IDs.
2. `.cursorrules` — behavioral rules including:
   - **Rule 1:** No diagnostic `.py` at repo root (use `/notebooks` for diagnostics, `/scripts` or `/src` for permanent utility)
   - **Rule 3:** Configuration over hardcoding — thresholds go in `configs/*.yaml`, not in Python
   - **Rule 4:** No `_v2`/`_final` suffixes; strategy-prefixed filenames
   - **Rule 5:** Zero-trust data governance — HALT if any variable's unit is undefined in `data_dictionary.yaml`
   - **Rule 9:** Strict UTC enforcement everywhere
3. `ARCHITECTURE.md` — system layout including 2026-04-20/21 path-resolution fixes
4. `data_dictionary.yaml` — column-level unit definitions (required read per Rule 5)

---

## 6. Data gotchas (from `docs/DATA_LAKE_CONTEXT.md` — re-emphasizing the relevant ones)

- **`fact_volume.volume` is rolling-24h quote USD**, NOT a calendar-day bar. For distributional analysis across tickers it's fine (same distortion everywhere), but note this if you ever compute diff-based features.
- **CoinGecko Basic tier limit:** only last 2 years reliable. Pre-2024 data may have errors from a broken Analyst-tier fetch.
- **Symbol ≠ asset_id**: `apathy_bleed_book.ticker` is uppercase symbol like "ARIA"; `fact_*.asset_id` is CoinGecko slug like "aria-ai". Resolve via `dim_asset.parquet` (symbol column → coingecko_id). Fallback: `map_provider_asset.parquet`.
- **PiT discipline (Rule 4 of `.cursorrules` Data Integrity Baseline):** every feature at time `t` must only use data available at `t-1` or earlier. For this audit that means: when computing stats "as of 2026-04-09", only include data with `date <= 2026-04-09`.

---

## 7. How to talk to Dan

- Direct, no hedging. He's the quant developer and has strong opinions; if the spot-check says "no signal," say so plainly rather than hoping for something to come out of the noise.
- When you state thresholds or cutoffs, flag whether they're picked from data or guessed.
- Commit messages include motivation, not just "what changed." Pattern: short first line, blank line, paragraph explaining why and what it unblocks.
- Don't invent a shell/git command execution capability you don't have. If Claude Code needs to commit, use its actual commit tool; don't hallucinate pushing.

---

## 8. When to come back to the claude.ai chat

The previous conversational context has the full reasoning history. Come back there if:
- Gate 5 turns out to be dead and you need to redesign the gate concept
- Scope expands to planning the full backtest (needs cross-cutting design thinking)
- Results are surprising and you want a second opinion on interpretation

Otherwise, stay in Claude Code for implementation iteration.

---

## 9. Expected session flow

1. Read `docs/DATA_LAKE_CONTEXT.md` and `.cursorrules` (required)
2. Run `python scripts/apathy_bleed_gate5_spotcheck.py`
3. Interpret output; render the PNG if produced; report findings to Dan
4. **Decision gate with Dan:** Gate 5 alive or dead?
5a. If alive: draft `scripts/apathy_bleed_gate5_volmcap_audit.py` per §4 above
5b. If dead: report back to Dan with interpretation; do NOT proceed to audit
6. Dan decides whether to proceed to formal audit, change scope, or return to the other chat

Do not do step 5 before the decision gate in step 4.
