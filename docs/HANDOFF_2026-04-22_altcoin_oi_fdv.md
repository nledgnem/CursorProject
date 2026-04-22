# Session Handoff — Altcoin OI Ingestion + FDV Coverage Audit

**Date:** 2026-04-22
**Previous session:** Gate 5 spot-check (partially alive, slope signal real, CV inverted, ARIA contaminated by same-day exit). Decision deferred; pivoted to data infrastructure work.
**Next:** Expand CoinGlass OI fetch from BTC-only to the ~510-altcoin universe with 2024-01-01 history backfill. Audit FDV coverage in existing `fact_markets_snapshot`. Wire into daily pipeline.
**Working environment:** Claude Code against the local repo for code changes. Render shell for executing the backfill.

---

## 1. Context — what's already there

**OI fetcher is already generic.** `scripts/fetch_coinglass_oi.py` has:
- The whole `fetch_oi_for_symbols(...)` loop
- Correct rate limiting (2.2s between requests, 30/min cap for Hobbyist tier)
- Retry with exponential backoff
- Skip-and-log failure handling (`[SKIP] {symbol}: {error_msg}` for "not found"/"invalid")
- `--incremental` mode that auto-detects last date per asset_id and only fetches new days
- Metadata sidecar JSON output

The BTC-only behavior today is **purely the CLI default**:
```python
symbols = args.symbols
if symbols is None:
    symbols = ["BTC"]
```
Pass `--symbols BTC ETH SOL ...` and it fetches them all. No fetcher rewrite needed.

**Funding fetcher has universe auto-resolution already.** `scripts/fetch_coinglass_funding.py` tries:
1. `data/curated/universe_eligibility.parquet` (from strategy)
2. `data/curated/universe_snapshots.parquet` (basket snapshots)
3. `data/curated/perp_listings_binance.parquet` (fallback)

This is the pattern to copy for OI's symbol resolution. Current funding covers ~510 altcoins with zero nulls — use the same set for OI.

**FDV is already ingested.** `fact_markets_snapshot.parquet` contains `fully_diluted_valuation_usd` as a column, written daily. Other relevant columns: `circulating_supply`, `total_supply`, `max_supply`, `market_cap_rank`. No new ingestion needed — just an audit of coverage.

---

## 2. The scope of this session — three phases

### Phase 1: OI altcoin expansion (~1h of real work + 20m backfill run)

**Goal:** `fact_open_interest.parquet` covers the same ~510 altcoins as `fact_funding.parquet`, from 2024-01-01 to present.

**Subtask 1.1 — Copy universe resolution from funding fetcher.**

`scripts/fetch_coinglass_oi.py` currently has this default:
```python
if symbols is None:
    symbols = ["BTC"]
    print(f"  No symbols provided, defaulting to BTC only")
```

Replace with the universe-loading block from `scripts/fetch_coinglass_funding.py` (lines ~290–340). It tries universe_eligibility → universe_snapshots → perp_listings. Adapt variable names to match OI fetcher conventions. Keep `--symbols` CLI flag as an explicit override.

**Subtask 1.2 — One-off historical backfill.**

On Render shell (not locally):
```bash
# First set API key if not already in env
export COINGLASS_API_KEY=<value from Render env>

# Backfill altcoins from 2024-01-01
python scripts/fetch_coinglass_oi.py --start-date 2024-01-01 --incremental
```

Wall-clock estimate: 510 symbols × 2.2s = ~19 minutes. The `--incremental` flag means if the run dies partway, re-running picks up where it left off (symbols that returned data won't be re-fetched on subsequent passes, since their last date gets auto-detected).

**Expected output:** `fact_open_interest.parquet` grows from ~8.6 KB (BTC only) to approx 3-5 MB (510 alts × 2.5 years × daily rows).

**Warning signs to watch for:**
- Many [SKIP] lines: CoinGlass doesn't track OI for some low-liquidity alts. Expected for maybe 10-20% of the list. Log but don't worry unless it's >50%.
- Rate limit 429 errors: shouldn't happen at 2.2s spacing, but if they do, the retry logic handles it.
- Total runtime >30 min: check if backoffs are triggering; inspect log.

**Subtask 1.3 — Wire into the daily pipeline.**

`run_live_pipeline.py` currently has a CoinGlass funding step. Look at how it's invoked, then add an analogous step for OI right after. Use `--incremental` so daily runs only fetch new data (~510 × 2.2s = 19 min still, same budget).

Render is Singapore instance Standard (1 CPU, 2GB). 19 extra minutes on daily pipeline run is fine within the current schedule (pipeline kicks off 00:05 UTC, nothing after that waits until ~01:24 UTC Drive sync). Keep an eye on total daily pipeline wall-clock: if it creeps past ~60 minutes, reconsider.

### Phase 2: FDV coverage audit (~15 minutes)

**Goal:** Know what fraction of `fact_markets_snapshot` has non-null `fully_diluted_valuation_usd`. This isn't ingestion work — it's understanding what's already there.

**One-off script at `scripts/diagnostic_fdv_coverage.py`:**

```python
# Pseudocode
import pandas as pd
from repo_paths import data_lake_root

lake = data_lake_root()
df = pd.read_parquet(lake / "fact_markets_snapshot.parquet")

# Latest snapshot only
latest_date = df["date"].max()
latest = df[df["date"] == latest_date]

print(f"Latest snapshot: {latest_date}")
print(f"Total coins: {len(latest)}")
print(f"FDV non-null: {latest['fully_diluted_valuation_usd'].notna().sum()}")
print(f"FDV null: {latest['fully_diluted_valuation_usd'].isna().sum()}")
print(f"max_supply non-null: {latest['max_supply'].notna().sum()}")

# Cross-tabulate: coins with market_cap_rank <= 300 (the strategy universe)
top300 = latest[latest["market_cap_rank"] <= 300]
print(f"\nTop 300 by rank:")
print(f"  FDV non-null: {top300['fully_diluted_valuation_usd'].notna().sum()}/{len(top300)}")
```

Report the output. If FDV is null for a meaningful fraction of top-300 coins, that constrains how Gate 1 (supply ratio) can be designed. This is a 2-minute query, not a full-scale audit — the output informs the later gate-design conversation.

Per `.cursorrules` Rule 1, this goes in `/notebooks` (diagnostic) or `/scripts` (permanent utility). Call this one **permanent utility** because we may want to re-run it as snapshots accumulate — put it in `/scripts`.

### Phase 3: Documentation updates (~20 minutes)

Update three files:

**`data_dictionary.yaml`:**
- Confirm `open_interest_usd` is documented. If not, add entry under a `coinglass_oi` section: unit = USD, frequency = daily, source = coinglass.
- Confirm `fully_diluted_valuation_usd`, `circulating_supply`, `total_supply`, `max_supply` are documented in the snapshot section. If not, add them.

**`ARCHITECTURE.md`:**
- Section listing fact tables: update `fact_open_interest` from "BTC-only" to "~510 altcoins + BTC, 2024-01 to present".
- "Live Pipeline DAG" section: add the altcoin OI step.
- Known gaps section: remove altcoin OI from the list.

**`docs/DATA_LAKE_CONTEXT.md`:**
- Table catalog: update `fact_open_interest` coverage description.
- Known gaps: remove altcoin OI from the list.
- Data quality issues: if FDV coverage audit shows meaningful gaps, add a note.

---

## 3. What NOT to do this session

- Don't build a scanner. Gate design is pending a real decision on Gate 5. This session is pure data plumbing.
- Don't touch the funding fetcher. It's already broad.
- Don't add new OI endpoints or exchange coverage. `aggregated-history` on CoinGlass is the right call — it already cross-exchange aggregates.
- Don't add perp-vs-spot volume ingestion. That's Gate 2 infrastructure, a separate session.

---

## 4. Design decisions already made

- **Universe**: match funding's 510-altcoin universe (via `universe_eligibility.parquet` → `perp_listings_binance.parquet` fallback). Dan's call: coins not on Binance perp aren't tradable for this strategy, so they're not worth the API budget.
- **History depth**: 2024-01-01 forward. Matches other tables' reliable window (CoinGecko Basic tier's 2-year history limit). Pre-2024 data would be suspect anyway.
- **Failure behavior**: skip-and-log (already how the fetcher behaves).
- **Incremental updates**: use `--incremental` in daily pipeline.
- **FDV**: audit coverage, don't re-ingest. Ingestion is already happening.

---

## 5. Repo conventions (must follow)

From `.cursorrules`:
- **Rule 1:** No diagnostic `.py` at repo root. New scripts in `/scripts`.
- **Rule 3:** No hardcoded symbols/dates/thresholds. Use config files or CLI args.
- **Rule 4:** Strategy-prefixed filenames, no `_v2`/`_final` suffixes.
- **Rule 5:** Zero-trust. Any new column added to a fact table must be registered in `data_dictionary.yaml` with unit + frequency + source. HALT if unclear.
- **Rule 9:** Strict UTC everywhere.

Commit messages should have a short first line + a body explaining what and why, matching the existing pattern in git log.

---

## 6. How to talk to Dan

- Direct, concrete. If something feels off, say so.
- Flag unknowns before acting on them (e.g., "the pipeline step invocation uses X pattern — is that intentional or a candidate to refactor?").
- Commit granularly. One logical change per commit, pushed to main.
- After Render's backfill runs, report back with specific numbers: rows added, assets covered, wall-clock time, any [SKIP]s that happened.

---

## 7. When to come back to the claude.ai chat

- If the universe resolution reveals a design question (e.g., "universe_eligibility has 580 symbols but perp_listings has 510 — which is canonical?")
- If the FDV audit shows a result that challenges Gate 1 assumptions
- If the Render backfill fails in an unexpected way that needs new judgment
- Otherwise, stay in Claude Code until the three phases are done.

---

## 8. Expected session flow

1. Read this doc, `.cursorrules`, `DATA_LAKE_CONTEXT.md`
2. Phase 1.1: edit `scripts/fetch_coinglass_oi.py` to add universe auto-resolution. Dry-run, verify, commit, push.
3. Phase 1.2: on Render shell, run the backfill with `--start-date 2024-01-01 --incremental`. Report when done.
4. Phase 1.3: wire into `run_live_pipeline.py`. Commit, push, Render redeploys.
5. Phase 2: write `scripts/diagnostic_fdv_coverage.py`. Run on Render. Report output to Dan.
6. Phase 3: update the three docs. Single commit. Push.
7. Confirm to Dan that the session is done, with a summary of: rows added, commits pushed, docs updated, anomalies found.

Commit pattern: phase 1.1 + 1.3 = one commit (code change). Phase 1.2 = no commit (just a Render-shell execution). Phase 2 = one commit (new diagnostic script). Phase 3 = one commit (docs). Total: ~3 commits.
