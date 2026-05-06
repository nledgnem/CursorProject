# CoinGecko Allowlist Quarterly Refresh

The CoinGecko ingestion universe (`data/perp_allowlist.csv`) is a **static**
top-1,000-by-mcap snapshot. It is refreshed **quarterly** to track market drift.

| | |
|---|---|
| **Refresh cadence** | Quarterly (~90 days) |
| **Last refresh** | 2026-05-05 (snapshot of top 1,000 by mcap) |
| **Next refresh due** | 2026-08-05 |
| **Why quarterly** | Reproducibility-friendly. Backtests re-run within a quarter use the same universe; mid-quarter drift is small relative to top-1,000 membership stability. See `docs/DATA_LAKE_CONTEXT.md` §4 "Ingestion universe" for the static-vs-dynamic rationale. |

The system heartbeat (`system_heartbeat.py`) fires a once-per-day Telegram
reminder after the next-refresh-due date until the refresh lands. If you see
that ping, this is the doc to follow.

## Procedure

Run from repo root, on a clean working tree, on a fresh feature branch (don't push directly to main).

### 1. Set the date variables

Replace `<YYYYMMDD>` (compact form) and `<YYYY-MM-DD>` (hyphenated form) with today's UTC date throughout. Example: `20260805` and `2026-08-05`.

### 2. Verify clean working tree

```bash
git status
# Expect: nothing pending. If there are uncommitted changes, stash or commit first.
```

### 3. Backup current allowlist with date stamp

```bash
cp data/perp_allowlist.csv data/perp_allowlist.<YYYYMMDD>_pre_refresh.bak.csv
wc -l data/perp_allowlist.csv data/perp_allowlist.<YYYYMMDD>_pre_refresh.bak.csv
md5sum data/perp_allowlist.csv data/perp_allowlist.<YYYYMMDD>_pre_refresh.bak.csv
# Expect: identical line counts, identical md5sums.
```

### 4. Re-run the builder (top 1,000 by mcap, $1M floor)

`expand_allowlist.py` calls CoinGecko `/coins/markets` directly. Requires `COINGECKO_API_KEY` env var to be set in the shell.

```bash
python scripts/archive/expand_allowlist.py \
  --n 1000 \
  --min-mcap 1000000 \
  --output data/perp_allowlist.csv
```

The builder includes Phase B's `is_unique` assert and dedupe-by-symbol logic (kept canonical = highest-mcap variant per symbol). If the assert fires, **stop** — the writer-race trigger condition has been re-introduced and you should investigate before continuing.

### 5. Verify the rebuild

```bash
wc -l data/perp_allowlist.csv
# Expect: ~1000 rows + 1 header. Exact count varies (some coins may dedupe out
# if the same symbol has multiple slugs in the top 1000).

python -c "
import pandas as pd
df = pd.read_csv('data/perp_allowlist.csv')
assert df['symbol'].is_unique, 'WRITER-RACE TRIGGER REINTRODUCED'
assert not df['symbol'].isna().any(), 'NaN symbol present'
assert not df['coingecko_id'].isna().any(), 'NaN coingecko_id present'
print(f'rows: {len(df)}, unique symbols: {df[\"symbol\"].nunique()}')
"
```

### 6. Diff against the prior version

```bash
diff <(cut -d, -f1 data/perp_allowlist.<YYYYMMDD>_pre_refresh.bak.csv | sort) \
     <(cut -d, -f1 data/perp_allowlist.csv | sort)
```

**Expected:** ~50–200 symbols added/removed, reflecting market drift over a quarter.

**Halt and investigate** if:

- More than 300 symbols changed (suggests upstream API anomaly or bug — don't ship).
- Critical symbols (BTC, ETH, USDT, USDC, SOL, BNB, XRP, DOGE) dropped out (extremely unlikely — they would never leave top 1,000 — almost certainly an API error).
- Any of the still-OPEN Apathy Bleed picks dropped out (check `data/curated/data_lake/apathy_bleed_book.csv` for `status='OPEN'` rows; cross-check tickers against the new allowlist). Surface to Mads + Dan before committing — open positions losing universe coverage is a strategy concern, not a docs decision.

### 7. Spot-check the new CSV

```bash
head -10 data/perp_allowlist.csv
# Expect: BTC, ETH, USDT, BNB, XRP, USDC, SOL ... at the top (highest mcap).

tail -5 data/perp_allowlist.csv
# Expect: small-cap coins around the rank-1000 mcap floor (~$10–20M).
```

### 8. Commit and PR

```bash
# Create a feature branch
git checkout -b allowlist-refresh-<YYYY-MM-DD>

# Stage both files
git add data/perp_allowlist.csv data/perp_allowlist.<YYYYMMDD>_pre_refresh.bak.csv

# Commit. Tag the message body with [DECISION YYYY-MM-DD] per documentation policy.
git commit -m "data: quarterly allowlist refresh <YYYY-MM-DD>

[DECISION <YYYY-MM-DD>] Quarterly refresh per docs/runbooks/allowlist_refresh.md.
Diff vs prior: <N> added, <M> removed, <K> kept (paste counts from step 6).
Backup of prior state at data/perp_allowlist.<YYYYMMDD>_pre_refresh.bak.csv."

git push origin allowlist-refresh-<YYYY-MM-DD>
# Open PR via GitHub UI; merge after review.
```

### 9. Update `docs/DATA_LAKE_CONTEXT.md` §4 ingestion-universe subsection

Update the **last refresh** and **next refresh due** dates. Same PR or separate doc-only commit on the refresh branch:

```diff
- **Last refresh:** 2026-05-05.
- **Next refresh due:** 2026-08-05 (quarterly).
+ **Last refresh:** <YYYY-MM-DD>.
+ **Next refresh due:** <YYYY-MM-DD + 90 days> (quarterly).
```

Also update `data_dictionary.yaml::data_sources.coingecko.ingestion_universe.last_refresh` and `next_refresh_due`. The system heartbeat reads these to decide whether to fire the reminder; updating them silences the reminder until the next quarter.

### 10. Post-merge — confirm tomorrow's nightly runs on the new allowlist

```bash
# After merge to main and Render redeploys, on Render shell tomorrow morning:
tail -50 /tmp/run_live_pipeline.log  # or wherever pipeline stdout lands
# Expect: "Downloading data for ~1000 coins" (or whatever expand_allowlist produced)
```

If the daily call count matches the new allowlist size, the refresh has fully landed. Otherwise investigate (Render may be on a stale checkout, daemon may not have restarted, etc.).

## When to refresh OUTSIDE the quarterly cadence

- **Mads asks for it.** A new strategy needs visibility into specific coins not in the current allowlist.
- **Major market event.** A 50%+ correction or major rotation may shift top-1,000 membership materially. Refresh sooner if Mads wants the post-event view.
- **Open Apathy Bleed position drops out of top 1,000.** Surface to Mads + Dan; refresh decision per their call (could be: refresh early, or accept the drop-out and let the position freeze at last-fetched mcap until next quarterly).

## When NOT to refresh

- **The allowlist disagrees with what someone "thinks" the top 1,000 should be.** Trust the snapshot — it's the empirical answer at refresh time.
- **Mid-incident** (e.g., during a writer-race-class investigation). Stabilize the pipeline first; refresh only when the data lake is in a known-good state.
- **CoinGecko is having API issues.** `/coins/markets` returning anomalous data shouldn't be committed. Wait it out.

## Sanity checks to keep in the head

| Check | Pass criterion | Fail action |
|---|---|---|
| Row count after dedupe | ~1,000 ± 50 | Investigate the dedupe; symbol-count mismatch suggests duplicate-symbol slugs in the API response |
| `is_unique` assert | True | The writer-race trigger has been re-introduced. Halt. |
| Diff vs prior | <300 symbols changed | >300 changed = upstream API anomaly; don't ship |
| Top 10 by mcap | BTC, ETH, USDT, BNB, ... | Out-of-order or missing critical symbols = API error |
| Open Apathy picks | All present | Halt and surface to Mads + Dan |

## History

- **2026-05-05** — initial cut to top 1,000 by mcap (Phase E). 977 rows.
- **(next refresh)** — 2026-08-05 due.
