# Handoff: CoinGlass Liquidations Ingestion

**Context to load first:** `docs/DATA_LAKE_CONTEXT.md`, `.cursorrules`, `data_dictionary.yaml`.

## Why this work

The lake has funding and OI from CoinGlass. Liquidations are the direct "shorts got squeezed" signal — aggregate daily long-liquidation-USD and short-liquidation-USD per coin across exchanges. High signal density for short-selection strategies, since it directly measures the pain shorts took in the past. Cheap to add: same provider, same auth, same ingestion pattern as the OI work committed on 2026-04-22.

## Endpoint reference (verified from CoinGlass v4 docs)

**URL:** `https://open-api-v4.coinglass.com/api/futures/liquidation/aggregated-history`

**Query params:**
- `exchange_list` (required, string) — comma-separated exchanges, e.g. `"Binance,OKX,Bybit"`. Default `"Binance"`.
- `symbol` (required, string) — base coin, e.g. `"BTC"`.
- `interval` (required, string) — `1m|3m|5m|15m|30m|1h|4h|6h|8h|12h|1d|1w`. We want `1d`.
- `limit` (int, default 1000, max 1000).
- `start_time` (int64, ms).
- `end_time` (int64, ms).

**Auth:** same header as OI: `CG-API-KEY: <COINGLASS_API_KEY env var>`.

**Rate limit:** same Hobbyist tier limits as OI. 30 req/min with 2.2s spacing. History cap likely ~334d per symbol (same tier constraint; verify empirically).

**Expected response shape** (based on the OI endpoint pattern — verify in Step 1's first successful fetch):
```json
{
  "code": "0",
  "msg": "success",
  "data": [
    {"time": 1717200000000, "long_liquidation_usd": 12345.67, "short_liquidation_usd": 8901.23},
    ...
  ]
}
```

The exact field names for long/short liquidation USD may differ (e.g. `aggregated_long_liquidation_usd`, `longLiquidationUsd`). Step 1 must log the first response verbatim and confirm field names before writing the parser. Do not guess.

## Scope

Add a third branch to `scripts/fetch_coinglass_data.py`, mirroring the funding and OI branches:

1. New `--fetch-liquidations` arg flag at module scope (alongside `--fetch-funding` and `--fetch-oi`).
2. New `fetch_liquidation_history(api_key, symbol, start_date, end_date, ...)` function mirroring `fetch_oi_history` in structure (retry logic, rate-limit handling, fail-fast on 400/404 with "not found" / "invalid" / "does not exist" / "supported exchange" in error msg).
3. New `fetch_liquidations_for_symbols(...)` mirroring `fetch_oi_for_symbols` (per-symbol incremental logic, existing_data dedup, ETA reporting).
4. New `--liquidations-output` arg with default `data_lake_root() / "fact_liquidations.parquet"`.
5. New `if args.fetch_liquidations:` block in `main()` mirroring the OI block (resolve universe via `_resolve_coinglass_symbol_universe()`, load existing for incremental/merge, fetch, dedupe+sort, merge-with-existing, write parquet).
6. Update the "if neither flag set → fetch both" fallback to "if no fetch flag set → fetch funding + OI + liquidations".
7. Update `scripts/fetch_coinglass_data.py` docstring.
8. Update `run_live_pipeline.py` Step 1 invocation (`SCRIPT_FUNDING` command) to pass `--fetch-funding --fetch-oi --fetch-liquidations` explicitly (rather than relying on the "no flag → fetch all" default) so the pipeline's behavior is explicit in the code.

## Output schema

**File:** `data/curated/data_lake/fact_liquidations.parquet`

**Columns:**
- `asset_id` (string) — canonical base symbol, uppercase. Matches `fact_open_interest.asset_id` and `fact_funding.asset_id`.
- `date` (date) — UTC calendar day.
- `long_liquidation_usd` (float, USD) — aggregated long-side liquidations for the UTC day, summed across exchanges in the `exchange_list` param.
- `short_liquidation_usd` (float, USD) — aggregated short-side liquidations for the UTC day.
- `source` (string) — `"coinglass"`.

**Aggregation from 8h/intraday to daily:** for `interval=1d`, CoinGlass should return pre-aggregated daily values. Confirm this in Step 1. If the endpoint returns sub-daily values even with `interval=1d`, sum within each UTC day (long + long, short + short — do NOT mean-aggregate; liquidations are flow quantities, not levels).

**Dedup key:** `(asset_id, date)`.

**Exchange list:** start with the same aggregation CoinGlass uses by default for OI. Read what the `aggregated-history` endpoint for OI passes by default; match it for liquidations. If the OI code explicitly passes an exchange list, use the same one. If it uses the endpoint's default, use the endpoint's default here too.

## Tier structure and pause points

Same tier-paused review pattern as the OI cleanup work on 2026-04-22. Commit each tier separately on a branch so each can be reverted independently.

### Tier 1 — Code change + small-scale smoke test (one commit)

1. Work on a branch (e.g. `claude/liquidations-ingestion-<timestamp>`).
2. Make the code changes above.
3. Do NOT touch standalone scripts in `scripts/archive/`.
4. Do NOT change funding branch behavior. Do NOT change OI branch behavior. Only add the third branch and update the pipeline invocation.

5. **Small-scale smoke test** before committing:
   ```bash
   # Only BTC, recent week. Confirms end-to-end including parquet write.
   python scripts/fetch_coinglass_data.py --fetch-liquidations \
       --symbols BTC --start-date 2026-04-15 --end-date 2026-04-22
   ```
   Expected: fetches ~7 daily rows for BTC, writes `fact_liquidations.parquet`.

6. Inspect the first row of the raw API response — confirm field names match what the parser expects. Update the parser if the real field names differ from the expected `long_liquidation_usd` / `short_liquidation_usd` (CoinGlass sometimes uses camelCase). Record the actual field names in a comment in the parser.

7. Verify with a pandas read:
   ```python
   import pandas as pd
   from repo_paths import data_lake_root
   df = pd.read_parquet(data_lake_root() / "fact_liquidations.parquet")
   print(df.head())
   print(df.dtypes)
   print(f"Rows: {len(df)}, Assets: {df['asset_id'].nunique()}, Date range: {df['date'].min()} to {df['date'].max()}")
   ```

8. Also test the module import of `run_live_pipeline`:
   ```bash
   python -c 'import run_live_pipeline'  # must exit 0
   ```

9. Commit. Message: `feat(coinglass): add liquidations ingestion (fact_liquidations.parquet)` with a body that describes what was added and references this handoff.

10. **PAUSE. Report back to Dan before Tier 2** with: (a) commit hash, (b) actual API response field names (verbatim, as sanity check), (c) the `df.head()` output, (d) the BTC row count.

### Tier 2 — Full backfill on Render shell (no new commit)

After Dan approves Tier 1 and merges it to main:

1. Render will auto-redeploy. Confirm latest commit is live on Render via `git log --oneline -1` in Render shell.
2. On Render shell, run the full backfill:
   ```bash
   nohup python scripts/fetch_coinglass_data.py --fetch-liquidations \
       --start-date 2024-01-01 --incremental --merge-existing \
       > /tmp/liquidations_backfill.log 2>&1 &
   echo "Backfill PID: $!"
   tail -f /tmp/liquidations_backfill.log
   ```
3. Watch first minute for universe resolution (should show ~500-600 symbols, not 1 BTC) and a few clean fetches.
4. Walk away for ~25-30 min. Expected runtime similar to OI backfill (~26 min for ~600 symbols at 2.2s spacing).
5. When done, verify:
   ```bash
   python -c "
   import pandas as pd
   from repo_paths import data_lake_root
   df = pd.read_parquet(data_lake_root() / 'fact_liquidations.parquet')
   df['date'] = pd.to_datetime(df['date'])
   print(f'Rows: {len(df):,}, Assets: {df[\"asset_id\"].nunique()}')
   print(f'Date range: {df[\"date\"].min().date()} to {df[\"date\"].max().date()}')
   print(f'Per-asset date ranges (sample):')
   for a in ['BTC', 'ETH', 'SOL', 'ARIA', 'DEXE', 'PIEVERSE']:
       sub = df[df['asset_id'] == a]
       if len(sub):
           print(f'  {a}: {sub[\"date\"].min().date()} to {sub[\"date\"].max().date()} ({len(sub)} rows)')
       else:
           print(f'  {a}: NO DATA')
   "
   ```
6. Report back to Dan with: success/failed/skipped counts, final row count, assets count, per-asset date ranges for the 6 sample assets. Same format as the OI backfill summary from 2026-04-22.

### Tier 3 — Documentation update (one commit)

After Tier 2 data confirms, update the three doc files with actual coverage numbers (not projections):

1. **`data_dictionary.yaml`** — add a new top-level `fact_liquidations:` block mirroring the `fact_open_interest:` block structure. Include:
   - `description`, `path`, `source: coinglass`, `frequency: daily`
   - `coverage:` sub-block with `universe`, `assets_approx` (actual from Tier 2), `start_btc`, `start_alts_typical`, `history_cap_days` (observed empirically, likely ~334).
   - `columns:` with all 5 columns, units = USD for the two liquidation columns, UTC_calendar_day for date.
   - In the `data_sources:` `coinglass:` block, extend the `notes:` field to mention liquidations are now covered.

2. **`ARCHITECTURE.md`** — in "Core fact tables" section, add a new bullet for `fact_liquidations` mirroring the existing `fact_open_interest` bullet. Same coverage caveats.

3. **`docs/DATA_LAKE_CONTEXT.md`**:
   - Section 4 (CoinGlass): mention liquidations are now ingested alongside funding and OI.
   - Section 5 table catalog: add a new row for `fact_liquidations.parquet`. Fill in actual size from Tier 2.
   - Section 13 known gaps: nothing to remove here; adding liquidations doesn't close any pre-existing gap, it's a new capability.

4. Commit. Message: `docs: register fact_liquidations in data_dictionary after 2026-04-23 backfill` with a body describing coverage observed.

5. **Done.** Report final state: branch commits, final file counts, backfill summary, coverage numbers.

## Explicit NOT-dos

- Do NOT change anything in `fact_funding` or `fact_open_interest` behavior.
- Do NOT touch anything under `scripts/archive/`.
- Do NOT create a new standalone script (e.g. `scripts/fetch_coinglass_liquidations.py`). The work belongs inside `scripts/fetch_coinglass_data.py` as a third branch. Avoids the "two plausible scripts" trap that caused the 2026-04-22 handoff bug.
- Do NOT assume history depth. Empirically determine whether the Hobbyist tier applies the same ~334-day cap to liquidations as it does to OI. Document what you find.
- Do NOT mean-aggregate liquidations if the endpoint returns sub-daily rows even for `interval=1d`. Liquidations are flow quantities (USD per time window); sum them, don't average.
- Do NOT skip the small-scale smoke test in Tier 1. Going straight to a full backfill burns API budget if the parser is wrong.

## What to verify empirically (unknowns)

Flag back if any of these turn out differently than expected:

1. **Field names in the API response.** Whether long/short liquidation USD are `long_liquidation_usd` or `longLiquidationUsd` or something else — the docs I read didn't show an example response, only the endpoint signature.
2. **Whether `interval=1d` returns daily-aggregated values or finer granularity.** This changes the parser (daily → one row per day; finer → aggregate in code).
3. **History depth on Hobbyist tier.** OI is capped at ~334 days; liquidations may or may not share the cap.
4. **Default exchange_list behavior.** Whether the endpoint's Binance default is what we want, or whether matching OI's exchange aggregation requires an explicit list.

If any of these surface unexpectedly, pause and report before continuing. Don't guess.

## Deliverables summary

- Tier 1: code change, smoke test output, field-name confirmation, one commit.
- Tier 2: backfill execution summary, coverage report for 6 sample assets.
- Tier 3: docs updated with actual numbers, one commit.
- Total: one branch, three tier boundaries, two code commits (Tier 1 + Tier 3), backfill between them.
