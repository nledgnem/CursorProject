# Incident 2026-09-18 — asset identity in the CoinGecko fact tables

**Status:** repair prepared, **not applied**. Branch `fix/lake-asset-identity`. The lake apply runs on
Render (the only writer) after the fixes below are deployed. Nothing in the live lake or on Drive has
been modified.

## 1. What is wrong

`fact_price` / `fact_marketcap` / `fact_volume` (and `silver_fact_price` / `silver_fact_marketcap`) are
keyed by `asset_id` — an upper-case **ticker**. The coin behind a ticker was whatever
`data/perp_allowlist.csv` named at fetch time, and nothing recorded that choice. Audit of the live lake
(Drive export of 2026-09-18, pinned with SHA-256 at
`C:\Users\Admin\Documents\lake_snapshots\2026-09-18_pre_asset_identity_repair\MANIFEST.json`) against
Binance USDT-M perps, `/coins/markets` and the committed git vintages:

**Current values are right; history is spliced.**

| Window | What | Scope |
|---|---|---|
| 2026-03-04 → 03-30 | Rows re-injected from the stale git wide files (root cause A) | 72 assets carry another coin's **market cap** (ETH/SOL/DOGE ≈ $2M; TRX/AVAX/HYPE/ZEC/TON $14–27M; USDT/USDC/WBTC/WETH…); ~23 another coin's **price** (DOGE, SHIB, TRUMP, TON, PUMP, FARTCOIN, MOODENG, CAT…). Silver passes them unflagged. |
| to 2026-01-28 | Ticker re-bound to a different coin on 2026-01-28; everything before is the other coin | ~60 tickers: NEIRO, BRETT, PNUT, BABY, ARIA, ARC, SHELL, GOAT, HOLO, KITE… |
| pre-2024-05 | Writer-race / Analyst-tier residue | LINK 2020 → 2024-01; LUNC, JOE, XAI, TON early 2024 |
| now | Wrong coin today | Binance BOB (`1000000BOBUSDT`), FRAX, TREE (and PUMPBTC, stale) |

Also: `dim_asset.coingecko_id` = lower-cased ticker in 100% of rows (only 137 of 851 allowlisted assets
carry the id actually fetched; 69 carry a real id of a **different** coin, so a lookup returns the wrong
coin, not a 404). `asset_id = 'BITCOIN'` is HarryPotterObamaSonic10Inu; real Bitcoin is `'BTC'` — but
`fact_markets_snapshot` stamps real Bitcoin `'BITCOIN'` and re-keys coins when CoinGecko renames a ticker
(Toncoin `TON` → `GRAM` on 2026-06-15; 13 coins; 106 ids duplicated per day).

## 2. Root causes (each reproduced)

- **A. Stale wide files re-injected into fact tables.** `scripts/incremental_update.py` merged each
  download into `data/curated/*_daily.parquet` with `combine_first` (existing wins) and converted the
  *merged* rows into fact rows. On Render those wide files live in the repo checkout and are re-seeded
  from git on every deploy (599e5cb, ending 2026-03-30, writer-race era). A run starting 2026-03-04
  therefore upserted the stale values over 03-04 → 03-30. Evidence: live `fact_marketcap` for those
  dates equals the git `marketcap_daily.parquet` in **100% of 68,769 cells**. This undid the 2026-05-05
  writer-race refetch (whose spot checks on 2026-03-06 had passed). Which run started on 03-04 is not
  visible without Render logs.
- **B. Symbol-based binding.** The allowlist builder binds each ticker to the highest-market-cap
  CoinGecko coin with no Binance check; re-binding a ticker silently splices its history (2026-01-28).
- **C. `fact_markets_snapshot` frozen 2026-08-04 → 09-18.** CoinGecko returns `fully_diluted_valuation`
  ≈ 1e24 for linqai and peipeicoin-vip; polars inferred Int64 and the frame build raised daily; Step 0
  failures were log-only and the pipeline exited 0.
- **D. CoinGecko's own history is corrupt for some coins in 2026-01 → 03** (1inch 0.156 vs Binance
  0.0925 on 2026-03-22; ACH, ACX, S, SHELL…) while the lake, captured live, matches Binance. A
  CoinGecko re-fetch is therefore **not** a safe repair source unless each value is validated.

## 3. Impact

- **Live, high — MSM basket → `F_tk` → `funding_regime` → `is_mrf_active`.** `msm_universe` ranks the
  top-30 alts on `fact_marketcap`. Live `msm_timeseries.csv` decisions 2026-03-09 → 03-30 dropped
  SOL, DOGE, TRX, AVAX, TON, HYPE, ZEC, PUMP (replaced by ATOM, KAS, MORPHO, POL, QNT, RENDER, WLD,
  SIREN); 2026-03-23 shows `is_mrf_active=True` on that basket. The run recomputes 730 days nightly,
  so those weeks feed today's 52-week percentile. Before/after: §6.
- **Not exposed:** the Apathy Bleed "Gate-1 circulating/total ratio" does not exist (STRATEGIES §8 #8;
  production gates are perp check, Cold Flush APR, Oct-2025 quarantine; strategy exited 2026-05-12).
  Live BTCDOM is Binance's official index (ADR 004); the lake-native reconstruction is not wired in (it
  would spike in March — its denominator loses ~$300B of ETH/SOL/stable caps).
- **Medium:** `single_coin_panel` (symbol-matched `/coins/markets`; TON is GRAM there now),
  `perp_coverage_summary` (Apathy's real Gate 1 matches venue tickers to lake asset_ids),
  `enrichment/build_enrichment.py` (overrides silver caps with the stale snapshot).
- **Research:** anything using lake price/cap history across the windows above (Track F paused).

## 4. Decisions (2026-09-18)

- Repair **approved in principle**, via a dry-run manifest with before/after row counts and hashes,
  applied on Render with backups; recoverable periods first; unrecoverable wrong-coin history
  **quarantined, never invented**; silver + downstream rebuilt; identity checks re-run before closing.
- **Allowlist refresh frozen** until binding keys on real ids with price validation.
- Identity checks **nightly, non-blocking** at first; after a clean baseline, block on invalid
  CoinGecko id, duplicate/conflicting effective mapping and mass coin-switch events. Per-asset Binance
  mismatches quarantine that asset's new rows rather than halting ingestion (Binance-side events —
  delistings, redenominations — would otherwise stop the pipeline for reasons that are not ours).
- Identity model: **immutable internal `asset_uid`** (today's `asset_id` string, never re-pointed),
  with effective-dated `coingecko_id` / `binance_symbol` (`data/asset_registry.csv`).
- Recompute MSM / funding-regime / MRF after the repair and compare; keep the as-decided record.
- Freshness SLA for `fact_markets_snapshot` (alert > 48h).
- Pause Track F analysis that joins lake market cap or CoinGecko identity.

## 5. What is on the branch

| Commit | Change |
|---|---|
| `413ae3f` | `verify_ingestion_integrity.py --mode asset_identity` (A1–A5) + `src/data_lake/asset_identity.py` |
| `e08fb02` | **Root cause A fix**: fact rows built only from this run's download (regression test reproduces the incident) |
| `4a2eb91` | Allowlist refresh frozen (`data_dictionary.yaml` `refresh_frozen`; script refuses; reminder silenced) |
| `94ad4ac` | **Root cause C fix**: snapshot frame built against the stored schema; Step 0 failure alerts |
| `25ac34b` | `--mode freshness` (content SLA) + nightly non-blocking `src/exports/lake_integrity.py` |
| `739c591` | `scripts/repair_asset_identity.py` (registry / dry-run / apply) |
| `21438e9` | `convert_to_fact_tables.py` refuses a full rebuild that would rewind a newer lake |
| `da758b7` | A3 uses the registry's dated Binance bindings; repair emits registry-keyed dims |
| `4a2bb86` + final | `data/asset_registry.csv`, conflicts, and the dry-run manifest below |

## 6. Repair — dry run (done) against the pinned snapshot

Manifest: `manifest/` (snapshot hashes in `manifest/snapshot_MANIFEST.json`; before/after file hashes in
`manifest/summary.json`). Built by `scripts/repair_asset_identity.py registry` then `dry-run`. Nothing
was written to the lake.

**Registry** (`data/asset_registry.csv`): 2,842 asset_uids (977 active allowlist + 1,865 cut on 2026-05-05
that still hold history), 571 Binance bindings, each validated against the coin's *own* CoinGecko
history (≥14 days, median gap ≤10%), including renamed perps (TON: `TONUSDT` → `GRAMUSDT` from
2026-07-02; IP: `IPUSDT` → `DATAIPUSDT`). Not bound, with proposed ids (`data/asset_registry_conflicts.csv`):
`1000000BOBUSDT` → build-on-bnb, `FRAXUSDT` → frax-share, `TREEUSDT` → treehouse, `ARXUSDT` → arcium,
`PUMPBTCUSDT` unresolved; `CGPTUSDT` and the hakimi perp lack enough CoinGecko data to confirm.

**Evidence → action.** A lake row is treated as another coin only on independent evidence; a CoinGecko
re-fetch replaces it only when the re-fetched value is itself validated; otherwise the row is
quarantined (moved to `quarantine_fact_identity.parquet`), never invented.

| Class | Evidence | Replacement accepted if | Segments | Assets | Rows replaced | Rows quarantined | Default |
|---|---|---|---|---|---|---|---|
| `mass_window_2026_03` | re-injection signature (3x in on 03-04, 3x out on 03-31) or price vs Binance | price vs Binance ≤10%, or implied supply within 1.25x of the lake's outside the window; no perp: continuity ≤1.5x | 138 | 129 | 5,498 | 1,443 | apply |
| `long_splice` (≥7 d) | lake price vs Binance >10% | re-fetched price vs Binance ≤10% | 201 | 121 | 19,668 | 1,146 | apply |
| `short_splice` (<7 d) | ≥3 days median >10%, or any day >2x, vs Binance | same | 220 | 145 | 1,910 | 298 | apply |
| `pre_window_unrecoverable` | vs Binance >10% for 14+ days before the 725-day re-fetch window | — | 12 | 7 | 0 | 5,907 | apply |
| `predates_coin` | rows before the coin's CoinGecko history starts, >3x break at hand-over | — | 96 | 96 | 0 | 53,349 | apply |
| `predates_coin_tail` | old coin continuing past the hand-over (continuous with the quarantined series, >3x from the new coin) | — | 58 | 58 | 0 | 9,552 | apply |
| `rebinding_2026_01_28` | no perp; 3x jump on 01-28; before it >25x off the coin's CoinGecko history (largest CoinGecko error vs Binance measured: 24x), after it ≤1.5x | — | 22 | 22 | 0 | 8,838 | apply |
| `predates_coin` (no break) | as above but hand-over within 3x (mostly stablecoins) | — | 14 | 14 | 0 | 11,604 | **review** |

Row totals with the defaults applied (rows counted per table; quarantined = moved out):

| Table | Before | After | Replaced | Quarantined |
|---|---|---|---|---|
| `fact_price` | 1,748,828 | 1,722,096 | 8,479 | 26,732 (1.5%) |
| `fact_marketcap` | 1,739,791 | 1,713,349 | 9,204 | 26,442 |
| `fact_volume` | 1,743,456 | 1,716,679 | 9,391 | 26,777 |
| `dim_asset` / `map_provider_asset` | 2,717 | 2,717 | coingecko_id = registry id for 2,716 (null otherwise) | — |

Reported, **not** applied: `timing_noise_not_applied.csv` (1–2 day gaps of 10–90% vs Binance on crash
days); `rebinding_2026_01_28_review.csv` (21 coins with a 01-28 jump but a 1–25x gap or a post-switch
mismatch — CoinGecko history alone cannot settle them); `coingecko_history_defects.csv` (331 assets
where CoinGecko's current history disagrees with Binance while the lake agrees — the reason every
re-fetched value is validated; a bulk CoinGecko re-fetch would have injected errors).

**Checks, live lake vs repaired candidate** (`manifest/identity_check_*.json`):

| Signal | Live | After repair |
|---|---|---|
| A1 dim_asset ids = fetched ids | FAIL (714/851 wrong) | PASS |
| A2 ticker spells another coin's slug | FAIL | FAIL — needs the uid-rename decision (§7) |
| A3 lake close = Binance (registry bindings) | FAIL (62 wrong-coin runs) | PASS (0; 7 known conflicts) |
| A4 mass coin-switch dates (≥20 assets 3x overnight) | FAIL (01-28 ×67, 03-04 ×64, 03-31 ×55) | FAIL at the threshold: market cap 01-28 ×20 (the review list) |
| A5 price + cap = `/coins/markets` | FAIL (snapshot stale) | FAIL until the snapshot fix is deployed |
| Freshness F1–F7 | F7 FAIL (snapshot 45 d old) | same until deployed |

**MSM / funding regime / MRF — sandbox before vs after** (`manifest/msm_before_after.csv`): silver rebuilt
and `msm_run` re-run on both lakes with identical code; the "before" run reproduces the live
`msm_timeseries.csv` baskets on 100% of decision dates.

| Decision | Basket change (repair) | funding_pct_rank | Regime | MRF |
|---|---|---|---|---|
| 2025-02-24, 03-03 | S in (FIL / RENDER out) | — (warm-up) | — | — |
| 2026-03-09 | +AVAX DOGE HYPE PUMP SOL TON TRX ZEC, −ATOM ENA KAS MORPHO NIGHT POL QNT WLD | 0.096 → 0.212 | Q1 = Q1 | off = off |
| 2026-03-16 | same 8 restored | 0.192 → 0.231 | Q1 = Q1 | off = off |
| 2026-03-23 | 7 restored | 0.269 → 0.288 | Q2 = Q2 | on = on |
| **2026-03-30** | 7 restored | 0.250 → 0.288 | **Q1 → Q2** | **off → on** |
| 2026-04-13, 06-01, 06-15 | order only | ≤0.02 | = | = |
| 2026-08-10 | BEAT in, WLD out | 0.846 → 0.865 | Q4 = Q4 | off = off |
| **2026-09-18 (today)** | none | 0.673 = 0.673 | Q3 = Q3 | off = off |

One historical gate decision differs (2026-03-30: MRF would have been active); today's regime is
unaffected. Keep the as-decided record (runbook step 0) and annotate 2026-03-09 → 03-30.

## 7. Decisions still needed

1. **Rename misleading uids** (A2): `BITCOIN` (HarryPotterObamaSonic10Inu), `METAL` (metal-blockchain;
   Metal DAO is `MTL`), `NUSD` (nusd-2; `nusd` is SUSD's coin), `USA`, `WINK` — proposed: new uids named
   after the coin (e.g. `HPOS10I`), recorded in the registry. Until then A2 stays red.
2. **Binance perps that are other coins** (registry conflicts): onboard as new uids with the proposed
   CoinGecko ids, or leave unbound.
3. **Review lists:** 14 `predates_coin` without a break; 21 coins in `rebinding_2026_01_28_review.csv`.
4. **Blocking promotion** once the repaired lake is clean: A1, A3 (as per-asset quarantine), A4, and
   registry duplicate/conflicting bindings.
5. Delete or stop seeding the stale git wide files `data/curated/*_daily.parquet` (harmless after
   `e08fb02`, but a trap for any other reader).

## 8. Apply runbook (Render shell — after the branch is merged and deployed)

Prerequisites: `e08fb02` deployed (otherwise a backfill can re-inject stale rows); the next nightly
run shows `fact_markets_snapshot` fresh and the `[LAKE INTEGRITY]` Telegram arrives; run outside the
00:05 UTC pipeline and the ~01:24 UTC export.

```bash
cd /opt/render/project/src
M=reports/incidents/2026-09-18_asset_identity/manifest
# 0. keep the as-decided MSM record (synced to Drive from the lake folder)
cp /data/exports/msm_timeseries.csv /data/curated/data_lake/msm_timeseries.as_decided_pre_identity_repair.csv
# 1. dry confirmation: re-hashes every segment against the live lake, writes nothing
python scripts/repair_asset_identity.py apply --lake-dir /data/curated/data_lake --manifest-dir $M
# 2. apply: backs up fact_price/marketcap/volume + dims to /data/curated/data_lake/_backup_asset_identity_<utc>/
python scripts/repair_asset_identity.py apply --lake-dir /data/curated/data_lake --manifest-dir $M --include-dim --yes
# 3. rebuild silver, then verify
python scripts/data_ingestion/build_silver_layer.py
python scripts/verify_ingestion_integrity.py --mode asset_identity
python scripts/verify_ingestion_integrity.py --mode freshness
# 4. recompute MSM / funding regime / MRF now (or wait for the nightly run)
python run_live_pipeline.py --skip-ingestion
```

- **Hash mismatch in step 1:** rows changed since 2026-09-18. Re-run `registry` + `dry-run` against the
  current Drive export and ship the new manifest; `--skip-changed` applies only unchanged segments.
- **Rollback:** copy the files from `_backup_asset_identity_<utc>/` back and rebuild silver.
- **Expected after:** A1 and A3 PASS; A4 at most the 01-28 review residue; A2 until §7.1; A5 once the
  snapshot is fresh.
