# Incident 2026-09-18 — asset identity in the CoinGecko fact tables

**Status:** OPEN — repair prepared and dry-run validated, **not applied**. Branch `fix/lake-asset-identity`
(PR). The apply runs on Render (the only writer) after the preventive fixes are deployed, in the order
of §9, and the incident closes only through the acceptance gate of §10. Nothing in the live lake or on
Drive has been modified.

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
  top-30 alts on `fact_marketcap`. For decisions 2026-03-09 → 03-30 the basket dropped SOL, DOGE, TRX,
  AVAX, TON, HYPE, ZEC, PUMP (in: ATOM, KAS, MORPHO, POL, QNT, RENDER, WLD, SIREN). The corruption was
  already live **at the time** (MSM runs committed in March show the same baskets); the 2026-05-05
  refetch removed it and root cause A brought it back. Two latent MSM flaws made gaps dangerous: it
  ranked an asset on its latest cap with **no age limit** and priced returns the same way, so a coin
  that stopped updating (cut 2026-05-05) competed on a months-old cap and a quarantine would have been
  bridged with pre-gap values (fixed, §7).
- **Not exposed:** the Apathy Bleed "Gate-1 circulating/total ratio" does not exist (STRATEGIES §8 #8;
  strategy exited 2026-05-12). Live BTCDOM is Binance's official index (ADR 004).
- **Medium:** `single_coin_panel` (symbol-matched `/coins/markets`), `perp_coverage_summary` (Apathy's
  perp gate matches venue tickers to asset_ids), `enrichment/build_enrichment.py` (fixed, §5-B).
- **Research:** anything using lake price/cap history across the windows above. Track F analysis that
  joins lake market cap or CoinGecko identity is **paused** until the repair is applied.

## 4. Decisions

**Round 1 (2026-09-18):** repair via dry-run manifest → Render apply with backups; allowlist refresh
**frozen**; identity checks nightly and non-blocking, then blocking for invalid ids, conflicting
mappings and mass coin switches once clean (a single asset's Binance mismatch quarantines that asset,
it does not halt ingestion); immutable internal `asset_uid` with dated attributes; recompute MSM /
regime / MRF and keep the as-decided record; snapshot freshness SLA; pause Track F lake-cap work.

**Round 2 (2026-09-18):**
1. **One `asset_uid` = one economic asset for all time.** Ids are never re-pointed or reinterpreted.
   Misleading ids keep their meaning and are acknowledged in `configs/asset_identity_policy.yaml`
   (11: BITCOIN = HarryPotterObamaSonic10Inu, METAL, NUSD, USA, WINK, ALLO, BLEND, DYDX, NAVI, TENSOR,
   ZETA — each verified as one continuous coin); a coin under two uids after a ticker rename is a
   declared **alias** (QAI → QFI, AITECH → ACN), not merged.
2. **Binance-perp conflicts resolved one by one**, each needing ≥ 2 strong identity forms (Binance
   contract/announcement, CoinGecko contract, rebrand record, price). Evidence:
   `data/asset_registry_conflict_resolutions.csv`; decisions: `data/asset_registry_overrides.csv`.
   New uids: `BUILD-ON-BNB` (1000000BOBUSDT), `ARCIUM` (ARXUSDT), `TREEHOUSE` (TREEUSDT), `FRAX-SHARE`
   (FXSUSDT until 2026-01-05, FRAXUSDT from 2026-01-15 — the 1 FXS = 1 FRAX rebrand). Bound to existing
   uids: CGPTUSDT → CGPT, 哈基米USDT → 哈基米. **PUMPBTCUSDT unresolved → unbound** (contracts agree, but
   CoinGecko's derivatives mapping contradicts and its price history mixes in Pump.fun markets).
3. **Uncertain history is quarantined, not guessed:** the 14 predates-coin segments without a clean
   break and the 21 ambiguous 2026-01-28 re-bindings are quarantined with `status=manual_review`.
4. Push and open a PR; do not merge or apply yet. Deployment order §9.
5. Prove quarantine semantics before apply (§7).
6. Keep AS_DECIDED and RECOMPUTED_ON_CORRECTED_DATA side by side; never overwrite history (§8).
7. Close only through the acceptance gate (§10).

## 5. What is on the branch

**A. Preventive / root-cause fixes**
- `scripts/incremental_update.py` — fact rows only from this run's download (root cause A; the
  regression test reproduces the incident on the old code).
- `scripts/convert_to_fact_tables.py` — refuses a full rebuild that would rewind a newer lake.
- `scripts/fetch_high_priority_data.py` + `run_live_pipeline.py` — snapshot frame built against the
  stored schema; Step 0 failure alerts on Telegram (root cause C).
- Allowlist refresh frozen (`data_dictionary.yaml` `refresh_frozen`; script refuses; reminder off).
- MSM: `max_mcap_age_days: 3` and bounded `get_close_asof` — no stale cap or price across a gap.

**B. Registry / identity model**
- `data/asset_registry.csv` (2,846 uids, 578 dated Binance bindings validated against each coin's own
  CoinGecko history, incl. renames TON: TONUSDT → GRAMUSDT, IP: IPUSDT → DATAIPUSDT),
  `asset_registry_overrides.csv`, `asset_registry_conflict_resolutions.csv`, `asset_registry_conflicts.csv`
  (1 left: PUMPBTCUSDT).
- `configs/asset_identity_policy.yaml`, `src/data_lake/asset_registry.py`.
- `fact_markets_snapshot.asset_id` stamped from the registry by CoinGecko id (real Bitcoin stops
  carrying the memecoin's uid; Toncoin stays TON); unknown coins get `CG:<id>`.
- `enrichment/build_enrichment.py` maps snapshot rows by CoinGecko id and ignores a stale snapshot.
- Dictionary: `asset_id` is a ticker-style uid, not a CoinGecko slug.

**C. Monitoring**
- `verify_ingestion_integrity.py`: `--mode asset_identity` (A1–A6), `--mode freshness` (content SLA,
  snapshot 2 days), `--mode identity_acceptance` (closing gate, §10).
- `src/exports/lake_integrity.py` — nightly, non-blocking Telegram alert after the Drive export.
- `scripts/msm_decision_record.py log-today` — append-only as-decided MSM log, run nightly.

**D. One-off repair tooling and manifest**
- `scripts/repair_asset_identity.py` (registry / dry-run / apply, `--include-dim`, `--rekey-snapshot`).
- `manifest/` — dry run against the pinned snapshot (§6); `scripts/msm_decision_record.py`
  (as-decided reconstruction, freeze, compare).
- Silver marks quarantined dates `is_identity_quarantined` (`src/data_lake/quarantine.py`).

## 6. Repair — dry run against the pinned 2026-09-18 snapshot

Every replaced value is validated (price vs Binance within 10%, or continuity with good lake rows,
or implied supply within 1.25x); everything else is quarantined — moved to
`quarantine_fact_identity.parquet` with `status` confirmed | manual_review — never invented.
CoinGecko's current history disagrees with Binance while the lake agrees on 14,438 days across 331
assets (worst 24x), which is why no re-fetch is trusted unvalidated.

| Class | Evidence | Segments | Assets | Rows replaced | Rows quarantined | Status |
|---|---|---|---|---|---|---|
| `mass_window_2026_03` | re-injection signature / price vs Binance | 138 | 129 | 5,498 | 1,443 | confirmed |
| `long_splice` (≥7 d) | price vs Binance >10% | 201 | 121 | 19,668 | 1,146 | confirmed |
| `short_splice` | ≥3 d median >10% or a day >2x vs Binance | 220 | 145 | 1,910 | 298 | confirmed |
| `pre_window_unrecoverable` | vs Binance >10% for 14+ d before the re-fetch window | 12 | 7 | 0 | 5,907 | confirmed |
| `predates_coin` | rows before the coin's CoinGecko history, >3x hand-over break | 96 | 96 | 0 | 53,349 | confirmed |
| `predates_coin_tail` | old coin continuing past the hand-over | 58 | 58 | 0 | 9,552 | confirmed |
| `rebinding_2026_01_28` | no perp; >25x off the coin before 01-28, ≤1.5x after | 22 | 22 | 0 | 10,686 | confirmed |
| `predates_coin` (no break) | hand-over within 3x | 14 | 14 | 0 | 11,604 | **manual_review** |
| `rebinding_2026_01_28_ambiguous` | 01-28 jump, identity before it uncertain | 21 | 21 | 0 | 13,011 | **manual_review** |

(Rows summed over the three fact tables.) Per table:

| Table | Before | After | Replaced | Quarantined (confirmed + manual_review) |
|---|---|---|---|---|
| `fact_price` | 1,748,828 | 1,713,275 | 8,479 | 35,553 (27,348 + 8,205) — 2.0% |
| `fact_marketcap` | 1,739,791 | 1,704,528 | 9,204 | 35,263 |
| `fact_volume` | 1,743,456 | 1,707,858 | 9,391 | 35,598 |
| `dim_asset` / `map_provider_asset` | 2,717 | 2,717 | coingecko_id = registry id (2,716), else null | — |
| `fact_markets_snapshot` | 267,500 | 267,500 | 56,092 rows re-keyed (1,380 coins); uids holding two coins on one date 106/day → 0 | — |

400 assets affected; 35 carry manual-review quarantine. Not applied (reported only):
`timing_noise_not_applied.csv`, `coingecko_history_defects.csv`.

**Identity checks** (`manifest/identity_check_*.json`, candidate with registry-keyed dims):

| Signal | Live lake | After repair |
|---|---|---|
| A1 dim_asset ids = fetched ids | FAIL (714/851) | **PASS** |
| A2 unacknowledged slug collisions | FAIL → PASS with the policy | **PASS** (11 acknowledged) |
| A3 lake close = Binance (registry bindings) | FAIL (62 wrong-coin runs) | **PASS** (0) |
| A4 mass coin-switch dates | FAIL (01-28 ×67, 03-04 ×64, 03-31 ×55) | **PASS** |
| A5 price + cap = `/coins/markets` | FAIL (snapshot stale) | FAIL until the snapshot fix is deployed |
| A6 one coin ↔ one uid | PASS | **PASS** |

## 7. Quarantine semantics (proved before apply)

`tests/test_quarantine_semantics.py` runs the real `apply`, silver builder, MSM loader, universe
selection and returns on a tiny lake:
- quarantined price / cap / volume rows are **absent** from the fact tables;
- silver keeps the dates as NaN rows with `is_identity_quarantined=True` — including leading dates
  before an asset's first surviving row — and never forward-fills (`is_ffilled` stays False);
- an ordinary gap is NaN with the flag False, so consumers can tell the two apart;
- the MSM loader never sees quarantined values; universe selection never ranks an asset on a pre-gap
  cap (`max_mcap_age_days: 3`); weekly returns never bridge the gap (`get_close_asof` max age 3 days).
  The pre-fix unbounded lookups are shown to do both;
- the acceptance gate catches a quarantined row that reappears in a fact table.

`src/universe/snapshot.py` (decommissioned) reads one global as-of date, so a missing asset is NaN
and excluded. Effect of the MSM age bounds alone on the live baskets (branch code vs live file, same
unrepaired lake): **1 week's `funding_pct_rank` moves (2025-09-15, 0.654 → 0.635); no basket, regime
or MRF change.**

## 8. MSM / regime / MRF: as decided vs recomputed

`msm_timeseries.csv` and `macro_state.db` are **recomputed over 730 days every night**, so their past
rows are not what the system said at the time. The only decision-time records are MSM runs committed
to git (March 2026) and, from now on, the append-only `msm_decision_log.csv`.
`manifest/msm_decision_reconstruction.csv` holds all three per date and field:

| Decision | AS_DECIDED (git run that day) | PRE_REPAIR_RECOMPUTE (live file) | RECOMPUTED_ON_CORRECTED_DATA |
|---|---|---|---|
| 2026-03-09 | Q1, MRF off (rank 0.038; run 03-16) | Q1, off (0.096) | Q1, off (0.212) |
| 2026-03-16 | Q1, off (0.115) | Q1, off (0.192) | Q1, off (0.231) |
| 2026-03-23 | Q1, off (0.173) | Q2, **on** (0.269) | Q2, **on** (0.288) |
| 2026-03-30 | Q2, off (0.288) | Q1, off (0.250) | Q2, **on** (0.288) |
| 2026-08-10 | not recorded | Q4, off | Q4, off (basket: BEAT in, WLD out) |
| 2026-09-18 (today) | — | Q3 Neutral, off (0.673) | Q3 Neutral, off (0.673) |

What the system actually said in March 2026: **MRF was never on.** Today's code turns it on for
03-23 even on the unrepaired lake (code / data drift since, e.g. the BTCDOM source change of ADR 004);
the identity repair itself flips one decision relative to today's recompute (2026-03-30). Baskets
change on 10 decision dates (Feb–Mar 2025: S in; 2026-03-09 → 03-30: the 7–8 majors restored; three
order-only; 2026-08-10). Today's decision is unchanged.

## 9. Deployment order (decision 2026-09-18)

1. Review and merge the PR (A–D; nothing in it touches the lake by itself).
2. Render redeploys.
3. Verify root cause A is gone: the deployed SHA (Telegram footer) includes `e08fb02`; no new mass
   coin-switch date appears (A4 lists only the known dates).
4. Verify the snapshot job is healthy: `--mode freshness` F7 PASS after the next nightly run.
5. `python scripts/verify_ingestion_integrity.py --mode asset_identity` on Render.
6. **Regenerate the manifest against the then-current live lake** (`registry`, then `dry-run`, from
   the Drive export); never apply an old manifest to rows that changed — `apply` re-hashes every
   segment and refuses on mismatch.
7. Apply (runbook below).
8. Rebuild silver.
9. Recompute MSM / funding regime / MRF.
10. Run `--mode identity_acceptance`; write the final incident report.

```bash
cd /opt/render/project/src
M=reports/incidents/2026-09-18_asset_identity/manifest     # or the regenerated manifest dir
L=/data/curated/data_lake
# keep the as-decided record: frozen pre-repair recompute (the git-reconstructed AS_DECIDED is in $M)
python scripts/msm_decision_record.py freeze --timeseries /data/exports/msm_timeseries.csv \
  --out $L/msm_timeseries.pre_identity_repair_$(date -u +%F).csv
# dry confirmation (re-hashes every segment, writes nothing), then apply with backups
python scripts/repair_asset_identity.py apply --lake-dir $L --manifest-dir $M
python scripts/repair_asset_identity.py apply --lake-dir $L --manifest-dir $M --include-dim --rekey-snapshot --yes
python scripts/data_ingestion/build_silver_layer.py
python run_live_pipeline.py --skip-ingestion
python scripts/msm_decision_record.py compare --as-decided $M/msm_as_decided_from_git.csv \
  --pre-repair $L/msm_timeseries.pre_identity_repair_*.csv \
  --recomputed /data/exports/msm_timeseries.csv --out $L/msm_decision_reconstruction.csv
python scripts/verify_ingestion_integrity.py --mode identity_acceptance
```

Rollback: copy the files in `$L/_backup_asset_identity_<utc>/` back and rebuild silver.

## 10. Acceptance (the incident closes only when all hold)

`--mode identity_acceptance` must print `INCIDENT CLOSABLE: YES` — every signal PASS, INDETERMINATE
counts as open: A1, A3 pass; A4 passes (residuals quarantined as manual_review); A5 passes after the
snapshot fix; A2 passes under the explicit policy; A6 passes; F1–F7 fresh; X1 allowlist still frozen;
X2 nightly monitor ran; X3 March 2026 re-injection gone; X4 quarantine consistent; X5 as-decided records
kept. Then: final incident report (root cause, rows repaired / quarantined, unresolved assets,
downstream changes, checks before/after, what prevents recurrence) — and back to Track F.

## 11. Still open

- PUMPBTCUSDT identity (unbound), 35 manual-review assets, the 21-coin 2026-01-28 review list.
- Retired aliases QAI / AITECH keep their own history; merging it into QFI / ACN is a separate decision.
- Stale git wide files `data/curated/*_daily.parquet` (harmless after `e08fb02`, still a trap).
- Unfreezing the allowlist refresh needs a registry-based builder (bind by CoinGecko id + Binance
  evidence, never by highest market cap per ticker).
- CoinGecko `/coins/{id}/tickers` `coin_id` holds the web slug, not the API id (Binance FRAX/USDT shows
  `frax` = the legacy dollar's API id) — never map by it.
