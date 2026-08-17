# ADR 004 — Use Binance's official BTCDOM index instead of reconstructing it

- **Date**: 2026-08-17
- **Status**: Accepted
- **Related**: [ADR 003](003-remove-btcdom-from-mrf-gate.md), `docs/runbooks/btcdom_macro_index.md`

## Context

`btcdom_reconstructed.csv` is a home-made replica of Binance's BTC Dominance
Index: a fixed-quantity, price-weighted basket of the top-20 alts priced in BTC
terms, rebalanced Thursdays, anchored to `base_index_level = 2448.02529635` —
which `scripts/audit_btcdom_assumption_ledger.py` records as *the Binance BTCDOM
close on 2024-07-04*. The reconstruction was always trying to reproduce Binance's
index; `binance_btcdom.csv` is described in that same ledger as the "official
Binance BTCDOM historical baseline".

ADR 003 made this field display-only. That prompted the question of whether the
reconstruction subsystem — a fatal pipeline step, a state DB, divisor logic, four
guards, a 200-line runbook and two documented incidents — was still worth
carrying to populate a dashboard cell.

Checking the **authoritative Render/Drive copy** (not the repo seed) against the
real index answered it decisively.

### The reconstruction is the real index, lagged one day

Lead/lag scan of daily changes, 773 overlapping days (2024-07-04 → 2026-08-16):

| shift | correlation | sign agreement |
|---|---|---|
| −1 | −0.066 | 48.4% |
| **0 (same day)** | **+0.003** | **47.0%** |
| **+1** | **+0.760** | **84.7%** |
| +2 | +0.035 | 47.0% |

Same-day correlation is zero. Shift the real index forward one day and it jumps
to +0.76. The reconstruction is yesterday's dominance wearing today's date.

**Root cause:** `btcdom_backfill.py` builds from lake `fact_price` /
`fact_marketcap`, and `fact_price.date` is stamped **one day after** the UTC
close it represents (verified separately: naive same-date join to exchange data
gives −0.06 daily-return correlation; shifting one day gives 0.9982). The index
computed "for date t" is built from t−1's prices.

Two further defects in the same comparison:

- **110 flat/repeated days out of 772** (14%) — forward fill leaking into the
  output. The real index has 0 of 1,882.
- **4.49% median level difference.**

None of this was visible to any existing guard. The file updated daily, passed
every freshness check, sat in the right numeric range and had plausible
volatility. Fresh, confident and wrong — the same failure class as the 2026-02
incident, one layer deeper.

### The real index is free, live, and longer

`BTCDOMUSDT` is a live Binance USDⓈ-M perpetual (`TRADING`, onboarded
2021-06-17). Its underlying index is served by
`/fapi/v1/indexPriceKlines?pair=BTCDOMUSDT` — no key, no meaningful rate limit
at daily granularity.

| | Reconstruction | Official index |
|---|---|---|
| Coverage | 2024-07-04 → (773 rows) | **2021-06-21 → (1,883 rows)** |
| Flat/repeated days | 110 of 772 | **0 of 1,882** |
| One-day lag | yes | no |
| Machinery | backfill + state DB + divisor + 4 guards | one API call |

## Decision

**Fetch Binance's official BTCDOM index; stop reconstructing it.**

- New producer `scripts/data_ingestion/btcdom_binance_index.py` writes
  `btcdom_binance_index.csv` (`date`, `btcdom_index`), daily UTC closes.
- `msm_run.py` reads it via two named constants (`BTCDOM_SOURCE_FILE`,
  `BTCDOM_VALUE_COLUMN`) and renames the column internally, so every downstream
  consumer and the persisted schema are unchanged.
- `run_live_pipeline.py` Step 3 invokes the new fetcher, **non-fatally** (ADR 003:
  BTCDOM gates nothing, so its failure must not stop `Environment_APR` /
  `w_risk` / `funding_regime` from updating).

**Take the index, not the perp price.** `/fapi/v1/klines?symbol=BTCDOMUSDT` is
the perpetual's traded price — index plus basis and funding pressure. They differ
by ~0.04%, but the index is the actual dominance measure and carries no
derivatives artefacts.

**Keep `btcdom_backfill.py` and `btcdom_reconstructed.csv` on disk.** Not deleted:
rollback is restoring two constants in `msm_run.py` and re-pointing Step 3. Delete
once the new source has run in production for a while.

## Consequences

- `BTCDOM_Trend` becomes correct rather than one day late. Verified on the three
  most recent decision dates: `Falling` / `Rising` / `Falling`, where the old
  path produced `Unknown`.
- **Three extra years of history** (2021-06 vs 2024-07), which is what made the
  ADR 003 analysis impossible on the production series in the first place.
- `btcd_index_decision` values change — different (correct) source. Anything
  comparing to previously published BTCDOM columns will differ.
- Step 3 no longer writes `btcdom_state.db`, so research runs stop dirtying the
  repo root.
- One new external dependency: Binance futures API availability. Mitigated by
  ADR 003's degradation path — a fetch failure yields `btcdom_status` and a daily
  Telegram nag, not a halted pipeline.
- The producer validates before writing (null, duplicate, ordering and plausible-
  range checks) and exits non-zero rather than writing a bad file. Calendar gaps
  warn but do not fail, since consumers merge as-of with a 3-day tolerance.

## Alternatives considered

- **Fix the reconstruction's one-day lag.** Rejected: it would leave the whole
  subsystem in place — state DB, divisor logic, guards, runbook — to approximate
  a series available directly, and would still carry the ffill and level drift.
- **Use the perp price.** Rejected in favour of the index; see above.
- **Use `binance_btcdom.csv`.** That is the right *data*, but it has no producer —
  it is a manual snapshot, last written 2026-03-02. The new fetcher supersedes it.
- **Delete BTCDOM entirely.** Defensible — it gates nothing and ADR 003 found the
  signal does not hold out of sample. Kept because the dominance read is still
  wanted on the dashboard, and it now costs one API call.

## Verification

- `tests/test_btcdom_binance_index.py` — 12 tests on normalisation and validation,
  plus a network test (`--run-network`) asserting freshness and history depth.
- Producer run against live API: 1,883 rows, 2021-06-21 → 2026-08-16, drift 1 day.
- End-to-end: `btcdom_status = ok`, trend resolves on all three recent decision
  dates.
