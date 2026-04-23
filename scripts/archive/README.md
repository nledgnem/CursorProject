# scripts/archive/

Orphaned scripts retained for reference. Not on any production path.

Do not reintroduce into `scripts/` without first verifying the active pipeline
(`run_live_pipeline.py`, `start_render.sh`, `system_heartbeat.py`,
`scripts/apathy_alert_runner.py`, `scripts/danlongshort_alert_runner.py`,
`scripts/danlongshort_bot.py`) actually needs them.

## Contents

| File | Archived | Reason |
|---|---|---|
| `fetch_coinglass_funding.py` | 2026-04-22 | Superseded by combined `scripts/fetch_coinglass_data.py`, which the live pipeline (`run_live_pipeline.py` Step 1) invokes. The standalone funding fetcher is not called from any production entry point. |
| `fetch_coinglass_oi.py` | 2026-04-22 | Superseded by combined `scripts/fetch_coinglass_data.py`. The altcoin-OI universe expansion landed in the combined script (commit `6f093dc`), not in this standalone. |
| `backfill_early_2026.py` | 2026-04-23 | One-shot backfill, no live callers. |
| `backfill_historical_data.py` | 2026-04-23 | One-shot historical backfill helper, referenced only from how-to docs; not called from any code path. |
| `fetch_low_priority_data.py` | 2026-04-23 | CoinGecko Analyst-tier era fetcher. We're on Basic tier (see `docs/DATA_LAKE_CONTEXT.md` §4); no live callers. |
| `fetch_medium_priority_data.py` | 2026-04-23 | CoinGecko Analyst-tier era fetcher. Same reason as `fetch_low_priority_data.py`. |
| `consolidate_to_database.py` | 2026-04-23 | DuckDB consolidation helper, no live callers (only a description line in `PROJECT_OVERVIEW.md`). |
| `expand_allowlist.py` | 2026-04-23 | Perp allowlist helper, no live callers (only a description line in `PROJECT_OVERVIEW.md`). |
| `extract_l1_sample.py` | 2026-04-23 | One-shot sampling script, no references anywhere outside itself. |
| `run_sensitivity.py` | 2026-04-23 | Research driver that shells out to `run_backtest.py`; no external callers, only a description line in `PROJECT_OVERVIEW.md`. |
| `strategy_simulator.py` | 2026-04-23 | Research simulator, no references anywhere outside itself. |
| `simulation_sticky_hysteresis.py` | 2026-04-23 | Research simulator, zero references anywhere in the tree. |
| `validate_canonical_ids.py` | 2026-04-23 | 51 KB file. Contains a pre-existing `SyntaxError` at line 1066 ("expected an indented block after 'else'") present since the initial commit. Never successfully imported or executed; therefore no live code depends on it. If revived, the syntax error at line 1066 must be fixed first. |

## Tier 3 review outcome (2026-04-23)

Four candidates flagged at the end of Tier 2 (commit `5f15367`) were reviewed
under the stricter "6-month mtime AND zero live refs" criterion defined in
`docs/HANDOFF_scripts_cleanup_tier3.md`. All four were retained in `scripts/`.
Live-ref categories counted per handoff §2: production pipeline / alert
runners / bots / heartbeat / `start_render.sh` / `run_live_pipeline.py` /
`src/` / `configs/` / `dashboards/`. Stale `.md` docs, archived scripts, and
never-run tests (the repo has no CI) were NOT counted.

Files reviewed:

- **`run_pipeline.py`** — explicit KEEP per Tier 3 handoff §"Explicit NOT-dos"
  (active research pipeline, 53 run directories through 2026-01-06, may be
  revisited); also has live config refs in `configs/golden.yaml` and
  `configs/golden_2year.yaml`.
- **`fetch_analyst_tier_data.py`** — KEEP; called by `scripts/run_data_updates.py`
  (research orchestrator, itself unaudited) and fails mtime gate (mtime
  2026-02-04, 2.5 months).
- **`run_golden.py`** — KEEP on mtime gate alone (mtime 2026-02-04, 2.5 months).
  Zero live code refs; only doc refs in stale `.md` files and invocations
  against configs (`golden_top30.yaml`, `golden_quarterly.yaml`) that do not
  exist in `configs/`. Revisit in a future cleanup pass if mtime crosses 6
  months.
- **`run_monitor_eval.py`** — KEEP; live `configs/monitor_eval.yaml` companion
  (archiving would orphan the config) and fails mtime gate.
