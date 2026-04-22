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
