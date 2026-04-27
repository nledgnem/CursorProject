## Google Drive Nightly Export (Render)

This repo can export a small “latest snapshot” bundle to Google Drive once per UTC day after a successful pipeline run.

### What gets exported (static filenames)

The exporter uploads **static filenames** (overwrite semantics) so downstream consumers can always read “the latest”:

- `silver_fact_price.parquet`
- `universe_eligibility.parquet`
- `single_coin_panel.csv`
- `msm_timeseries.csv`
- `stablecoins.csv`

On Render, sources are expected to live on the **persistent disk** mounted at `/data`.

### 1) Create an OAuth client + refresh token

This exporter authenticates via **OAuth refresh token** (no service account).

1. In Google Cloud Console, create a project (or use an existing one).
2. Enable the **Google Drive API**.
3. Create an **OAuth Client ID** (Desktop or Web is fine for refresh token generation).
4. **⚠️ Set OAuth consent screen → Audience → Publishing status to "In production".** Apps left in "Testing" mode auto-revoke refresh tokens after **7 days**, causing silent Drive sync failures. For `drive.file`-scope-only apps, publishing is auto-approved with no Google review process. This was the root cause of a 4-day silent staleness incident on 2026-04-23 → 2026-04-27.
5. Generate a refresh token for the Google account that owns (or can access) the target Drive folder.

**Recovery procedure** (if Drive sync starts failing): see `docs/runbooks/drive_export.md`.

### 2) Create / choose a target Drive folder

1. Create a folder in Google Drive (e.g. `nightly_exports`).
2. Copy the folder ID from the URL. Example URL shape:
   - `https://drive.google.com/drive/folders/<FOLDER_ID>`

Put that folder id into `configs/gdrive_export.yaml`:

- `export_gdrive.gdrive.target_folder_id: "REPLACE_ME_FOLDER_ID"`

### 3) Configure Render environment variables

In Render → your service → **Environment**, set:

- `GDRIVE_OAUTH_CLIENT_ID`
- `GDRIVE_OAUTH_CLIENT_SECRET`
- `GDRIVE_OAUTH_REFRESH_TOKEN`
- `RENDER_DATA_LAKE_PATH`: `/data/curated/data_lake` (so Silver outputs persist on disk)
- *(existing vars)* `MACRO_STATE_DB_PATH=/data/macro_state.db`, Telegram vars, API keys, etc.

### 4) Configure export paths

Edit `configs/gdrive_export.yaml` as needed. Key fields:

- `export_gdrive.sources.*`: absolute `/data/...` paths for the 5 sources
- `export_gdrive.msm_reports_root`: where ephemeral MSM run folders are written (default `reports/msm_funding_v0`)
- `export_gdrive.gdrive.filenames.*`: static Drive filenames

### 5) Expected runtime behavior

- The heartbeat runs the live pipeline on schedule.
- After a **successful** pipeline run, the heartbeat triggers the nightly export.
- A dedup marker is written to `/data/.last_export_utc_day` so export happens **once per UTC day**.
- MSM PiT outputs remain ephemeral; the exporter finds the newest `msm_timeseries.csv` under
  `reports/msm_funding_v0/` and copies it to:
  - `/data/exports/msm_timeseries.csv`
  before uploading.

### 6) How to verify after first run

1. Check Render logs for `Nightly export completed successfully`.
2. In Drive, open the target folder and confirm the 5 files exist with recent modified times.

### Telegram alerts

Two alert classes are wired up:

1. **Disk-space alert**: if free space under `/data` drops below **500MB**, export is skipped and a Telegram message is sent (if Telegram is configured).
2. **Failure alert** (added 2026-04-27): if `nightly_export.run()` raises *any* exception (OAuth, network, file missing, etc.), `system_heartbeat.py` catches it and sends a Telegram alert with the exception class + message. Drive export failures are non-fatal to the pipeline (the heartbeat doesn't crash), so this Telegram alert is the **only operational signal** that Drive sync has stopped working. Don't disable it without first ensuring some other channel will catch staleness.

The alert format is:

```
⚠️ Nightly Drive export FAILED [YYYY-MM-DD UTC]
<ExceptionClass>: <truncated message>

Pipeline data is current on Render but Drive is stale. Check Render logs and `nightly_export.run()` output. Common causes: OAuth refresh token expired/revoked, Drive API quota, network.
```

