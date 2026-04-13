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

### 1) Create a Service Account

1. In Google Cloud Console, create a project (or use an existing one).
2. Enable the **Google Drive API**.
3. Create a **Service Account**.
4. Create a **JSON key** for that service account and download it.

### 2) Create / choose a target Drive folder

1. Create a folder in Google Drive (e.g. `nightly_exports`).
2. **Share** the folder with the service account email (Editor).
3. Copy the folder ID from the URL. Example URL shape:
   - `https://drive.google.com/drive/folders/<FOLDER_ID>`

Put that folder id into `configs/gdrive_export.yaml`:

- `export_gdrive.gdrive.target_folder_id: "REPLACE_ME_FOLDER_ID"`

### 3) Base64 encode the service account JSON

On your local machine (any OS), base64 encode the entire JSON file contents.

Linux/macOS example:

```bash
base64 -w 0 service_account.json
```

Windows PowerShell example:

```powershell
[Convert]::ToBase64String([IO.File]::ReadAllBytes("service_account.json"))
```

### 4) Configure Render environment variables

In Render → your service → **Environment**, set:

- `GDRIVE_SERVICE_ACCOUNT_JSON`: base64-encoded JSON key contents
- `RENDER_DATA_LAKE_PATH`: `/data/curated/data_lake` (so Silver outputs persist on disk)
- *(existing vars)* `MACRO_STATE_DB_PATH=/data/macro_state.db`, Telegram vars, API keys, etc.

On boot, `start_render.sh` decodes `GDRIVE_SERVICE_ACCOUNT_JSON` and writes:

- `/data/secrets/gdrive_service_account.json` (permissions best-effort `0600`)

### 5) Configure export paths

Edit `configs/gdrive_export.yaml` as needed. Key fields:

- `export_gdrive.sources.*`: absolute `/data/...` paths for the 5 sources
- `export_gdrive.msm_reports_root`: where ephemeral MSM run folders are written (default `reports/msm_funding_v0`)
- `export_gdrive.gdrive.filenames.*`: static Drive filenames

### 6) Expected runtime behavior

- The heartbeat runs the live pipeline on schedule.
- After a **successful** pipeline run, the heartbeat triggers the nightly export.
- A dedup marker is written to `/data/.last_export_utc_day` so export happens **once per UTC day**.
- MSM PiT outputs remain ephemeral; the exporter finds the newest `msm_timeseries.csv` under
  `reports/msm_funding_v0/` and copies it to:
  - `/data/exports/msm_timeseries.csv`
  before uploading.

### 7) How to verify after first run

1. Check Render logs for `Nightly export completed successfully`.
2. In Drive, open the target folder and confirm the 5 files exist with recent modified times.

### Telegram alerts

If free space under `/data` drops below **500MB**, export is skipped and a Telegram message is sent (if Telegram is configured).

