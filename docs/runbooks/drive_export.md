# Google Drive Export — Operational Runbook (Render)

This runbook covers the **nightly Google Drive export** performed by `src/exports/nightly_export.py` (invoked by `system_heartbeat.py` after a successful live pipeline run).

## Auth model (OAuth refresh token)

Drive auth uses **user-delegated OAuth** via env vars (Render dashboard):

- `GDRIVE_OAUTH_CLIENT_ID`
- `GDRIVE_OAUTH_CLIENT_SECRET`
- `GDRIVE_OAUTH_REFRESH_TOKEN`

Scope is `https://www.googleapis.com/auth/drive.file` (the app can only access files it creates).

## Regenerating OAuth refresh tokens

This repo includes a helper script (committed elsewhere in the repo) named **`get_refresh_token.py`** that walks through the OAuth flow and prints a **refresh token**.

- Run it locally (never on Render).
- Do **not** paste tokens into this repo, issues, or chat logs.
- Copy only the resulting refresh token into Render’s environment variable `GDRIVE_OAUTH_REFRESH_TOKEN`.

If you can’t find the script, search the repo for `get_refresh_token.py`.

## When refresh tokens “expire” (Testing mode)

If the OAuth app is still in Google Cloud Console **“Testing”** mode, refresh tokens may stop working after ~**7 days** (Google’s testing-mode restriction).

Symptoms:

- Render logs show Drive auth failures in the nightly export path
- The pipeline succeeds, but export logs contain errors after the pipeline completion

Fix options:

- **Operational fix**: regenerate a refresh token using `get_refresh_token.py` and update `GDRIVE_OAUTH_REFRESH_TOKEN` in Render.
- **Structural fix**: publish/verify the OAuth app (move out of “Testing”) to remove the short refresh-token lifetime restriction.

## Identify the target Google account / Drive

Exports go to the **Google account that owns the OAuth refresh token**.

To identify it:

- Determine which Google account was used to generate `GDRIVE_OAUTH_REFRESH_TOKEN`.
- In that account’s **My Drive**, look for a folder named **`Render Exports`**.

The exporter also persists the target folder ID at:

- `/data/exports/.drive_target_folder_id.txt`

This file lives on Render’s persistent disk and tells you which folder the service believes it is using.

## If the “Render Exports” folder was deleted

If the Drive folder is deleted or becomes inaccessible, reset the persisted folder pointer so the exporter recreates it:

- Delete: `/data/exports/.drive_target_folder_id.txt`

On the next successful export run, the exporter will resolve/create a new **`Render Exports`** folder and persist the new folder ID.

## Add a new file to the explicit export list

There are **two** ways files get exported:

1. **Directory sync** (preferred): everything matching `sync_patterns` under `sync_directory` (typically `/data/curated/data_lake/`)
2. **Explicit sources** (for files outside the data lake): `export_gdrive.sources` + `export_gdrive.gdrive.filenames` in `configs/gdrive_export.yaml`

If your new file lives **outside** the data lake, add it explicitly:

- Add its local path under `export_gdrive.sources.<key>`
- Add a stable Drive filename under `export_gdrive.gdrive.filenames.<key>`

Notes:

- Explicit sources are uploaded first and use the stable Drive filenames.
- Directory sync uploads files using their **local basename** (e.g. `fact_price.parquet`).

