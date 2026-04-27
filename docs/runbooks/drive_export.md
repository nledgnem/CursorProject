# Google Drive Export — Operational Runbook (Render)

This runbook covers the **nightly Google Drive export** performed by `src/exports/nightly_export.py` (invoked by `system_heartbeat.py` after a successful live pipeline run).

## Auth model (OAuth refresh token)

Drive auth uses **user-delegated OAuth** via env vars (Render dashboard):

- `GDRIVE_OAUTH_CLIENT_ID`
- `GDRIVE_OAUTH_CLIENT_SECRET`
- `GDRIVE_OAUTH_REFRESH_TOKEN`

Scope is `https://www.googleapis.com/auth/drive.file` (the app can only access files it creates).

**OAuth app publishing status**: as of 2026-04-27 the OAuth consent app is in **"In production"** mode in Google Cloud Console. This is critical — "Testing" mode revokes refresh tokens every 7 days. Verify periodically: Cloud Console → APIs & Services → OAuth consent screen → Audience. If it ever shows "Testing", click **Publish app**; for `drive.file`-scope-only apps the change is instant with no Google review.

## Failure alerting

`system_heartbeat.py` sends a Telegram alert on any `nightly_export.run()` exception (added 2026-04-27 after a 4-day silent staleness incident). The export remains non-fatal to the pipeline, but the Telegram alert is the operational signal that something is broken. Format:

```
⚠️ Nightly Drive export FAILED [YYYY-MM-DD UTC]
<ExceptionClass>: <message>
```

If you stop receiving these, also confirm Telegram itself is working (test with a manual `send_telegram_text("test")` from Render shell).

## Regenerating OAuth refresh tokens

The repo does **not** ship a `get_refresh_token.py` script (deliberately — the script needs the client secret in plaintext to run, so it shouldn't be committed). Generate one ad-hoc when needed and delete after use.

**Procedure** (verified 2026-04-27):

1. On a local machine (never Render), create a temporary file `get_refresh_token.py` with this content:

   ```python
   from google_auth_oauthlib.flow import InstalledAppFlow

   CLIENT_ID = "<paste from Render env GDRIVE_OAUTH_CLIENT_ID>"
   CLIENT_SECRET = "<paste from Render env GDRIVE_OAUTH_CLIENT_SECRET>"
   SCOPES = ["https://www.googleapis.com/auth/drive.file"]

   client_config = {
       "installed": {
           "client_id": CLIENT_ID,
           "client_secret": CLIENT_SECRET,
           "auth_uri": "https://accounts.google.com/o/oauth2/auth",
           "token_uri": "https://oauth2.googleapis.com/token",
           "redirect_uris": ["http://localhost"],
       }
   }

   flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
   creds = flow.run_local_server(port=0, access_type="offline", prompt="consent")
   print("\nRefresh token (paste into Render env GDRIVE_OAUTH_REFRESH_TOKEN):\n")
   print(creds.refresh_token)
   ```

2. Install dep if needed: `python -m pip install google-auth-oauthlib`
3. Run: `python get_refresh_token.py`. A browser opens. Sign in with the Google account that owns the **"Render Exports"** Drive folder. Click **Advanced → Go to render-exports (unsafe) → Allow**.
4. The terminal prints a refresh token starting with `1//`. Copy it.
5. **Delete the script immediately** — it contains your client secret in plaintext: `del get_refresh_token.py` (Windows) / `rm get_refresh_token.py` (Unix).
6. Update Render env var `GDRIVE_OAUTH_REFRESH_TOKEN`. Render will auto-redeploy (~2 min).
7. Verify on Render shell: `python -c "from src.exports import nightly_export; nightly_export.run()" 2>&1 | tail -30`. Expected: `Drive upload OK` lines, no traceback.

**Common gotchas:**
- If you get `Error 400: redirect_uri_mismatch`, your OAuth client is type "Web application" instead of "Desktop app". Either create a new Desktop OAuth client, or add `https://developers.google.com/oauthplayground` to the Web client's authorized redirect URIs and use OAuth Playground instead of the local script.
- If you get `Access blocked: Google hasn't verified this app`, that's expected for unverified apps — click "Advanced" → "Go to render-exports (unsafe)".
- If sign-in succeeds but no refresh token is returned, the user already authorized this client. Revoke at https://myaccount.google.com/permissions and retry, or use a different Google account.

## When refresh tokens “expire” (Testing mode)

If the OAuth app is still in Google Cloud Console **“Testing”** mode, refresh tokens stop working after **7 days** (Google's testing-mode restriction). **As of 2026-04-27 the app has been moved to production mode**, so this should no longer be the cause of failures — but verify if you suspect token expiry.

Symptoms:

- **Telegram alert fires**: `⚠️ Nightly Drive export FAILED ... RefreshError: invalid_grant: Token has been expired or revoked.`
- Render logs show Drive auth failures in the nightly export path
- The pipeline succeeds, but Drive folder timestamps stop advancing

Fix:

1. **Verify publishing status** at Cloud Console → OAuth consent screen → Audience. Should be "In production". If "Testing", click **Publish app** to fix the recurring cause.
2. **Regenerate the refresh token** using the procedure above. Update Render env var.
3. **Verify** with `nightly_export.run()` from Render shell.

This exact incident played out 2026-04-23 → 2026-04-27 — Drive sync was silently broken for 4 days because the app had been left in Testing mode and the alerting hook didn't exist yet. Both fixed.

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

