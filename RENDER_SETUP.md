# Deploy the Macro Regime Monitor (Render.com, low–no DevOps)

This project runs:

1. **Streamlit** — team dashboard (`dashboards/app_regime_monitor.py`).
2. **Heartbeat** (`system_heartbeat.py`) — starts Streamlit, restarts it if it crashes, and runs the live data pipeline on a UTC schedule (plus catch-up if a day was missed).

On **Render**, you run **only** the heartbeat. It already starts Streamlit for you — do **not** add a second `streamlit run` command.

---

## What you need before you start

- A **GitHub** account and this repository pushed to GitHub (Render connects to GitHub).
- Your **API keys** (same names you use in local `.env`).
- **Optional but recommended:** a **paid** Render web instance so the service does not “sleep” (free tier spins down when idle; scheduled jobs and 24/7 dashboard need always-on).

---

## Important limitations (read once)

- **Data lake size:** The live pipeline downloads and stores a large **data lake** under `data/`. Render’s default disk is small; a **persistent disk** is required for SQLite and you may need **much more than 1 GB** if you run the full pipeline on the server. If the build or run runs out of space, use a larger disk or run the heavy pipeline on another machine and only host the dashboard + DB on Render.
- **First deploy:** The SQLite file must exist or be created. Use `scripts/live/init_macro_state_db.py` (see below) or copy a prepared `macro_state.db` to the persistent path.
- **Secrets:** Never commit `.env` or passwords to Git. Put them only in Render **Environment**.

---

## Step 1 — Push code to GitHub

Ensure the latest code (including `system_heartbeat.py`, `repo_paths.py`, `start_render.sh`) is on the branch Render will deploy (usually `main`).

---

## Step 2 — Create a Render account

1. Open [render.com](https://render.com) and sign up.
2. Connect your **GitHub** account when asked.

---

## Step 3 — Create a Web Service

1. Click **New +** → **Web Service**.
2. Select **this repository**.
3. Use these settings:

| Field | Value |
|--------|--------|
| **Name** | e.g. `macro-regime-monitor` |
| **Region** | **Singapore** (or closest to your team) |
| **Branch** | `main` (or your deploy branch) |
| **Runtime** | **Python** |
| **Build command** | `pip install -r requirements.txt` |
| **Start command** | `bash start_render.sh` |

---

## Step 4 — Persistent disk (for SQLite)

Without a disk, **restarts wipe the database**.

1. In the service, open **Settings** → **Disks** (or **Advanced** → **Add disk**).
2. **Mount path:** `/data`
3. **Size:** Start with **1 GB** for DB + logs only; **increase** if you run the full pipeline on Render.

---

## Step 5 — Environment variables

In **Environment** (or **Environment Variables**), add:

| Variable | Purpose |
|----------|---------|
| `MACRO_STATE_DB_PATH` | `/data/macro_state.db` |
| `PYTHON_VERSION` | e.g. `3.12.1` (match a version Render supports) |
| *(your API keys)* | Same variable names as in local `.env` (e.g. Coinglass, etc.) |
| `DASHBOARD_PASSWORD` | *(optional)* Shared password for the team; if unset, no login screen |

**Note:** `PYTHONPATH` is set inside `start_render.sh`. Do not override it unless you know you need to.

---

## Step 6 — First-time database on the server

After the first successful deploy, the app may show “DB not found” until SQLite exists at `/data/macro_state.db`.

**Option A — One-off Shell on Render** (if your plan includes SSH/shell):

```bash
cd /opt/render/project/src   # or your service root; use Render’s doc for exact path
export MACRO_STATE_DB_PATH=/data/macro_state.db
export PYTHONPATH=$(pwd)
python scripts/live/init_macro_state_db.py --help
# Then run init with a path to master_macro_features.csv if required by your init script
```

**Option B — Upload:** Create `macro_state.db` locally, then upload to the disk path Render documents for your service (or restore from backup).

Use your repo’s `scripts/live/init_macro_state_db.py` / `seed_historical_db.py` as documented in `ARCHITECTURE.md` for your usual workflow.

---

## Step 7 — Deploy

Click **Save** / **Deploy**. Wait until status is **Live**. Open the URL Render shows.

- If you set `DASHBOARD_PASSWORD`, enter it on the login screen.
- The dashboard default DB path in the sidebar should match `MACRO_STATE_DB_PATH` (or use the default from env).

---

## Step 8 — Check that the heartbeat is working

In Render **Logs**, you should see lines like:

- `System heartbeat starting`
- `Dashboard started (pid=...)`
- On schedule or catch-up: `Live pipeline (scheduled UTC ...)` or `Catch-up pipeline`
- On success: `Live pipeline completed successfully`

If the pipeline fails, logs will show errors; the dashboard may still load with **old** data until a run succeeds.

---

## Local vs Render summary

| Topic | Local | Render |
|--------|--------|--------|
| Start | `python system_heartbeat.py` | `bash start_render.sh` |
| DB path | `data/state/macro_state.db` | Set `MACRO_STATE_DB_PATH=/data/macro_state.db` |
| Streamlit port | Often `8501` | Render sets `PORT` automatically |
| Password | Optional | Set `DASHBOARD_PASSWORD` if you want a simple team gate |

---

## Getting help in Cursor

Ask: *“Help me debug Render logs for exit code X”* or *“Our pipeline OOMs on Render — what to trim or move off-box?”* with a paste of the log snippet (redact secrets).
