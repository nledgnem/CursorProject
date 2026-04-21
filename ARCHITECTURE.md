# 🏛️ Quant Trading Engine Architecture

## 📂 1. Directory Structure

### 🧠 The Core Engine
This is the production-grade execution module. All backtesting, data-loading, and signal generation logic lives here.

### 💾 The Data Lake (`/data`)
* `/curated/data_lake`: The single source of truth. Contains standardized Parquet files and CSV extracts.

## 💾 Data Lake Storage (Render persistent disk)

### 📍 Location + path resolution (hard rule)

- **Production (Render)**: `/data/curated/data_lake/` (Render persistent disk mounted at `/data`)
- **Local**: `<repo_root>/data/curated/data_lake/`
- **How code resolves it**: all ingestion + pipeline code must call `repo_paths.data_lake_root()` (or use `repo_paths.DATA_LAKE_ROOT`)
- **Never do**: hardcode `data/curated/data_lake/` anywhere in ingestion/ETL scripts

### 🧹 Git hygiene (hard rule)

- **Parquet files are gitignored** — do not commit new `.parquet` outputs to git.

### 🚀 Render boot seeding behavior

- On Render boot, `start_render.sh` ensures `/data/...` directories exist.
- If `/data` is empty on first boot, `start_render.sh` **seeds** the persistent disk from the repo snapshot (e.g. `data/curated/data_lake/**`) and **never overwrites existing `/data` files** on subsequent deploys.

### 🧾 Perp listings snapshots (Hyperliquid + Variational)

- **What it is**: daily append-only CSV snapshots of perp listings/metadata for two venues:
  - `perps_hyperliquid.csv`
  - `perps_variational.csv`
  - plus derived coverage/mapping reports:
    - `perp_coverage_summary.csv`
    - `perp_ticker_mapping.csv`
- **How it’s triggered**: **automated daily** as **Step 0.5** in `run_live_pipeline.py` (invoked by `system_heartbeat.py`)
  - Wrapper script: `scripts/run_perp_listings_ingestion.py`
  - Code entrypoint: `src/data_lake/perp_listings.py` → `run_daily_perp_ingestion(repo_root=...)`
  - Operator xref tool (reads snapshots): `scripts/xref_perp_listings.py`
- **Path resolution**: `configs/perp_listings.yaml` intentionally does **not** set `output.curated_data_lake_dir`. With the key absent, `load_perp_listings_config()` falls through to `repo_paths.data_lake_root()` — the same resolver every other lake writer uses — so outputs land on the Render Disk at `/data/curated/data_lake/` (and locally at `<repo>/data/curated/data_lake/`). Do not re-introduce a relative path override here; it silently routes writes to ephemeral container storage on Render and breaks the Drive backup.
- **Why it lives on `/data` (not git)**: these are **generated artifacts** that accumulate by date, similar to other lake outputs; they’re gitignored.
- **Backup**: the nightly Google Drive export syncs `/data/curated/data_lake/` (including these CSVs), so a wiped Render disk can be restored/rebuilt from Drive history (but first boot seeding from repo will not include prior perp snapshot history).

### 🧾 Core fact tables (curated lake)

- **`fact_funding`**: perp funding history by exchange/instrument (CoinGlass)
- **`fact_price`**: daily close prices by asset (CoinGecko)
- **`fact_marketcap`**: daily market caps by asset (CoinGecko)
- **`fact_volume`**: daily spot volumes by asset (CoinGecko)
- **`fact_open_interest`**: open interest history (CoinGlass); **BTC-only currently**
- **`fact_markets_snapshot`**: daily market snapshot (circulating + total supply, etc.); **daily accumulating**

**Rule 1 – Curated Lake Only**
* The AI is **strictly forbidden** from reading any CSV or Parquet files **outside** of `data/curated/data_lake/`.
* **Primary Macro Signal**: `data/curated/data_lake/btcdom_reconstructed.csv`  
  Use this series (column `reconstructed_index_value`) for all BTCDOM macro trend and regime modeling.
* **Reference Exchange Data**: `data/curated/data_lake/binance_btcdom.csv`  
  This is the pristine Binance BTCDOM index used only for verification / benchmarking, never as the primary modeling feature.

**Rule 2 – Mandatory Bounds Checks**
* All notebooks that operate on price or index features **must** run a `.describe()` bounds check before regressions, heatmaps, or alpha calculations.
* At minimum, assert that index-like series have sensible maxima (e.g. BTCDOM < 6000) before proceeding.

* **Hard rule**: **Do NOT read any data from `archive/` or `archive_data/` (or any subfolders such as legacy `OldV*` scratchpads).** All research and production analytics must source raw market data from `/data/curated/data_lake` only.

* *(Note: Legacy wide-format files are deprecated and must remain quarantined in `archive_data/`.)*

### 🧪 The Lab (`/notebooks` & `/scripts`)
* `/scripts`: Permanent utility scripts for data fetching and QC.
* `/notebooks`: **[MANDATORY]** Quarantine zone for all diagnostic checks, EDA, and temporary scratchpad code.

## 🛑 Data Integrity Baseline (SOP)
Before writing any data analysis, feature engineering, or plotting scripts, the following 6 rules must be enforced:

1. **The Univariate Mandate:** Always plot a new data series on its own first (e.g., histogram or boxplot) to verify its standalone shape against market reality before mapping it against other variables.
2. **The Outlier Inquisition:** Trace extreme outliers on the edges of a distribution back to the raw Bronze layer. Looking at the extremes often exposes structural math errors or unit drift.
3. **Conservation of Data:** Ensure a strict, physical match between exploratory data and final outputs (e.g., the N-count in a histogram must exactly match the master N-count in downstream scatter plots).
4. **The Look-Ahead Quarantine:** Every engineered feature must be strictly shifted so the signal at time `t` only relies on data available at `t-1`, preventing future data leakage.
5. **The Stale Data Tripwire:** Build checks for maximum consecutive identical values to flag frozen API feeds and prevent the pipeline from flying blind.
6. **The Sensor vs. Strategy Distinction:** Evaluate "Macro Sensors" (environmental filters) without execution drag to preserve the pure macroeconomic signal. Evaluate "Executable Strategies" (direct capital allocation) by explicitly subtracting pessimistic execution drag (spread, exchange fees, gas) from historical returns.

## 📁 The Output Layer: Point-in-Time (PiT) Audit Trail

The pipeline utilizes a **Non-Destructive Audit Trail** for its Gold Layer data.

* **Run ID Directories:** Every execution of `msm_run.py` generates a new, unique directory under `reports/msm_funding_v0/` (e.g., timestamped or UUID-based).
* **No Overwriting:** The engine writes a fresh `msm_timeseries.csv` into this new directory. It **strictly forbids** overwriting historical run files. 
* **Why:** This ensures strict Point-in-Time (PiT) integrity. It allows Portfolio Managers to audit the exact state of the data lake and the mathematical sensor as it existed on any specific day in the past.
* **Downstream Consumption:** Because of this structure, all downstream dashboards and read-only UIs must dynamically locate the *newest* directory by modification time to find the live `msm_timeseries.csv`. Do not hardcode file paths.

## LIVE PRODUCTION ARCHITECTURE
The system has transitioned from a static historical backtest to a live, automated operational state. 

* **The Orchestrator:** `system_heartbeat.py` is the master daemon. It runs continuously, schedules the ETL pipeline once per day (00:05 UTC), and keeps the dashboard alive.
* **The ETL Engine:** `run_live_pipeline.py` computes the feature space. It utilizes a robust 730-day lookback window to safely and accurately calculate all rolling features (e.g., 90-Day Z-Score, Environment APR) across the Data Lake.
* **The State Ingestion:** `scripts/live/live_data_fetcher.py` handles the database write-path. It slices off only the terminal `decision_date` row and UPSERTs it into `macro_state.db` (`macro_features`) keyed strictly on `decision_date` via `ON CONFLICT(decision_date) DO UPDATE`.
* **The Presentation Layer:** `dashboards/app_regime_monitor.py` (Streamlit) strictly reads from the database. It is forbidden from performing raw mathematical transformations.

## 🧬 Live Pipeline DAG (production)

### ⏱️ Trigger + orchestration chain

- **Trigger**: `system_heartbeat.py` runs continuously in the foreground of the Render web service
- **Schedule**: runs daily at **00:05 UTC** via `UTC_RUN_TIMES` in `system_heartbeat.py`
- **Chain**:
  - `system_heartbeat.py`
  - → `scripts/live/live_data_fetcher.py`
  - → `run_live_pipeline.py`
  - → Steps **0–4** (below)

### 🧱 Steps 0–4 (halt-on-failure except Step 0)

- **Step 0 (non-fatal)**: market snapshot via `scripts/fetch_high_priority_data.py`
  - Non-fatal by design (pipeline continues if this step fails)
  - Note: CoinGecko exchange volume endpoints returning **401** on Analyst tier is expected; snapshot is the critical output
- **Step 0.5 (non-fatal)**: perp listings snapshot (Hyperliquid + Variational)
- **Step 1 (fatal)**: funding via CoinGlass
- **Step 2 (fatal)**: price/mcap via CoinGecko
- **Step 3 (fatal)**: macro index build
- **Step 3.5 (fatal)**: silver layer build
- **Step 4 (fatal)**: strategy run

### 📤 Drive export hook (heartbeat-only)

- After a **successful** pipeline run, `system_heartbeat.py` invokes `src/exports/nightly_export.run()` to push exports to Google Drive.
- Manual `python run_live_pipeline.py` runs the DAG but **does not** trigger Drive export (only the heartbeat chain does).

## 📤 Google Drive Export (nightly)

### 🔐 Auth (OAuth delegation)

- **Auth**: OAuth refresh token (user-delegated)
- **Env vars**:
  - `GDRIVE_OAUTH_CLIENT_ID`
  - `GDRIVE_OAUTH_CLIENT_SECRET`
  - `GDRIVE_OAUTH_REFRESH_TOKEN`
- **Scope**: `https://www.googleapis.com/auth/drive.file` (can only access files the app creates)

### 🎯 Target folder + persistence

- Target is a folder named **`Render Exports`** in the owning Google account’s **My Drive**
- Folder ID persistence file: `/data/exports/.drive_target_folder_id.txt`
  - If missing, the exporter resolves/creates the folder and persists the ID

### 🔄 Sync behavior

- **Directory sync**: uploads all `.parquet` and `.csv` under `/data/curated/data_lake/` when `sync_directory` is configured
- **Explicit sources**: also uploads the explicit files listed in `export_gdrive.sources` (for files outside the data lake)

### 🧯 Historical note

- Service account auth was removed (commit **`chore: remove unused service account auth code after oauth migration`**) — do not reintroduce it.

## 🧾 Strategy: apathy_bleed (primary)
**Purpose:** Long BTC / Short alt pair trades across 4 cohorts (C1–C4), entered in a single formation window on 2026-04-09, with staggered exit targets at 40 / 85 / 130 / 175 days from entry (C1: 2026-05-19, C2: 2026-07-03, C3: 2026-08-17, C4: 2026-10-01). Exits are triggered by time (target date), stop (+60% adverse move on the short leg), or manual intervention. Each cohort has a matching LONG_BTC hedge row sized to the cohort's total short notional.

### 📄 Apathy Bleed trade book

- **What it is**: `apathy_bleed_book.csv` — the canonical trade ledger. Columns: `trade_id, cohort, ticker, side, entry_date_utc, entry_price_usd, notional_usd, quantity, stop_price_usd, exit_date_target_utc, status, exit_date_utc, exit_price_usd, pnl_usd, pnl_pct, notes`.
- **Canonical location**: `/data/curated/data_lake/apathy_bleed_book.csv` (Render persistent disk); `<repo>/data/curated/data_lake/apathy_bleed_book.csv` (local).
- **Git policy**: **committed intentionally** (not gitignored — see `.gitignore` comment). The repo copy serves as the first-boot seed for fresh Render deploys, so it should be kept in sync with Render's runtime state.
- **Backup**: captured by the nightly Drive directory sync of `/data/curated/data_lake/`.

### 📝 Apathy Bleed runtime state (co-located with book)

- `apathy_alert_log.csv` — append-only audit log of every Telegram alert emitted by `scripts/apathy_alert_runner.py`. Gitignored.
- `apathy_stop_proximity_state.json` — last-warned thresholds per (trade_id) for stop-approach alerts.
- `apathy_exit_reminder_state.json` — last-fired state for T-7 / T-3 / T-1 exit reminders.
- `apathy_scanner_reminder_state.json` — last-fired state for formation-window (40 / 43 / 45-day) scan reminders.
- `apathy_daily_snapshot_state.json` — last-fired state for daily portfolio snapshot (08:00 UTC).
- `apathy_daily_bundle_state.json` — last-fired state for combined daily bundle (exit reminders + scanner).

### 📍 Path resolution (hard rule)

- `configs/apathy_alerts.yaml` sets absolute paths under `/data/curated/data_lake/` for the book, alert log, and all state JSON files. These **must** live inside `/data/curated/data_lake/` so the nightly Drive directory sync backs them up.
- A previous version used `/data/*` (one level shallower than the curated lake). This routed writes outside the sync directory, so every trade closure, alert firing, and state update was silently unbacked-up for four days despite the pipeline claiming success. Ops scripts appeared to succeed because they were reading and writing the same (correct-but-orphaned) location.
- **Do not** move these paths back to `/data/*` or anywhere outside `/data/curated/data_lake/` without also updating `gdrive_export.yaml` to explicitly include them. Better: follow the Rule 1 convention and have `config_loader._p()` resolve via `repo_paths.data_lake_root()` instead of hardcoded absolute paths (follow-up refactor).

### 🔧 Operational scripts (run on Render shell)

- `scripts/apathy_log_entry.py` — record a new short-leg entry in the book
- `scripts/apathy_log_btc_hedge.py` — record a cohort's BTC long-hedge entry
- `scripts/apathy_close_trade.py` — close an OPEN row with `--exit-price` + `--reason` (`manual` / `stop` / `expiry`), optionally `--exit-date` and `--notes`; computes PnL and updates status to `CLOSED_*`
- `scripts/apathy_book_status.py` — read-only summary of the current book state
- Never edit the book CSV by hand — these scripts ensure correct formatting, atomic writes, and PnL computation.

### 📣 Telegram alerts (Render)

- **Alert runner**: `scripts/apathy_alert_runner.py` — stop-proximity warnings (adverse move thresholds 0.45 / 0.55 / 0.60), exit-date reminders (T-7 / T-3 / T-1), formation scanner reminders, daily portfolio snapshot at 08:00 UTC. Prefix: `[apathy]`.
- **Config**: `configs/apathy_alerts.yaml` (thresholds, cadence, paths).
- No dedicated bot yet (unlike danlongshort); manual actions are via Render shell scripts above.

## 🧾 Strategy: danlongshort (independent)
**Purpose:** Beta-neutral long/short crypto portfolio (beta vs BTC). Target is zero net beta exposure to BTC.  
**Relationship to other strategies:** Fully independent from `apathy_bleed` (separate config, state, CSVs, and runner).

### Inputs / State (Render persistent disk)
- **Positions (manual ledger):** `/data/curated/data_lake/danlongshort_positions.csv`
  - Template in repo root: `danlongshort_positions.csv`
  - Columns: `ticker, side, notional_usd, entry_price, entry_date`
- **Optional cache (30d daily closes):** `/data/curated/data_lake/danlongshort_price_cache.parquet` (freshness gate < 12h)
- **Alert runner state/logs:**
  - `/data/curated/data_lake/danlongshort_snapshot_state.json`
  - `/data/curated/data_lake/danlongshort_alert_log.csv`

### 📍 Path resolution (hard rule)

- All runtime state lives under `/data/curated/data_lake/` so it's captured by the nightly Drive directory sync. Do not move paths back to `/data/*`, which was the previous (broken) location — fixed 2026-04-21, same class of bug as the Apathy fix on 2026-04-20.

### Price data
- **Source:** Live fetch each run using CoinGecko via `src/providers/coingecko.py` → `fetch_price_history()`.
- **Window:** 30-day rolling window of daily closes; daily log returns are computed and aligned in UTC calendar time.
- **Storage dependency:** **No dependency** on local Parquet panels for danlongshort.

### Beta calculation
- **Method:** 30-day OLS regression of each alt’s daily log returns vs BTC daily log returns.
- **Definition:** BTC beta to itself is **1.0** by construction.
- **Portfolio beta exposure:** \(\sum_i \text{notional}_i \times \text{direction}_i \times \beta_i\) where LONG=+1 and SHORT=-1.
- **Rebalancing:** Manual. Script prints suggested BTC adjustment and (with `--rebalance`) the exact BTC leg notional to neutralize beta (adjust BTC only).

### Funding rates (risk/drag)
- **Source:** Live fetch via CCXT (Binance USD-M perps).
- **Output:** Per-position current funding rate (per 8h) and estimated daily funding PnL in USD (approx \(3 \times\) 8h rate).

### Telegram alerts (Render)
- **Runner:** `scripts/danlongshort_alert_runner.py` (separate process from apathy)
- **Config:** `configs/danlongshort_alerts.yaml`
- **Labeling:** Every message is prefixed with **`[danlongshort]`** for visual separation in shared Telegram groups.

### Telegram bot commands (Render)
- **Bot:** `scripts/danlongshort_bot.py` (separate long-poll loop; same bot token + group)
- **Security:** responds only when `message.chat.id == TELEGRAM_CHAT_ID`
- **Commands (read-only unless noted):**
  - `/beta <TICKER> <SIDE> <NOTIONAL>`: single-position beta + BTC neutralization suggestion
  - `/add <TICKER> <SIDE> <NOTIONAL> [ENTRY_PRICE]`: appends a row to `/data/curated/data_lake/danlongshort_positions.csv`
  - `/remove <TICKER>`: removes rows for ticker from `/data/curated/data_lake/danlongshort_positions.csv`
  - `/snapshot`: on-demand portfolio snapshot (same as scheduled runner)
  - `/rebalance`: BTC adjustment needed to reach beta neutrality (adjust BTC only)
- **State:** `/data/curated/data_lake/danlongshort_bot_state.json` stores Telegram update offset to avoid duplicate handling.

### Streamlit UI (Render)
- **Page:** `dashboards/pages/2_danlongshort.py` (Streamlit multipage; added without modifying `dashboards/app_regime_monitor.py`)
- **Features:** read-only beta calculator, CSV-backed portfolio manager, live snapshot table, neutrality indicator, and HTML snapshot preview.