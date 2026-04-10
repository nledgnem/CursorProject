# 🏛️ Quant Trading Engine Architecture

## 📂 1. Directory Structure

### 🧠 The Core Engine
This is the production-grade execution module. All backtesting, data-loading, and signal generation logic lives here.

### 💾 The Data Lake (`/data`)
* `/curated/data_lake`: The single source of truth. Contains standardized Parquet files and CSV extracts.

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