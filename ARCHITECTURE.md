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
* At minimum, assert that index-like series have sensible maxima (e.g. BTCDOM \< 6000) before proceeding.

* **Hard rule**: **Do NOT read any data from `archive/` or `archive_data/` (or any subfolders such as legacy `OldV*` scratchpads).** All research and production analytics must source raw market data from `/data/curated/data_lake` only.

* *(Note: Legacy wide-format files are deprecated and must remain quarantined in `archive_data/`.)*

### 🧪 The Lab (`/notebooks` & `/scripts`)
* `/scripts`: Permanent utility scripts for data fetching and QC.
* `/notebooks`: **[MANDATORY]** Quarantine zone for all diagnostic checks, EDA, and temporary scratchpad code.
