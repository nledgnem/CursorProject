# GitHub Setup Guide for Mads

## Quick Setup Instructions

### Option 1: If Git is Already Installed

1. **Initialize Git Repository** (if not already done):
```bash
cd c:\Users\Admin\Documents\Cursor
git init
```

2. **Add All Files**:
```bash
git add .
```

3. **Create Initial Commit**:
```bash
git commit -m "Initial commit: Crypto backtesting platform with data lake architecture"
```

4. **Create GitHub Repository**:
   - Go to https://github.com/new
   - Create a new repository (e.g., `crypto-backtesting-platform`)
   - **DO NOT** initialize with README, .gitignore, or license

5. **Connect and Push**:
```bash
git remote add origin https://github.com/YOUR_USERNAME/crypto-backtesting-platform.git
git branch -M main
git push -u origin main
```

### Option 2: Using GitHub Desktop (Easier)

1. **Download GitHub Desktop**: https://desktop.github.com/
2. **Install and Sign In**
3. **File → Add Local Repository**
4. **Select**: `c:\Users\Admin\Documents\Cursor`
5. **Publish Repository** to GitHub

### Option 3: Using GitHub CLI (gh)

```bash
# Install GitHub CLI first: https://cli.github.com/
gh repo create crypto-backtesting-platform --public --source=. --remote=origin --push
```

---

## ⚠️ CRITICAL: Remove API Keys First!

**Before pushing, you MUST remove hardcoded API keys:**

1. **`src/providers/coingecko.py`** - Contains CoinGecko API key
2. **`OwnScripts/longterm_funding/longterm_monitor.py`** - Contains Coinglass API key
3. **`OwnScripts/regime_backtest/regime_monitor.py`** - Contains Coinglass API key

**See `PRE_COMMIT_CHECKLIST.md` for detailed instructions on fixing this.**

---

## Important: Check `.gitignore` First

**Source of truth:** rules live in **`.gitignore`** at the project root. This section summarizes current policy; when in doubt, read that file.

### ✅ Typically excluded (see `.gitignore`)

| Area | What |
|------|------|
| **Secrets & env** | `.env`, `.env.local`, `.env.*` (except `.env.example`), `*_secret.json`, `secrets/`, `.streamlit/secrets.toml` |
| **Python / tooling** | `venv/`, `.venv/`, `__pycache__/`, `*.egg-info/`, `dist/`, `build/`, `.pytest_cache/`, `.mypy_cache/`, `.ruff_cache/`, coverage artifacts |
| **Runtime databases** | `*.duckdb`, `data/state/*.db`, `**/macro_state.db` |
| **Heartbeat state** | `**/heartbeat_last_pipeline_success.txt` (do not commit; a snapshot would confuse catch-up on deploy) |
| **Logs** | Everything under `logs/` except `logs/.gitkeep` |
| **Generated MSM runs** | New paths matching `reports/msm_funding_v0/20*/` (timestamped pipeline outputs) |
| **Scratch / one-offs** | e.g. `data/panel_*`, `data/tmp_*.csv`, `single_coin_panel.csv` |
| **Bundles** | `data/**/*.zip` |
| **Temp dirs** | `tmp/`, `temp/`, `scratch/` |

### ✅ Often still tracked (intentional for this repo)

- **Curated data** under `data/curated/` (e.g. `.parquet`) may be committed as shared snapshots—large, but not secrets. Use **Git LFS** if clones are too heavy.
- Older files under `reports/` may remain tracked; **new** MSM timestamped runs are covered by the ignore pattern above.

Do **not** paste a generic “ignore all `*.parquet` / `*.csv`” block: it would conflict with intentional data in the tree. Change `.gitignore` deliberately when adding a new category.

### ⚠️ Check for sensitive data

**API keys:** search before pushing:

```bash
grep -r "api.*key\|API.*KEY" --include="*.py" .
```

**Files to review:**

- `scripts/fetch_coinglass_funding.py` — credentials via args/env, not hardcoded
- `scripts/run_pipeline.py` — same
- Any config or notebook that might embed secrets

**If you find API keys:** remove them, use environment variables, and use a local `.env` (ignored).

---

## What to Include in Repository

### ✅ Include

- Source code (`src/`, `majors_alts_monitor/`, etc.)
- Scripts (`scripts/`)
- Configuration (`configs/`, committed `*.yaml` / `*.json` with **no** secrets)
- Documentation (`docs/`, `*.md`)
- Tests (`tests/`)
- `requirements.txt`, `pyproject.toml`, `.gitignore`, `README.md`
- Optional: curated datasets under `data/` when you want a reproducible snapshot (watch repo size)

### ❌ Do not commit

- API keys, tokens, `.env` files, Streamlit `secrets.toml`
- Virtual environments (`venv/`, `.venv/`)
- Local SQLite/DuckDB state (`macro_state.db`, `*.duckdb`, `data/state/*.db`)
- Log files and heartbeat marker files (see table above)
- Fresh timestamped MSM outputs under `reports/msm_funding_v0/20*/` (regenerate from the pipeline)

---

## Create README.md (If Missing)

Create a comprehensive README for Mads:

```markdown
# Crypto Backtesting Platform

Institutional-light crypto backtesting platform with data lake architecture.

## Features

- Data Lake Architecture (dim/map/fact tables)
- Three Data Sources: CoinGecko, Binance, Coinglass
- Mapping Validation (coverage, uniqueness, join sanity)
- Rate-Limited API Integration
- DuckDB Query Layer
- Comprehensive Validation Suite

## Quick Start

1. Install dependencies: `pip install -r requirements.txt`
2. Run pipeline: `python scripts/run_pipeline.py --config configs/golden.yaml`
3. Validate data: `python scripts/validate_all_parquet.py`

## Documentation

- `PROGRESS_UPDATE_FOR_MADS.md` - Full progress summary
- `MAPPING_VALIDATION_STATUS.md` - Validation implementation status
- `PARQUET_VALIDATION_GUIDE.md` - How to validate parquet datasets
- `docs/query_examples.md` - SQL query examples

## Data Sources

- **CoinGecko**: Price, marketcap, volume data
- **Binance**: Perpetual instrument listings
- **Coinglass**: Funding rate data (with rate limiting)

## Architecture

See `ARCHITECTURE.md` for detailed architecture documentation.
```

---

## Step-by-Step Checklist

- [ ] Review `.gitignore` - ensure secrets and runtime artifacts excluded (see “Important: Check `.gitignore` First” above)
- [ ] Search for API keys in code - remove or use env vars
- [ ] Initialize git repository (`git init`)
- [ ] Add files (`git add .`)
- [ ] Create initial commit (`git commit -m "Initial commit"`)
- [ ] Create GitHub repository (via web or CLI)
- [ ] Add remote (`git remote add origin <url>`)
- [ ] Push to GitHub (`git push -u origin main`)
- [ ] Share repository URL with Mads

---

## Troubleshooting

### Git Not Found
- Install Git: https://git-scm.com/download/win
- Or use GitHub Desktop: https://desktop.github.com/

### Large Files
- This repo may track large `.parquet` snapshots on purpose. If clones are slow, consider **Git LFS** for specific paths (e.g. `git lfs track "data/curated/**/*.parquet"`) after team agreement.
- If you accidentally commit a huge or secret file, remove it from history with BFG or `git filter-repo` and rotate any exposed credentials.

### Authentication Issues
- Use Personal Access Token (not password)
- Or use SSH keys
- Or use GitHub Desktop (handles auth automatically)

---

## Security Reminder

**Before pushing, ensure**:
1. ✅ No API keys in code
2. ✅ No secrets in config files
3. ✅ `.gitignore` excludes secrets and runtime artifacts (see table in “Important: Check `.gitignore` First”)
4. ✅ Large data commits are intentional (curated snapshots); use Git LFS if needed
5. ✅ Local DBs, logs, and heartbeat marker files are not committed

**If you accidentally push secrets**:
1. Remove them from code immediately
2. Rotate the API keys
3. Use `git filter-branch` or BFG Repo-Cleaner to remove from history

---

## Next Steps After Push

1. **Share Repository URL** with Mads
2. **Create Issues** for any known problems
3. **Add Collaborators** (Mads) to repository
4. **Set Up Branch Protection** (optional, for production)
5. **Add CI/CD** (optional, for automated testing)

---

## Quick Command Reference

```bash
# Initialize and first push
git init
git add .
git commit -m "Initial commit: Crypto backtesting platform"
git remote add origin https://github.com/USERNAME/REPO.git
git push -u origin main

# Future updates
git add .
git commit -m "Description of changes"
git push
```
