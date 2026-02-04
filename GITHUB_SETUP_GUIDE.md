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

## Important: Check .gitignore First

Before pushing, verify that sensitive data is excluded:

### ✅ Already Excluded (in .gitignore):
- `data/raw/*` - Raw data files
- `data/curated/*` - Curated data files (except .gitkeep)
- `outputs/*` - Pipeline outputs (except .gitkeep)
- `venv/` - Python virtual environment
- `__pycache__/` - Python cache

### ⚠️ Check for Sensitive Data:

**API Keys**: Search for any hardcoded API keys:
```bash
# Search for API keys in code
grep -r "api.*key\|API.*KEY" --include="*.py" .
```

**Files to Review Before Committing**:
- `scripts/fetch_coinglass_funding.py` - Check for hardcoded API keys
- `scripts/run_pipeline.py` - Check for hardcoded credentials
- Any config files with secrets

**If you find API keys**, either:
1. Remove them and use environment variables
2. Add them to `.gitignore`
3. Use a `.env` file (and add `.env` to `.gitignore`)

---

## Recommended .gitignore Additions

Add these to `.gitignore` if not already present:

```
# Environment variables
.env
.env.local
*.env

# API keys and secrets
*_secret.json
*_keys.json
secrets/

# Large data files
*.parquet
*.csv
*.json
!configs/*.json
!configs/*.yaml

# Database files
*.duckdb
*.db
*.sqlite

# Logs
*.log
logs/

# Temporary files
*.tmp
*.temp
test_*.py
verify_*.py
quick_*.py
```

---

## What to Include in Repository

### ✅ Include:
- All source code (`src/`)
- All scripts (`scripts/`)
- Configuration files (`configs/`)
- Documentation (`docs/`, `*.md`)
- Tests (`tests/`)
- Requirements (`requirements.txt`, `pyproject.toml`)
- `.gitignore`
- `README.md`

### ❌ Exclude:
- Data files (`data/raw/`, `data/curated/*.parquet`)
- Output files (`outputs/`)
- Virtual environment (`venv/`)
- API keys and secrets
- Large binary files
- Personal notes/temporary files

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

See `docs/architecture.md` for detailed architecture documentation.
```

---

## Step-by-Step Checklist

- [ ] Review `.gitignore` - ensure sensitive data excluded
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
- If you accidentally commit large files, use `git-lfs`:
```bash
git lfs install
git lfs track "*.parquet"
git add .gitattributes
```

### Authentication Issues
- Use Personal Access Token (not password)
- Or use SSH keys
- Or use GitHub Desktop (handles auth automatically)

---

## Security Reminder

**Before pushing, ensure**:
1. ✅ No API keys in code
2. ✅ No secrets in config files
3. ✅ `.gitignore` excludes sensitive data
4. ✅ Data files excluded (too large anyway)
5. ✅ Output files excluded

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
