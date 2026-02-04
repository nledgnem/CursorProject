# Pre-Commit Checklist - Remove API Keys Before Pushing

## ⚠️ CRITICAL: Remove API Keys Before Pushing to GitHub

### Files with Hardcoded API Keys:

1. **`src/providers/coingecko.py`**
   - Contains: `COINGECKO_API_KEY = "CG-RhUWZY31TcDFBPfj4GWwcsMS"`
   - **Action**: Remove or replace with environment variable

2. **`OwnScripts/longterm_funding/longterm_monitor.py`**
   - Contains: `COINGLASS_API_KEY = "b8ef3d2aaa6349aeaf873fdda0d1460a"`
   - **Action**: Remove or replace with environment variable

3. **`OwnScripts/regime_backtest/regime_monitor.py`**
   - Contains: `COINGLASS_API_KEY = "b8ef3d2aaa6349aeaf873fdda0d1460a"`
   - **Action**: Remove or replace with environment variable

### Quick Fix Options:

#### Option 1: Use Environment Variables (Recommended)

**For CoinGecko** (`src/providers/coingecko.py`):
```python
import os
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY", "")
```

**For Coinglass** (`OwnScripts/*/longterm_monitor.py`):
```python
import os
COINGLASS_API_KEY = os.getenv("COINGLASS_API_KEY", "")
```

Then create a `.env` file (already in .gitignore):
```
COINGECKO_API_KEY=CG-RhUWZY31TcDFBPfj4GWwcsMS
COINGLASS_API_KEY=b8ef3d2aaa6349aeaf873fdda0d1460a
```

#### Option 2: Remove Keys (If Not Needed in Repo)

Simply remove the hardcoded keys and require users to provide them via command-line arguments (which is already the case for most scripts).

#### Option 3: Use Placeholder Values

Replace with placeholders:
```python
COINGECKO_API_KEY = "YOUR_COINGECKO_API_KEY_HERE"
COINGLASS_API_KEY = "YOUR_COINGLASS_API_KEY_HERE"
```

---

## Complete Pre-Commit Checklist

- [ ] **Remove/redact all API keys** from code
- [ ] **Verify `.gitignore`** excludes sensitive files
- [ ] **Check for hardcoded secrets** (grep for "api.*key", "secret", "password")
- [ ] **Exclude data files** (already in .gitignore)
- [ ] **Exclude output files** (already in .gitignore)
- [ ] **Review temporary test files** (should be excluded)
- [ ] **Create README.md** with setup instructions
- [ ] **Document environment variables** needed

---

## Files to Review Before Committing

### Must Review:
- [ ] `src/providers/coingecko.py` - Contains API key
- [ ] `OwnScripts/longterm_funding/longterm_monitor.py` - Contains API key
- [ ] `OwnScripts/regime_backtest/regime_monitor.py` - Contains API key

### Should Review:
- [ ] `scripts/fetch_coinglass_funding.py` - Uses API key (but from args, OK)
- [ ] `scripts/run_pipeline.py` - Uses API key (but from args, OK)
- [ ] All config files - Check for secrets

### Already Safe:
- ✅ `scripts/fetch_coinglass_funding.py` - Uses `--api-key` argument (no hardcoded key)
- ✅ `scripts/run_pipeline.py` - Uses `--coinglass-api-key` argument (no hardcoded key)

---

## After Fixing API Keys

1. **Test that scripts still work** with environment variables
2. **Update documentation** to mention required environment variables
3. **Create `.env.example`** file (without real keys) as template:
   ```
   COINGECKO_API_KEY=your_key_here
   COINGLASS_API_KEY=your_key_here
   ```
4. **Add to README.md**:
   ```markdown
   ## Setup
   
   1. Copy `.env.example` to `.env`
   2. Add your API keys to `.env`
   3. Run: `python scripts/run_pipeline.py --config configs/golden.yaml`
   ```

---

## Security Reminder

**If you accidentally push API keys to GitHub**:
1. **Immediately rotate the keys** (get new ones from providers)
2. **Remove from code** and commit the fix
3. **Use `git filter-branch`** or **BFG Repo-Cleaner** to remove from git history
4. **Force push** (if you're the only one using the repo)

**Better**: Use environment variables from the start! 🔒
