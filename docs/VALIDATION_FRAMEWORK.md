# Validation Framework: Golden Run + Invariants

## Overview

This document describes the validation framework for ensuring the backtesting platform produces correct, reproducible results. The framework uses a **3-layer verification loop**:

1. **Unit tests** (math/logic pieces)
2. **Integration test** (pipeline produces expected artifacts)
3. **Sanity diagnostics** (numbers look plausible)

## Golden Run

### What is a Golden Run?

A **golden run** is a deterministic, repeatable test pipeline that:
- Uses a small, fixed config (90 days, top 20, monthly rebalance)
- Produces the same outputs given the same inputs (hash-stable)
- Runs fast enough to be part of CI/CD or daily checks

### Running a Golden Run

```powershell
# Basic golden run
python scripts\run_golden.py

# With Binance perp listings
python scripts\run_golden.py --fetch-perp-listings

# Skip QC (use existing curated data)
python scripts\run_golden.py --skip-qc

# Custom output directory
python scripts\run_golden.py --output-dir outputs\my_golden_run
```

### Success Criteria

✅ **Same inputs → same outputs** (file hashes stable)
✅ **All artifacts exist** (universe_eligibility, basket_snapshots, backtest_results, run_summary)
✅ **All invariants pass** (see below)

### Config: `configs/golden.yaml`

The golden run uses a minimal config:
- 90 days (2023-01-01 to 2023-03-31)
- Monthly rebalance
- Top 20 coins
- Equal weight capped
- `gap_fill_mode: none`
- `coverage_threshold: 0.90`

## Invariant Checks

### What are Invariants?

**Invariants** are things that must always be true. They catch silent logic errors that "running without crashing" would miss.

### 11 Core Invariants

#### Universe / Basket (5 invariants)

1. **Weights sum to 1 per rebalance** (± small tolerance)
   - Violation: `[VIOLATION] Weights don't sum to 1.0 on 2023-01-01: sum = 0.999999`

2. **Basket size == topN unless eligible < topN**
   - Violation: `[VIOLATION] Basket size 25 != top_n 20 on 2023-01-01 (eligible: 45)`

3. **No stablecoins/blacklist in basket**
   - Violation: `[VIOLATION] Stablecoins in basket: {'USDT'}`

4. **Every basket coin has eligible=True in universe_eligibility**
   - Violation: `[VIOLATION] Ineligible symbols in basket on 2023-01-01: {'SCAMCOIN'}`

5. **exclusion_reason is never null for excluded rows**
   - Violation: `[VIOLATION] Found 5 excluded rows with null exclusion_reason`

#### Perp Listing Proxy (2 invariants)

6. **For any coin with onboard_date, if rebalance_date < onboard_date then eligible must be false**
   - Violation: `[VIOLATION] Symbol NEWCOIN on 2023-01-01 has onboard_date 2023-01-15 (future) but is eligible`

7. **At least 80% of basket has perp_eligible_proxy true** (warning if lower)
   - Warning: `[WARN] Only 65.0% of basket has perp_eligible_proxy=True on 2023-01-01 (threshold: 80%)`

#### Backtest (4 invariants)

8. **Daily returns have no absurd spikes** (|ret| > 50% triggers warning)
   - Warning: `[WARN] Extreme basket return on 2023-01-15: 75.23%`

9. **Coverage never exceeds 1, never negative** (if tracked)

10. **Turnover is not always ~100% every rebalance** (common bug)
    - Warning: `[WARN] Turnover is >95% for all 12 rebalances (possible bug: weights not persisting)`

11. **Number of NaN return days is reported and explainable**
    - Warning: `[WARN] 45 (15.0%) basket return days are NaN (may indicate coverage threshold too strict)`

### Running Invariant Checks

```powershell
# Standalone validation
python scripts\validate_run.py \
    --universe data/curated/universe_eligibility.parquet \
    --basket data/curated/universe_snapshots.parquet \
    --results outputs/backtest_results.csv \
    --turnover outputs/rebalance_turnover.csv \
    --top-n 20 \
    --base-asset BTC \
    --summary-output outputs/run_summary.md

# As part of pipeline
python scripts\run_pipeline.py --config configs/strategy_benchmark.yaml --validate

# As part of golden run (automatic)
python scripts\run_golden.py
```

### Validation Output

The validation script produces:
1. **Console output**: Lists all violations and warnings
2. **Run summary markdown**: Human-readable summary with:
   - Eligible count per rebalance
   - Exclusions by reason
   - Basket size per rebalance
   - Turnover stats
   - Backtest performance metrics

## Sensitivity Tests

### What are Sensitivity Tests?

**Sensitivity tests** vary one parameter at a time to ensure the system behaves logically (not necessarily "correctly", but consistently).

### Recommended Sensitivity Configs

Run the same period with different settings:

1. **Top-N variation**: `top_n: 20` vs `top_n: 30`
   - Expected: More names → lower concentration

2. **Rebalance frequency**: `monthly` vs `quarterly`
   - Expected: Quarterly → lower turnover

3. **Gap fill mode**: `gap_fill_mode: none` vs `gap_fill_mode: 1d`
   - Expected: `1d` → fewer NaN days

### Running Sensitivity Tests

```powershell
# Create sensitivity configs
cp configs/golden.yaml configs/golden_top30.yaml
# Edit: top_n: 30

cp configs/golden.yaml configs/golden_quarterly.yaml
# Edit: rebalance_frequency: quarterly

# Run each
python scripts\run_golden.py --config configs/golden_top30.yaml --output-dir outputs/sensitivity_top30
python scripts\run_golden.py --config configs/golden_quarterly.yaml --output-dir outputs/sensitivity_quarterly

# Compare results
python scripts\query_duckdb.py --sql "SELECT ..."
```

## Manual Spot-Check

### What is a Manual Spot-Check?

A **manual spot-check** is a 10-minute human audit of one rebalance date to verify selection logic.

### Spot-Check Checklist

Pick one rebalance date (e.g., `2023-01-01`) and verify:

1. **Eligible universe count**
   ```sql
   SELECT COUNT(*) FROM universe_eligibility 
   WHERE rebalance_date = '2023-01-01' AND exclusion_reason IS NULL;
   ```

2. **Top 20 by mcap after filters**
   ```sql
   SELECT symbol, marketcap, exclusion_reason 
   FROM universe_eligibility 
   WHERE rebalance_date = '2023-01-01' 
   ORDER BY marketcap DESC 
   LIMIT 20;
   ```

3. **Basket members match the top 20 list**
   ```sql
   SELECT symbol, rank, weight 
   FROM universe_snapshots 
   WHERE rebalance_date = '2023-01-01' 
   ORDER BY rank;
   ```

4. **Entered/exited makes sense vs prior rebalance**
   ```sql
   -- Compare with previous rebalance
   SELECT ... FROM universe_snapshots WHERE rebalance_date = '2022-12-01';
   ```

### Spot-Check Queries

See `docs/query_examples.md` for ready-to-use queries.

## Integration with CI/CD

### Recommended Workflow

1. **On every commit**: Run golden run (fast, ~2-5 minutes)
2. **On PR**: Run golden run + sensitivity tests
3. **Before release**: Full manual spot-check

### Example GitHub Actions

```yaml
name: Golden Run
on: [push, pull_request]
jobs:
  golden-run:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions/setup-python@v2
      - run: pip install -r requirements.txt
      - run: python scripts/run_golden.py --skip-qc
      - run: |
          # Check that all artifacts exist
          test -f outputs/golden_*/data/curated/universe_eligibility.parquet
          test -f outputs/golden_*/data/curated/universe_snapshots.parquet
          test -f outputs/golden_*/backtest_results.csv
```

## Troubleshooting

### Common Issues

**Issue**: Golden run produces different hashes on each run
- **Cause**: Non-deterministic data (e.g., timestamps in metadata)
- **Fix**: Exclude metadata files from hash comparison, or normalize timestamps

**Issue**: Invariants fail but pipeline "works"
- **Cause**: Logic error in snapshot builder or backtest engine
- **Fix**: Review violation messages, check exclusion ordering, verify return calculation

**Issue**: Sensitivity tests show unexpected behavior
- **Cause**: Config not applied correctly, or logic bug
- **Fix**: Verify config is loaded correctly, check logs

## Next Steps

- [ ] Add file hash comparison to golden run
- [ ] Add performance regression tests (Sharpe, max DD thresholds)
- [ ] Add data quality regression tests (coverage, missingness)
- [ ] Integrate with CI/CD pipeline



