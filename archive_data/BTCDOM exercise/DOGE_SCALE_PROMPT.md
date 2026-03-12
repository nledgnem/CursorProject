# Prompt: Check and fix DOGE scale in fact_price

Use this prompt (with an AI or a colleague) to check and fix DOGE scale in the data lake:

---

**Prompt:**

Check if DOGE in `data/curated/data_lake/fact_price.parquet` has correct USD scale.

- DOGE/USD is typically in the range ~0.05–0.50 (or higher). If the stored `close` values are in the range 0.0001–0.01, they are likely in the wrong unit (e.g. 100x or 1000x too small).
- **Check:** Load fact_price, filter to `asset_id == "DOGE"`, and report min/max/median of `close`. If median < 0.05 or max < 0.05, the scale is likely wrong.
- **Fix (if wrong):** Either (a) scale DOGE so that on a known reference date the close equals a known USD price (e.g. 2025-01-15 = 0.35), i.e. multiply all DOGE closes by `reference_price / close_on_reference_date`; or (b) multiply all DOGE closes by a factor that puts the median near 0.25 USD (e.g. factor = 0.25 / median(DOGE)). Then overwrite the DOGE rows in fact_price and save the parquet.
- Preserve all other columns and all other assets unchanged; only update the `close` values for `asset_id == "DOGE"`.

---

## Or run the script

From the repo root:

```bash
# Check only (no write)
python "BTCDOM exercise/check_and_fix_doge_scale.py" --dry-run

# Fix using heuristic (median → 0.25 USD)
python "BTCDOM exercise/check_and_fix_doge_scale.py"

# Fix using a reference date and price (more accurate)
python "BTCDOM exercise/check_and_fix_doge_scale.py" --reference-date 2025-01-15 --reference-price 0.35
```

Optional: `--data-lake PATH` to point to another data lake directory.
