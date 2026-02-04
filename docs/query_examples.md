# SQL Query Examples for Crypto Backtest Data

This document provides copy/paste SQL queries for inspecting universe snapshots, baskets, data quality, QC results, and **mapping validation** using DuckDB.

## Setup

Run the query interface:

```bash
python scripts/query_duckdb.py --sql "YOUR_QUERY_HERE"
```

Or use interactive mode:

```bash
python scripts/query_duckdb.py
```

## 1. Top 30 Eligible Alts by Market Cap on a Rebalance Date

Find the top 30 eligible altcoins by market cap on a specific rebalance date:

```sql
SELECT 
    symbol,
    marketcap,
    weight,
    rank
FROM universe_snapshots
WHERE rebalance_date = '2024-01-01'
ORDER BY rank
LIMIT 30;
```

## 2. What Changed in Basket Constituents Between Two Rebalances

Compare basket composition between two rebalance dates:

```sql
WITH prev_rebal AS (
    SELECT symbol, rebalance_date
    FROM universe_snapshots
    WHERE rebalance_date = '2024-01-01'
),
curr_rebal AS (
    SELECT symbol, rebalance_date
    FROM universe_snapshots
    WHERE rebalance_date = '2024-02-01'
)
SELECT 
    COALESCE(p.symbol, c.symbol) AS symbol,
    CASE 
        WHEN p.symbol IS NULL THEN 'ENTERED'
        WHEN c.symbol IS NULL THEN 'EXITED'
        ELSE 'MAINTAINED'
    END AS change_type
FROM prev_rebal p
FULL OUTER JOIN curr_rebal c ON p.symbol = c.symbol
ORDER BY change_type, symbol;
```

## 2a. Basket Turnover Per Rebalance (Entered/Exited Counts)

Calculate turnover metrics (entered/exited counts) for each rebalance date by comparing consecutive rebalances:

```sql
WITH rebalance_dates AS (
    SELECT DISTINCT rebalance_date
    FROM universe_snapshots
),
pairs AS (
    SELECT
        rebalance_date AS curr_date,
        LAG(rebalance_date) OVER (ORDER BY rebalance_date) AS prev_date
    FROM rebalance_dates
    QUALIFY prev_date IS NOT NULL
),
curr_basket AS (
    SELECT 
        p.curr_date AS rebalance_date,
        s.symbol
    FROM pairs p
    JOIN universe_snapshots s ON s.rebalance_date = p.curr_date
),
prev_basket AS (
    SELECT 
        p.curr_date AS rebalance_date,
        s.symbol
    FROM pairs p
    JOIN universe_snapshots s ON s.rebalance_date = p.prev_date
)
SELECT
    COALESCE(c.rebalance_date, p.rebalance_date) AS rebalance_date,
    COUNT(DISTINCT c.symbol) AS total_constituents,
    COUNT(DISTINCT CASE WHEN p.symbol IS NULL THEN c.symbol END) AS entered_count,
    COUNT(DISTINCT CASE WHEN c.symbol IS NULL THEN p.symbol END) AS exited_count,
    (COUNT(DISTINCT CASE WHEN p.symbol IS NULL THEN c.symbol END) +
     COUNT(DISTINCT CASE WHEN c.symbol IS NULL THEN p.symbol END)) * 100.0 /
     NULLIF(COUNT(DISTINCT c.symbol), 0) AS turnover_pct
FROM curr_basket c
FULL OUTER JOIN prev_basket p 
    ON c.rebalance_date = p.rebalance_date AND c.symbol = p.symbol
GROUP BY COALESCE(c.rebalance_date, p.rebalance_date)
ORDER BY COALESCE(c.rebalance_date, p.rebalance_date);
```

## 3. Coins with Most QC Edits in Last 30 Days

Find which coins had the most QC repairs applied (if repair_log exists):

```sql
SELECT 
    symbol,
    COUNT(*) AS edit_count,
    COUNT(DISTINCT rule) AS rule_count
FROM repair_log
WHERE date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY symbol
ORDER BY edit_count DESC
LIMIT 20;
```

## 4. Missingness and Coverage Per Coin (Prices)

Calculate data coverage (non-NA percentage) for a specific coin. Note: This query assumes wide format (one column per symbol like BTC, ETH). For multiple coins, run separately:

```sql
-- Coverage for BTC
SELECT 
    COUNT(*) FILTER (WHERE BTC IS NOT NULL) * 100.0 / COUNT(*) AS btc_coverage_pct,
    COUNT(*) AS total_dates,
    COUNT(*) FILTER (WHERE BTC IS NOT NULL) AS non_null_dates
FROM prices_daily;

-- Coverage for ETH (run separately)
SELECT 
    COUNT(*) FILTER (WHERE ETH IS NOT NULL) * 100.0 / COUNT(*) AS eth_coverage_pct,
    COUNT(*) AS total_dates,
    COUNT(*) FILTER (WHERE ETH IS NOT NULL) AS non_null_dates
FROM prices_daily;
```

## 5. Confirm a Coin is Excluded Due to Stablecoin/Blacklist Rules

Check if a coin appears in universe snapshots (indicating it passed filters):

```sql
SELECT 
    symbol,
    COUNT(DISTINCT rebalance_date) AS times_in_basket,
    MIN(rebalance_date) AS first_appearance,
    MAX(rebalance_date) AS last_appearance
FROM universe_snapshots
WHERE symbol = 'USDT'  -- Replace with coin to check
GROUP BY symbol;
```

If this returns 0 rows, the coin was excluded by filters.

## 6. Basket Weights Over Time for a Specific Coin

Track how a coin's weight changed across rebalances:

```sql
SELECT 
    rebalance_date,
    symbol,
    weight,
    marketcap,
    rank
FROM universe_snapshots
WHERE symbol = 'ETH'
ORDER BY rebalance_date;
```

## 7. Price vs Market Cap Correlation

Check price and market cap for a specific coin on a specific date:

```sql
SELECT 
    p.date,
    p.ETH AS price_eth,
    m.ETH AS mcap_eth
FROM prices_daily p
JOIN marketcap_daily m ON p.date = m.date
WHERE p.date = '2024-01-15'
  AND p.ETH IS NOT NULL
  AND m.ETH IS NOT NULL;
```

## 8. Number of Unique Coins Per Rebalance Date

Count how many unique coins were in the basket at each rebalance:

```sql
SELECT 
    rebalance_date,
    COUNT(DISTINCT symbol) AS num_constituents,
    SUM(weight) AS total_weight  -- Should be ~1.0
FROM universe_snapshots
GROUP BY rebalance_date
ORDER BY rebalance_date;
```

## 9. QC Repair Rules Breakdown

Summary of QC edits by rule type (if repair_log exists):

```sql
SELECT 
    rule,
    COUNT(*) AS count,
    COUNT(DISTINCT symbol) AS affected_symbols,
    COUNT(DISTINCT date) AS affected_dates
FROM repair_log
GROUP BY rule
ORDER BY count DESC;
```

## 10. Find Rebalance Dates Where a Coin Was Top Ranked

Find when a specific coin was ranked #1 (or top N):

```sql
SELECT 
    rebalance_date,
    symbol,
    rank,
    weight,
    marketcap
FROM universe_snapshots
WHERE symbol = 'BTC'
  AND rank <= 5
ORDER BY rebalance_date, rank;
```

## 11. Average Market Cap of Basket Constituents Over Time

Track the average market cap of coins in the basket:

```sql
SELECT 
    rebalance_date,
    COUNT(*) AS num_coins,
    AVG(marketcap) AS avg_mcap,
    MIN(marketcap) AS min_mcap,
    MAX(marketcap) AS max_mcap
FROM universe_snapshots
GROUP BY rebalance_date
ORDER BY rebalance_date;
```

## 12. Price Returns for Basket Constituents

Calculate returns for all coins in a basket on a specific date:

```sql
WITH basket_coins AS (
    SELECT DISTINCT symbol
    FROM universe_snapshots
    WHERE rebalance_date = '2024-01-01'
),
price_changes AS (
    SELECT 
        date,
        BTC / LAG(BTC) OVER (ORDER BY date) - 1 AS btc_return,
        ETH / LAG(ETH) OVER (ORDER BY date) - 1 AS eth_return
        -- Add more columns as needed
    FROM prices_daily
    WHERE date >= '2024-01-01' AND date <= '2024-01-31'
)
SELECT 
    date,
    btc_return,
    eth_return
FROM price_changes
WHERE date > '2024-01-01';
```

## Manager-Focused Queries

### Query 1: Why did we have 0 eligible assets on date D?

Breakdown by exclusion_reason on a specific date:

```sql
-- Replace '2024-01-01' with the date in question
SELECT 
    exclusion_reason,
    COUNT(*) AS exclusion_count,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM universe_eligibility WHERE rebalance_date = '2024-01-01') AS exclusion_pct
FROM universe_eligibility
WHERE rebalance_date = '2024-01-01'
  AND exclusion_reason IS NOT NULL
GROUP BY exclusion_reason
ORDER BY exclusion_count DESC;
```

### Query 2: Coverage by rebalance date

Eligible count, snapshot count, and percentage coverage:

```sql
SELECT 
    rebalance_date,
    COUNT(*) FILTER (WHERE eligible = true) AS eligible_count,
    COUNT(*) FILTER (WHERE eligible = false) AS excluded_count,
    COUNT(*) AS total_candidates,
    COUNT(*) FILTER (WHERE eligible = true) * 100.0 / COUNT(*) AS eligible_pct,
    (SELECT COUNT(*) FROM universe_snapshots WHERE universe_snapshots.rebalance_date = universe_eligibility.rebalance_date) AS snapshot_count,
    CASE 
        WHEN COUNT(*) FILTER (WHERE eligible = true) > 0 THEN
            (SELECT COUNT(*) FROM universe_snapshots WHERE universe_snapshots.rebalance_date = universe_eligibility.rebalance_date) * 100.0 / 
            COUNT(*) FILTER (WHERE eligible = true)
        ELSE 0
    END AS coverage_pct
FROM universe_eligibility
GROUP BY rebalance_date
ORDER BY rebalance_date;
```

### Query 3: Top excluded symbols by frequency

Find symbols that are most frequently excluded over a date window:

```sql
-- Replace date range as needed
SELECT 
    symbol,
    exclusion_reason,
    COUNT(*) AS exclusion_count,
    COUNT(DISTINCT rebalance_date) AS affected_dates,
    MIN(rebalance_date) AS first_exclusion,
    MAX(rebalance_date) AS last_exclusion
FROM universe_eligibility
WHERE exclusion_reason IS NOT NULL
  AND rebalance_date >= '2024-01-01'
  AND rebalance_date <= '2024-12-31'
GROUP BY symbol, exclusion_reason
ORDER BY exclusion_count DESC
LIMIT 50;
```

### Exclusions by Reason Over Time

Track how many coins were excluded for each reason across rebalance dates:

```sql
SELECT 
    rebalance_date,
    exclusion_reason,
    COUNT(*) AS exclusion_count
FROM universe_eligibility
WHERE exclusion_reason IS NOT NULL
GROUP BY rebalance_date, exclusion_reason
ORDER BY rebalance_date, exclusion_count DESC;
```

Summary view (total exclusions per reason across all dates):

```sql
SELECT 
    exclusion_reason,
    COUNT(*) AS total_exclusions,
    COUNT(DISTINCT rebalance_date) AS affected_rebalance_dates,
    COUNT(DISTINCT symbol) AS affected_symbols
FROM universe_eligibility
WHERE exclusion_reason IS NOT NULL
GROUP BY exclusion_reason
ORDER BY total_exclusions DESC;
```

### Eligible Universe Size Over Time

Track how many coins were eligible (passed all filters) at each rebalance:

```sql
SELECT 
    rebalance_date,
    COUNT(*) FILTER (WHERE exclusion_reason IS NULL) AS eligible_count,
    COUNT(*) FILTER (WHERE exclusion_reason IS NOT NULL) AS excluded_count,
    COUNT(*) AS total_candidates,
    COUNT(*) FILTER (WHERE exclusion_reason IS NULL) * 100.0 / COUNT(*) AS eligible_pct
FROM universe_eligibility
GROUP BY rebalance_date
ORDER BY rebalance_date;
```

Breakdown by exclusion reason:

```sql
SELECT 
    rebalance_date,
    exclusion_reason,
    COUNT(*) AS count
FROM universe_eligibility
WHERE exclusion_reason IS NOT NULL
GROUP BY rebalance_date, exclusion_reason
ORDER BY rebalance_date, count DESC;
```

### QC Edits Per Asset (Top Offenders)

Find which assets had the most QC repairs applied (if repair_log exists):

```sql
SELECT 
    symbol,
    COUNT(*) AS total_edits,
    COUNT(DISTINCT rule) AS distinct_rules,
    COUNT(DISTINCT date) AS affected_dates,
    MIN(date) AS first_edit_date,
    MAX(date) AS last_edit_date
FROM repair_log
GROUP BY symbol
ORDER BY total_edits DESC
LIMIT 20;
```

Breakdown by rule type for top offenders:

```sql
WITH top_offenders AS (
    SELECT symbol
    FROM repair_log
    GROUP BY symbol
    ORDER BY COUNT(*) DESC
    LIMIT 10
)
SELECT 
    r.symbol,
    r.rule,
    COUNT(*) AS edit_count,
    COUNT(DISTINCT r.date) AS affected_dates
FROM repair_log r
JOIN top_offenders t ON r.symbol = t.symbol
GROUP BY r.symbol, r.rule
ORDER BY r.symbol, edit_count DESC;
```

### Perp Eligibility Over Time (Binance Proxy)

Track how many coins were excluded due to perp not being listed yet (if using Binance onboard dates):

```sql
SELECT 
    rebalance_date,
    COUNT(*) FILTER (WHERE exclusion_reason = 'perp_not_listed_yet') AS perp_not_listed_count,
    COUNT(*) FILTER (WHERE perp_eligible_proxy = true) AS perp_eligible_count,
    COUNT(*) FILTER (WHERE perp_eligible_proxy = false) AS perp_not_eligible_count
FROM universe_eligibility
GROUP BY rebalance_date
ORDER BY rebalance_date;
```

## Mapping Validation Queries

These queries validate data lake mappings: coverage, uniqueness, join sanity, and conflicts.

### Coverage: Marketcap Assets with Price (by Date)

Check how many marketcap assets have price data on the same date:

```sql
SELECT
  m.date,
  COUNT(DISTINCT m.asset_id) AS mcap_assets,
  COUNT(DISTINCT p.asset_id) AS priced_assets,
  COUNT(DISTINCT CASE WHEN p.asset_id IS NOT NULL THEN m.asset_id END) AS mcap_with_price,
  ROUND(100.0 * COUNT(DISTINCT CASE WHEN p.asset_id IS NOT NULL THEN m.asset_id END) / NULLIF(COUNT(DISTINCT m.asset_id),0), 2) AS pct_mcap_with_price
FROM fact_marketcap m
LEFT JOIN fact_price p
  ON m.asset_id = p.asset_id AND m.date = p.date
GROUP BY m.date
ORDER BY m.date DESC
LIMIT 30;
```

### Coverage on Rebalance Dates

Check coverage specifically for eligible assets on rebalance dates:

```sql
SELECT
  ue.rebalance_date,
  COUNT(DISTINCT ue.asset_id) AS eligible_assets,
  COUNT(DISTINCT CASE WHEN p.asset_id IS NOT NULL THEN ue.asset_id END) AS eligible_with_price,
  ROUND(100.0 * COUNT(DISTINCT CASE WHEN p.asset_id IS NOT NULL THEN ue.asset_id END) / NULLIF(COUNT(DISTINCT ue.asset_id),0), 2) AS pct_eligible_with_price
FROM universe_eligibility ue
LEFT JOIN fact_price p
  ON ue.asset_id = p.asset_id AND ue.snapshot_date = p.date
WHERE ue.exclusion_reason IS NULL  -- Only eligible assets
GROUP BY 1
ORDER BY 1;
```

### Uniqueness: Provider Asset ID Duplicates

Find provider_asset_ids that map to multiple asset_ids (violation):

```sql
SELECT provider, provider_asset_id, COUNT(DISTINCT asset_id) AS n_asset_ids
FROM map_provider_asset
GROUP BY 1,2
HAVING COUNT(DISTINCT asset_id) > 1
ORDER BY n_asset_ids DESC
LIMIT 100;
```

### Missing Price: Top Offenders

Which assets are in marketcap but missing price data (top 50 by missing days):

```sql
SELECT a.symbol, m.asset_id, COUNT(*) AS missing_days
FROM fact_marketcap m
LEFT JOIN fact_price p
  ON m.asset_id = p.asset_id AND m.date = p.date
LEFT JOIN dim_asset a
  ON m.asset_id = a.asset_id
WHERE p.asset_id IS NULL
GROUP BY 1,2
ORDER BY missing_days DESC
LIMIT 50;
```

### Perp Coverage

How many assets have perpetual instruments available:

```sql
SELECT
  COUNT(DISTINCT da.asset_id) AS total_assets,
  COUNT(DISTINCT di.base_asset_symbol) AS assets_with_perp,
  ROUND(100.0 * COUNT(DISTINCT di.base_asset_symbol) / NULLIF(COUNT(DISTINCT da.asset_id),0), 2) AS pct_with_perp
FROM dim_asset da
LEFT JOIN dim_instrument di
  ON da.symbol = di.base_asset_symbol
WHERE di.instrument_type = 'perpetual';
```

### Suspected Duplicates: Same Symbol → Multiple Asset IDs

Find symbols that map to multiple asset_ids (potential duplicates):

```sql
SELECT 
  symbol,
  COUNT(DISTINCT asset_id) AS n_asset_ids,
  STRING_AGG(DISTINCT asset_id, ', ') AS asset_ids
FROM dim_asset
GROUP BY symbol
HAVING COUNT(DISTINCT asset_id) > 1
ORDER BY n_asset_ids DESC;
```

### Human Spot-Check: Top 10 Assets

Verify mapping for top assets (BTC, ETH, etc.):

```sql
SELECT 
  da.asset_id,
  da.symbol,
  COUNT(DISTINCT fp.date) AS days_with_price,
  COUNT(DISTINCT fm.date) AS days_with_mcap,
  COUNT(DISTINCT fv.date) AS days_with_volume,
  AVG(fp.close) AS avg_price,
  AVG(fm.marketcap) AS avg_mcap
FROM dim_asset da
LEFT JOIN fact_price fp ON da.asset_id = fp.asset_id
LEFT JOIN fact_marketcap fm ON da.asset_id = fm.asset_id
LEFT JOIN fact_volume fv ON da.asset_id = fv.asset_id
WHERE da.symbol IN ('BTC', 'ETH', 'BNB', 'XRP', 'ADA', 'SOL', 'DOGE', 'DOT', 'MATIC', 'AVAX')
GROUP BY da.asset_id, da.symbol
ORDER BY avg_mcap DESC NULLS LAST;
```

## Tips

- Use `SHOW TABLES` in interactive mode to see all available views
- Parquet files use columnar storage - queries on indexed columns (like `date`) are fast
- For large result sets, use `LIMIT` to preview results first
- Date columns may be DATE/TIMESTAMP depending on how pandas wrote the index; use `CAST(date AS DATE)` if needed for comparisons
