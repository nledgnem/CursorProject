# Data Dictionary

Canonical definitions for strategy and index variables used across the MSM funding pipeline and diagnostics.

---

## F_tk

**Definition:** The 7-day moving average of the daily funding rate for the strategy basket.  
**Unit:** % per day (decimal, e.g. 0.0001 = 0.01% per day).  
**Source:** Aggregated from Silver Layer funding rates over the Top 30 Altcoin basket at decision time.

---

## F_tk_apr

**Definition:** The annualized funding rate.  
**Unit:** APR % (e.g. 3.65 = 3.65% APR).  
**Formula:** `F_tk_apr = F_tk * 365 * 100` (convert decimal % per day to annualized %).

---

## y

**Definition:** The 7-day log return of the Long Majors / Short Alts spread strategy.  
**Unit:** Log return (dimensionless).  
**Formula:** `y = ln(1 + Y)` where `Y = R_majors - R_alts` (arithmetic spread over the week). Cumulative gross return = `exp(sum(y))`.

---

## btcdom_7d_ret

**Definition:** The strict 7-day point-to-point log return of the BTCDOM index.  
**Unit:** Log return (dimensionless).  
**Formula:** `btcdom_7d_ret = ln(price_end / price_start)` where `price_start` is the BTCDOM index level on `decision_date` and `price_end` is the level on `next_date`. No single-date or 1-day shifted returns; alignment is exact over the same window as the strategy return `y`.
