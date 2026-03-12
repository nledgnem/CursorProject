🛡️ The Silver Layer: Institutional Data Vault
============================================

### 1. Architectural Philosophy (Separation of Church & State)

The Silver Layer (`silver_fact_price.parquet` and `silver_fact_marketcap.parquet`) is an unforgiving, objective mirror of the exchange data.

- **Rule Zero: No Imputation.**  
  The Silver Layer does not forward-fill, back-fill, or guess missing prices. If the data provider transmitted a catastrophic anomaly, the Silver Layer neutralizes it by converting the corrupted data to `NaN`.

- **Data vs. Analysis responsibilities**
  - **Data Layer (Silver)**: strictly identifies and destroys impossible physics.
  - **Analysis Layer (Gold/Strategy)**: decides how to handle the resulting `NaN`s (e.g., dropping the asset from the basket, or imputing for low-frequency macro tests).

### 2. The Anomaly Shields (What We Correct)

The Silver Layer implements a multi-stage **Wide Net** to preserve organic extreme volatility (crucial for micro-cap alpha) while explicitly hunting down and destroying API hallucinations.

#### A. The Bouncer (Manual Quarantine)

- **Trigger**: Any `asset_id` listed in the root `blacklist.csv`.
- **Action**: The asset is completely dropped from the pipeline before calendar math begins.
- **Why**: Used to manually quarantine fundamentally broken tokens (e.g., tokens undergoing 100:1 reverse splits that break historical continuity).

#### B. The Absolute Nuke (Single-Day API Glitches)

- **Trigger**: A daily price return > +1000% (10x) or < -95%.
- **Action**: The Day T `close` is converted to `NaN`.
- **Why**: Mathematically isolates catastrophic decimal-place errors or toxic micro-cap pump-and-dumps that systematic strategies must avoid.

#### C. Standard Slingshots (The 24-Hour Rebound)

- **Trigger**: Day T price spikes > +100%, and Day T+1 crashes < -50%.
- **Action**: The Day T `close` is converted to `NaN`.
- **Why**: API providers frequently broadcast a fake spike that automatically corrects itself the next calendar day. Wiping Day T protects the timeline.

#### D. Fat Slingshots (The Multi-Day Plateau)

- **Trigger**: Day T price spikes > +100%, stays "stuck", and crashes < -50% on Day T+2 or Day T+3.
- **Action**: The entire hallucinated plateau (Day T, T+1, and/or T+2) is converted to `NaN`.
- **Why**: Neutralizes "stuck" API feeds where a fake price persists for 48–72 hours before reverting to reality.

#### E. Implied Supply Slingshots (Cross-Sectional Identity)

- **Trigger**:
  - Calculate **Implied Supply** = `market_cap / close`.
  - If implied supply spikes > +50% and crashes < -33% within a 3-day window.
- **Action**: The `market_cap` value for the entire plateau is converted to `NaN`. (`close` is left alone if it didn't trigger price rules).
- **Why**: Protects against Market Cap API hallucinations. Prevents the strategy engine from accidentally drafting rank-500 micro-caps into a Top 30 universe due to a temporary multi-billion dollar market cap glitch.

### 3. Downstream Usage Guide (For Quants & Analysts)

If you are building an analysis script (`msm_returns.py`, etc.) on top of the Silver Layer, you must account for the following:

#### A. The NaN Contagion

Because Silver contains `NaN`s, calculating cross-sectional basket returns with a standard Pandas `.mean()` will crash your equity curve.  
You must explicitly use `np.nanmean()` or `.mean(skipna=True)` to safely drop neutralized assets from your active trading universe for that specific window.

#### B. Weekly Boundary Hazards

If you are resampling to weekly returns (e.g., Friday-to-Friday), and the boundary Friday is a `NaN`, **do not shift dates** to borrow Thursday's price.  
The weekly return for that asset must evaluate to `NaN` to prevent lookahead / look-behind bias.

#### C. Embrace the Volatility

The Silver Layer explicitly allows daily moves up to +1000%.  
Do **not** hardcode volatility caps in your general analysis unless you are building a strategy-specific, intra-week risk filter for a highly liquid universe (e.g., Top 30 Majors).  
If you cap volatility globally, you will delete right-tail alpha during market crashes.

