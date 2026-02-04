# CHZ World Cup Event Study Analysis

Comprehensive event study and correlation analysis for Chiliz (CHZ) around major football events, with focus on FIFA World Cup performance and 2026 positioning strategy.

## Overview

This analysis quantifies whether CHZ has historically outperformed around World Cup windows, whether any "World Cup pump" is statistically meaningful after controlling for crypto beta, and how tradable the pattern is.

## Deliverables

1. **Research Memo** (`research_memo.md`): 1-2 page summary with conclusions, key stats, and monitoring signals
2. **Tradeable Playbook** (`tradeable_playbook.md`): Entry/exit rules, position sizing, risk management
3. **Analysis Scripts**: Reproducible Python code for all computations
4. **Charts & Tables**: Visualizations and summary statistics

## Quick Start

### Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Optional: Install CCXT for Binance data (otherwise uses CoinGecko)
pip install ccxt
```

### Run Complete Analysis

```bash
# Run all steps in sequence
python run_analysis.py
```

Or run steps individually:

```bash
# Step 1: Fetch data and compute metrics
python chz_event_study.py

# Step 2: Generate visualizations
python visualize_results.py

# Step 3: Generate memo and playbook
python generate_memo.py
```

## Output Files

All outputs are saved to `outputs/` directory:

### Documents
- `research_memo.md` - Research memo with conclusions
- `tradeable_playbook.md` - Trading strategy playbook

### Data Files
- `chz_data.csv` - CHZ price data
- `btc_data.csv` - BTC price data (benchmark)
- `eth_data.csv` - ETH price data (benchmark)
- `window_metrics.csv` - Returns and metrics for each event window
- `abnormal_returns.csv` - Cumulative abnormal returns (CAR)
- `rolling_beta.csv` - Rolling 60-day beta of CHZ vs BTC
- `btc_regimes.csv` - BTC trend and volatility regimes
- `statistical_tests.csv` - Bootstrap CIs and Wilcoxon tests
- `summary_table.csv` - Summary statistics table

### Charts
- `price_chart_with_events.png` - Price chart with shaded event windows
- `window_returns_barchart.png` - Returns by window across events
- `car_by_event.png` - Cumulative abnormal returns per event
- `rolling_beta.png` - Rolling beta over time
- `drawdown_curves.png` - Drawdown curves around events

## Events Analyzed

1. **FIFA World Cup 2018** (Russia): June 14 - July 15, 2018
2. **FIFA World Cup 2022** (Qatar): November 20 - December 18, 2022
3. **UEFA Euro 2020** (played 2021): June 11 - July 11, 2021
4. **UEFA Euro 2024**: June 14 - July 14, 2024
5. **Copa América 2024**: June 20 - July 14, 2024

## Window Definitions

### Pre-Event Windows
- `[-120, -90]` days before event start
- `[-90, -60]` days
- `[-60, -30]` days
- `[-30, -14]` days
- `[-14, 0]` days

### Event Windows
- `[0, +7]` days (first week)
- `[0, +14]` days (first two weeks)
- `[0, +30]` days (first month)

### Post-Event Windows
- `[+14, +30]` days after event start
- `[+30, +60]` days
- `[+60, +90]` days

## Methodology

### 1. Simple Performance Metrics
- Absolute returns
- Excess returns vs BTC and ETH
- Hit rate (frequency of positive returns)
- Volatility-adjusted returns (Sharpe-like)
- Maximum drawdown

### 2. Event Study (Abnormal Returns)
- Market model: `r_CHZ = α + β × r_BTC + ε`
- Beta estimation window: `[-180, -60]` days before event
- Cumulative Abnormal Return (CAR) for each window

### 3. Statistical Tests
- Bootstrap confidence intervals (10,000 iterations)
- Wilcoxon signed-rank test (non-parametric)
- Effect sizes with uncertainty bounds

### 4. Regime Controls
- BTC trend regime (200D moving average)
- BTC volatility regime (rolling 30D vol vs median)

## Key Questions Answered

1. Did CHZ reliably outperform in the 60–120 days BEFORE World Cups?
2. Was the move mostly pre-event or during the event?
3. How bad were post-event drawdowns?
4. After controlling for BTC beta, is there still abnormal performance?
5. What would be the "best simple rule" historically?

## Data Sources

- **Primary:** CCXT (Binance) for OHLCV data
- **Fallback:** CoinGecko API (if CCXT unavailable)
- **Benchmarks:** BTC, ETH

## Limitations

1. **Small Sample Size:** Only 2 World Cup events (2018, 2022) for primary analysis
2. **Survivorship Bias:** CHZ may have benefited from being early in fan token space
3. **Structural Changes:** Tokenomics and market structure have evolved
4. **Market Regime Dependency:** Results vary by crypto market regime
5. **Narrative Decay:** Pattern may become less effective as it becomes known

## Requirements

- Python 3.8+
- pandas, numpy, matplotlib, seaborn, scipy
- requests (for CoinGecko API)
- ccxt (optional, for Binance data)

## Notes

- All code is modular and parameterized
- Analysis is fully reproducible
- Claims are backed by computed statistics with uncertainty bounds
- Risk management and position sizing frameworks included

## Contact

For questions or issues, refer to the code comments or analysis outputs.

---

*This analysis is for research purposes only and does not constitute financial advice. Past performance does not guarantee future results.*
