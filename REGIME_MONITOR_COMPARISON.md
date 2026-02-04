# Regime Monitor Comparison

## Overview

This document compares two regime monitoring systems:
1. **Current Monitor** (`majors_alts_monitor/`) - New, data-lake based system
2. **Legacy Monitor** (`OwnScripts/regime_backtest/regime_monitor.py`) - Original API-based system

## Key Differences

### 1. Data Source

| Aspect | Current Monitor | Legacy Monitor |
|--------|----------------|---------------|
| **Data Source** | Read-only data lake (parquet files, DuckDB) | External APIs (CoinGecko, CoinGlass) |
| **Funding Data** | From data lake `fact_funding` table | CoinGlass API (OI-weighted funding) |
| **Open Interest** | Not used | CoinGlass BTC OI (3d change) |
| **Price Data** | Data lake `fact_price` | CoinGecko API |
| **Universe** | Dynamic from data lake | Hardcoded list of 191 ALT symbols |

### 2. Feature Engineering

#### Current Monitor Features:
- **ALT Breadth & Dispersion**: % alts up on 1d, median(1d ret), IQR, 5d/20d breadth slopes
- **BTC Dominance Shift**: BTC_mcap / (BTC_mcap + ALT_mcap), Δ1d, Δ5d, rolling z-scores
- **Funding Skew**: median funding(alts) − funding(BTC/ETH), 3d z-score
- **Liquidity/Flow Proxies**: 7d rolling median ALT $volume, z-Δ, fraction at 30d volume highs
- **Volatility Spread**: 7d realized vol (cap-weighted ALT index − BTC)
- **Cross-sectional Momentum**: median 3d/7d returns of alts vs BTC/ETH

#### Legacy Monitor Features:
- **Trend Component**: BTC 7d vs ALT 7d spread, vol-adjusted (spread / |BTC_7d|)
- **Funding Risk**: "Heating" = short-term (10d) vs long-term (20d) funding spread
- **OI Risk**: BTC OI 3d change, gated by BTC 3d return quality
- **Breadth Risk**: % of alts outperforming BTC on 3d horizon
- **High-Vol Gate**: Caps regime score at 60 if BTC 7d > 15%

### 3. Regime Score Computation

#### Current Monitor:
```python
score = Σ (w_i * z_i)  # Weighted sum of z-scored features
regime = classify(score, thresholds, hysteresis)
```

**Features used:**
- 6 feature groups (breadth, dominance, funding, liquidity, volatility, momentum)
- All z-scored with rolling windows
- Configurable weights (defaults from config.yaml)

**Regime Classification:**
- 3 regimes: RISK_ON_MAJORS, BALANCED, RISK_ON_ALTS
- 5 regimes: STRONG_RISK_ON_MAJORS, WEAK_RISK_ON_MAJORS, BALANCED, WEAK_RISK_ON_ALTS, STRONG_RISK_ON_ALTS
- Uses hysteresis bands to reduce churn

#### Legacy Monitor:
```python
trend_component = (BTC_7d - ALT_7d_avg) / (|BTC_7d| + ε) / 3.0  # Vol-adjusted
funding_penalty = W_FUNDING * funding_risk  # 25% weight
oi_penalty = 0.15 * oi_risk
breadth_penalty = 0.10 * breadth_risk
combined = trend_component - total_penalty
regime_score = (combined + 1) / 2 * 100  # 0-100 scale
```

**Regime Classification:**
- 5 buckets: GREEN (≥70), YELLOWGREEN (55-69), YELLOW (45-54), ORANGE (30-44), RED (<30)
- High-vol gate: Caps at 60 if BTC 7d > 15%

### 4. Regime Gating

#### Current Monitor:
- Trades in: `RISK_ON_MAJORS` (3-regime) or `STRONG_RISK_ON_MAJORS` + `WEAK_RISK_ON_MAJORS` (5-regime)
- Exits on: `BALANCED` or `RISK_ON_ALTS`

#### Legacy Monitor:
- Would trade in: `GREEN` (regime_score ≥ 70)
- Would exit on: `YELLOW`, `ORANGE`, or `RED`

### 5. Backtest Framework

#### Current Monitor:
- **Walk-forward backtest** with train/test windows
- **Dual-beta neutral** portfolio construction
- **Enhanced ALT selection** (volatility, correlation, momentum filters)
- **Risk management** (stop-loss, take-profit, trailing stop, volatility targeting)
- **Costs**: Maker/taker fees, slippage (ADV-scaled), funding carry
- **Position sizing**: Regime-based scaling, volatility targeting

#### Legacy Monitor:
- **No built-in backtest** (only regime scoring)
- Uses external APIs (not suitable for historical backtesting)
- Designed for live monitoring only

## Advantages

### Current Monitor:
✅ **Read-only data lake** - No API dependencies, faster, more reliable
✅ **PIT-safe features** - No lookahead bias
✅ **Comprehensive feature set** - 6 feature groups vs 4 in legacy
✅ **Walk-forward validation** - Proper out-of-sample testing
✅ **Full backtest framework** - Costs, funding, risk management
✅ **Configurable** - Easy to tune via config.yaml
✅ **Modular design** - Easy to extend and test

### Legacy Monitor:
✅ **Live API integration** - Real-time data from CoinGecko/CoinGlass
✅ **OI-weighted funding** - More accurate funding rates
✅ **Funding "heating" concept** - Captures short-term vs long-term funding trends
✅ **High-vol gate** - Prevents overconfidence in extreme moves
✅ **Simpler logic** - Easier to understand and debug

## Recommendations

1. **For Historical Backtesting**: Use Current Monitor (data lake based)
2. **For Live Monitoring**: Could use Legacy Monitor (API-based) or adapt Current Monitor to pull live data
3. **Best of Both Worlds**: 
   - Use Current Monitor's feature engineering and backtest framework
   - Incorporate Legacy Monitor's "funding heating" concept
   - Add OI data to data lake if available
   - Add high-vol gate to Current Monitor

## Next Steps

1. Run comparison backtest to see which performs better
2. If Legacy Monitor performs better, incorporate its best ideas into Current Monitor
3. Consider hybrid approach: use Current Monitor's framework with Legacy Monitor's funding/OI logic
