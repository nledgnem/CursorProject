# ALT Selection Criteria for Shorting

## Current Configuration

### Basic Criteria
- **Basket Size**: Top 20 liquid ALTs
- **Minimum Market Cap**: $50,000,000
- **Minimum Volume**: $1,000,000 (7-day median)
- **Per-Name Cap**: 10% maximum weight per ALT

### Enhanced Selection Filters (ENABLED)

1. **Volatility Filter**
   - Excludes ALTs with >100% annualized volatility
   - Lookback: 20 days
   - Purpose: Avoid extremely volatile assets that could cause large losses

2. **Correlation Filter**
   - Excludes ALTs with correlation to BTC/ETH < 0.3
   - Lookback: 60 days
   - Purpose: Avoid idiosyncratic risk (assets that don't move with majors)

3. **Momentum Filter**
   - Excludes ALTs with extreme positive momentum (>50% in 7 days)
   - Excludes ALTs with extreme negative momentum (<-50% in 7 days)
   - Lookback: 7 days
   - Purpose: 
     - Avoid "catching falling knives" (extreme positive momentum)
     - Avoid shorting at the bottom (extreme negative momentum)

4. **Weighting Method**
   - **Inverse Volatility Weighting**: Enabled
   - Less volatile ALTs get higher weights
   - More volatile ALTs get lower weights
   - Purpose: Reduce portfolio volatility by giving more weight to stable assets

## Selection Process

1. Start with all ALTs (excluding BTC, ETH, stables)
2. Filter by basic criteria (mcap ≥ $50M, volume ≥ $1M)
3. Sort by volume (7d median), descending
4. Apply enhanced filters:
   - Remove ALTs with volatility > 100%
   - Remove ALTs with correlation < 0.3
   - Remove ALTs with extreme momentum
5. Take top 20 after filtering
6. Weight by inverse volatility (1/volatility)
7. Cap per-name weight at 10%
8. Renormalize to sum to 100%

## What This Means

The system shorts:
- **Top liquid ALTs** (by volume)
- **With reasonable volatility** (<100% annualized)
- **That are correlated to BTC/ETH** (correlation > 0.3)
- **Without extreme recent momentum** (between -50% and +50% in 7d)
- **Weighted by inverse volatility** (less volatile = higher weight)

This creates a basket of relatively stable, liquid ALTs that move with the majors, avoiding idiosyncratic risk and extreme volatility.
