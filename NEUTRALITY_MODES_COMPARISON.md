# Neutrality Modes Comparison

## Overview

The system now supports two neutrality modes for position sizing:

1. **`dollar_neutral`** (default): Enforces dollar-neutrality first, then minimizes beta exposure
2. **`beta_neutral`**: Enforces beta-neutrality first, allows non-zero net exposure

## Mode 1: Dollar-Neutral First (`dollar_neutral`)

### Approach:
1. Scale ALT weights to 50% (short)
2. Size majors to sum to +50% (long) for dollar-neutrality
3. Within that constraint, minimize BTC and ETH factor exposure

### Characteristics:
- **Net exposure**: 0% (perfectly dollar-neutral)
- **ALT gross**: 50%
- **Major gross**: 50%
- **Total gross**: 100%
- **Beta exposure**: Minimized subject to dollar-neutrality constraint

### Trade-offs:
- ✅ Perfect dollar-neutrality (no directional market exposure)
- ⚠️ May have residual beta exposure if ALT betas don't perfectly offset with 50/50 split

## Mode 2: Beta-Neutral First (`beta_neutral`)

### Approach:
1. Scale ALT weights to some level (e.g., 33%)
2. Calculate ALT beta exposure to BTC and ETH
3. Size majors to directly offset beta exposure: `btc_weight = -alt_btc_exp`, `eth_weight = -alt_eth_exp`
4. Allow net exposure to be non-zero if needed

### Characteristics:
- **Net exposure**: May be non-zero (e.g., 33% net long in test)
- **ALT gross**: ~33%
- **Major gross**: ~67%
- **Total gross**: 100%
- **Beta exposure**: Minimized (often near zero)

### Trade-offs:
- ✅ Better beta-neutrality (minimal BTC/ETH factor exposure)
- ⚠️ Non-zero net exposure (directional market risk)
- ⚠️ Larger major positions (more capital required)

## Test Results Comparison

### Dollar-Neutral Mode:
- Max drawdown: -46.39%
- Sharpe: -6.66
- Gross: ALT 50% + Major 50% = 100%
- Net: 0%

### Beta-Neutral Mode:
- Max drawdown: -46.52%
- Sharpe: -8.26
- Gross: ALT 33% + Major 67% = 100%
- Net: ~33% (net long)

## When to Use Each Mode

### Use `dollar_neutral` when:
- You want zero directional market exposure
- Capital efficiency is important (equal long/short sizing)
- You're comfortable with some residual beta exposure
- You want to isolate the ALT vs Major relative performance

### Use `beta_neutral` when:
- Factor exposure (BTC/ETH beta) is your primary risk concern
- You're okay with directional market exposure
- You have sufficient capital for larger major positions
- You want to minimize correlation with BTC/ETH factors

## Configuration

Set in `config.yaml`:
```yaml
universe:
  neutrality_mode: "dollar_neutral"  # or "beta_neutral"
```

## Implementation Details

Both modes:
- Scale ALT weights to cap total gross exposure
- Use analytical solutions (no optimization needed)
- Cap individual positions to prevent extreme sizing
- Log beta exposures and net exposure for monitoring

The key difference is the constraint priority:
- Dollar-neutral: `sum(majors) = -sum(alts)` (hard constraint)
- Beta-neutral: `btc_exp = 0, eth_exp = 0` (hard constraint), net exposure is free
