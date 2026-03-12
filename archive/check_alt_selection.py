"""Check what ALT selection criteria are being used and what ALTs are selected."""

import polars as pl
import yaml
from pathlib import Path

print("=" * 80)
print("ALT SELECTION CRITERIA")
print("=" * 80)

# Load config
config_path = Path("majors_alts_monitor/config.yaml")
with open(config_path, 'r') as f:
    config = yaml.safe_load(f)

universe_config = config["universe"]
alt_selection_config = universe_config.get("alt_selection", {})

print("\nBASIC CRITERIA:")
print(f"  Basket Size: {universe_config['basket_size']} ALTs")
print(f"  Min Market Cap: ${universe_config['min_mcap_usd']:,.0f}")
print(f"  Min Volume (7d median): ${universe_config['min_volume_usd']:,.0f}")
print(f"  Per-Name Cap: {universe_config['per_name_cap']*100:.0f}%")

print("\nENHANCED SELECTION FILTERS:")
if alt_selection_config.get("enabled", False):
    print("  [ENABLED]")
    print(f"  Max Volatility: {alt_selection_config.get('max_volatility', 'N/A')*100:.0f}% annualized")
    print(f"  Min Correlation: {alt_selection_config.get('min_correlation', 'N/A'):.2f} (to BTC/ETH)")
    print(f"  Max Momentum: {alt_selection_config.get('max_momentum', 'N/A')*100:.0f}% (7d)")
    print(f"  Min Momentum: {alt_selection_config.get('min_momentum', 'N/A')*100:.0f}% (7d)")
    print(f"  Weight by Inverse Vol: {alt_selection_config.get('weight_by_inverse_vol', False)}")
    print(f"  Volatility Lookback: {alt_selection_config.get('volatility_lookback_days', 20)} days")
    print(f"  Correlation Lookback: {alt_selection_config.get('correlation_lookback_days', 60)} days")
    print(f"  Momentum Lookback: {alt_selection_config.get('momentum_lookback_days', 7)} days")
else:
    print("  ✗ DISABLED")

print("\n" + "=" * 80)
print("SELECTION PROCESS:")
print("=" * 80)
print("1. Filter by basic criteria (mcap, volume)")
print("2. Sort by volume (7d median), descending")
print("3. Apply enhanced filters (if enabled):")
print("   - Exclude ALTs with volatility > 100%")
print("   - Exclude ALTs with correlation < 0.3 to BTC/ETH")
print("   - Exclude ALTs with extreme momentum (>50% or <-50% in 7d)")
print("4. Take top N (basket_size) after filtering")
print("5. Weight by inverse volatility (if enabled) or equal weight")
print("6. Cap per-name weight at per_name_cap")

print("\n" + "=" * 80)
print("WHAT THIS MEANS:")
print("=" * 80)
print("The system shorts:")
print("  - Top liquid ALTs (by volume)")
print("  - With reasonable volatility (<100% annualized)")
print("  - That are correlated to BTC/ETH (correlation > 0.3)")
print("  - Without extreme recent momentum")
print("  - Weighted by inverse volatility (less volatile = higher weight)")
print("=" * 80)
