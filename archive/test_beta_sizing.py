"""
Test the beta-neutral sizing calculation directly
"""

# Simulate what happens in the solver
alt_weights = {"ALT1": 0.05, "ALT2": 0.05, "ALT3": 0.05}  # Sum to 0.15, but normalized to 1.0
alt_total = sum(abs(w) for w in alt_weights.values())  # 0.15
alt_scale = 0.5 / alt_total  # 0.5 / 0.15 = 3.33
scaled_alt_weights = {k: -abs(v) * alt_scale for k, v in alt_weights.items()}
print(f"Scaled ALT weights: {scaled_alt_weights}")
print(f"ALT total (abs): {sum(abs(w) for w in scaled_alt_weights.values())}")

# Simulate beta exposures (alts have beta ~1.0 to BTC, ~0.8 to ETH)
alt_betas = {
    "ALT1": {"BTC": 1.0, "ETH": 0.8},
    "ALT2": {"BTC": 1.0, "ETH": 0.8},
    "ALT3": {"BTC": 1.0, "ETH": 0.8},
}

alt_btc_beta_exp = sum(scaled_alt_weights.get(a, 0.0) * alt_betas.get(a, {}).get("BTC", 1.0) 
                       for a in alt_weights.keys())
alt_eth_beta_exp = sum(scaled_alt_weights.get(a, 0.0) * alt_betas.get(a, {}).get("ETH", 1.0) 
                       for a in alt_weights.keys())

print(f"\nALT beta exposures:")
print(f"  BTC: {alt_btc_beta_exp:.4f}")
print(f"  ETH: {alt_eth_beta_exp:.4f}")

# Calculate optimal major weights
optimal_btc_weight = 0.25 + 0.5 * (alt_eth_beta_exp - alt_btc_beta_exp)
optimal_btc_weight = max(0.0, min(0.5, optimal_btc_weight))
optimal_eth_weight = 0.5 - optimal_btc_weight

print(f"\nOptimal major weights:")
print(f"  BTC: {optimal_btc_weight:.4f}")
print(f"  ETH: {optimal_eth_weight:.4f}")
print(f"  Sum: {optimal_btc_weight + optimal_eth_weight:.4f}")
print(f"  Gross (abs sum): {abs(optimal_btc_weight) + abs(optimal_eth_weight):.4f}")

majors = {"BTC": optimal_btc_weight, "ETH": optimal_eth_weight}
print(f"\nMajors dict: {majors}")
print(f"Major gross from dict: {sum(abs(w) for w in majors.values()):.4f}")
