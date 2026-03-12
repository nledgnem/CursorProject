"""
Simple test for eligibility point-in-time requirement (no pytest dependency).
"""
import pandas as pd
from pathlib import Path

print("=" * 80)
print("TEST: Eligible implies has_price when require_price=true")
print("=" * 80)

eligibility_path = Path("data/curated/universe_eligibility.parquet")
if not eligibility_path.exists():
    print(f"ERROR: {eligibility_path} not found")
    exit(1)

df = pd.read_parquet(eligibility_path)
eligible_rows = df[df["eligible"] == True]

if len(eligible_rows) == 0:
    print("WARNING: No eligible assets found")
    exit(0)

eligible_without_price = eligible_rows[eligible_rows["has_price"] == False]

if len(eligible_without_price) > 0:
    print(f"FAIL: Found {len(eligible_without_price)} eligible assets without price data")
    print(eligible_without_price[["rebalance_date", "symbol", "eligible", "has_price"]].head())
    exit(1)
else:
    print(f"PASS: All {len(eligible_rows)} eligible assets have has_price=true")

print("\n" + "=" * 80)
print("TEST: Rebalance coverage is high (eligible_with_price / eligible_assets)")
print("=" * 80)

date_col = "snapshot_date" if "snapshot_date" in df.columns else "rebalance_date"
all_passed = True

for rb_date in sorted(df[date_col].unique()):
    date_df = df[df[date_col] == rb_date]
    eligible_count = len(date_df[date_df["eligible"] == True])
    eligible_with_price = len(date_df[(date_df["eligible"] == True) & (date_df["has_price"] == True)])
    
    if eligible_count > 0:
        coverage_pct = (eligible_with_price / eligible_count) * 100
        status = "PASS" if coverage_pct >= 95.0 else "FAIL"
        print(f"{rb_date}: {status} - Coverage: {coverage_pct:.1f}% (eligible={eligible_count}, with_price={eligible_with_price})")
        if coverage_pct < 95.0:
            all_passed = False
    else:
        print(f"{rb_date}: SKIP - No eligible assets")

if all_passed:
    print("\n[PASS] All tests passed!")
else:
    print("\n[FAIL] Some tests failed")
    exit(1)

