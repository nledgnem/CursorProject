import pandas as pd
from pathlib import Path

base = Path("data/curated/data_lake")

mc = pd.read_parquet(base / "fact_marketcap.parquet")
dim = pd.read_parquet(base / "dim_asset.parquet")

mc["date"] = pd.to_datetime(mc["date"]).dt.date
target = pd.to_datetime("2026-03-05").date()

mc_day = mc[mc["date"] == target]
merged = mc_day.merge(dim, on="asset_id", how="left")

syms = ["SOL", "BNB", "FARTCOIN", "CLBTC"]
subset = merged[merged["symbol"].str.upper().isin(syms)]

print("Rows for SOL, BNB, FARTCOIN, CLBTC on", target)
if subset.empty:
    print("  None")
else:
    print(subset[["asset_id", "symbol", "marketcap"]].to_string(index=False))

present_syms = set(subset["symbol"].str.upper())
missing = [s for s in syms if s not in present_syms]
if missing:
    print("Missing symbols on that date:", missing)

