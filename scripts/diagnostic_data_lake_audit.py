"""
Data Lake Audit: January glitch teardown + global contamination sweep.

Task 1: For 2024-01-01 basket, audit weekly returns from fact_price to find
        corrupt assets (e.g. 0 price, -99% return).
Task 2: Global sanity checks on fact_price: non-positive prices, gaps, extreme daily returns.
"""

import pandas as pd
import numpy as np
from pathlib import Path

# Paths relative to repo root (run from Cursor repo root)
REPO_ROOT = Path(__file__).resolve().parent.parent
MSM_CSV = REPO_ROOT / "reports/msm_funding_v0/20260310_103356/msm_timeseries.csv"
FACT_PRICE_PATH = REPO_ROOT / "data/curated/data_lake/fact_price.parquet"

DECISION_2024_01_01 = pd.Timestamp("2024-01-01").date()
NEXT_DATE_2024_01_08 = pd.Timestamp("2024-01-08").date()


def run_january_teardown():
    """Task 1: January glitch teardown for 2024-01-01 basket."""
    print("\n" + "=" * 60)
    print("TASK 1: JANUARY GLITCH TEARDOWN (2024-01-01 basket)")
    print("=" * 60)

    if not MSM_CSV.exists():
        print(f"[ERROR] msm_timeseries.csv not found: {MSM_CSV}")
        return
    if not FACT_PRICE_PATH.exists():
        print(f"[ERROR] fact_price.parquet not found: {FACT_PRICE_PATH}")
        return

    # Load timeseries and get basket for 2024-01-01
    msm = pd.read_csv(MSM_CSV, parse_dates=["decision_date"])
    row = msm[msm["decision_date"].dt.date == DECISION_2024_01_01]
    if row.empty:
        print(f"[ERROR] No row for decision_date {DECISION_2024_01_01} in msm_timeseries.csv")
        return

    basket_str = row["basket_members"].iloc[0]
    asset_ids = [s.strip() for s in basket_str.split(",")]
    print(f"\nBasket for {DECISION_2024_01_01}: {len(asset_ids)} assets")
    print(f"Assets: {asset_ids}")

    # Load fact_price
    fp = pd.read_parquet(FACT_PRICE_PATH)
    # Normalize date column to date type for filtering
    if hasattr(fp["date"].dtype, "date"):
        fp["date"] = pd.to_datetime(fp["date"]).dt.date
    else:
        fp["date"] = pd.to_datetime(fp["date"]).dt.date

    # Prices on decision date and next date
    d1 = fp[(fp["date"] == DECISION_2024_01_01) & (fp["asset_id"].isin(asset_ids))][["asset_id", "close"]].rename(columns={"close": "close_t0"})
    d2 = fp[(fp["date"] == NEXT_DATE_2024_01_08) & (fp["asset_id"].isin(asset_ids))][["asset_id", "close"]].rename(columns={"close": "close_t1"})

    merged = d1.merge(d2, on="asset_id", how="outer")
    merged["close_t0"] = merged["close_t0"].astype(float)
    merged["close_t1"] = merged["close_t1"].astype(float)
    # Arithmetic return for the week
    merged["ret_week"] = np.where(merged["close_t0"] > 0, (merged["close_t1"] / merged["close_t0"]) - 1.0, np.nan)
    merged = merged.sort_values("ret_week", ascending=True)

    print(f"\n--- Bottom 5 (worst returns; possible corruption) ---")
    print(merged.head(5).to_string(index=False))
    print(f"\n--- Top 5 (best returns) ---")
    print(merged.tail(5).to_string(index=False))

    # Explicit flags
    zero_t0 = merged[merged["close_t0"] <= 0]
    zero_t1 = merged[merged["close_t1"] <= 0]
    extreme_neg = merged[merged["ret_week"] <= -0.99]
    if not zero_t0.empty:
        print(f"\n[FLAG] Assets with close_t0 <= 0: {zero_t0['asset_id'].tolist()}")
    if not zero_t1.empty:
        print(f"[FLAG] Assets with close_t1 <= 0: {zero_t1['asset_id'].tolist()}")
    if not extreme_neg.empty:
        print(f"[FLAG] Assets with weekly return <= -99%: {extreme_neg['asset_id'].tolist()}")
    missing = merged[merged["ret_week"].isna()]
    if not missing.empty:
        print(f"[INFO] Assets with missing return (missing price): {missing['asset_id'].tolist()}")


def run_global_contamination_sweep():
    """Task 2: Global sanity checks on fact_price."""
    print("\n" + "=" * 60)
    print("TASK 2: GLOBAL DATA LAKE CONTAMINATION SWEEP")
    print("=" * 60)

    if not FACT_PRICE_PATH.exists():
        print(f"[ERROR] fact_price.parquet not found: {FACT_PRICE_PATH}")
        return

    fp = pd.read_parquet(FACT_PRICE_PATH)
    fp["date"] = pd.to_datetime(fp["date"]).dt.date

    # --- 2a: Prices <= 0 ---
    bad_prices = fp[fp["close"] <= 0]
    n_bad = len(bad_prices)
    print(f"\n--- 2a: Prices <= 0 ---")
    print(f"Count: {n_bad}")
    if n_bad > 0:
        print("Affected (asset_id, date, close):")
        print(bad_prices[["asset_id", "date", "close"]].to_string(index=False))
        print(f"Asset IDs: {bad_prices['asset_id'].unique().tolist()}")

    # --- 2b: Missing data gaps (asset stops pricing mid-series) ---
    print(f"\n--- 2b: Missing data gaps (asset_id, last_date, gap_start) ---")
    fp_sorted = fp.sort_values(["asset_id", "date"])
    fp_sorted["next_date"] = fp_sorted.groupby("asset_id")["date"].shift(-1)
    # date column is Python date; (next - date) is timedelta
    fp_sorted["date_diff"] = (fp_sorted["next_date"] - fp_sorted["date"]).apply(
        lambda x: x.days if hasattr(x, "days") and pd.notna(x) else np.nan
    )
    gaps = fp_sorted[fp_sorted["date_diff"] > 1].copy()
    gaps = gaps.rename(columns={"date": "gap_start", "next_date": "next_date_after_gap"})
    if gaps.empty:
        print("No gaps > 1 day found.")
    else:
        # Summarize: for each asset, show gaps (e.g. mid-week stop)
        gap_summary = gaps.groupby("asset_id").agg({"gap_start": "min", "date_diff": "max"}).reset_index()
        print(gap_summary.head(30).to_string(index=False))
        if len(gap_summary) > 30:
            print(f"... and {len(gap_summary) - 30} more assets with gaps.")

    # --- 2c: Daily return extremes (< -90% or > 500%) ---
    print(f"\n--- 2c: Daily return extremes (daily_ret < -90% or > 500%) ---")
    fp_sorted = fp.sort_values(["asset_id", "date"])
    fp_sorted["prev_close"] = fp_sorted.groupby("asset_id")["close"].shift(1)
    fp_sorted["daily_ret"] = np.where(
        fp_sorted["prev_close"] > 0,
        (fp_sorted["close"] / fp_sorted["prev_close"]) - 1.0,
        np.nan,
    )
    extreme = fp_sorted[(fp_sorted["daily_ret"] < -0.90) | (fp_sorted["daily_ret"] > 5.0)]
    extreme = extreme[["asset_id", "date", "prev_close", "close", "daily_ret"]].dropna(subset=["daily_ret"])
    if extreme.empty:
        print("No daily returns < -90% or > 500%.")
    else:
        print(extreme.to_string(index=False))
        print(f"\nAsset IDs with extreme daily returns: {extreme['asset_id'].unique().tolist()}")

    print("\n" + "=" * 60)


if __name__ == "__main__":
    run_january_teardown()
    run_global_contamination_sweep()
