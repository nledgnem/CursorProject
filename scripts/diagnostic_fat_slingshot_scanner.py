#!/usr/bin/env python3
"""
Diagnostic scanner for "Fat Slingshots" in Bronze fact_price.

Definition:
  - Day T return > +100% (ret_T > 1.0)
  - AND (Day T+2 return < -50% OR Day T+3 return < -50%)
  - AND NOT a standard T+1 Slingshot (i.e., Day T+1 return < -50%)

This script:
  - Loads Bronze fact_price.parquet
  - Reindexes to continuous daily frequency per asset_id
  - Computes calendar-day returns
  - Scans for Fat Slingshot patterns
  - Prints a summary report and up to 5 sample occurrences
"""

from pathlib import Path

import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_LAKE = REPO_ROOT / "data" / "curated" / "data_lake"


def _normalize_date_series(s: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(s):
        return pd.to_datetime(s).dt.date
    return s


def load_and_prepare_fact_price(data_lake_dir: Path) -> pd.DataFrame:
    path = data_lake_dir / "fact_price.parquet"
    if not path.exists():
        raise FileNotFoundError(f"fact_price.parquet not found: {path}")

    df = pd.read_parquet(path)
    if len(df) == 0:
        return df

    # Normalize and sort
    df["date"] = pd.to_datetime(_normalize_date_series(df["date"]))
    df = df.sort_values(["asset_id", "date"]).reset_index(drop=True)

    # Reindex to continuous daily calendar per asset_id
    reindexed = []
    for asset_id, g in df.groupby("asset_id", sort=False):
        g = g.sort_values("date").set_index("date")
        g = g.asfreq("D")  # introduce missing days as NaNs
        g["asset_id"] = asset_id
        reindexed.append(g.reset_index())

    df = pd.concat(reindexed, ignore_index=True)
    df = df.sort_values(["asset_id", "date"]).reset_index(drop=True)
    return df


def compute_calendar_returns(df: pd.DataFrame) -> pd.DataFrame:
    # Calendar-day returns per asset_id
    df = df.copy()
    df["_prev_close"] = df.groupby("asset_id")["close"].shift(1)
    df["_ret"] = (df["close"] / df["_prev_close"]) - 1.0

    # Pre-compute shifted returns for T+1, T+2, T+3
    df["_ret_p1"] = df.groupby("asset_id")["_ret"].shift(-1)
    df["_ret_p2"] = df.groupby("asset_id")["_ret"].shift(-2)
    df["_ret_p3"] = df.groupby("asset_id")["_ret"].shift(-3)
    return df


def find_fat_slingshots(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a DataFrame of rows corresponding to Day T where a Fat Slingshot starts.
    """
    if len(df) == 0:
        return df

    ret = df["_ret"]
    ret_p1 = df["_ret_p1"]
    ret_p2 = df["_ret_p2"]
    ret_p3 = df["_ret_p3"]

    # Base spike condition on Day T
    spike = ret.notna() & (ret > 1.0)

    # Standard T+1 Slingshot (to be excluded)
    standard_slingshot = ret_p1.notna() & (ret_p1 < -0.5)

    # Fat Slingshot crash condition on T+2 or T+3
    crash_p2 = ret_p2.notna() & (ret_p2 < -0.5)
    crash_p3 = ret_p3.notna() & (ret_p3 < -0.5)
    crash_fat = crash_p2 | crash_p3

    mask_fat = spike & (~standard_slingshot) & crash_fat
    return df.loc[mask_fat].copy()


def print_report(df: pd.DataFrame, fat_df: pd.DataFrame) -> None:
    if len(fat_df) == 0:
        print("No Fat Slingslhots Detected.")
        return

    print(f"Total Fat Slingshot sequences detected: {len(fat_df):,}")

    # Show up to 5 sample sequences
    print("\nSample occurrences (up to 5):")
    samples = fat_df.head(5)
    for _, row in samples.iterrows():
        asset_id = row["asset_id"]
        date_t = row["date"]

        # Extract T..T+3 for this asset
        mask_seq = (
            (df["asset_id"] == asset_id)
            & (df["date"] >= date_t)
            & (df["date"] <= date_t + pd.Timedelta(days=3))
        )
        seq = df.loc[mask_seq, ["asset_id", "date", "close"]].copy()
        seq["date"] = seq["date"].dt.date  # pretty print

        print("\n---")
        print(f"asset_id: {asset_id}")
        print(seq.to_string(index=False))


def main() -> None:
    data_lake_dir = DATA_LAKE
    print(f"Loading Bronze fact_price from: {data_lake_dir}")
    df = load_and_prepare_fact_price(data_lake_dir)

    if len(df) == 0:
        print("Bronze fact_price is empty. Nothing to scan.")
        return

    df_with_ret = compute_calendar_returns(df)
    fat_df = find_fat_slingshots(df_with_ret)
    print_report(df_with_ret, fat_df)


if __name__ == "__main__":
    main()

