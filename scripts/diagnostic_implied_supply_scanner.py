#!/usr/bin/env python3
"""
Diagnostic scanner for implied supply "Slingshots" using Bronze fact_price and fact_marketcap.

CRITICAL: Read-only diagnostic. This script does NOT write any files.

Cross-Sectional Identity Check:
  - Implied Supply = market_cap / close
  - Organic supply changes are slow / stepwise.
  - Violent multi-day mean-reversions in implied supply indicate API glitches.

Logic:
  1) Load Bronze fact_price.parquet and fact_marketcap.parquet.
  2) Outer-join them on (asset_id, date) in memory.
  3) Reindex merged data to continuous daily frequency per asset_id.
  4) Compute implied_supply and its daily percentage change.
  5) Detect sequences where:
        Day T implied_supply_ret > +50%  ( > 0.5 )
     AND any of Day T+1, T+2, or T+3 implied_supply_ret < -33% ( < -0.33 ).
  6) Filter out days where price returns themselves are extreme
     (Absolute Nuke / Price Slingshot) so we isolate hidden mcap-driven anomalies.
  7) Print a terminal report only.
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


def load_bronze_fact_price(data_lake_dir: Path) -> pd.DataFrame:
    path = data_lake_dir / "fact_price.parquet"
    if not path.exists():
        raise FileNotFoundError(f"fact_price.parquet not found: {path}")
    return pd.read_parquet(path)


def load_bronze_fact_marketcap(data_lake_dir: Path) -> pd.DataFrame:
    path = data_lake_dir / "fact_marketcap.parquet"
    if not path.exists():
        raise FileNotFoundError(f"fact_marketcap.parquet not found: {path}")
    df = pd.read_parquet(path)
    # Normalize column name to 'market_cap'
    if "market_cap" not in df.columns:
        if "marketcap" in df.columns:
            df = df.rename(columns={"marketcap": "market_cap"})
        else:
            raise KeyError("No 'market_cap' or 'marketcap' column found in fact_marketcap.parquet")
    return df


def prepare_calendar(df: pd.DataFrame) -> pd.DataFrame:
    if len(df) == 0:
        return df

    df = df.copy()
    df["date"] = pd.to_datetime(_normalize_date_series(df["date"]))
    df = df.sort_values(["asset_id", "date"]).reset_index(drop=True)

    reindexed = []
    for asset_id, g in df.groupby("asset_id", sort=False):
        g = g.sort_values("date").set_index("date")
        g = g.asfreq("D")
        g["asset_id"] = asset_id
        reindexed.append(g.reset_index())

    df = pd.concat(reindexed, ignore_index=True)
    df = df.sort_values(["asset_id", "date"]).reset_index(drop=True)
    return df


def compute_implied_supply(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Avoid division by zero or negative prices / caps
    close = df["close"].astype(float)
    mcap = df["market_cap"].astype(float)
    invalid = (close <= 0) | (mcap <= 0) | close.isna() | mcap.isna()
    implied = pd.Series(np.nan, index=df.index, dtype=float)
    valid_idx = (~invalid)
    implied.loc[valid_idx] = mcap.loc[valid_idx] / close.loc[valid_idx]

    df["implied_supply"] = implied
    df["_supply_prev"] = df.groupby("asset_id")["implied_supply"].shift(1)
    df["_supply_ret"] = (df["implied_supply"] / df["_supply_prev"]) - 1.0

    df["_supply_ret_p1"] = df.groupby("asset_id")["_supply_ret"].shift(-1)
    df["_supply_ret_p2"] = df.groupby("asset_id")["_supply_ret"].shift(-2)
    df["_supply_ret_p3"] = df.groupby("asset_id")["_supply_ret"].shift(-3)
    return df


def find_supply_slingshots(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return rows corresponding to Day T where an Implied Supply Slingshot starts.

    Condition:
      _supply_ret_T > 0.5  (spike > +50%)
      AND any of _supply_ret_{T+1}, _supply_ret_{T+2}, _supply_ret_{T+3} < -0.33 (crash < -33%)
    """
    if len(df) == 0:
        return df

    ret = df["_supply_ret"]
    ret_p1 = df["_supply_ret_p1"]
    ret_p2 = df["_supply_ret_p2"]
    ret_p3 = df["_supply_ret_p3"]

    spike = ret.notna() & (ret > 0.5)
    crash_p1 = ret_p1.notna() & (ret_p1 < -0.33)
    crash_p2 = ret_p2.notna() & (ret_p2 < -0.33)
    crash_p3 = ret_p3.notna() & (ret_p3 < -0.33)

    crash_any = crash_p1 | crash_p2 | crash_p3
    mask_origin = spike & crash_any

    return df.loc[mask_origin].copy()


def compute_price_returns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute daily price returns and shifted returns for price-based anomaly masking.
    """
    df = df.copy()
    close = df["close"].astype(float)
    df["_price_prev"] = df.groupby("asset_id")["close"].shift(1)
    df["_price_ret"] = (close / df["_price_prev"]) - 1.0

    df["_price_ret_p1"] = df.groupby("asset_id")["_price_ret"].shift(-1)
    df["_price_ret_p2"] = df.groupby("asset_id")["_price_ret"].shift(-2)
    df["_price_ret_p3"] = df.groupby("asset_id")["_price_ret"].shift(-3)
    return df


def build_price_anomaly_mask(df: pd.DataFrame) -> pd.Series:
    """
    Build a boolean mask for days where price itself shows an extreme anomaly:
      - Absolute Nuke: |return| > 10.0 (or < -0.95, consistent with ETL)
      - Price Slingshot: return_T > 1.0 and any of T+1..T+3 < -0.5
    """
    ret = df["_price_ret"]
    ret_p1 = df["_price_ret_p1"]
    ret_p2 = df["_price_ret_p2"]
    ret_p3 = df["_price_ret_p3"]

    valid = ret.notna()
    mask_nuke = valid & ((ret > 10.0) | (ret < -0.95))

    spike = valid & (ret > 1.0)
    crash_p1 = ret_p1.notna() & (ret_p1 < -0.5)
    crash_p2 = ret_p2.notna() & (ret_p2 < -0.5)
    crash_p3 = ret_p3.notna() & (ret_p3 < -0.5)
    crash_any = crash_p1 | crash_p2 | crash_p3

    mask_slingshot = spike & crash_any
    return mask_nuke | mask_slingshot


def print_report(anoms: pd.DataFrame, full_df: pd.DataFrame) -> None:
    if anoms.empty:
        print("No Implied Supply Slingshots detected that bypassed price filters.")
        return

    print(f"Total Implied Supply Slingshots detected (bypassing price filters): {len(anoms):,}")
    print("\nSample occurrences (up to 5):")

    samples = anoms.head(5)
    for _, row in samples.iterrows():
        asset_id = row["asset_id"]
        date_t = pd.to_datetime(row["date"])

        # Show window T..T+3
        mask_window = (
            (full_df["asset_id"] == asset_id)
            & (full_df["date"] >= date_t)
            & (full_df["date"] <= date_t + pd.Timedelta(days=3))
        )
        seq = full_df.loc[
            mask_window,
            ["asset_id", "date", "close", "market_cap", "implied_supply"],
        ].copy()
        seq["date"] = pd.to_datetime(seq["date"]).dt.date

        print("\n---")
        print(f"asset_id: {asset_id}")
        print(seq.to_string(index=False))


def main() -> None:
    data_lake_dir = DATA_LAKE
    print(f"Loading Bronze fact_price and fact_marketcap from: {data_lake_dir}")
    price = load_bronze_fact_price(data_lake_dir)
    mcap = load_bronze_fact_marketcap(data_lake_dir)

    # Step 1: Data Integration – outer join in memory
    price = price.copy()
    mcap = mcap.copy()
    price["date"] = pd.to_datetime(_normalize_date_series(price["date"]))
    mcap["date"] = pd.to_datetime(_normalize_date_series(mcap["date"]))

    merged = pd.merge(
        price[["asset_id", "date", "close"]],
        mcap[["asset_id", "date", "market_cap"]],
        on=["asset_id", "date"],
        how="outer",
    )

    # Step 2: Calendar + Math
    cal = prepare_calendar(merged)
    cal = compute_implied_supply(cal)
    cal = compute_price_returns(cal)

    # Supply Slingshots
    supply_slings = find_supply_slingshots(cal)

    # Price-based anomaly mask: filter out rows where price would have been caught
    price_mask = build_price_anomaly_mask(cal)
    # Align mask to supply_slings index (same DataFrame, same index)
    supply_indices = supply_slings.index
    price_mask_at_supply = price_mask.reindex(supply_indices).fillna(False)
    supply_slings_filtered = supply_slings.loc[~price_mask_at_supply].copy()

    # Step 3: Validation – report
    print_report(supply_slings_filtered, cal)


if __name__ == "__main__":
    main()

