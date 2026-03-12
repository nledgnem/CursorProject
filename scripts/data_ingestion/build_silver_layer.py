#!/usr/bin/env python3
"""
Medallion Silver Layer Builder.

Reads Bronze (curated/data_lake) fact_price and fact_funding; applies winsorization/capping
and forward-fill; appends quality flags (is_ffilled, is_winsorized, is_capped); writes
silver_* parquet files and SILVER_LAYER_METADATA.md.
"""

import sys
from pathlib import Path

import pandas as pd
import numpy as np

# Repo root: script lives in scripts/data_ingestion/
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_LAKE = REPO_ROOT / "data" / "curated" / "data_lake"

FFILL_LIMIT = 3
FUNDING_CAP = 0.05

# Slingshot thresholds (returns on consecutive calendar days)
RETURN_CAP_UP = 1.0      # +100% on day T
RETURN_CAP_DOWN = -0.5   # -50% on subsequent day(s)

# Absolute Nuke thresholds (single-day glitches)
NUKE_RETURN_UP = 10.0    # +1000%
NUKE_RETURN_DOWN = -0.95 # -95%


def _normalize_date_series(s: pd.Series) -> pd.Series:
    """Convert to date (date64 or Timestamp -> Python date or date32)."""
    if pd.api.types.is_datetime64_any_dtype(s):
        return pd.to_datetime(s).dt.date
    return s


def build_silver_fact_price(data_lake_dir: Path) -> tuple[pd.DataFrame, dict]:
    """
    Task 1: Load fact_price, apply blacklist, winsorize extreme/Slingshot returns (set to NaN).
    """
    path = data_lake_dir / "fact_price.parquet"
    if not path.exists():
        raise FileNotFoundError(f"fact_price.parquet not found: {path}")

    df = pd.read_parquet(path)

    # Initialize stats
    n_blacklisted = 0
    n_absolute_nuke = 0
    n_slingshot_standard = 0
    n_slingshot_fat = 0

    if len(df) == 0:
        df["is_ffilled"] = False
        df["is_winsorized"] = False
        stats = {
            "n_blacklisted": 0,
            "n_absolute_nuke": 0,
            "n_slingshot_standard": 0,
            "n_slingshot_fat": 0,
        }
        return df, stats

    # 1. The Bouncer: drop blacklisted assets
    blacklist_path = REPO_ROOT / "blacklist.csv"
    if blacklist_path.exists():
        blacklist_df = pd.read_csv(blacklist_path)
        if "asset_id" not in blacklist_df.columns:
            raise ValueError(f"blacklist.csv must contain an 'asset_id' column: {blacklist_path}")
        blacklist_assets = set(blacklist_df["asset_id"].astype(str))
        # Ensure comparable dtypes
        asset_ids_as_str = df["asset_id"].astype(str)
        mask_blacklisted = asset_ids_as_str.isin(blacklist_assets)
        n_blacklisted = int(mask_blacklisted.sum())
        if n_blacklisted > 0:
            df = df.loc[~mask_blacklisted].copy()
    else:
        blacklist_assets = set()

    if len(df) == 0:
        df["is_ffilled"] = False
        df["is_winsorized"] = False
        stats = {
            "n_blacklisted": n_blacklisted,
            "n_absolute_nuke": 0,
            "n_slingshot_standard": 0,
            "n_slingshot_fat": 0,
        }
        return df, stats

    # Normalize dates and ensure datetime index for calendar reindex
    df["date"] = pd.to_datetime(_normalize_date_series(df["date"]))
    df = df.sort_values(["asset_id", "date"]).reset_index(drop=True)

    # 2. The Calendar: reindex each asset to continuous daily frequency
    reindexed = []
    for asset_id, g in df.groupby("asset_id", sort=False):
        g = g.sort_values("date").set_index("date")
        g = g.asfreq("D")  # creates missing days as rows with NaNs
        g["asset_id"] = asset_id
        reindexed.append(g.reset_index())
    df = pd.concat(reindexed, ignore_index=True)
    df = df.sort_values(["asset_id", "date"]).reset_index(drop=True)

    # 3. Initialize flags
    df["is_winsorized"] = False
    df["is_ffilled"] = False

    # 4. The Math: daily returns on calendar days
    # Use previous calendar day's close; if either side is NaN we get NaN returns.
    df["_prev_close"] = df.groupby("asset_id")["close"].shift(1)
    df["_ret"] = (df["close"] / df["_prev_close"]) - 1.0

    # Pre-compute shifted returns for T+1, T+2, T+3 within each asset group
    df["_ret_p1"] = df.groupby("asset_id")["_ret"].shift(-1)
    df["_ret_p2"] = df.groupby("asset_id")["_ret"].shift(-2)
    df["_ret_p3"] = df.groupby("asset_id")["_ret"].shift(-3)

    # 5. Absolute Nuke: single-day extreme moves
    ret_valid = df["_ret"].notna()
    mask_nuke = ret_valid & ((df["_ret"] > NUKE_RETURN_UP) | (df["_ret"] < NUKE_RETURN_DOWN))
    n_absolute_nuke = int(mask_nuke.sum())
    df.loc[mask_nuke, "close"] = np.nan
    if "source" in df.columns:
        df.loc[mask_nuke, "source"] = np.nan
    df.loc[mask_nuke, "is_winsorized"] = True

    # 6. Standard Slingshot (T+1): spike on T, crash on T+1
    mask_slingshot_standard = (
        df["_ret"].notna()
        & df["_ret_p1"].notna()
        & (df["_ret"] > RETURN_CAP_UP)
        & (df["_ret_p1"] < RETURN_CAP_DOWN)
    )
    n_slingshot_standard = int(mask_slingshot_standard.sum())

    # 7. Fat Slingshot (T+2): spike on T, crash on T+2 (wipe T and T+1)
    mask_fat_t2_origin = (
        df["_ret"].notna()
        & df["_ret_p2"].notna()
        & (df["_ret"] > RETURN_CAP_UP)
        & (df["_ret_p2"] < RETURN_CAP_DOWN)
    )
    # Plateau mask for T and T+1
    mask_fat_t2_plateau = mask_fat_t2_origin | df.groupby("asset_id")["close"].transform(
        lambda s: mask_fat_t2_origin.reindex(s.index).shift(1).fillna(False)
    )

    # 8. Fat Slingshot (T+3): spike on T, crash on T+3 (wipe T, T+1, T+2)
    mask_fat_t3_origin = (
        df["_ret"].notna()
        & df["_ret_p3"].notna()
        & (df["_ret"] > RETURN_CAP_UP)
        & (df["_ret_p3"] < RETURN_CAP_DOWN)
    )
    # Plateau mask for T, T+1, and T+2
    def _plateau_t3(group: pd.Series) -> pd.Series:
        origin = mask_fat_t3_origin.reindex(group.index)
        return (
            origin
            | origin.shift(1).fillna(False)
            | origin.shift(2).fillna(False)
        )

    mask_fat_t3_plateau = df.groupby("asset_id")["close"].transform(_plateau_t3)

    n_slingshot_fat = int(mask_fat_t2_origin.sum() + mask_fat_t3_origin.sum())

    # 9. Apply all anomaly masks to close/source and flags
    mask_any_anomaly = (
        mask_nuke
        | mask_slingshot_standard
        | mask_fat_t2_plateau
        | mask_fat_t3_plateau
    )
    df.loc[mask_any_anomaly, "close"] = np.nan
    if "source" in df.columns:
        df.loc[mask_any_anomaly, "source"] = np.nan
    df.loc[mask_any_anomaly, "is_winsorized"] = True

    # Drop helper columns used for return logic
    df.drop(columns=["_prev_close", "_ret", "_ret_p1", "_ret_p2", "_ret_p3"], inplace=True)

    # Keep date in the same style as the lake (date object)
    df["date"] = pd.to_datetime(df["date"]).dt.date

    stats = {
        "n_blacklisted": n_blacklisted,
        "n_absolute_nuke": n_absolute_nuke,
        "n_slingshot_standard": n_slingshot_standard,
        "n_slingshot_fat": n_slingshot_fat,
    }

    return df, stats


def build_silver_fact_funding(data_lake_dir: Path) -> pd.DataFrame:
    """
    Task 2: Load fact_funding, sort, add flags, cap funding_rate to ±5%, forward-fill gaps.
    """
    path = data_lake_dir / "fact_funding.parquet"
    if not path.exists():
        raise FileNotFoundError(f"fact_funding.parquet not found: {path}")

    df = pd.read_parquet(path)
    if len(df) == 0:
        df["is_ffilled"] = False
        df["is_capped"] = False
        return df
    df["date"] = _normalize_date_series(df["date"])
    df = df.sort_values(["asset_id", "instrument_id", "exchange", "date"]).reset_index(drop=True)

    df["is_ffilled"] = False
    df["is_capped"] = False

    # Capper: funding_rate in [-0.05, 0.05]
    mask_high = df["funding_rate"] > FUNDING_CAP
    mask_low = df["funding_rate"] < -FUNDING_CAP
    df.loc[mask_high, "funding_rate"] = FUNDING_CAP
    df.loc[mask_low, "funding_rate"] = -FUNDING_CAP
    df.loc[mask_high | mask_low, "is_capped"] = True

    # Gap filler: full date range per (asset_id, instrument_id, exchange)
    key = ["asset_id", "instrument_id", "exchange"]
    groups = df.groupby(key)["date"].agg(["min", "max"]).reset_index()
    full_rows = []
    for _, row in groups.iterrows():
        dr = pd.date_range(start=row["min"], end=row["max"], freq="D")
        for d in dr:
            full_rows.append({
                "asset_id": row["asset_id"],
                "instrument_id": row["instrument_id"],
                "exchange": row["exchange"],
                "date": d.date() if hasattr(d, "date") else d,
            })
    full_index = pd.DataFrame(full_rows)

    cols_merge = key + ["date", "funding_rate", "source", "is_ffilled", "is_capped"]
    merged = full_index.merge(
        df[cols_merge],
        on=key + ["date"],
        how="left",
    )
    merged = merged.sort_values(key + ["date"]).reset_index(drop=True)
    merged["_rate_orig"] = merged["funding_rate"].copy()
    merged["funding_rate"] = merged.groupby(key)["funding_rate"].ffill(limit=FFILL_LIMIT)
    merged["source"] = merged.groupby(key)["source"].ffill(limit=FFILL_LIMIT)
    filled = merged["_rate_orig"].isna() & merged["funding_rate"].notna()
    merged.loc[filled, "is_ffilled"] = True
    merged.drop(columns=["_rate_orig"], inplace=True)
    merged["is_ffilled"] = (merged["is_ffilled"] == True)
    merged["is_capped"] = (merged["is_capped"] == True)
    merged = merged.dropna(subset=["funding_rate"]).copy()
    merged = merged.sort_values(key + ["date"]).reset_index(drop=True)

    return merged


def build_silver_fact_marketcap(
    data_lake_dir: Path,
    silver_price: pd.DataFrame,
) -> tuple[pd.DataFrame, dict]:
    """
    Task 3: Load fact_marketcap, apply blacklist, align to Silver price, and
    neutralize implied-supply Slingshots (set market_cap to NaN).
    """
    path = data_lake_dir / "fact_marketcap.parquet"
    if not path.exists():
        raise FileNotFoundError(f"fact_marketcap.parquet not found: {path}")

    df = pd.read_parquet(path)

    # Normalize column name to market_cap if needed
    if "market_cap" not in df.columns and "marketcap" in df.columns:
        df = df.rename(columns={"marketcap": "market_cap"})

    n_blacklisted = 0
    n_supply_origins = 0

    if len(df) == 0:
        df["is_winsorized"] = False
        stats = {
            "n_blacklisted": 0,
            "n_supply_slingshot_origins": 0,
        }
        return df, stats

    # 1. The Bouncer: drop blacklisted assets
    blacklist_path = REPO_ROOT / "blacklist.csv"
    if blacklist_path.exists():
        blacklist_df = pd.read_csv(blacklist_path)
        if "asset_id" not in blacklist_df.columns:
            raise ValueError(f"blacklist.csv must contain an 'asset_id' column: {blacklist_path}")
        blacklist_assets = set(blacklist_df["asset_id"].astype(str))
        asset_ids_as_str = df["asset_id"].astype(str)
        mask_blacklisted = asset_ids_as_str.isin(blacklist_assets)
        n_blacklisted = int(mask_blacklisted.sum())
        if n_blacklisted > 0:
            df = df.loc[~mask_blacklisted].copy()

    if len(df) == 0:
        df["is_winsorized"] = False
        stats = {
            "n_blacklisted": n_blacklisted,
            "n_supply_slingshot_origins": 0,
        }
        return df, stats

    # 2. The Calendar: continuous daily frequency per asset_id
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

    # 3. Initialize flags
    df["is_winsorized"] = False

    # 4. The Integration: merge cleaned Silver price close
    price = silver_price.copy()
    price["date"] = pd.to_datetime(price["date"])
    df = df.merge(
        price[["asset_id", "date", "close"]],
        on=["asset_id", "date"],
        how="left",
        suffixes=("", "_price"),
    )

    # 5. The Math: implied_supply and its daily return
    close = df["close"].astype(float)
    mcap = df["market_cap"].astype(float)
    invalid = (close <= 0) | (mcap <= 0) | close.isna() | mcap.isna()
    implied = pd.Series(np.nan, index=df.index, dtype=float)
    valid_idx = ~invalid
    implied.loc[valid_idx] = mcap.loc[valid_idx] / close.loc[valid_idx]

    df["_implied_supply"] = implied
    df["_supply_prev"] = df.groupby("asset_id")["_implied_supply"].shift(1)
    df["_supply_ret"] = (df["_implied_supply"] / df["_supply_prev"]) - 1.0

    df["_supply_ret_p1"] = df.groupby("asset_id")["_supply_ret"].shift(-1)
    df["_supply_ret_p2"] = df.groupby("asset_id")["_supply_ret"].shift(-2)
    df["_supply_ret_p3"] = df.groupby("asset_id")["_supply_ret"].shift(-3)

    # 6. Supply Slingshots: spike at T, crash within T+1..T+3
    ret = df["_supply_ret"]
    ret_p1 = df["_supply_ret_p1"]
    ret_p2 = df["_supply_ret_p2"]
    ret_p3 = df["_supply_ret_p3"]

    spike = ret.notna() & (ret > 0.5)
    crash_p1 = ret_p1.notna() & (ret_p1 < -0.33)
    crash_p2 = ret_p2.notna() & (ret_p2 < -0.33)
    crash_p3 = ret_p3.notna() & (ret_p3 < -0.33)

    # Assign crash to earliest available day to define plateau length
    origin_t1 = spike & crash_p1
    origin_t2 = spike & (~crash_p1) & crash_p2
    origin_t3 = spike & (~crash_p1) & (~crash_p2) & crash_p3

    origins_any = origin_t1 | origin_t2 | origin_t3
    n_supply_origins = int(origins_any.sum())

    grp = df["asset_id"]

    # Plateau for crash at T+1: wipe T and T+1
    plateau_t1 = origin_t1 | origin_t1.groupby(grp).shift(1).fillna(False)

    # Plateau for crash at T+2: wipe T, T+1, T+2
    plateau_t2 = (
        origin_t2
        | origin_t2.groupby(grp).shift(1).fillna(False)
        | origin_t2.groupby(grp).shift(2).fillna(False)
    )

    # Plateau for crash at T+3: wipe T, T+1, T+2, T+3
    plateau_t3 = (
        origin_t3
        | origin_t3.groupby(grp).shift(1).fillna(False)
        | origin_t3.groupby(grp).shift(2).fillna(False)
        | origin_t3.groupby(grp).shift(3).fillna(False)
    )

    mask_supply = plateau_t1 | plateau_t2 | plateau_t3

    # 7. The Wipe: neutralize market_cap on plateau
    df.loc[mask_supply, "market_cap"] = np.nan
    if "source" in df.columns:
        df.loc[mask_supply, "source"] = np.nan
    df.loc[mask_supply, "is_winsorized"] = True

    # Drop helper columns, including integrated close
    df.drop(
        columns=[
            "close",
            "_implied_supply",
            "_supply_prev",
            "_supply_ret",
            "_supply_ret_p1",
            "_supply_ret_p2",
            "_supply_ret_p3",
        ],
        inplace=True,
        errors="ignore",
    )

    # Keep date as Python date type, consistent with lake
    df["date"] = pd.to_datetime(df["date"]).dt.date

    stats = {
        "n_blacklisted": n_blacklisted,
        "n_supply_slingshot_origins": n_supply_origins,
    }

    return df, stats


def write_metadata(
    data_lake_dir: Path,
    silver_price: pd.DataFrame,
    silver_funding: pd.DataFrame,
    silver_marketcap: pd.DataFrame,
) -> None:
    """Task 3: Write SILVER_LAYER_METADATA.md with row counts and flag percentages."""
    n_price = len(silver_price)
    n_funding = len(silver_funding)
    n_mcap = len(silver_marketcap)

    pct_winsorized_price = (silver_price["is_winsorized"].sum() / n_price * 100) if n_price else 0.0
    pct_winsorized_mcap = (silver_marketcap["is_winsorized"].sum() / n_mcap * 100) if n_mcap else 0.0
    pct_capped = (silver_funding["is_capped"].sum() / n_funding * 100) if n_funding else 0.0
    pct_ffilled_price = (silver_price["is_ffilled"].sum() / n_price * 100) if n_price else 0.0
    pct_ffilled_funding = (silver_funding["is_ffilled"].sum() / n_funding * 100) if n_funding else 0.0
    n_winsorized_price = int(silver_price["is_winsorized"].sum()) if n_price else 0
    n_winsorized_mcap = int(silver_marketcap["is_winsorized"].sum()) if n_mcap else 0
    n_capped = int(silver_funding["is_capped"].sum()) if n_funding else 0
    n_ffilled_price = int(silver_price["is_ffilled"].sum()) if n_price else 0
    n_ffilled_funding = int(silver_funding["is_ffilled"].sum()) if n_funding else 0

    lines = [
        "# Silver Layer Metadata",
        "",
        "Generated by `scripts/data_ingestion/build_silver_layer.py`.",
        "Quality flags: `is_ffilled`, `is_winsorized` (price, market cap), `is_capped` (funding).",
        "",
        "## silver_fact_price.parquet",
        "",
        f"- **Total row count:** {n_price:,}",
        f"- **Rows flagged `is_winsorized`:** {n_winsorized_price:,} ({pct_winsorized_price:.4f}%)",
        f"- **Rows flagged `is_ffilled`:** {n_ffilled_price:,} ({pct_ffilled_price:.4f}%)",
        "",
        "## silver_fact_marketcap.parquet",
        "",
        f"- **Total row count:** {n_mcap:,}",
        f"- **Rows flagged `is_winsorized`:** {n_winsorized_mcap:,} ({pct_winsorized_mcap:.4f}%)",
        "",
        "## silver_fact_funding.parquet",
        "",
        f"- **Total row count:** {n_funding:,}",
        f"- **Rows flagged `is_capped`:** {n_capped:,} ({pct_capped:.4f}%)",
        f"- **Rows flagged `is_ffilled`:** {n_ffilled_funding:,} ({pct_ffilled_funding:.4f}%)",
        "",
    ]
    out_path = data_lake_dir / "SILVER_LAYER_METADATA.md"
    out_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"  Wrote {out_path}")


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Build Silver layer from Bronze fact_price and fact_funding.")
    parser.add_argument(
        "--data-lake",
        type=Path,
        default=DATA_LAKE,
        help="Data lake directory (default: repo data/curated/data_lake)",
    )
    args = parser.parse_args()
    data_lake_dir = args.data_lake.resolve()
    if not data_lake_dir.is_dir():
        print(f"Error: not a directory: {data_lake_dir}", file=sys.stderr)
        sys.exit(1)

    print("Building Silver layer...")
    # Task 1: Price
    print("  [1/4] silver_fact_price...")
    silver_price, price_stats = build_silver_fact_price(data_lake_dir)
    out_price = data_lake_dir / "silver_fact_price.parquet"
    silver_price.to_parquet(out_price, index=False)
    print(f"        -> {out_price} ({len(silver_price):,} rows)")
    print(
        "        Price anomalies:"
        f" Blacklisted Dropped = {price_stats['n_blacklisted']:,},"
        f" Absolute Nuke = {price_stats['n_absolute_nuke']:,},"
        f" Standard Slingshot = {price_stats['n_slingshot_standard']:,},"
        f" Fat Slingshot (T+2/T+3) = {price_stats['n_slingshot_fat']:,}"
    )

    # Task 2: Funding
    print("  [2/4] silver_fact_funding...")
    silver_funding = build_silver_fact_funding(data_lake_dir)
    out_funding = data_lake_dir / "silver_fact_funding.parquet"
    silver_funding.to_parquet(out_funding, index=False)
    print(f"        -> {out_funding} ({len(silver_funding):,} rows)")

    # Task 3: Market Cap
    print("  [3/4] silver_fact_marketcap...")
    silver_marketcap, mcap_stats = build_silver_fact_marketcap(data_lake_dir, silver_price)
    out_mcap = data_lake_dir / "silver_fact_marketcap.parquet"
    silver_marketcap.to_parquet(out_mcap, index=False)
    print(f"        -> {out_mcap} ({len(silver_marketcap):,} rows)")
    print(
        "        Market Cap anomalies:"
        f" Blacklisted Dropped = {mcap_stats['n_blacklisted']:,},"
        f" Supply Slingshot origins = {mcap_stats['n_supply_slingshot_origins']:,}"
    )

    # Task 4: Metadata
    print("  [4/4] SILVER_LAYER_METADATA.md...")
    write_metadata(data_lake_dir, silver_price, silver_funding, silver_marketcap)
    print("Done.")


if __name__ == "__main__":
    main()
