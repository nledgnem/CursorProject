"""Identity quarantine: rows moved out of the fact tables because they belong to another coin.

scripts/repair_asset_identity.py apply moves such rows from fact_price / fact_marketcap /
fact_volume into `quarantine_fact_identity.parquet` (asset_id, date, table, value, seg_id, reason,
quarantined_utc). The fact tables then simply lack those rows. Missing must stay missing downstream:
silver reindexes to daily and marks these dates `is_identity_quarantined=True` (value NaN) so a
consumer can tell "another coin's data was removed here" from an ordinary gap, and nothing may
forward-fill across them (see tests/test_quarantine_semantics.py).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

QUARANTINE_FILE = "quarantine_fact_identity.parquet"


def identity_quarantine_keys(lake_dir: Path, table: str) -> pd.DataFrame:
    """(asset_id, date) quarantined from `table` ('fact_price', 'fact_marketcap', 'fact_volume').
    Empty frame when nothing has been quarantined. `date` is datetime64 (midnight)."""
    path = Path(lake_dir) / QUARANTINE_FILE
    if not path.exists():
        return pd.DataFrame({"asset_id": pd.Series(dtype=str), "date": pd.Series(dtype="datetime64[ns]")})
    q = pd.read_parquet(path, columns=["asset_id", "date", "table"])
    q = q[q["table"] == table]
    q = q.assign(date=pd.to_datetime(q["date"]).dt.normalize())[["asset_id", "date"]]
    return q.drop_duplicates().reset_index(drop=True)


def with_quarantine_rows(df: pd.DataFrame, keys: pd.DataFrame, exclude_assets=()) -> pd.DataFrame:
    """Add a value-less row for every quarantined (asset_id, date) the frame lacks, so a daily
    reindex spans quarantined dates even before an asset's first surviving row."""
    if keys.empty:
        return df
    keys = keys[~keys["asset_id"].isin(set(exclude_assets))]
    have = pd.MultiIndex.from_frame(df[["asset_id", "date"]])
    missing = keys[~pd.MultiIndex.from_frame(keys).isin(have)]
    return pd.concat([df, missing], ignore_index=True) if len(missing) else df


def flag_identity_quarantine(df: pd.DataFrame, keys: pd.DataFrame) -> pd.Series:
    """Boolean Series aligned to df: True where (asset_id, date) was quarantined."""
    if keys.empty:
        return pd.Series(False, index=df.index)
    idx = pd.MultiIndex.from_frame(df[["asset_id", "date"]].assign(date=pd.to_datetime(df["date"]).dt.normalize()))
    return pd.Series(idx.isin(pd.MultiIndex.from_frame(keys)), index=df.index)
