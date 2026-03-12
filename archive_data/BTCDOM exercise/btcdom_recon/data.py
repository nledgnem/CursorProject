"""
Data access abstraction: load prices from the data lake or wide-format prices_daily.parquet.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from .config import DEFAULT_DATA_LAKE_PATH

logger = logging.getLogger(__name__)

# Cache for wide-format parquet so we don't re-read per symbol
_wide_prices_cache: dict[str, pd.DataFrame] = {}


def _project_root() -> Path:
    """Repository root (parent of BTCDOM exercise)."""
    return Path(__file__).resolve().parent.parent.parent


def _resolve_price_source(data_lake_path: Optional[str | Path]) -> tuple[Path, str]:
    """
    Resolve path to either fact_price.parquet (in a directory) or prices_daily.parquet (wide file).
    Returns (resolved_path, "fact" | "wide").
    """
    root = _project_root()
    if data_lake_path is None:
        # Try data lake first, then curated/raw prices_daily
        data_lake_dir = root / DEFAULT_DATA_LAKE_PATH
        fact_file = data_lake_dir / "fact_price.parquet"
        if fact_file.exists():
            return fact_file.parent, "fact"
        for candidate in [
            root / "data" / "curated" / "prices_daily.parquet",
            root / "data" / "raw" / "prices_daily.parquet",
        ]:
            if candidate.exists():
                return candidate, "wide"
        raise FileNotFoundError(
            f"No price data found. Tried: {fact_file}, "
            f"{root / 'data/curated/prices_daily.parquet'}, "
            f"{root / 'data/raw/prices_daily.parquet'}"
        )
    path = Path(data_lake_path)
    if not path.is_absolute():
        path = root / path
    if path.is_file():
        if path.name == "prices_daily.parquet" or "prices_daily" in path.name:
            return path, "wide"
        raise ValueError(f"Unknown price file: {path}. Use fact_price.parquet or prices_daily.parquet.")
    # Directory: look for fact_price.parquet
    fact_file = path / "fact_price.parquet"
    if fact_file.exists():
        return path, "fact"
    # Directory might contain prices_daily.parquet
    wide_file = path / "prices_daily.parquet"
    if wide_file.exists():
        return wide_file, "wide"
    raise FileNotFoundError(f"No fact_price.parquet or prices_daily.parquet in {path}")


def _load_wide_prices(path: Path, symbol: str, start: str, end: str, freq: str) -> pd.DataFrame:
    """Load one symbol from wide-format prices_daily.parquet (date index, columns = symbols)."""
    global _wide_prices_cache
    path_str = str(path.resolve())
    if path_str not in _wide_prices_cache:
        df = pd.read_parquet(path)
        # Wide: index = date (or datetime), columns = BTC, ETH, ...
        if df.index.name in (None, "") and "date" in df.columns:
            df = df.set_index("date")
        df.index = pd.to_datetime(df.index)
        _wide_prices_cache[path_str] = df
    df = _wide_prices_cache[path_str]
    if symbol not in df.columns:
        raise KeyError(f"Symbol {symbol} not in prices_daily.parquet columns: {list(df.columns)[:10]}...")
    ser = df[symbol].dropna().sort_index()
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    ser = ser.loc[(ser.index >= start_ts) & (ser.index <= end_ts)]
    freq_pd = freq if freq in ("1D", "D", "1d", "d") else freq
    try:
        resampled = ser.resample(freq_pd).last().ffill()
    except Exception:
        freq_pd = freq.replace("1d", "D").replace("1h", "h").replace("1m", "min")
        resampled = ser.resample(freq_pd).last().ffill()
    out = resampled.to_frame("price_usd").dropna(how="all").reset_index()
    out.columns = ["timestamp", "price_usd"]
    return out


def load_prices(
    symbol: str,
    start: str,
    end: str,
    freq: str,
    *,
    data_lake_path: Optional[str | Path] = None,
) -> pd.DataFrame:
    """
    Load price series for a symbol from the data lake or prices_daily.parquet.

    Supports:
    - Data lake: directory containing fact_price.parquet (asset_id, date, close).
    - Wide format: path to prices_daily.parquet (date index, one column per symbol).

    Returns a DataFrame with columns ["timestamp", "price_usd"].

    Parameters
    ----------
    symbol : str
        Asset symbol (e.g. BTC, ETH). Column name in wide format or asset_id in fact table.
    start, end : str
        Date range (YYYY-MM-DD).
    freq : str
        Frequency, e.g. "1D", "1h".
    data_lake_path : str or Path, optional
        Directory with fact_price.parquet, or path to prices_daily.parquet.
        If None, tries data/curated/data_lake, then data/curated/prices_daily.parquet, then data/raw/prices_daily.parquet.
    """
    path, kind = _resolve_price_source(data_lake_path)
    if kind == "wide":
        return _load_wide_prices(path, symbol, start, end, freq)

    price_file = path / "fact_price.parquet"
    df = pd.read_parquet(price_file)
    if "asset_id" not in df.columns or "close" not in df.columns:
        raise ValueError("fact_price.parquet must have columns: asset_id, date, close")

    date_col = "date" if "date" in df.columns else "timestamp"
    df = df.loc[df["asset_id"] == symbol, [date_col, "close"]].copy()
    df = df.rename(columns={"close": "price_usd", date_col: "timestamp"})
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    df = df.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last")
    df = df.set_index("timestamp")

    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    df = df.loc[(df.index >= start_ts) & (df.index <= end_ts)]

    freq_pd = freq if freq in ("1D", "D", "1d", "d") else freq
    try:
        resampled = df["price_usd"].resample(freq_pd).last().ffill()
    except Exception:
        freq_pd = freq.replace("1d", "D").replace("1h", "h").replace("1m", "min")
        resampled = df["price_usd"].resample(freq_pd).last().ffill()

    out = resampled.to_frame("price_usd").dropna(how="all")
    out = out.reset_index()
    out.columns = ["timestamp", "price_usd"]
    logger.debug("load_prices %s: %d rows from %s to %s at %s", symbol, len(out), start, end, freq)
    return out
