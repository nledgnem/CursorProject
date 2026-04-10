from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

POSITIONS_FIELDS = ["ticker", "side", "notional_usd", "entry_price", "entry_date"]


def ensure_positions_csv(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.stat().st_size > 0:
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=POSITIONS_FIELDS)
        w.writeheader()


def read_positions_df(path: Path) -> pd.DataFrame:
    ensure_positions_csv(path)
    df = pd.read_csv(path)
    for c in POSITIONS_FIELDS:
        if c not in df.columns:
            raise ValueError(f"Positions CSV missing column {c!r}: {path}")
    # Normalize
    df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
    df["side"] = df["side"].astype(str).str.strip().str.upper()
    return df


def write_positions_df_atomic(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    out = df.copy()
    out = out.loc[:, POSITIONS_FIELDS]
    tmp = path.with_suffix(path.suffix + ".tmp")
    out.to_csv(tmp, index=False)
    tmp.replace(path)


def append_position_row(
    path: Path,
    *,
    ticker: str,
    side: str,
    notional_usd: float,
    entry_price: float | None = None,
    entry_date: str | None = None,
) -> None:
    df = read_positions_df(path)
    row = {
        "ticker": str(ticker).strip().upper(),
        "side": str(side).strip().upper(),
        "notional_usd": float(notional_usd),
        "entry_price": (float(entry_price) if entry_price is not None else ""),
        "entry_date": (str(entry_date).strip() if entry_date else datetime.now(timezone.utc).date().isoformat()),
    }
    df2 = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    write_positions_df_atomic(path, df2)


def remove_positions_by_ticker(path: Path, ticker: str) -> int:
    df = read_positions_df(path)
    t = str(ticker).strip().upper()
    before = len(df)
    df2 = df[df["ticker"].astype(str).str.upper() != t].copy()
    removed = before - len(df2)
    write_positions_df_atomic(path, df2)
    return int(removed)

