"""
Quick spot-check: compare vol/mcap trajectories of ARIA (win) vs DEXE/PIEVERSE (losses)
leading up to the 2026-04-09 Apathy Bleed entry date.

This is NOT a production scanner — just a 5-minute eye-test to see if
"attention leaving" (falling vol/mcap) is a visible signal.

Usage:
    python scripts/apathy_bleed_gate5_spotcheck.py

Outputs (printed to stdout):
    - Resolved asset_ids for ARIA, DEXE, PIEVERSE
    - 30-day vol/mcap series for each
    - Summary stats (level, 7d-mean, 14d-slope, 21d-CV) at as_of = 2026-04-09

Optional: saves a PNG to /tmp/ or current dir if matplotlib is available.
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pandas as pd

# Resolve repo root so we can use data_lake_root()
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from repo_paths import data_lake_root

AS_OF = date(2026, 4, 9)
LOOKBACK_DAYS = 30
TICKERS = ["ARIA", "DEXE", "PIEVERSE"]  # symbols from apathy_bleed_book.csv


def resolve_symbols_to_asset_ids(symbols: list[str], lake: Path) -> dict[str, str]:
    """Best-effort symbol -> coingecko_id resolution via dim_asset."""
    dim = pd.read_parquet(lake / "dim_asset.parquet")
    # Try common column names
    sym_col = next((c for c in dim.columns if c.lower() in ("symbol", "coin_symbol", "ticker")), None)
    id_col = next((c for c in dim.columns if c.lower() in ("asset_id", "coingecko_id", "id")), None)
    if sym_col is None or id_col is None:
        print(f"WARN: couldn't find symbol/id columns in dim_asset. Columns: {list(dim.columns)}")
        return {s: s.lower() for s in symbols}  # fall back to lowercase

    out = {}
    for sym in symbols:
        matches = dim[dim[sym_col].astype(str).str.upper() == sym.upper()]
        if matches.empty:
            print(f"WARN: {sym} not found in dim_asset.{sym_col}")
            out[sym] = sym.lower()
        elif len(matches) > 1:
            # Prefer largest market cap if available, else first
            print(f"NOTE: {sym} resolved to {len(matches)} candidates; using first: {matches.iloc[0][id_col]}")
            out[sym] = matches.iloc[0][id_col]
        else:
            out[sym] = matches.iloc[0][id_col]
    return out


def load_series(asset_id: str, lake: Path) -> pd.DataFrame:
    """Return daily vol/mcap ratio for a single asset_id over the lookback window."""
    vol = pd.read_parquet(lake / "fact_volume.parquet")
    mcap = pd.read_parquet(lake / "fact_marketcap.parquet")

    vol = vol[vol["asset_id"] == asset_id].copy()
    mcap = mcap[mcap["asset_id"] == asset_id].copy()
    vol["date"] = pd.to_datetime(vol["date"]).dt.date
    mcap["date"] = pd.to_datetime(mcap["date"]).dt.date

    window_start = AS_OF - pd.Timedelta(days=LOOKBACK_DAYS - 1)
    vol = vol[(vol["date"] >= window_start.date() if hasattr(window_start, "date") else window_start) & (vol["date"] <= AS_OF)]
    mcap = mcap[(mcap["date"] >= window_start.date() if hasattr(window_start, "date") else window_start) & (mcap["date"] <= AS_OF)]

    df = vol.merge(mcap[["date", "marketcap"]], on="date", how="inner")
    df = df.sort_values("date").reset_index(drop=True)
    if df.empty:
        return df
    df["vol_mcap"] = df["volume"] / df["marketcap"]
    return df[["date", "volume", "marketcap", "vol_mcap"]]


def summary_stats(df: pd.DataFrame) -> dict:
    """Compute level, 7d-mean, 14d-slope, 21d-CV at the last row."""
    if len(df) < 21:
        return {"n_obs": len(df), "note": f"insufficient data (need 21, got {len(df)})"}

    s = df.set_index("date")["vol_mcap"]
    last = s.iloc[-1]
    mean_7d = s.iloc[-7:].mean()

    # 14d slope of the 7d-mean (units: Δratio per day)
    rolling_7d = s.rolling(7, min_periods=5).mean()
    last14 = rolling_7d.iloc[-14:].dropna()
    if len(last14) >= 5:
        x = range(len(last14))
        # OLS slope: cov(x,y) / var(x)
        import statistics
        x_mean = statistics.mean(x)
        y_mean = statistics.mean(last14.values)
        num = sum((xi - x_mean) * (yi - y_mean) for xi, yi in zip(x, last14.values))
        den = sum((xi - x_mean) ** 2 for xi in x)
        slope_14d = num / den if den > 0 else None
    else:
        slope_14d = None

    # 21d CV (stdev/mean) of raw daily ratio
    last21 = s.iloc[-21:]
    cv_21d = last21.std() / last21.mean() if last21.mean() != 0 else None

    return {
        "n_obs": len(df),
        "vol_mcap_last": last,
        "vol_mcap_7d_mean": mean_7d,
        "vol_mcap_14d_slope": slope_14d,
        "vol_mcap_21d_cv": cv_21d,
    }


def main() -> None:
    lake = data_lake_root()
    print(f"Lake: {lake}")
    print(f"As-of: {AS_OF}, lookback: {LOOKBACK_DAYS} days")
    print(f"Tickers: {TICKERS}")
    print()

    resolved = resolve_symbols_to_asset_ids(TICKERS, lake)
    print("Symbol resolution:")
    for sym, aid in resolved.items():
        print(f"  {sym:10s} -> {aid}")
    print()

    series_by_sym = {}
    for sym, aid in resolved.items():
        df = load_series(aid, lake)
        series_by_sym[sym] = df
        stats = summary_stats(df)
        print(f"=== {sym} ({aid}) ===")
        if df.empty:
            print("  NO DATA")
        else:
            print(f"  Date range: {df['date'].min()} to {df['date'].max()} ({len(df)} obs)")
            for k, v in stats.items():
                if isinstance(v, float):
                    print(f"  {k:25s} = {v:.6e}")
                else:
                    print(f"  {k:25s} = {v}")
        print()

    # Print the 30-day series for all three side-by-side
    print("=== 30-day vol/mcap series (side-by-side) ===")
    combined = None
    for sym, df in series_by_sym.items():
        if df.empty:
            continue
        s = df[["date", "vol_mcap"]].rename(columns={"vol_mcap": sym})
        combined = s if combined is None else combined.merge(s, on="date", how="outer")
    if combined is not None:
        combined = combined.sort_values("date").reset_index(drop=True)
        print(combined.to_string(index=False))

    # Save PNG if matplotlib available
    try:
        import matplotlib.pyplot as plt
        fig, ax = plt.subplots(figsize=(11, 5))
        for sym, df in series_by_sym.items():
            if df.empty:
                continue
            ax.plot(df["date"], df["vol_mcap"], marker="o", markersize=3, label=sym)
        ax.set_title(f"vol/mcap trajectories (30d ending {AS_OF})")
        ax.set_ylabel("volume / marketcap (ratio)")
        ax.set_xlabel("date")
        ax.legend()
        ax.grid(True, alpha=0.3)
        fig.autofmt_xdate()
        out_path = Path("apathy_bleed_gate5_spotcheck.png")
        fig.savefig(out_path, dpi=120, bbox_inches="tight")
        print(f"\nSaved plot: {out_path.resolve()}")
    except ImportError:
        print("\n(matplotlib not available — skipping plot)")


if __name__ == "__main__":
    main()
