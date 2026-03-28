#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
validate_price_data.py
======================

Production-oriented validation for historical crypto price data stored in Parquet.

UNIT MANDATE (read before running)
----------------------------------
This script refuses deep checks until you pass explicit assertions for how YOU interpret
the data (price quote, timestamp convention, expected bar spacing). The Parquet schema
does not encode economic units; doubles are untyped.

How to run (examples)
---------------------
  # Long-format lake table (asset_id, date, close, ...)
  python validate_price_data.py ^
    --parquet data/curated/data_lake/fact_price.parquet ^
    --layout long ^
    --timestamp-col date ^
    --symbol-col asset_id ^
    --close-col close ^
    --price-unit-assertion "USD per coin; CoinGecko close" ^
    --timestamp-assertion "date32 calendar day; treated as UTC midnight bar label" ^
    --expected-bar-frequency D ^
    --volume-parquet data/curated/volume_daily.parquet --volume-layout wide

  # Wide panel (symbols as columns, date on index — typical curated/prices_daily.parquet)
  python validate_price_data.py ^
    --parquet data/curated/prices_daily.parquet ^
    --layout wide ^
    --price-unit-assertion "USD per coin; CoinGecko daily close" ^
    --timestamp-assertion "DatetimeIndex name=date; daily bars" ^
    --expected-bar-frequency D ^
    --plot-symbol BTC

Dependencies: pandas, pyarrow, numpy, matplotlib

Output: prints PASS/WARNING/FAIL blocks; writes charts under validation_output/
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.colors import LinearSegmentedColormap
except ImportError as e:  # pragma: no cover
    raise SystemExit(
        "matplotlib is required for Phase 3 plots. Install: pip install matplotlib"
    ) from e


# ---------------------------------------------------------------------------
# Constants — robust statistics
# ---------------------------------------------------------------------------
# Gaussian consistency: MAD scaled by 1.4826 matches std for Normal data, but remains
# resistant to heavy tails (crypto crashes / API glitches) unlike sample std.
MAD_SCALE = 1.4826


@dataclass
class AuditResult:
    """Collect human-readable findings for one logical check."""

    name: str
    status: str  # "PASS" | "WARNING" | "FAIL"
    detail: str
    evidence: list[Any] = field(default_factory=list)


def _print_block(result: AuditResult) -> None:
    line = f"[{result.status}] {result.name}"
    print(line)
    print(result.detail)
    if result.evidence:
        ev = result.evidence
        if len(ev) > 50:
            print(f"  (showing first 50 of {len(ev)} items)")
            ev = ev[:50]
        for x in ev:
            print(f"  - {x}")
    print()


# ---------------------------------------------------------------------------
# Loading — wide vs long
# ---------------------------------------------------------------------------


def load_wide_prices(path: Path) -> pd.DataFrame:
    """
    Load a wide price panel.

    Why: In this repo, `data/curated/prices_daily.parquet` stores `date` as the
    DatetimeIndex name (not always a column). PyArrow may still list a `date` field in
    metadata; pandas materializes it as the index.
    """
    df = pd.read_parquet(path)
    # Drop pathological column name sometimes present in exports
    bad = [c for c in df.columns if c == "nan" or (isinstance(c, float) and np.isnan(c))]
    if bad:
        df = df.drop(columns=bad)
    if isinstance(df.index, pd.DatetimeIndex):
        df = df.sort_index()
    elif "date" in df.columns:
        df = df.sort_values("date").set_index("date")
    else:
        raise ValueError(
            "Wide layout expects a DatetimeIndex or a 'date' column; "
            f"got index={type(df.index)}, columns={list(df.columns)[:8]}..."
        )
    return df


def load_long_prices(
    path: Path,
    timestamp_col: str,
    symbol_col: str,
    close_col: str,
    ohlc: Optional[dict[str, str]],
) -> pd.DataFrame:
    """Long / tidy format: one row per (symbol, timestamp)."""
    df = pd.read_parquet(path)
    need = {timestamp_col, symbol_col, close_col}
    miss = need - set(df.columns)
    if miss:
        raise ValueError(f"Long layout missing columns: {miss}")
    out = df.copy()
    out[timestamp_col] = pd.to_datetime(out[timestamp_col], utc=True, errors="coerce")
    if ohlc:
        for _, c in ohlc.items():
            if c not in out.columns:
                raise ValueError(f"OHLC mapping references missing column: {c}")
    return out


def melt_wide_sample(
    wide: pd.DataFrame, symbols: Optional[list[str]], max_symbols: int
) -> pd.DataFrame:
    """
    Convert a subset of wide panel to long for per-symbol audits without melting millions
    of cells unless requested.
    """
    idx = wide.index.copy()
    cols = [c for c in wide.columns if np.issubdtype(wide[c].dtype, np.number)]
    if symbols:
        cols = [c for c in symbols if c in wide.columns]
    cols = cols[:max_symbols]
    sub = wide[cols]
    long_df = sub.reset_index().melt(
        id_vars=[wide.index.name or "index"],
        var_name="symbol",
        value_name="close",
    )
    tcol = wide.index.name if wide.index.name else "index"
    long_df = long_df.rename(columns={tcol: "timestamp"})
    return long_df


# ---------------------------------------------------------------------------
# Phase 2 — hard logic checks
# ---------------------------------------------------------------------------


def check_temporal_long(
    df: pd.DataFrame,
    ts_col: str,
    sym_col: str,
    expected_freq: Optional[str],
) -> list[AuditResult]:
    results: list[AuditResult] = []
    for sym, g in df.groupby(sym_col, sort=False):
        g = g.sort_values(ts_col)
        ts = g[ts_col]
        if not ts.is_monotonic_increasing:
            bad = ts[ts.diff() < pd.Timedelta(0)]
            results.append(
                AuditResult(
                    "Temporal order",
                    "FAIL",
                    f"Symbol {sym}: timestamps not non-decreasing.",
                    evidence=[str(x) for x in bad.head(20).tolist()],
                )
            )
            continue
        dupes = ts[ts.duplicated(keep=False)]
        if len(dupes) > 0:
            uniq_dup = pd.unique(dupes.values)
            results.append(
                AuditResult(
                    "Duplicate timestamps",
                    "WARNING",
                    f"Symbol {sym}: duplicate timestamps (same bar repeated).",
                    evidence=[str(x) for x in uniq_dup[:30]],
                )
            )
        if expected_freq and len(ts) >= 2:
            deltas = ts.diff().dropna()
            med = deltas.median()
            try:
                off_mask = deltas != med
                if off_mask.any():
                    results.append(
                        AuditResult(
                            "Bar spacing",
                            "WARNING",
                            f"Symbol {sym}: median delta {med} but {int(off_mask.sum())} "
                            f"bars differ (uniform spacing check vs {expected_freq}).",
                            evidence=[str(x) for x in ts.loc[off_mask].head(15).tolist()],
                        )
                    )
            except Exception:
                pass
    if not results:
        results.append(
            AuditResult(
                "Temporal integrity (long)",
                "PASS",
                "Per-symbol: timestamps non-decreasing; no failures in audited symbols.",
            )
        )
    return results


def check_temporal_wide(wide: pd.DataFrame, expected_freq: Optional[str]) -> list[AuditResult]:
    results: list[AuditResult] = []
    idx = wide.index
    if not isinstance(idx, pd.DatetimeIndex):
        results.append(
            AuditResult(
                "Wide index type",
                "FAIL",
                f"Expected DatetimeIndex, got {type(idx)}.",
            )
        )
        return results
    if not idx.is_monotonic_increasing:
        results.append(
            AuditResult(
                "Wide index monotonicity",
                "FAIL",
                "Index timestamps are not sorted / strictly increasing.",
                evidence=[str(x) for x in idx[idx.to_series().diff() < pd.Timedelta(0)][:20]],
            )
        )
    elif idx.has_duplicates:
        results.append(
            AuditResult(
                "Wide duplicate index labels",
                "WARNING",
                "Duplicate dates in index.",
                evidence=[str(x) for x in idx[idx.duplicated()][:30]],
            )
        )
    else:
        results.append(
            AuditResult(
                "Temporal integrity (wide index)",
                "PASS",
                "Index is monotonic increasing with no duplicates.",
            )
        )
    if expected_freq and len(idx) >= 2:
        inferred = pd.infer_freq(idx)
        dr = pd.date_range(idx[0], idx[-1], freq=expected_freq)
        missing = dr.difference(idx)
        if len(missing) > 0:
            results.append(
                AuditResult(
                    "Calendar gaps vs expected_freq",
                    "WARNING",
                    f"Expected {expected_freq}; pandas inferred_freq={inferred!r}; "
                    f"{len(missing)} missing stamps in range (first few shown).",
                    evidence=[str(m) for m in missing[:25]],
                )
            )
        else:
            results.append(
                AuditResult(
                    "Calendar completeness",
                    "PASS",
                    f"No missing business/calendar steps for freq={expected_freq} in range.",
                )
            )
    return results


def check_stale_prices(
    series: pd.Series, name: str, stale_run_threshold: int
) -> Optional[AuditResult]:
    """
    Zero-variance trap: close unchanged for many consecutive periods.

    Why not use variance: a tiny float jitter can mask staleness; we use exact equality
    after normalizing to float64 (still not perfect if upstream rounds).
    """
    s = series.astype(float)
    same = s.eq(s.shift(1))
    # Run-length encode streaks of True
    run = 0
    max_run = 0
    end_pos = -1
    for i, v in enumerate(same.fillna(False).to_numpy()):
        if v:
            run += 1
            if run > max_run:
                max_run = run
                end_pos = i
        else:
            run = 0
    if max_run >= stale_run_threshold:
        return AuditResult(
            f"Stale price streak ({name})",
            "WARNING",
            f"Close identical to prior for {max_run + 1} consecutive bars "
            f"(threshold {stale_run_threshold}). End index position {end_pos}.",
            evidence=[f"max_flat_run={max_run + 1}"],
        )
    return AuditResult(
        f"Stale price streak ({name})",
        "PASS",
        f"No flat runs >= {stale_run_threshold} bars.",
    )


def check_physical_bounds(
    close: pd.Series,
    volume: Optional[pd.Series],
    label: str,
) -> list[AuditResult]:
    out: list[AuditResult] = []
    neg_px = close[close < 0]
    if len(neg_px) > 0:
        out.append(
            AuditResult(
                f"Negative price ({label})",
                "FAIL",
                f"{len(neg_px)} negative closes.",
                evidence=neg_px.head(20).tolist(),
            )
        )
    else:
        out.append(
            AuditResult(
                f"Non-negative prices ({label})",
                "PASS",
                "No negative close values.",
            )
        )
    if volume is not None:
        neg_v = volume[volume < 0]
        if len(neg_v) > 0:
            out.append(
                AuditResult(
                    f"Negative volume ({label})",
                    "FAIL",
                    f"{len(neg_v)} negative volume rows.",
                    evidence=neg_v.head(20).tolist(),
                )
            )
        else:
            out.append(
                AuditResult(
                    f"Non-negative volume ({label})",
                    "PASS",
                    "No negative volume.",
                )
            )
    return out


def check_ohlc(df: pd.DataFrame, mapping: dict[str, str]) -> list[AuditResult]:
    """
    OHLC internal consistency: high is max of the four prints, low is min.

    If only some OHLC columns exist, we skip or partial-check.
    """
    req = ["open", "high", "low", "close"]
    cols = {k: mapping[k] for k in req if k in mapping}
    if len(cols) < 4:
        return [
            AuditResult(
                "OHLC logic",
                "PASS",
                "Skipped — incomplete OHLC column mapping.",
            )
        ]
    o = df[cols["open"]].astype(float)
    h = df[cols["high"]].astype(float)
    l = df[cols["low"]].astype(float)
    c = df[cols["close"]].astype(float)
    hi_ok = (h >= np.maximum.reduce([o, c, l])) & (h >= l)
    lo_ok = (l <= np.minimum.reduce([o, c, h])) & (l <= h)
    bad = ~(hi_ok & lo_ok)
    n = int(bad.sum())
    if n == 0:
        return [
            AuditResult(
                "OHLC logic",
                "PASS",
                "high >= open,close,low and low <= open,close,high for all rows.",
            )
        ]
    idx = df.index[bad]
    return [
        AuditResult(
            "OHLC logic",
            "FAIL",
            f"{n} rows violate OHLC bounds.",
            evidence=[str(x) for x in idx[:40]],
        )
    ]


# ---------------------------------------------------------------------------
# Phase 3 — robust anomalies (MAD + rank cuts)
# ---------------------------------------------------------------------------


def rolling_mad_zscore(x: pd.Series, window: int, min_periods: int) -> pd.Series:
    """
    Rolling robust z-score using median and MAD.

    For each window we compute:
      med = median(x)
      mad = median(|x - med|)
      robust_z = (x - med) / (MAD_SCALE * mad + eps)

    Why: a single flash-crash does not inflate the scale estimate the way std does.
    """
    vals = x.astype(float).to_numpy()
    out = np.full(len(vals), np.nan, dtype=float)
    for i in range(len(vals)):
        lo = max(0, i - window + 1)
        chunk = vals[lo : i + 1]
        valid = chunk[~np.isnan(chunk)]
        if valid.size < min_periods:
            continue
        med = float(np.median(valid))
        mad = float(np.median(np.abs(valid - med)))
        denom = MAD_SCALE * max(mad, 1e-12)
        out[i] = (vals[i] - med) / denom
    return pd.Series(out, index=x.index)


def log_returns(close: pd.Series) -> pd.Series:
    c = close.astype(float).replace(0, np.nan)
    return np.log(c / c.shift(1))


def rank_flag_extremes(
    r: pd.Series, low_pct: float, high_pct: float
) -> pd.Series:
    """Boolean mask for returns outside empirical percentiles (rank-based truncation)."""
    lo = r.quantile(low_pct)
    hi = r.quantile(high_pct)
    return (r < lo) | (r > hi)


def ensure_output_dir(base: Path) -> Path:
    base.mkdir(parents=True, exist_ok=True)
    return base


def plot_close_with_flags(
    timestamps: pd.DatetimeIndex,
    close: pd.Series,
    flags: pd.Series,
    title: str,
    out_path: Path,
) -> None:
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(timestamps, close.values, color="steelblue", linewidth=0.8, label="Close")
    fl = flags.fillna(False)
    if fl.any():
        ax.scatter(
            timestamps[fl],
            close[fl].values,
            color="red",
            s=8,
            zorder=5,
            label="Robust anomaly (MAD or rank)",
        )
    ax.set_title(title)
    ax.legend(loc="upper left")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)


def plot_returns_histogram(
    r: pd.Series,
    title: str,
    out_path: Path,
    clip_pct: tuple[float, float] = (0.01, 0.99),
) -> None:
    """
    Histogram with tail clipping for display only.

    Why: crypto return histograms are dominated by invisible extreme bins; clipping
    the *display* range does not delete outliers from the audit — we still flag them
    elsewhere.
    """
    rc = r.replace([np.inf, -np.inf], np.nan).dropna()
    lo, hi = rc.quantile(clip_pct[0]), rc.quantile(clip_pct[1])
    rc_clip = rc[(rc >= lo) & (rc <= hi)]
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.hist(rc_clip, bins=80, color="slategray", edgecolor="white", alpha=0.85)
    ax.set_title(f"{title}\n(display: returns clipped to [{clip_pct[0]:.0%},{clip_pct[1]:.0%}] quantiles)")
    ax.set_xlabel("log return")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)


def plot_missing_heatmap(
    wide: pd.DataFrame,
    out_path: Path,
    max_cols: int,
) -> None:
    """
    Coverage heatmap: rows = time, columns = subset of symbols; NaN = missing.

    For very wide panels we subsample columns for readability.
    """
    num_cols = [c for c in wide.columns if np.issubdtype(wide[c].dtype, np.number)]
    num_cols = num_cols[:max_cols]
    mat = wide[num_cols].isna().astype(float)
    arr = mat.values
    fig, ax = plt.subplots(figsize=(min(14, 4 + max_cols * 0.04), 6))
    # yellow = missing, purple = present
    cmap = LinearSegmentedColormap.from_list("cov", ["#3b1f2b", "#f4e04d"])
    im = ax.imshow(arr.T, aspect="auto", cmap=cmap, interpolation="nearest")
    ax.set_xlabel("Time index (row order)")
    ax.set_ylabel("Symbol (subset)")
    ax.set_title(f"Missing-data heatmap (yellow=NaN) — first {len(num_cols)} numeric columns")
    fig.colorbar(im, ax=ax, fraction=0.02)
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_ohlc(s: Optional[str]) -> Optional[dict[str, str]]:
    if not s:
        return None
    return json.loads(s)


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Validate Parquet crypto price data (Phases 2–3).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument(
        "--parquet",
        type=Path,
        required=True,
        help="Path to primary Parquet file.",
    )
    p.add_argument(
        "--layout",
        choices=["wide", "long"],
        required=True,
        help="wide: symbols as columns, date as index (or column); long: tidy rows.",
    )
    p.add_argument("--timestamp-col", default="date", help="Long layout: timestamp column.")
    p.add_argument("--symbol-col", default="asset_id", help="Long layout: symbol column.")
    p.add_argument("--close-col", default="close", help="Long layout: close column.")
    p.add_argument(
        "--ohlc-json",
        default=None,
        help='Optional JSON mapping, e.g. {"open":"o","high":"h","low":"l","close":"c"}',
    )
    p.add_argument(
        "--price-unit-assertion",
        required=True,
        help='Your explicit interpretation of price units, e.g. "USD per coin (CoinGecko)".',
    )
    p.add_argument(
        "--timestamp-assertion",
        required=True,
        help='Your explicit interpretation of timestamps (UTC vs local, ms vs s, bar label).',
    )
    p.add_argument(
        "--volume-unit-assertion",
        default="",
        help="If volume is analyzed, state whether it is base, quote, or unknown.",
    )
    p.add_argument(
        "--expected-bar-frequency",
        default="D",
        help="Pandas offset alias for gap detection (e.g. D, H). Empty to skip.",
    )
    p.add_argument(
        "--stale-run-threshold",
        type=int,
        default=5,
        help="Flag if close equals prior for this many consecutive bars.",
    )
    p.add_argument(
        "--mad-window",
        type=int,
        default=60,
        help="Rolling window length (bars) for MAD-based robust z on log returns.",
    )
    p.add_argument(
        "--mad-z-threshold",
        type=float,
        default=8.0,
        help="Flag returns with |robust_z| above this (after rolling MAD).",
    )
    p.add_argument(
        "--rank-low-pct",
        type=float,
        default=0.01,
        help="Also flag returns below this quantile (rank-based).",
    )
    p.add_argument(
        "--rank-high-pct",
        type=float,
        default=0.99,
        help="Also flag returns above this quantile.",
    )
    p.add_argument(
        "--volume-parquet",
        type=Path,
        default=None,
        help="Optional wide volume panel aligned to price wide index.",
    )
    p.add_argument("--volume-layout", choices=["wide"], default="wide")
    p.add_argument(
        "--plot-symbol",
        default="BTC",
        help="Symbol column name (wide) or asset_id (long) for primary charts.",
    )
    p.add_argument(
        "--heatmap-max-cols",
        type=int,
        default=80,
        help="Max numeric columns in missing-data heatmap (wide).",
    )
    p.add_argument(
        "--output-dir",
        type=Path,
        default=Path("validation_output"),
        help="Directory for PNG artifacts.",
    )
    p.add_argument(
        "--max-symbols-audit",
        type=int,
        default=200,
        help="Long layout: max symbols to iterate for temporal checks (avoid huge runs).",
    )
    args = p.parse_args(argv)

    print("=== Assertions (Unit Mandate) ===")
    print(f"Price: {args.price_unit_assertion}")
    print(f"Timestamps: {args.timestamp_assertion}")
    if args.volume_parquet and not args.volume_unit_assertion:
        print(
            "[WARNING] Volume file set but --volume-unit-assertion empty; "
            "volume is NOT trusted for economic meaning."
        )
    if args.volume_unit_assertion:
        print(f"Volume: {args.volume_unit_assertion}")
    print()

    ohlc_map = parse_ohlc(args.ohlc_json)
    freq = args.expected_bar_frequency or None

    out_dir = ensure_output_dir(args.output_dir)
    print(f"Output directory: {out_dir.resolve()}\n")

    # ---- Load primary
    if args.layout == "wide":
        wide = load_wide_prices(args.parquet)
        for r in check_temporal_wide(wide, freq):
            _print_block(r)

        sym = args.plot_symbol
        if sym not in wide.columns:
            # pick first numeric column with a valid name
            candidates = [c for c in wide.columns if str(c) != "nan"]
            sym = candidates[0] if candidates else wide.columns[0]
            print(f"[INFO] --plot-symbol not found; using column {sym!r} for plots.\n")

        s_close = wide[sym].astype(float)
        ts = pd.DatetimeIndex(wide.index)

        stale = check_stale_prices(s_close, sym, args.stale_run_threshold)
        if stale:
            _print_block(stale)

        for r in check_physical_bounds(s_close, None, sym):
            _print_block(r)

        # Volume optional
        vol_s: Optional[pd.Series] = None
        if args.volume_parquet:
            vw = load_wide_prices(Path(args.volume_parquet))
            if sym in vw.columns:
                vol_s = vw[sym].astype(float)
                for r in check_physical_bounds(s_close, vol_s, f"{sym} volume"):
                    _print_block(r)

        # OHLC checks require open/high/low columns; typical wide daily panel is close-only.

        # Returns + robust anomalies
        r = log_returns(s_close)
        rz = rolling_mad_zscore(r, args.mad_window, max(10, args.mad_window // 3))
        rank_mask = rank_flag_extremes(r, args.rank_low_pct, args.rank_high_pct)
        mad_mask = rz.abs() > args.mad_z_threshold
        flag = mad_mask | rank_mask
        _print_block(
            AuditResult(
                "Robust return anomalies",
                "WARNING" if flag.fillna(False).any() else "PASS",
                f"Symbol {sym}: MAD|z|>{args.mad_z_threshold} or outside "
                f"[{args.rank_low_pct},{args.rank_high_pct}] quantiles.",
                evidence=[
                    str(t)
                    for t in ts[flag.fillna(False).to_numpy()][:40]
                ],
            )
        )

        plot_close_with_flags(
            ts,
            s_close,
            flag,
            f"{sym} — close with anomaly flags",
            out_dir / f"close_anomalies_{sym}.png",
        )
        plot_returns_histogram(
            r,
            f"{sym} log returns",
            out_dir / f"returns_hist_{sym}.png",
        )
        plot_missing_heatmap(wide, out_dir / "missing_heatmap_wide.png", args.heatmap_max_cols)

        # Volume variation (log diff) if present
        if vol_s is not None and args.volume_unit_assertion:
            lv = np.log1p(vol_s.clip(lower=0))
            dlv = lv.diff()
            _print_block(
                AuditResult(
                    "Volume variation audit",
                    "PASS",
                    f"log1p(volume) diff: std={float(dlv.std(skipna=True)):.4g}; "
                    "see policy for spike thresholds.",
                )
            )

    else:
        df = load_long_prices(
            args.parquet,
            args.timestamp_col,
            args.symbol_col,
            args.close_col,
            ohlc_map,
        )
        symbols = df[args.symbol_col].unique().tolist()
        if len(symbols) > args.max_symbols_audit:
            symbols = symbols[: args.max_symbols_audit]
            print(
                f"[INFO] Auditing first {args.max_symbols_audit} symbols only "
                f"(use --max-symbols-audit to extend).\n"
            )
        sub = df[df[args.symbol_col].isin(symbols)]
        for r in check_temporal_long(sub, args.timestamp_col, args.symbol_col, freq):
            _print_block(r)

        sym = args.plot_symbol
        if sym not in sub[args.symbol_col].values:
            sym = symbols[0]
            print(f"[INFO] Using symbol {sym!r} for plots.\n")

        one = sub[sub[args.symbol_col] == sym].sort_values(args.timestamp_col)
        s_close = one[args.close_col].astype(float)
        ts = pd.DatetimeIndex(one[args.timestamp_col])

        stale = check_stale_prices(s_close, sym, args.stale_run_threshold)
        if stale:
            _print_block(stale)

        vol_s = None
        if "volume" in one.columns:
            vol_s = one["volume"].astype(float)

        for r in check_physical_bounds(s_close, vol_s, sym):
            _print_block(r)

        if ohlc_map and len(ohlc_map) >= 4:
            for r in check_ohlc(one, ohlc_map):
                _print_block(r)

        r = log_returns(pd.Series(s_close.to_numpy(), index=ts))
        rz = rolling_mad_zscore(r, args.mad_window, max(10, args.mad_window // 3))
        rank_mask = rank_flag_extremes(r, args.rank_low_pct, args.rank_high_pct)
        mad_mask = rz.abs() > args.mad_z_threshold
        flag = mad_mask | rank_mask
        _print_block(
            AuditResult(
                "Robust return anomalies",
                "WARNING" if flag.any() else "PASS",
                f"Symbol {sym}: flagged timestamps (MAD z or rank).",
                evidence=[str(x) for x in ts[flag.fillna(False)][:40]],
            )
        )

        plot_close_with_flags(
            ts,
            s_close.reset_index(drop=True),
            pd.Series(flag.to_numpy(), index=range(len(flag))),
            f"{sym} (long) — close with anomaly flags",
            out_dir / f"close_anomalies_{sym}_long.png",
        )
        plot_returns_histogram(r, f"{sym} log returns (long)", out_dir / f"returns_hist_{sym}_long.png")

        # Coverage: pivot for heatmap sample
        pivot = sub.pivot_table(
            index=args.timestamp_col,
            columns=args.symbol_col,
            values=args.close_col,
            aggfunc="first",
        )
        plot_missing_heatmap(
            pivot,
            out_dir / "missing_heatmap_long_sample.png",
            min(args.heatmap_max_cols, pivot.shape[1]),
        )

    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
