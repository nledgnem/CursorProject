"""Data and backtest integrity checks.

Each check returns CheckResult(name, passed, severity, detail). 'error' severity
means results should not be trusted; 'warn' means inspect before relying on them.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import numpy as np
import pandas as pd


@dataclass
class CheckResult:
    name: str
    passed: bool
    severity: str = "error"
    detail: str = ""


def duplicates(df: pd.DataFrame, keys: list[str], name: str = "duplicate_keys") -> CheckResult:
    n = int(df.duplicated(keys).sum())
    return CheckResult(name, n == 0, "error", f"{n} duplicate rows on {keys}")


def datetime_dtype(df: pd.DataFrame, col: str) -> CheckResult:
    """String dates sort alphabetically ('2024-10' < '2024-9') -> silent misordering."""
    ok = pd.api.types.is_datetime64_any_dtype(df[col])
    return CheckResult("datetime_dtype", ok, "error", f"{col} dtype={df[col].dtype}")


def sorted_within(df: pd.DataFrame, time_col: str, group_col: str | None = None) -> CheckResult:
    if group_col:
        bad = int((~df.groupby(group_col)[time_col].apply(lambda s: s.is_monotonic_increasing)).sum())
    else:
        bad = int(not df[time_col].is_monotonic_increasing)
    return CheckResult("sorted_by_time", bad == 0, "error", f"{bad} groups not time-sorted")


def alphabetical_sort_trap(values: pd.Series) -> CheckResult:
    """Detects a series ordered lexicographically instead of chronologically/numerically."""
    s = pd.Series(values).astype(str)
    lex = list(s) == sorted(s)
    try:
        num = pd.to_numeric(s)
        natural = list(num) == sorted(num)
    except (ValueError, TypeError):
        num = pd.to_datetime(s, errors="coerce")
        natural = num.notna().all() and list(num) == sorted(num)
    trap = lex and not natural
    return CheckResult("alphabetical_sort_trap", not trap, "error",
                       "order is lexicographic, not natural" if trap else "ok")


def missing_bars(df: pd.DataFrame, time_col: str, freq: str, group_col: str | None = None,
                 max_gap_share: float = 0.01) -> CheckResult:
    worst = 0.0
    detail = []
    groups = df.groupby(group_col) if group_col else [("all", df)]
    for g, x in groups:
        t = pd.DatetimeIndex(x[time_col]).sort_values()
        full = pd.date_range(t.min(), t.max(), freq=freq)
        share = 1 - len(t.unique()) / max(len(full), 1)
        if share > max_gap_share:
            detail.append(f"{g}:{share:.1%}")
        worst = max(worst, share)
    return CheckResult("missing_bars", not detail, "warn", f"worst gap share {worst:.2%}; " + ", ".join(detail[:10]))


def stale_values(df: pd.DataFrame, value_col: str, group_col: str | None = None, max_run: int = 3) -> CheckResult:
    """Runs of identical consecutive values (frozen feeds)."""
    worst, bad = 0, []
    groups = df.groupby(group_col) if group_col else [("all", df)]
    for g, x in groups:
        v = x[value_col].to_numpy()
        if len(v) < 2:
            continue
        same = np.r_[False, v[1:] == v[:-1]]
        run = 0
        longest = 0
        for s in same:
            run = run + 1 if s else 0
            longest = max(longest, run)
        worst = max(worst, longest)
        if longest >= max_run:
            bad.append(f"{g}:{longest}")
    return CheckResult("stale_values", not bad, "warn", f"longest repeat run {worst}; " + ", ".join(bad[:10]))


def frozen_tail(df: pd.DataFrame, group_col: str = "asset", volume_col: str = "quote_volume",
                max_run: int = 3) -> CheckResult:
    """Zero-volume, high==low bars at the end of a series = delisted instrument still being served."""
    bad = []
    for g, x in df.sort_values("ts").groupby(group_col):
        frozen = ((x[volume_col] <= 0) & (x["high"] == x["low"])).to_numpy()
        run = 0
        for z in frozen[::-1]:
            if not z:
                break
            run += 1
        if run >= max_run:
            bad.append(f"{g}:{run}")
    return CheckResult("frozen_tail", not bad, "error", f"{len(bad)} series with frozen tails: " + ", ".join(bad[:10]))


def ranking_consistent(scores: pd.Series, ranks: pd.Series, ascending: bool = False) -> CheckResult:
    """Ranks must be a monotone function of scores (catches rank-by-wrong-column bugs)."""
    expected = scores.rank(ascending=ascending, method="min")
    bad = int((expected != ranks).sum())
    return CheckResult("ranking_consistent", bad == 0, "error", f"{bad} rank mismatches")


def universe_changes(df: pd.DataFrame, time_col: str, asset_col: str, max_jump: float = 0.25) -> CheckResult:
    counts = df.groupby(time_col)[asset_col].nunique().sort_index()
    jumps = counts.pct_change().abs()
    big = jumps[jumps > max_jump]
    return CheckResult("universe_changes", big.empty, "warn",
                       f"{len(big)} periods with universe size change >{max_jump:.0%}: "
                       + ", ".join(f"{i}:{counts[i]}" for i in big.index[:10]))


def causal(signal_fn: Callable[[pd.DataFrame], pd.Series], df: pd.DataFrame, probes: int = 5,
           min_history: int = 300, tol: float = 1e-9) -> CheckResult:
    """Look-ahead test: signal at row k computed on df[:k+1] must equal the full-sample value."""
    full = signal_fn(df)
    n = len(df)
    points = np.linspace(min_history, n - 2, probes).astype(int) if n > min_history + 2 else []
    bad = []
    for k in points:
        part = signal_fn(df.iloc[:k + 1])
        a, b = full.iloc[k], part.iloc[-1]
        if not ((pd.isna(a) and pd.isna(b)) or (np.isfinite(a) and np.isfinite(b) and abs(a - b) <= tol * max(1, abs(a)))):
            bad.append(f"row {k}: full={a} truncated={b}")
    return CheckResult("causal_no_lookahead", not bad, "error", "; ".join(bad) or f"{len(points)} probes ok")


def execution_lag(signal: pd.Series, position: pd.Series, min_lag: int = 1) -> CheckResult:
    """Position at t must not depend on signal information from bars later than t-min_lag."""
    s, p = signal.astype(float), position.astype(float)
    lagged = s.shift(min_lag)
    mism_lag = float((p - lagged).abs().fillna(0).sum())
    mism_same = float((p - s).abs().fillna(0).sum())
    same_bar = mism_same < mism_lag and mism_same == 0
    return CheckResult("signal_execution_lag", not same_bar, "error",
                       "position equals same-bar signal (look-ahead)" if same_bar else
                       f"|pos - signal.shift({min_lag})| sum = {mism_lag:.3g}")


def fills_within_range(fill_px: pd.Series, low: pd.Series, high: pd.Series, tol: float = 1e-6) -> CheckResult:
    bad = int(((fill_px < low * (1 - tol)) | (fill_px > high * (1 + tol))).sum())
    return CheckResult("fills_within_bar_range", bad == 0, "error", f"{bad} fills outside [low, high]")


def abnormal_turnover(turnover: pd.Series, max_daily: float = 2.0, max_active_share: float = 0.25,
                      z: float = 6.0) -> CheckResult:
    """Flags turnover that is IMPOSSIBLE or implausible for the strategy, not merely rare.

    * any day above max_daily (e.g. >2.0 for a strategy bounded in [-1, 1]) -> error-level problem;
    * sparse strategies (median turnover 0, e.g. trend flips): flag if trading days exceed max_active_share;
    * dense strategies: robust z-score spikes.
    """
    t = turnover.dropna()
    if len(t) < 30:
        return CheckResult("abnormal_turnover", True, "warn", "too short")
    over = int((t > max_daily + 1e-9).sum())
    if over:
        return CheckResult("abnormal_turnover", False, "error", f"{over} days with turnover > {max_daily}")
    if t.median() == 0:
        share = float((t > 0).mean())
        return CheckResult("abnormal_turnover", share <= max_active_share, "warn",
                           f"sparse strategy: trades on {share:.1%} of days")
    med, mad = t.median(), (t - t.median()).abs().median() or 1e-12
    spikes = t[(t - med) / (1.4826 * mad) > z]
    return CheckResult("abnormal_turnover", len(spikes) <= max(1, int(0.002 * len(t))), "warn",
                       f"{len(spikes)} turnover spikes > {z} robust z")


def position_match(intended: pd.Series, actual: pd.Series, tol: float = 1e-6) -> CheckResult:
    diff = (intended - actual).abs()
    bad = int((diff > tol).sum())
    return CheckResult("position_match", bad == 0, "error", f"{bad} bars with position mismatch; max {diff.max():.3g}")


def summarize(results: list[CheckResult]) -> pd.DataFrame:
    return pd.DataFrame([r.__dict__ for r in results])
