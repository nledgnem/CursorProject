"""Chronological validation: folds, walk-forward selection, parameter stability.

Rules enforced here:
  * folds are calendar-contiguous; training data always ends before test data starts;
  * observations are never shuffled;
  * selection metrics and picks are recorded per fold so instability is visible;
  * plateau (neighbourhood) scores are preferred over single best parameter values.
"""

from __future__ import annotations

from typing import Callable

import numpy as np
import pandas as pd

from qlib import stats


def calendar_folds(index: pd.DatetimeIndex, test_years, anchored: bool = True,
                   train_years: int | None = None, start=None) -> list[dict]:
    """One fold per test year. Anchored: train from start; rolling: last train_years."""
    idx = pd.DatetimeIndex(index)
    start = pd.Timestamp(start) if start is not None else idx.min()
    folds = []
    for y in test_years:
        test_lo, test_hi = pd.Timestamp(f"{y}-01-01"), pd.Timestamp(f"{y}-12-31 23:59:59")
        train_lo = start if anchored or train_years is None else max(start, pd.Timestamp(f"{y - train_years}-01-01"))
        train = (idx >= train_lo) & (idx < test_lo)
        test = (idx >= test_lo) & (idx <= test_hi)
        if train.sum() and test.sum():
            folds.append({"label": str(y), "train": train, "test": test})
    return folds


def train_val_test(index: pd.DatetimeIndex, val_start, test_start, start=None) -> dict:
    idx = pd.DatetimeIndex(index)
    s = pd.Timestamp(start) if start is not None else idx.min()
    v, t = pd.Timestamp(val_start), pd.Timestamp(test_start)
    return {"train": (idx >= s) & (idx < v), "validation": (idx >= v) & (idx < t), "test": idx >= t}


def walk_forward_select(returns: pd.DataFrame, folds: list[dict],
                        metric: Callable = stats.sharpe, min_obs: int = 60) -> tuple[pd.Series, pd.DataFrame]:
    """Pick the best candidate column on each fold's training data; stitch its test returns."""
    stitched, rows = [], []
    for f in folds:
        tr, te = returns.loc[f["train"]], returns.loc[f["test"]]
        scores = tr.apply(lambda s: metric(s) if s.dropna().size >= min_obs else np.nan)
        if scores.dropna().empty:
            continue
        pick = scores.idxmax()
        stitched.append(te[pick])
        rows.append({"fold": f["label"], "pick": pick, "train_metric": scores[pick],
                     "test_metric": metric(te[pick]), "n_candidates": int(scores.notna().sum())})
    oos = pd.concat(stitched) if stitched else pd.Series(dtype=float)
    return oos, pd.DataFrame(rows)


def metric_by_fold(returns: pd.DataFrame, folds: list[dict], metric: Callable = stats.sharpe) -> pd.DataFrame:
    """Rows = candidates, columns = folds (test-period metric) -> stability table."""
    out = {f["label"]: returns.loc[f["test"]].apply(metric) for f in folds}
    return pd.DataFrame(out)


def stability_summary(metric_table: pd.DataFrame, baseline: pd.Series | None = None) -> pd.DataFrame:
    """Per candidate: mean/min across folds, share of folds positive (or beating baseline)."""
    t = metric_table
    res = pd.DataFrame({"mean": t.mean(axis=1), "median": t.median(axis=1), "min": t.min(axis=1),
                        "share_positive": (t > 0).mean(axis=1)})
    if baseline is not None:
        res["share_beat_baseline"] = t.gt(baseline, axis=1).mean(axis=1)
    return res


def plateau_score(values: pd.Series, window: int = 1) -> pd.Series:
    """Neighbourhood mean over an ordered 1-D parameter grid (window = neighbours each side).

    A robust region scores high; an isolated spike is averaged down by its neighbours.
    """
    v = values.sort_index()
    return v.rolling(2 * window + 1, center=True, min_periods=1).mean()


def sensitivity_table(fn: Callable[[float], dict], grid) -> pd.DataFrame:
    """Evaluate fn(param) -> dict of metrics over a grid of parameter values."""
    return pd.DataFrame([{"param": p, **fn(p)} for p in grid])
