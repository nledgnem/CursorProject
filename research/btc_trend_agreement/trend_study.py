"""
Core TrendScore construction and conditional forward-return tables.

SIGNAL DEFINITION (verbatim from Coinbase's description)
--------------------------------------------------------
    R30(t)  = P(t)/P(t-30)  - 1
    R90(t)  = P(t)/P(t-90)  - 1
    R365(t) = P(t)/P(t-365) - 1
    TrendScore(t) = 1[R30>0] + 1[R90>0] + 1[R365>0]     in {0,1,2,3}

Because every price series is reindexed onto a complete daily calendar
(see data_io), `shift(k)` is exactly k calendar days.

TrendScore(t) uses only closes up to and including 23:59:59 UTC on date t.

Forward returns for the descriptive study are
    F_h(t) = P(t+h)/P(t) - 1
which begins at the signal timestamp. This is the standard event-study
convention Coinbase is reporting and contains no look-ahead in the signal --
but note that it is NOT tradable as stated, because you cannot transact at the
instant of the close you used to compute the signal. The strategy section
applies an explicit execution lag on top.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from config import CORE_LOOKBACKS, FORWARD_HORIZONS


def trailing_returns(price: pd.Series, lookbacks=CORE_LOOKBACKS) -> pd.DataFrame:
    out = {}
    for lb in lookbacks:
        out[f"R{lb}"] = price / price.shift(lb) - 1.0
    return pd.DataFrame(out, index=price.index)


def trend_score(price: pd.Series, lookbacks=CORE_LOOKBACKS) -> pd.Series:
    tr = trailing_returns(price, lookbacks)
    valid = tr.notna().all(axis=1)
    score = (tr > 0).sum(axis=1).astype(float)
    score[~valid] = np.nan
    return score.rename("trend_score")


def forward_returns(price: pd.Series, horizons=FORWARD_HORIZONS) -> pd.DataFrame:
    return pd.DataFrame(
        {f"fwd_{h}": price.shift(-h) / price - 1.0 for h in horizons},
        index=price.index,
    )


def conditional_table(score: pd.Series, fwd: pd.DataFrame) -> pd.DataFrame:
    """Per-(score, horizon) descriptive statistics plus excess vs unconditional.

    The unconditional benchmark is computed on exactly the same date set the
    conditional statistic uses (i.e. rows where the score is defined and the
    forward return exists), so the excess figure is not contaminated by the
    pre-365-day warm-up period.
    """
    rows = []
    for col in fwd.columns:
        h = int(col.split("_")[1])
        df = pd.concat([score, fwd[col].rename("fwd")], axis=1).dropna()
        uncond = df["fwd"].mean()
        for k in (0, 1, 2, 3):
            g = df.loc[df["trend_score"] == k, "fwd"]
            rows.append({
                "horizon_days": h,
                "trend_score": k,
                "n_obs": int(len(g)),
                "pct_of_sample": float(len(g) / len(df) * 100) if len(df) else np.nan,
                "mean": float(g.mean()) if len(g) else np.nan,
                "median": float(g.median()) if len(g) else np.nan,
                "std": float(g.std(ddof=1)) if len(g) > 1 else np.nan,
                "win_rate": float((g > 0).mean()) if len(g) else np.nan,
                "p25": float(g.quantile(0.25)) if len(g) else np.nan,
                "p75": float(g.quantile(0.75)) if len(g) else np.nan,
                "unconditional_mean": float(uncond),
                "mean_excess_vs_uncond": float(g.mean() - uncond) if len(g) else np.nan,
            })
    return pd.DataFrame(rows)


def build_asset_frame(price: pd.Series, lookbacks=CORE_LOOKBACKS,
                      horizons=FORWARD_HORIZONS) -> pd.DataFrame:
    """One tidy frame: price, trailing returns, score, all forward returns."""
    df = pd.concat(
        [price.rename("close"), trailing_returns(price, lookbacks),
         trend_score(price, lookbacks), forward_returns(price, horizons)],
        axis=1,
    )
    return df


# --------------------------------------------------------------------------
# Parameter-robustness diagnostics
# --------------------------------------------------------------------------
def parameter_diagnostics(price: pd.Series, short: int, medium: int, long: int,
                          horizon: int, start: str | None = None,
                          end: str | None = None) -> dict:
    """Three simple quality diagnostics for one (short, medium, long) triple."""
    from stats_tools import spearman_score_vs_fwd

    sc = trend_score(price, (short, medium, long))
    fw = price.shift(-horizon) / price - 1.0
    df = pd.concat([sc, fw.rename("fwd")], axis=1).dropna()
    if start:
        df = df[df.index >= start]
    if end:
        df = df[df.index <= end]
    if len(df) < 200:
        return {}

    means = df.groupby("trend_score")["fwd"].mean()
    means = means.reindex([0, 1, 2, 3])
    present = means.dropna()
    rho, p = spearman_score_vs_fwd(df["trend_score"], df["fwd"])

    n_up = int((present.diff().dropna() > 0).sum())
    n_steps = int(len(present) - 1)

    return {
        "short": short, "medium": medium, "long": long,
        "n_obs": int(len(df)),
        "score0_mean": float(means.get(0, np.nan)) if 0 in means.index else np.nan,
        "score1_mean": float(means.get(1, np.nan)),
        "score2_mean": float(means.get(2, np.nan)),
        "score3_mean": float(means.get(3, np.nan)),
        "spread_3_0": float(means.get(3, np.nan) - means.get(0, np.nan)),
        "monotone_steps": n_up,
        "monotone_steps_possible": n_steps,
        "strictly_monotone": bool(n_steps > 0 and n_up == n_steps),
        "spearman_rho": rho,
        "spearman_p": p,
    }
