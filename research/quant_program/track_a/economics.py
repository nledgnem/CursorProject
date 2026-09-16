"""Track A path-dependency economics: the price of waiting vs the price of false early entry.

For every Day-1 event and every alternative rule we measure the INCREMENTAL P&L of the
rule versus the current 3-close rule over the event window (from the first qualifying
close until both rules hold the same exposure again after close #3, capped at 30 days).
This uses the full daily P&L engine, so it already includes fees, slippage, the gap to
the next open and (optionally) perp funding.

  confirmed events -> positive incremental P&L = the cost of WAITING
  failed events    -> negative incremental P&L = the false-entry LOSS that waiting AVOIDED
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from qlib import stats

MAX_WINDOW = 30


def incremental_pnl(obs: pd.DataFrame, bt_rule: pd.DataFrame, bt_base: pd.DataFrame, w_rule: pd.Series,
                    w_base: pd.Series, K: int = 3) -> pd.Series:
    idx = bt_base.index
    pos = {d: i for i, d in enumerate(idx)}
    dr = (bt_rule["ret"] - bt_base["ret"]).to_numpy()
    wr, wb = w_rule.ffill().fillna(0).to_numpy(), w_base.ffill().fillna(0).to_numpy()
    out = np.full(len(obs), np.nan)
    for j, t in enumerate(obs["ts"]):
        i0 = pos.get(t)
        if i0 is None or i0 + 2 >= len(idx):
            continue
        acc, i = 0.0, i0
        while i < min(i0 + MAX_WINDOW, len(idx) - 1):
            # exposure decided at close i affects returns on day i+1 (and the gap into it)
            acc += dr[i + 1]
            i += 1
            if i > i0 + K and abs(wr[i] - wb[i]) < 1e-9 and abs(wr[i - 1] - wb[i - 1]) < 1e-9:
                break
        out[j] = acc
    return pd.Series(out, index=obs.index)


def summarize(obs: pd.DataFrame, col: str) -> dict:
    conf, fail = obs[obs["confirmed"] == 1], obs[obs["confirmed"] == 0]
    g, l = conf[col].mean(), -fail[col].mean()
    p = obs["confirmed"].mean()
    ev = p * g - (1 - p) * l
    n_years = max((obs["ts"].max() - obs["ts"].min()).days / 365.25, 1e-9)
    ci = stats.cluster_bootstrap_ci(obs[col], obs["cluster"], np.mean, 1000) if "cluster" in obs else (np.nan,) * 3
    return {
        "n_events": len(obs), "p_confirm": p,
        "cost_of_waiting_per_confirmed": g, "loss_avoided_per_failed": l,
        "expected_value_per_signal": ev, "ev_ci_lo": ci[1], "ev_ci_hi": ci[2],
        "breakeven_p_confirm": l / (g + l) if (g + l) > 0 else np.nan,
        "total_cost_of_waiting": conf[col].sum(), "total_loss_avoided": -fail[col].sum(),
        "net_per_year": obs[col].sum() / n_years,
        "left_tail_p05": obs[col].quantile(0.05), "worst": obs[col].min(), "best": obs[col].max(),
    }


def by_bucket(obs: pd.DataFrame, col: str, feature: str, bins, labels) -> pd.DataFrame:
    x = obs.assign(bucket=pd.cut(obs[feature], bins, labels=labels))
    rows = []
    for bk, g in x.groupby("bucket", observed=False):
        if len(g) < 3:
            rows.append({"feature": feature, "bucket": str(bk), "n_events": len(g)})
            continue
        rows.append({"feature": feature, "bucket": str(bk), **summarize(g, col)})
    return pd.DataFrame(rows)
