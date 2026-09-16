"""Three-state trend confirmation machine and Day-1 event labelling (Track A).

Signals
-------
side_t in {+1 (long state), 0 (neutral state), -1 (short state)}, NaN during warm-up.
  Gerhard SMA120 : +1 close > SMA else -1   (two-state; never neutral)
  LL (3-state)   : +1 Gold, 0 None/grey, -1 Blue
  LL (2-state)   : Gold/Blue flip the regime, grey closes only reset the count
                   (the interpretation used in research/btc_confirmation_lag)

Confirmation rule (reconstruction of the production rule)
---------------------------------------------------------
Confirmed regime R. A qualifying close has side_t = s != R (for 2-state LL, s != 0).
The count n increments while consecutive closes keep the SAME target s, and resets
when the side changes. At n == K the regime becomes s. Decided at close t, executed
at the next open.

Staged / conditional entry
--------------------------
schedule_fn(t, R, s) -> (f1, f2): fraction of the transition from R to s completed
after qualifying close #1 and #2, decided on close #1 and fixed for the episode.
Close #K completes it. A reset before #K unwinds to R.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

EXPOSURE_MAPS = {
    "long_flat": {1: 1.0, 0: 0.0, -1: 0.0},
    "long_short": {1: 1.0, 0: 0.0, -1: -1.0},
}


def run_machine(side: pd.Series, K: int = 3, schedule_fn=None, exposure: str = "long_flat",
                neutral_flips: bool = True) -> pd.DataFrame:
    """Returns per-day target exposure w (decided at close), regime R, pending target s, count n, flip flag.

    neutral_flips=False gives the 2-state LL interpretation (0 closes reset but never become the regime).
    """
    emap = EXPOSURE_MAPS[exposure]
    sv = side.to_numpy(dtype=float)
    N = len(sv)
    w = np.full(N, np.nan)
    R_arr = np.full(N, np.nan)
    tgt = np.full(N, np.nan)
    n_arr = np.zeros(N, dtype=int)
    flip = np.zeros(N, dtype=int)
    R, s_pend, n, sched = None, None, 0, (0.0, 0.0)
    for t in range(N):
        x = sv[t]
        if np.isnan(x):
            continue
        x = int(x)
        if R is None:
            if x == 0 and not neutral_flips:
                continue
            R = x
        elif x != R and (neutral_flips or x != 0):
            if s_pend == x:
                n += 1
            else:
                s_pend, n = x, 1
                sched = schedule_fn(t, R, x) if schedule_fn is not None else (0.0, 0.0)
            if n >= K:
                R, s_pend, n, flip[t] = x, None, 0, 1
        else:
            s_pend, n, sched = None, 0, (0.0, 0.0)
        if n == 0:
            w[t] = emap[R]
        else:
            frac = sched[0] if n == 1 else sched[1] if n == 2 else 1.0
            w[t] = emap[R] + min(max(frac, 0.0), 1.0) * (emap[s_pend] - emap[R])
        R_arr[t], n_arr[t] = R, n
        tgt[t] = s_pend if s_pend is not None else np.nan
    return pd.DataFrame({"w": w, "R": R_arr, "target": tgt, "n": n_arr, "flip": flip}, index=side.index)


def day1_events(side: pd.Series, base: pd.DataFrame, K: int = 3, valid_horizons=(3, 5, 10, 20),
                exposure: str = "long_flat") -> pd.DataFrame:
    """One row per FIRST qualifying close against the K-close confirmed regime, with outcome labels.

    Labels (all known only AFTER the event; they are outcomes, never features):
      confirmed        : the same target persisted for K consecutive closes
      reversed_early   : count reset before K
      valid_{h}d       : raw side at close t0+h still equals the target
      regime_{h}d      : K-close confirmed regime at close t0+h equals the target
      exposure_change  : emap[target] - emap[R] (sign gives trade direction; 0 = no position change)
    """
    emap = EXPOSURE_MAPS[exposure]
    sv = side.to_numpy(dtype=float)
    n_arr, R_arr, tgt = base["n"].to_numpy(), base["R"].to_numpy(), base["target"].to_numpy()
    N = len(sv)
    idx = side.index
    rows = []
    for t0 in np.where(n_arr == 1)[0]:
        s, R = int(tgt[t0]), int(R_arr[t0])
        k, t = 1, t0
        while t + 1 < N and k < K and sv[t + 1] == s:
            t += 1
            k += 1
        rec = {"ts": idx[t0], "from_state": R, "target": s, "closes_reached": k, "confirmed": int(k >= K),
               "reversed_early": int(k < K and t + 1 < N),
               "exposure_change": emap[s] - emap[R]}
        for h in valid_horizons:
            if t0 + h < N:
                rec[f"valid_{h}d"] = int(sv[t0 + h] == s)
                rec[f"regime_{h}d"] = int(R_arr[t0 + h] == s)
            else:
                rec[f"valid_{h}d"] = np.nan
                rec[f"regime_{h}d"] = np.nan
        rows.append(rec)
    ev = pd.DataFrame(rows)
    if len(ev):
        ev["kind"] = ev.apply(lambda r: {1: "to_long", 0: "to_neutral", -1: "to_short"}[r["target"]], axis=1)
        ev["direction"] = np.sign(ev["exposure_change"]).astype(int)
        # For no-exposure-change events (e.g. short->neutral in long/flat) keep the price direction of the target
        ev.loc[ev["direction"] == 0, "direction"] = np.where(ev.loc[ev["direction"] == 0, "target"] >
                                                              ev.loc[ev["direction"] == 0, "from_state"], 1, -1)
    return ev
