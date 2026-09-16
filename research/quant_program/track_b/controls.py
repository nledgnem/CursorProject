"""Matched-control and outlier-concentration diagnostics for Track B signals.

    python -m track_b.controls            (after track_b.run)

Why: several pre-registered signals (OI/price quadrants, liquidation quadrants) fire mostly on days with a
large same-direction price move. A positive event return then says nothing about OI or liquidations unless it
beats a *price-only* control conditioned on the same move size. Controls:

  U1_control_price_up_1sd_long    every asset-day with ret_z >= +1 (long)
  U2_control_price_down_1sd_long  every asset-day with ret_z <= -1 (long)

Method (revised 2026-09-15 after external review)
-------------------------------------------------
* Comparisons use GROSS economic returns (BTC-hedged price + funding), never net returns. A short benchmark is
  the negated long gross return; costs are a property of the traded asset, not of the information, and are
  identical on both sides of the comparison, so excess = signal gross - direction * control gross.
  (v1 negated the control's NET return, which turned its transaction cost into a profit for short signals.)
* Uncertainty propagates from BOTH samples: a joint calendar-month cluster bootstrap resamples months and
  recomputes the control bucket means and the signal excess in every replicate. The v1 interval (control
  bucket means held fixed) is reported alongside for comparison.
* Excess is measured on the matched subset only (events with |ret_z| >= 1 in a bucket containing controls);
  `control_coverage` says how much of the signal that subset is. Claims apply to that subset.
* Balance diagnostics: standardised mean differences between matched signal events and the bucket-reweighted
  control pool on log ADV, beta, year, BTC bull regime and high-vol regime. |SMD| > 0.25 means the control is
  not a like-for-like population on that dimension.

Concentration: share of the OOS net P&L sum contributed by the five best events, and ex-top-5 / 5%-winsorised
means. An effect whose sign depends on five events is not a tradeable rule at any size.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from qlib.paths import TABLES  # noqa: E402

T = TABLES / "track_b"
CONTROLS = ("U1_control_price_up_1sd_long", "U2_control_price_down_1sd_long")
BINS = [-np.inf, -3, -2, -1.5, -1, 1, 1.5, 2, 3, np.inf]
NB = len(BINS) - 1
H = "5d"


def gross(df: pd.DataFrame, h: str = H) -> pd.Series:
    """Direction-signed economic return before costs: hedged price return + funding received."""
    return df[f"hedged_{h}"] + df[f"funding_{h}"]


def _bucket(z: pd.Series) -> np.ndarray:
    b = pd.cut(z, BINS, labels=False)
    return b.to_numpy(dtype=float)


def matched_excess(ev: pd.DataFrame, signal: str, h: str = H, reps: int = 2000, seed: int = 7) -> dict:
    ctrl = ev[ev["signal"].isin(CONTROLS)]
    ctrl = ctrl[gross(ctrl, h).notna() & ctrl["ret_z"].notna()]
    e = ev[ev["signal"] == signal]
    out = {"signal": signal, "n": len(e)}
    if e.empty or ctrl.empty:
        return out
    bc, gc = _bucket(ctrl["ret_z"]).astype(int), gross(ctrl, h).to_numpy()
    be, ge, de = _bucket(e["ret_z"]), gross(e, h).to_numpy(), e["direction"].to_numpy()
    n_c = np.bincount(bc, minlength=NB).astype(float)
    cmean = np.bincount(bc, weights=gc, minlength=NB) / np.where(n_c > 0, n_c, np.nan)
    ok = np.isfinite(be) & np.isfinite(ge)
    ok[ok] &= np.isfinite(cmean[be[ok].astype(int)])
    bi = np.where(ok, be, 0).astype(int)
    excess = ge - de * cmean[bi]
    out.update(control_coverage=float(ok.mean()), n_matched=int(ok.sum()),
               raw_gross_mean=float(np.nanmean(ge)), matched_signal_gross_mean=float(ge[ok].mean()) if ok.any() else np.nan,
               matched_control_gross_mean=float((de * cmean[bi])[ok].mean()) if ok.any() else np.nan)
    if ok.sum() < 20:
        out.update(excess_mean=np.nan, excess_lo=np.nan, excess_hi=np.nan, excess_p=np.nan,
                   excess_lo_fixed_control=np.nan, excess_hi_fixed_control=np.nan)
        return out
    months = pd.Index(sorted(set(ctrl["cluster"]) | set(e["cluster"])))
    mc, me = months.get_indexer(ctrl["cluster"]), months.get_indexer(e["cluster"])
    rng = np.random.default_rng(seed)
    joint, fixed = np.empty(reps), np.empty(reps)
    for r in range(reps):
        cnt = np.bincount(rng.integers(0, len(months), len(months)), minlength=len(months)).astype(float)
        wc, we = cnt[mc], cnt[me] * ok
        nb = np.bincount(bc, weights=wc, minlength=NB)
        cm_r = np.bincount(bc, weights=wc * gc, minlength=NB) / np.where(nb > 0, nb, np.nan)
        ex_r = ge - de * cm_r[bi]
        good = ok & np.isfinite(ex_r) & (we > 0)
        joint[r] = np.sum(we[good] * ex_r[good]) / np.sum(we[good]) if good.any() else np.nan
        fixed[r] = np.sum(we[ok] * excess[ok]) / np.sum(we[ok]) if we[ok].sum() > 0 else np.nan
    point = float(excess[ok].mean())
    out.update(excess_mean=point, excess_lo=float(np.nanpercentile(joint, 2.5)), excess_hi=float(np.nanpercentile(joint, 97.5)),
               excess_p=float(min(1.0, 2 * min(np.nanmean(joint <= 0), np.nanmean(joint >= 0)))),
               excess_lo_fixed_control=float(np.nanpercentile(fixed, 2.5)),
               excess_hi_fixed_control=float(np.nanpercentile(fixed, 97.5)))
    out.update(balance(e[ok], ctrl, bc, be[ok].astype(int)))
    return out


def balance(sig: pd.DataFrame, ctrl: pd.DataFrame, bc: np.ndarray, bs: np.ndarray) -> dict:
    """SMD between matched signal events and controls reweighted to the signal's ret_z-bucket mix."""
    n_s = np.bincount(bs, minlength=NB).astype(float)
    n_c = np.bincount(bc, minlength=NB).astype(float)
    w = (n_s / np.where(n_c > 0, n_c, np.nan))[bc]
    w = np.nan_to_num(w)       # control buckets the signal never visits get zero weight (not a missing return)
    feats = {"log_adv": lambda d: np.log(d["adv30"].clip(lower=1)), "beta": lambda d: d["beta60"],
             "year": lambda d: d["ts"].dt.year + d["ts"].dt.dayofyear / 366,
             "bull": lambda d: (d["btc_regime"] == "bull").astype(float),
             "high_vol": lambda d: (d["vol_regime"] == "high").astype(float)}
    out = {}
    for k, fn in feats.items():
        xs, xc = fn(sig).to_numpy(dtype=float), fn(ctrl).to_numpy(dtype=float)
        mc_ = np.isfinite(xc)
        if not np.isfinite(xs).any() or w[mc_].sum() == 0:
            out[f"smd_{k}"] = np.nan
            continue
        ms, vs = np.nanmean(xs), np.nanvar(xs)
        mcw = np.average(xc[mc_], weights=w[mc_])
        vcw = np.average((xc[mc_] - mcw) ** 2, weights=w[mc_])
        pooled = np.sqrt((vs + vcw) / 2)
        out[f"smd_{k}"] = float((ms - mcw) / pooled) if pooled > 0 else np.nan
    return out


def concentration(x: pd.Series, k: int = 5) -> dict:
    x = x.dropna().sort_values()
    if len(x) < 2 * k:
        return {"top5_share": np.nan, "mean_ex_top5": np.nan, "mean_winsor5": np.nan}
    w = x.clip(x.quantile(0.05), x.quantile(0.95))
    tot = x.sum()
    return {"top5_share": float(x.iloc[-k:].sum() / tot) if tot > 0 else np.nan,
            "mean_ex_top5": float(x.iloc[:-k].mean()), "mean_winsor5": float(w.mean())}


def build() -> pd.DataFrame:
    ev = pd.read_parquet(T / "events.parquet")
    panel = pd.read_parquet(T / "panel.parquet", columns=["ts", "asset", "ret_z"])
    ev = ev.merge(panel, on=["ts", "asset"], how="left")
    rows = []
    for win, sub in (("OOS", ev[ev["oos"]]), ("ALL", ev)):
        for s in sorted(set(sub["signal"]) - set(CONTROLS)):
            r = matched_excess(sub, s)
            r.update(concentration(sub.loc[sub["signal"] == s, f"net_{H}"]))
            r["window"] = win
            rows.append(r)
    out = pd.DataFrame(rows)
    out.to_csv(T / "control_matched_excess.csv", index=False)
    return out


if __name__ == "__main__":
    pd.set_option("display.width", 250)
    pd.set_option("display.max_columns", 40)
    print(build().round(4).to_string())
