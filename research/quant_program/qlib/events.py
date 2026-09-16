"""Reusable event-study engine.

Contract
--------
bars   : long DataFrame [ts, asset, open, high, low, close] (+ optional 'venue').
         ts is the bar OPEN time; the bar's close is known at ts + bar_length.
events : DataFrame [ts, asset, direction] (+ any feature / regime columns).
         An event stamped ts means "known at the CLOSE of the bar that opened at ts".

Entry conventions (no look-ahead):
  'next_open' : enter at the open of the bar after the event bar (default);
  'close'     : enter at the event bar's close (only for signals truly known before the close).

For each event and horizon h (in bars) the engine records, direction-signed:
  fwd_h      : exit close of bar i+h vs entry price;
  mfe_h/mae_h: best/worst excursion using highs/lows of bars i+1..i+h;
  voladj_h   : fwd_h / (prior realised vol * sqrt(h)), prior vol from bars i-W..i-1;
  excess_h   : fwd_h minus the same-asset, same-year unconditional mean for that
               direction (a date-matched baseline, so drift is not mistaken for edge).

Summaries resample calendar-month clusters for CIs and report cluster-robust t-stats.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from qlib import stats


@dataclass
class EventStudyConfig:
    horizons: dict = field(default_factory=lambda: {"1d": 1, "3d": 3, "5d": 5, "10d": 10})
    entry: str = "next_open"
    vol_window: int = 20
    min_separation: int = 0          # bars; per-asset declustering (0 = off)
    rank_col: str | None = None       # keep the largest |rank_col| within a cluster
    cluster_freq: str = "M"           # bootstrap / t-stat clusters
    reps: int = 1000
    seed: int = 20260915
    baseline: str = "asset_year"      # 'asset_year' | 'asset' | 'none'


# --------------------------------------------------------------------------
def decluster(events: pd.DataFrame, bars_ts: dict, min_sep: int, rank_col: str | None) -> pd.DataFrame:
    """Per asset, keep the strongest event in any window of min_sep bars."""
    if min_sep <= 0 or events.empty:
        return events
    keep = []
    for asset, g in events.groupby("asset"):
        idx_map = bars_ts.get(asset)
        if idx_map is None:
            continue
        pos = np.searchsorted(idx_map, g["ts"].to_numpy())
        g = g.assign(_pos=pos)
        order = g.sort_values(rank_col, ascending=False, key=lambda s: s.abs()) if rank_col else g.sort_values("ts")
        kept_pos: list[int] = []
        for i, row in order.iterrows():
            if all(abs(row["_pos"] - k) >= min_sep for k in kept_pos):
                kept_pos.append(row["_pos"])
                keep.append(i)
    return events.loc[sorted(keep)]


def _forward_block(o, h_, l_, c, idx, direction, horizons, entry, vol_window, logret):
    n = len(c)
    out = {}
    entry_px = o[np.minimum(idx + 1, n - 1)] if entry == "next_open" else c[idx]
    valid_entry = (idx + 1 < n) if entry == "next_open" else np.ones(len(idx), bool)
    sig = np.full(len(idx), np.nan)
    for j, i in enumerate(idx):
        if i - vol_window >= 1:
            w = logret[i - vol_window:i]
            sig[j] = np.nanstd(w, ddof=1)
    out["prior_vol"] = sig
    for lab, h in horizons.items():
        ex = idx + h
        ok = valid_entry & (ex < n)
        exit_px = np.where(ok, c[np.minimum(ex, n - 1)], np.nan)
        fwd = direction * (exit_px / entry_px - 1)
        win = np.arange(1, h + 1)
        gi = np.minimum(idx[:, None] + win[None, :], n - 1)
        hi = np.where(ok[:, None], h_[gi], np.nan)
        lo = np.where(ok[:, None], l_[gi], np.nan)
        best = np.where(direction > 0, np.nanmax(hi, axis=1), np.nanmin(lo, axis=1))
        worst = np.where(direction > 0, np.nanmin(lo, axis=1), np.nanmax(hi, axis=1))
        out[f"fwd_{lab}"] = fwd
        out[f"mfe_{lab}"] = direction * (best / entry_px - 1)
        out[f"mae_{lab}"] = direction * (worst / entry_px - 1)
        out[f"voladj_{lab}"] = fwd / (sig * np.sqrt(h))
    out["entry_px"] = entry_px
    return out


def compute_event_returns(bars: pd.DataFrame, events: pd.DataFrame, cfg: EventStudyConfig) -> pd.DataFrame:
    """Event-level observations with forward returns, excursions and baselines."""
    need = {"ts", "asset", "open", "high", "low", "close"}
    missing = need - set(bars.columns)
    if missing:
        raise ValueError(f"bars missing columns {missing}")
    bars = bars.sort_values(["asset", "ts"])
    bars_ts = {a: g["ts"].to_numpy() for a, g in bars.groupby("asset")}
    ev = events.copy()
    ev["direction"] = ev["direction"].astype(int)
    ev = decluster(ev, bars_ts, cfg.min_separation, cfg.rank_col)

    rows = []
    for asset, g in bars.groupby("asset"):
        e = ev[ev["asset"] == asset]
        c = g["close"].to_numpy(float)
        o, hh, ll = g["open"].to_numpy(float), g["high"].to_numpy(float), g["low"].to_numpy(float)
        logret = np.r_[np.nan, np.diff(np.log(c))]
        ts = g["ts"].to_numpy()
        if not e.empty:
            idx = np.searchsorted(ts, e["ts"].to_numpy())
            match = (idx < len(ts)) & (ts[np.minimum(idx, len(ts) - 1)] == e["ts"].to_numpy())
            e = e[match]
            idx = idx[match]
            if len(idx):
                blk = _forward_block(o, hh, ll, c, idx, e["direction"].to_numpy(), cfg.horizons,
                                     cfg.entry, cfg.vol_window, logret)
                rows.append(e.reset_index(drop=True).assign(**{k: v for k, v in blk.items()}))
    if not rows:
        return pd.DataFrame()
    obs = pd.concat(rows, ignore_index=True)

    if cfg.baseline != "none":
        base = unconditional_returns(bars, cfg)
        keys = ["asset", "direction"] + (["year"] if cfg.baseline == "asset_year" else [])
        obs["year"] = pd.to_datetime(obs["ts"]).dt.year
        obs = obs.merge(base, on=keys, how="left")
        for lab in cfg.horizons:
            obs[f"excess_{lab}"] = obs[f"fwd_{lab}"] - obs[f"base_{lab}"]
    obs["cluster"] = pd.to_datetime(obs["ts"]).dt.to_period(cfg.cluster_freq).astype(str)
    return obs


def unconditional_returns(bars: pd.DataFrame, cfg: EventStudyConfig) -> pd.DataFrame:
    """Mean forward return of every bar, by asset (and year), for each direction."""
    frames = []
    for asset, g in bars.sort_values(["asset", "ts"]).groupby("asset"):
        c, o = g["close"].to_numpy(float), g["open"].to_numpy(float)
        n = len(c)
        d = {"asset": asset, "year": pd.to_datetime(g["ts"]).dt.year.to_numpy()}
        entry = np.r_[o[1:], np.nan] if cfg.entry == "next_open" else c
        for lab, h in cfg.horizons.items():
            ex = np.r_[c[h:], np.full(h, np.nan)] if h < n else np.full(n, np.nan)
            d[lab] = ex / entry - 1
        frames.append(pd.DataFrame(d))
    allb = pd.concat(frames, ignore_index=True)
    keys = ["asset"] + (["year"] if cfg.baseline == "asset_year" else [])
    m = allb.groupby(keys)[list(cfg.horizons)].mean().reset_index()
    out = []
    for d in (1, -1):
        x = m.copy()
        x["direction"] = d
        for lab in cfg.horizons:
            x[f"base_{lab}"] = d * x[lab]
        out.append(x[keys + ["direction"] + [f"base_{lab}" for lab in cfg.horizons]])
    return pd.concat(out, ignore_index=True)


# --------------------------------------------------------------------------
def summarize(obs: pd.DataFrame, cfg: EventStudyConfig, by: list[str] | None = None,
              metric_prefix: str = "fwd") -> pd.DataFrame:
    """Standard summary per horizon (and optional grouping)."""
    if obs.empty:
        return pd.DataFrame()
    groups = obs.groupby(by, observed=True) if by else [(("all",), obs)]
    rows = []
    for key, g in groups:
        key = key if isinstance(key, tuple) else (key,)
        for lab in cfg.horizons:
            col = f"{metric_prefix}_{lab}"
            s = g[[col, "cluster"]].dropna()
            if s.empty:
                continue
            v = s[col].to_numpy()
            mean, lo, hi = stats.cluster_bootstrap_ci(v, s["cluster"], np.mean, cfg.reps, cfg.seed)
            med, mlo, mhi = stats.cluster_bootstrap_ci(v, s["cluster"], np.median, cfg.reps, cfg.seed)
            r = dict(zip(by or ["group"], key))
            r.update({"horizon": lab, "metric": metric_prefix, "n": len(v), "clusters": s["cluster"].nunique(),
                      "mean": mean, "mean_lo": lo, "mean_hi": hi, "median": med, "median_lo": mlo,
                      "median_hi": mhi, "win_rate": float((v > 0).mean()),
                      "t_cluster": stats.cluster_t(v, s["cluster"]),
                      "std": float(np.std(v, ddof=1)) if len(v) > 1 else np.nan})
            if f"mae_{lab}" in g:
                r["mae_median"] = float(g[f"mae_{lab}"].median())
                r["mfe_median"] = float(g[f"mfe_{lab}"].median())
            if f"excess_{lab}" in g and metric_prefix == "fwd":
                ex = g[[f"excess_{lab}", "cluster"]].dropna()
                em, elo, ehi = stats.cluster_bootstrap_ci(ex[f"excess_{lab}"], ex["cluster"], np.mean,
                                                          cfg.reps, cfg.seed)
                r.update({"excess_mean": em, "excess_lo": elo, "excess_hi": ehi,
                          "excess_t_cluster": stats.cluster_t(ex[f"excess_{lab}"], ex["cluster"])})
            rows.append(r)
    return pd.DataFrame(rows)


def conditional_curve(obs: pd.DataFrame, feature: str, bins, horizons: list[str],
                      outcome_cols: list[str] | None = None, labels=None) -> pd.DataFrame:
    """Bucket events by a feature and report outcomes per bucket.

    outcome_cols: extra 0/1 columns (e.g. 'confirmed', 'valid_10d') reported as rates with Wilson CIs.
    """
    x = obs.copy()
    x["bucket"] = pd.cut(x[feature], bins, labels=labels, include_lowest=True)
    rows = []
    for bk, g in x.groupby("bucket", observed=False):
        r = {"feature": feature, "bucket": str(bk), "n": len(g)}
        for oc in outcome_cols or []:
            k = int(g[oc].fillna(0).sum())
            lo, hi = stats.wilson(k, len(g))
            r[f"{oc}_rate"] = k / len(g) if len(g) else np.nan
            r[f"{oc}_lo"], r[f"{oc}_hi"] = lo, hi
        for lab in horizons:
            for pre in ("fwd", "mae", "mfe", "excess"):
                col = f"{pre}_{lab}"
                if col in g:
                    r[f"{pre}_{lab}_mean" if pre in ("fwd", "excess") else f"{pre}_{lab}_median"] = (
                        g[col].mean() if pre in ("fwd", "excess") else g[col].median())
            if f"fwd_{lab}" in g:
                r[f"fwd_{lab}_median"] = g[f"fwd_{lab}"].median()
                r[f"win_{lab}"] = (g[f"fwd_{lab}"] > 0).mean() if len(g) else np.nan
        rows.append(r)
    return pd.DataFrame(rows)


def plot_event_distribution(obs: pd.DataFrame, horizon: str, path, title: str = "") -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    col = f"fwd_{horizon}"
    v = obs[col].dropna() * 100
    fig, ax = plt.subplots(figsize=(7, 3.6))
    ax.hist(v.clip(v.quantile(0.01), v.quantile(0.99)), bins=40, color="#2a78d6", edgecolor="#fcfcfb", lw=0.5)
    ax.axvline(v.mean(), color="#eb6834", lw=2, label=f"mean {v.mean():.2f}%")
    ax.axvline(v.median(), color="#1baf7a", lw=2, ls="--", label=f"median {v.median():.2f}%")
    ax.axvline(0, color="#52514e", lw=1)
    ax.set_xlabel(f"forward return {horizon}, % (1-99% clipped)")
    ax.set_ylabel("events")
    ax.set_title(title or f"n={len(v)}", loc="left")
    ax.legend(frameon=False, fontsize=8)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    fig.tight_layout()
    fig.savefig(path, dpi=140)
    plt.close(fig)
