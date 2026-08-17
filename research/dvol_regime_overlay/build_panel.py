"""
Assemble the daily panel for the DVOL-vs-regime-monitor study.

WHAT THIS JOINS
---------------
    Environment_APR_daily_pct   rebuilt from silver funding via the SAME
    Fragmentation_Spread        production function the monitor uses
                                (macro_environment.build_daily_environment_table),
                                so this is not a re-implementation that could drift.
    w_risk                      gate_policy.calculate_risk_weight(APR) -- the actual
                                exposure multiplier the book runs on today.
    DVOL                        Deribit BTC volatility index, daily UTC close,
                                reused from ../btc_trend_agreement/cache.
    L/S proxy                   daily long-majors / short-alts return, built from
                                the cached point-in-time Binance alt basket.
    y (weekly)                  the REAL strategy return from msm_timeseries.

SAMPLE, AND WHY IT IS SHORT
---------------------------
The binding constraint is FUNDING, not DVOL:

    DVOL                2021-03-24 onward   (~1,970 days)
    silver funding      2023-04-19 onward   (~1,220 days)   <-- binds
    Environment_APR     only where funding exists
    weekly y            ~105-173 weekly rows

So the usable overlap is ~3.3 years daily / ~170 weekly observations. Every
conclusion in this study is bounded by that, and the weekly tests in particular
have very little power. Stated up front rather than discovered later.

READS from the Drive export (authoritative) when present, falls back to the repo
copy. WRITES only inside this research directory.

    python build_panel.py [--refresh-dvol]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "majors_alts_monitor" / "msm_funding_v0"))

from majors_alts_monitor.msm_funding_v0.macro_environment import (  # noqa: E402
    build_daily_environment_table,
)
from src.macro_regime.gate_policy import calculate_risk_weight  # noqa: E402

HERE = Path(__file__).resolve().parent
OUT_DIR = HERE / "data"
OUT_DIR.mkdir(parents=True, exist_ok=True)

DRIVE = Path("G:/My Drive/Render Exports")
REPO_LAKE = REPO_ROOT / "data" / "curated" / "data_lake"
TREND_CACHE = REPO_ROOT / "research" / "btc_trend_agreement" / "cache"


def _lake_file(name: str) -> Path:
    """Prefer the Drive export (authoritative); fall back to the repo seed."""
    drive = DRIVE / name
    if drive.exists():
        return drive
    repo = REPO_LAKE / name
    if repo.exists():
        print(f"  ! using REPO copy of {name} (Drive not available) -- may be stale")
        return repo
    raise SystemExit(f"cannot find {name} in {DRIVE} or {REPO_LAKE}")


def load_environment() -> pd.DataFrame:
    """Daily Environment_APR / Fragmentation_Spread via the production function."""
    path = _lake_file("silver_fact_funding.parquet")
    fd = pd.read_parquet(path)
    daily = build_daily_environment_table(fd)
    daily["date"] = pd.to_datetime(daily["date"]).dt.normalize()
    daily = daily.set_index("date").sort_index()
    daily["w_risk"] = daily["Environment_APR_daily_pct"].apply(calculate_risk_weight)
    return daily


def load_dvol() -> pd.Series:
    path = TREND_CACHE / "deribit_btc_dvol_daily.csv"
    if not path.exists():
        raise SystemExit(
            f"DVOL cache missing at {path}. Run "
            f"research/btc_trend_agreement/run_all.py first, or pass --refresh-dvol."
        )
    d = pd.read_csv(path, parse_dates=["date"]).set_index("date")["dvol"].sort_index()
    full = pd.date_range(d.index.min(), d.index.max(), freq="D")
    return d.reindex(full).ffill().rename("dvol")


def load_ls_proxy() -> pd.DataFrame:
    """Daily long-majors / short-alts proxy from the cached point-in-time basket.

    The weekly `y` the monitor actually trades has only ~170 observations. This
    daily proxy has ~1,200 over the same window, which is the only way to get
    any statistical power. It is validated against weekly y in study.py -- if it
    does not track, it is not used.
    """
    sys.path.insert(0, str(REPO_ROOT / "research" / "btc_trend_agreement"))
    from alt_basket import build_basket, clean_redenominations, load_panel, member_map

    panel, _ = clean_redenominations(load_panel(False))
    close, members = build_basket(panel, 50)
    memb = member_map(close.index, members)
    ret = close.pct_change(fill_method=None)

    alt = pd.Series(index=close.index, dtype=float)
    for d in close.index:
        m = memb.loc[d]
        if isinstance(m, list):
            r = ret.loc[d, [c for c in m if c in ret.columns]]
            if r.notna().sum() >= 5:
                alt.loc[d] = r.mean()

    import data_io
    px, _ = data_io.load_prices()
    btc = px["BTC"].pct_change()
    eth = px["ETH"].pct_change()
    majors = 0.7 * btc + 0.3 * eth          # the monitor's major weights
    out = pd.DataFrame({"r_alts": alt, "r_majors": majors}).dropna()
    out["ls_proxy"] = out["r_majors"] - out["r_alts"]
    return out


def load_weekly_y() -> pd.DataFrame:
    """The real weekly strategy return, from the live msm_timeseries."""
    for cand in (DRIVE / "msm_timeseries.csv",
                 REPO_ROOT / "reports" / "msm_funding_v0"):
        if cand.is_file():
            d = pd.read_csv(cand, parse_dates=["decision_date"])
            break
    else:
        cands = sorted((REPO_ROOT / "reports" / "msm_funding_v0").rglob("msm_timeseries.csv"))
        if not cands:
            raise SystemExit("no msm_timeseries.csv found")
        d = pd.read_csv(cands[-1], parse_dates=["decision_date"])
    keep = [c for c in ("decision_date", "y", "Environment_APR", "Fragmentation_Spread",
                        "w_risk", "funding_regime", "is_mrf_active") if c in d.columns]
    return d[keep].set_index("decision_date").sort_index()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--refresh-dvol", action="store_true")
    args = ap.parse_args()

    print("Building daily environment table from silver funding...")
    env = load_environment()
    print(f"  Environment_APR: {env.index.min().date()} -> {env.index.max().date()} "
          f"({len(env)} days)")

    dvol = load_dvol()
    print(f"  DVOL           : {dvol.index.min().date()} -> {dvol.index.max().date()} "
          f"({len(dvol)} days)")

    ls = load_ls_proxy()
    print(f"  L/S proxy      : {ls.index.min().date()} -> {ls.index.max().date()} "
          f"({len(ls)} days)")

    wk = load_weekly_y()
    print(f"  weekly y       : {wk.index.min().date()} -> {wk.index.max().date()} "
          f"({len(wk)} rows)")

    daily = env.join(dvol, how="inner").join(ls, how="inner")
    # Rolling percentile of DVOL -- the level drifted by half over its history,
    # so an absolute threshold decays into a dead switch (see the trend study).
    for w in (180, 365):
        daily[f"dvol_pct_{w}"] = daily["dvol"].rolling(w, min_periods=90).rank(pct=True)
    daily["dvol_z_365"] = (
        (daily["dvol"] - daily["dvol"].rolling(365, min_periods=90).mean())
        / daily["dvol"].rolling(365, min_periods=90).std()
    )

    daily.to_parquet(OUT_DIR / "daily_panel.parquet")
    wk.to_parquet(OUT_DIR / "weekly_y.parquet")

    print()
    print(f"JOINED DAILY PANEL: {daily.index.min().date()} -> {daily.index.max().date()} "
          f"({len(daily)} days)")
    print(f"  -> {OUT_DIR / 'daily_panel.parquet'}")
    print()
    print("binding constraint check:")
    for name, idx in (("Environment_APR", env.index), ("DVOL", dvol.index),
                      ("L/S proxy", ls.index)):
        print(f"  {name:<16} starts {idx.min().date()}")
    print(f"  -> joined sample starts {daily.index.min().date()} "
          f"(bound by {'funding' if env.index.min() > dvol.index.min() else 'DVOL'})")


if __name__ == "__main__":
    main()
