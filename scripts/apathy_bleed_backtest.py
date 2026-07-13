#!/usr/bin/env python3
"""
Apathy Bleed — continuous chronological portfolio backtest engine.

Rebuilds the engine described in docs/BACKTEST.md Part B (sections 11-19) from
spec. Strings monthly cohorts together over calendar time, retaining daily
mark-to-market volatility.

Per cohort:
  - Formation: rank eligible tickers by spot return over `formation` days ending
    at (exec_date - lag). Take Top N passing the is_perp_active gate on exec_date.
  - Harvest: daily walk over `harvest` days. Equal-weight short alt basket vs
    $1 notional BTC long. Per-leg adverse-excursion stop frozen at trigger.
    Funding on both legs at 3 settlements/day. 60bps round-trip drag at exit.
    Delistings forward-filled.
  - Gates: Oct-2025 quarantine over the full lifecycle; Cold Flush halt when
    Environment_APR < 2.0% on exec_date.

Sharpe uses ann_factor = 365/harvest; bootstrap CI = 1000 resamples.

Usage:
  python scripts/apathy_bleed_backtest.py --panel single_coin_panel.csv \
      --msm reports/.../msm_timeseries.csv \
      --start 2024-05-01 --end 2025-11-01 --top-n 5

This module is import-safe: run_backtest() returns a result dict for reuse by
the extension driver.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

# ---- Universe exclusion lists (docs/BACKTEST.md section 2, verbatim) ----
STABLES = set(
    "USDT USDC DAI FDUSD TUSD USDP PYUSD USDE FRAX LUSD BUSD UST USDS USDD "
    "EUSD EURC EURT USTC PAXG XAUT".split()
)
DERIVED = set("WBTC STETH CBETH RETH WBETH WETH BTCB RBTC MBTC LBTC HBTC TBTC SXP SUSD".split())
MEGACAP = set(
    "BTC ETH SOL BNB XRP ADA DOT TRX LTC BCH OKB KCS HT GT MX BGB LEO CRO FTT "
    "VGX WRX COCOS DYDX GMX CET WOO KNC CRV AERO CAKE RAY JUP UNI SUSHI".split()
)

FUNDING_SETTLEMENTS_PER_DAY = 3
QUARANTINE_START = pd.Timestamp("2025-10-01")
QUARANTINE_END = pd.Timestamp("2025-10-31")
COLD_FLUSH_APR = 2.0


def is_valid_ticker(t: str) -> bool:
    t = str(t).upper()
    if t in STABLES or t in DERIVED or t in MEGACAP:
        return False
    if len(t) <= 2:
        return False
    if any(c.isdigit() for c in t):
        return False
    return True


@dataclass
class Config:
    formation: int = 45
    lag: int = 0
    top_n: int = 5
    harvest: int = 150
    stop: float = 0.60          # per-leg adverse-excursion stop (fraction)
    drag: float = 0.0060        # round-trip drag applied at terminal exit
    funding: bool = True
    cold_flush: bool = True
    quarantine: bool = True
    btc_ticker: str = "BTC"


@dataclass
class CohortResult:
    exec_date: pd.Timestamp
    members: list = field(default_factory=list)
    terminal_pnl: float = np.nan   # fraction (e.g. 0.75 = +75%)
    worst_dd: float = np.nan
    funding_cost: float = np.nan   # signed fraction (negative = cost)
    n_stopped: int = 0
    peak_pnl: float = np.nan
    status: str = "ok"


def _load_panel(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, usecols=[
        "decision_date_utc", "ticker", "close_price_usd",
        "is_perp_active", "funding_rate_8h_decimal",
    ])
    df["date"] = pd.to_datetime(df["decision_date_utc"])
    df["ticker"] = df["ticker"].astype(str).str.upper()
    return df


def _load_msm(path: Path) -> pd.Series:
    m = pd.read_csv(path)
    dcol = [c for c in m.columns if "date" in c.lower()][0]
    m[dcol] = pd.to_datetime(m[dcol])
    m = m.sort_values(dcol)
    return pd.Series(m["Environment_APR"].values, index=m[dcol].values)


def _apr_on(msm: pd.Series, d: pd.Timestamp) -> float:
    """Latest MSM Environment_APR at or before exec date d (weekly cadence)."""
    prior = msm.index[msm.index <= np.datetime64(d)]
    if len(prior) == 0:
        return float("nan")
    return float(msm.loc[prior.max()])


def _touches_quarantine(life_start: pd.Timestamp, life_end: pd.Timestamp) -> bool:
    return not (life_end < QUARANTINE_START or life_start > QUARANTINE_END)


def run_backtest(panel: pd.DataFrame, msm: pd.Series, cfg: Config,
                 start: str, end: str, *, seed: int = 7,
                 verbose: bool = False, execs=None) -> dict:
    # Wide close-price matrix (date x ticker), ffilled for delistings.
    close = panel.pivot_table(index="date", columns="ticker",
                              values="close_price_usd", aggfunc="last").sort_index()
    close = close.ffill()
    perp = panel.pivot_table(index="date", columns="ticker",
                             values="is_perp_active", aggfunc="last").sort_index()
    fund = panel.pivot_table(index="date", columns="ticker",
                             values="funding_rate_8h_decimal", aggfunc="last").sort_index()
    all_dates = close.index

    eligible = [t for t in close.columns if is_valid_ticker(t)]
    if execs is None:
        execs = pd.date_range(start, end, freq="MS")
    else:
        execs = pd.DatetimeIndex(pd.to_datetime(execs))

    results: list[CohortResult] = []
    for ex in execs:
        cr = CohortResult(exec_date=ex)
        form_end = ex - pd.Timedelta(days=cfg.lag)
        form_start = form_end - pd.Timedelta(days=cfg.formation)
        harvest_end = ex + pd.Timedelta(days=cfg.harvest)

        # Gate: Oct-2025 quarantine over full lifecycle.
        if cfg.quarantine and _touches_quarantine(form_start, harvest_end):
            cr.status = "quarantined"
            results.append(cr)
            continue
        # Gate: Cold Flush.
        if cfg.cold_flush:
            apr = _apr_on(msm, ex)
            if np.isfinite(apr) and apr < COLD_FLUSH_APR:
                cr.status = f"cold_flush_halt(APR={apr:.2f})"
                results.append(cr)
                continue

        # Formation return: close(form_end)/close(form_start) using nearest
        # available panel dates within the window.
        def _px_at(t, target, lo, hi):
            col = close[t]
            sub = col[(col.index >= lo) & (col.index <= hi)].dropna()
            if sub.empty:
                return np.nan
            # value at nearest date <= target, else earliest in window
            le = sub[sub.index <= target]
            return float(le.iloc[-1]) if len(le) else float(sub.iloc[0])

        rets = {}
        for t in eligible:
            p0 = _px_at(t, form_start, form_start - pd.Timedelta(days=7), form_end)
            p1 = _px_at(t, form_end, form_start, form_end)
            if np.isfinite(p0) and np.isfinite(p1) and p0 > 0:
                rets[t] = p1 / p0 - 1.0
        ranked = sorted(rets.items(), key=lambda kv: kv[1], reverse=True)

        # is_perp_active gate on exec date; walk down the ranking.
        if ex in perp.index:
            perp_on = perp.loc[ex]
        else:
            pr = perp[perp.index <= ex]
            perp_on = pr.iloc[-1] if len(pr) else pd.Series(dtype=float)

        picked = []
        for t, r in ranked:
            if len(picked) >= cfg.top_n:
                break
            active = perp_on.get(t, 0)
            if pd.notna(active) and int(active) == 1:
                picked.append(t)
        if len(picked) < cfg.top_n:
            cr.status = f"insufficient_universe(n={len(picked)})"
            if not picked:
                results.append(cr)
                continue
        cr.members = picked

        # Harvest daily walk.
        hz = all_dates[(all_dates >= ex) & (all_dates <= harvest_end)]
        if len(hz) < 2:
            cr.status = "no_harvest_data"
            results.append(cr)
            continue
        btc = close[cfg.btc_ticker].reindex(hz).ffill()
        btc0 = btc.iloc[0]
        if not np.isfinite(btc0) or btc0 <= 0:
            cr.status = "no_btc_price"
            results.append(cr)
            continue

        alt0 = {t: close[t].reindex(hz).ffill().iloc[0] for t in picked}
        alt_px = {t: close[t].reindex(hz).ffill() for t in picked}
        stopped_at = {t: None for t in picked}  # index position where stop triggered

        cum_pair = []       # cumulative pair PnL (price component) per day
        cum_fund = []       # cumulative funding PnL per day
        run_fund = 0.0
        btc_fund = fund[cfg.btc_ticker].reindex(hz).ffill().fillna(0.0)
        alt_fund = {t: fund[t].reindex(hz).ffill().fillna(0.0) for t in picked}

        for i, d in enumerate(hz):
            btc_ret = float(btc.iloc[i]) / float(btc0) - 1.0
            leg_pnls = []
            for t in picked:
                p = float(alt_px[t].iloc[i])
                r = p / float(alt0[t]) - 1.0 if alt0[t] > 0 else 0.0
                # adverse excursion for a short = +r; stop when r >= cfg.stop
                if stopped_at[t] is None and r >= cfg.stop:
                    stopped_at[t] = i
                if stopped_at[t] is not None:
                    leg = -cfg.stop  # short frozen at -stop
                else:
                    leg = -r
                leg_pnls.append(leg)
            short_basket = float(np.mean(leg_pnls))
            pair = btc_ret + short_basket
            cum_pair.append(pair)

            # Funding for this day (3 settlements). Long BTC pays FR>0; short receives FR>0.
            if cfg.funding and i > 0:
                day_fund = -FUNDING_SETTLEMENTS_PER_DAY * float(btc_fund.iloc[i])
                alt_f = []
                for t in picked:
                    if stopped_at[t] is not None and stopped_at[t] < i:
                        continue  # funding stops on stopped legs
                    alt_f.append(FUNDING_SETTLEMENTS_PER_DAY * float(alt_fund[t].iloc[i]))
                if alt_f:
                    day_fund += float(np.mean(alt_f))
                run_fund += day_fund
            cum_fund.append(run_fund)

        cum = np.array(cum_pair) + np.array(cum_fund)
        running_max = np.maximum.accumulate(cum)
        cr.worst_dd = float(np.min(cum - running_max))
        cr.peak_pnl = float(np.max(cum))
        cr.terminal_pnl = float(cum[-1]) - cfg.drag
        cr.funding_cost = float(cum_fund[-1])
        cr.n_stopped = sum(1 for v in stopped_at.values() if v is not None)
        results.append(cr)

    clean = [r for r in results if r.status == "ok" and np.isfinite(r.terminal_pnl)]
    pnls = np.array([r.terminal_pnl for r in clean])
    ann = 365.0 / cfg.harvest
    if len(pnls) >= 2 and pnls.std(ddof=1) > 0:
        sharpe = float(pnls.mean() / pnls.std(ddof=1) * np.sqrt(ann))
    else:
        sharpe = float("nan")

    # Bootstrap CI on Sharpe.
    rng = np.random.default_rng(seed)
    boot = []
    if len(pnls) >= 2:
        for _ in range(1000):
            samp = rng.choice(pnls, size=len(pnls), replace=True)
            s = samp.std(ddof=1)
            if s > 0:
                boot.append(samp.mean() / s * np.sqrt(ann))
    ci = (float(np.percentile(boot, 5)), float(np.percentile(boot, 95))) if boot else (np.nan, np.nan)

    summary = {
        "n_clean": len(clean),
        "n_quarantined": sum(1 for r in results if r.status == "quarantined"),
        "n_cold_flush": sum(1 for r in results if r.status.startswith("cold_flush")),
        "sharpe": sharpe,
        "sharpe_ci": ci,
        "alpha_mean_terminal": float(pnls.mean()) if len(pnls) else np.nan,
        "alpha_peak_meanprofile": np.nan,  # filled below
        "win_rate": float(np.mean(pnls > 0)) if len(pnls) else np.nan,
        "worst_dd": float(min((r.worst_dd for r in clean), default=np.nan)),
        "avg_funding": float(np.mean([r.funding_cost for r in clean])) if clean else np.nan,
    }
    # Peak of the cross-sectional mean profile (event-study style alpha proxy):
    if clean:
        summary["alpha_peak_meanprofile"] = float(np.mean([r.peak_pnl for r in clean]))
    return {"cfg": cfg, "results": results, "clean": clean, "summary": summary}


def _print_report(res: dict, label: str) -> None:
    s = res["summary"]
    lo, hi = s["sharpe_ci"]
    print(f"\n=== {label} ===")
    print(f"clean cohorts        : {s['n_clean']}  (quarantined {s['n_quarantined']}, cold-flush {s['n_cold_flush']})")
    print(f"Sharpe               : {s['sharpe']:.2f}  [CI {lo:.2f}, {hi:.2f}]")
    print(f"Alpha (mean terminal): {s['alpha_mean_terminal']*100:.1f}%")
    print(f"Alpha (peak profile) : {s['alpha_peak_meanprofile']*100:.1f}%")
    print(f"Win rate             : {s['win_rate']*100:.0f}%")
    print(f"Worst cohort DD      : {s['worst_dd']*100:.1f}%")
    print(f"Avg funding / cohort : {s['avg_funding']*100:.2f}%")
    print("\nper-cohort:")
    for r in res["results"]:
        if r.status == "ok":
            print(f"  {r.exec_date.date()}  pnl={r.terminal_pnl*100:6.1f}%  dd={r.worst_dd*100:6.1f}%  "
                  f"fund={r.funding_cost*100:5.2f}%  stopped={r.n_stopped}  [{','.join(r.members)}]")
        else:
            print(f"  {r.exec_date.date()}  --  {r.status}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--panel", type=Path, default=Path("single_coin_panel.csv"))
    p.add_argument("--msm", type=Path, required=True)
    p.add_argument("--start", default="2024-05-01")
    p.add_argument("--end", default="2025-11-01")
    p.add_argument("--formation", type=int, default=45)
    p.add_argument("--lag", type=int, default=0)
    p.add_argument("--top-n", type=int, default=5)
    p.add_argument("--harvest", type=int, default=150)
    p.add_argument("--stop", type=float, default=0.60)
    p.add_argument("--no-funding", action="store_true")
    p.add_argument("--label", default="backtest")
    args = p.parse_args()

    cfg = Config(formation=args.formation, lag=args.lag, top_n=args.top_n,
                 harvest=args.harvest, stop=args.stop, funding=not args.no_funding)
    panel = _load_panel(args.panel)
    msm = _load_msm(args.msm)
    res = run_backtest(panel, msm, cfg, args.start, args.end)
    _print_report(res, args.label)


if __name__ == "__main__":
    main()
