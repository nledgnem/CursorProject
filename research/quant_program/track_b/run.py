"""Track B orchestration: structural-alpha scan over the survivorship-free Binance perp panel.

    python -m track_b.run          (from research/quant_program)

Outputs -> results/tables/track_b/, run manifest -> runs/.
"""

from __future__ import annotations

import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))

from qlib import checks, stats  # noqa: E402
from qlib.governance import RunLog  # noqa: E402
from qlib.paths import CACHE, REPO, TABLES  # noqa: E402
from track_b import panel as P  # noqa: E402
from track_b import studies as S  # noqa: E402

warnings.filterwarnings("ignore")
OUT = TABLES / "track_b"
OUT.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------------------------------
def carry_study(obs_f1: pd.DataFrame, p_idx: pd.DataFrame, h: int = 5) -> dict:
    """Delta-neutral carry on F1 events: short perp / long spot for h days.
    P&L = funding received + (premium_t - premium_{t+h}) - 4 legs of cost (perp + spot, in and out)."""
    if obs_f1.empty:
        return {}
    rows = []
    for r in obs_f1.itertuples():
        try:
            s = p_idx.loc[r.asset]
        except KeyError:
            continue
        if r.ts not in s.index:
            continue
        i = s.index.get_loc(r.ts)
        if i + h >= len(s):
            continue
        fund = s["funding_day"].iloc[i + 1:i + 1 + h].fillna(0).sum()
        dprem = s["prem_close"].iloc[i] - s["prem_close"].iloc[i + h]
        cost = 2 * r.alt_round_trip_cost      # perp + spot legs of the SAME asset; no BTC hedge in carry
        rows.append({"ts": r.ts, "asset": r.asset, "funding_received": fund, "premium_gain": dprem,
                     "cost": cost, "net": fund + (dprem if np.isfinite(dprem) else 0) - cost, "cluster": r.cluster})
    c = pd.DataFrame(rows)
    if c.empty:
        return {}
    c.to_csv(OUT / "carry_trade_events.csv", index=False)
    out = {"signal": "F1c_delta_neutral_carry", "family": "funding", "n_events": len(c)}
    for part, g in (("IS", c[c.ts < S.OOS_START]), ("OOS", c[c.ts >= S.OOS_START])):
        if len(g) < 5:
            continue
        m, lo, hi = stats.cluster_bootstrap_ci(g["net"], g["cluster"], np.mean, 500)
        out.update({f"{part}_n": len(g), f"{part}_net_mean": m, f"{part}_net_lo": lo, f"{part}_net_hi": hi,
                    f"{part}_funding_mean": g["funding_received"].mean(), f"{part}_premium_gain_mean": g["premium_gain"].mean(),
                    f"{part}_win": (g["net"] > 0).mean(), f"{part}_t": stats.cluster_t(g["net"], g["cluster"]),
                    f"{part}_p": stats.cluster_p(g["net"], g["cluster"]),
                    f"{part}_p_wild": stats.wild_cluster_p(g["net"], g["cluster"])})
    return out


def intraday_study() -> pd.DataFrame:
    """Hour-of-day and funding-settlement-hour returns for BTC/ETH/SOL perps (1h bars, UTC)."""
    rows = []
    for sym in ("BTCUSDT", "ETHUSDT", "SOLUSDT"):
        p = CACHE / "binance" / "klines_um_1h" / f"{sym}.parquet"
        if not p.exists():
            continue
        k = pd.read_parquet(p).sort_values("ts")
        k["r"] = k["close"] / k["open"] - 1
        k["hour"], k["dow"] = k["ts"].dt.hour, k["ts"].dt.dayofweek
        k["period"] = np.where(k["ts"] < S.OOS_START, "IS", "OOS")
        for (period, hour), g in k.groupby(["period", "hour"]):
            m, lo, hi = stats.block_bootstrap_ci(g["r"], np.mean, 500, block=5)
            rows.append({"asset": sym, "period": period, "bucket": f"hour_{hour:02d}", "n": len(g), "mean_bp": m * 1e4,
                         "lo_bp": lo * 1e4, "hi_bp": hi * 1e4, "vol_bp": g["r"].std() * 1e4,
                         "settlement_hour": hour in (0, 8, 16)})
        d = k.set_index("ts")["close"].resample("D").last().pct_change().dropna()
        for (period, dow), g in d.groupby([np.where(d.index < S.OOS_START, "IS", "OOS"), d.index.dayofweek]):
            m, lo, hi = stats.block_bootstrap_ci(g, np.mean, 500, block=2)
            rows.append({"asset": sym, "period": period, "bucket": f"dow_{dow}", "n": len(g), "mean_bp": m * 1e4,
                         "lo_bp": lo * 1e4, "hi_bp": hi * 1e4, "vol_bp": g.std() * 1e4, "settlement_hour": False})
    return pd.DataFrame(rows)


def coinbase_premium(p: pd.DataFrame, bars: pd.DataFrame, cache: dict):
    from qlib import data as D
    cb = D.coinbase_daily("BTC-USD").set_index("ts")["close"]
    bn = p[p.asset == "BTCUSDT"].set_index("ts")["close"]
    prem = (cb / bn - 1).dropna()
    prem = prem[prem.index >= "2020-06-01"]
    z = (prem - prem.rolling(90, min_periods=45).mean().shift(1)) / prem.rolling(90, min_periods=45).std().shift(1)
    res = []
    for name, m, d, hyp in (("CB1_coinbase_premium_high_long_BTC", z >= 2, 1,
                             "US USD spot demand (Coinbase premium >= +2 sd) leads offshore price: expect BTC up"),
                            ("CB2_coinbase_premium_low_short_BTC", z <= -2, -1,
                             "Coinbase discount <= -2 sd: US selling pressure: expect BTC down")):
        ev = pd.DataFrame({"ts": z.index[m.fillna(False)], "asset": "BTCUSDT", "direction": d})
        spec = S.SignalSpec(name, "cross_venue", hyp, "BTC perp", min_sep=5)
        r, obs = S.evaluate(spec, ev, p, bars, p.set_index(["asset", "ts"]).sort_index(), cache)
        res.append((spec, r, obs))
    return res


def main():
    with RunLog("track_b_structural_alpha", "1.0",
                params={"oos_start": str(S.OOS_START.date()), "min_adv_usd": P.MIN_ADV_USD,
                        "min_listed_days": P.MIN_LISTED_DAYS, "cost_tiers": "majors 20bp RT; ADV>=100m 40bp; >=20m 60bp; else 100bp"},
                universe="Binance USDT perps (current + delisted)", start="2019-09-01", end="latest",
                fees_bps=5, slippage_bps="tiered", data_files=[CACHE / "universe_binance_perps.csv"]) as run:
        p = P.build_panel()
        run.check(checks.duplicates(p, ["ts", "asset"]))
        run.check(checks.datetime_dtype(p, "ts"))
        run.check(checks.sorted_within(p, "ts", "asset"))
        run.check(checks.frozen_tail(p, "asset"))
        run.check(checks.universe_changes(p[p["eligible"]], "ts", "asset", max_jump=0.25))
        bars = p[["ts", "asset", "open", "high", "low", "close"]]
        p_idx = p.set_index(["asset", "ts"]).sort_index()
        wide = {c: p.pivot(index="ts", columns="asset", values=c).sort_index()
                for c in ("open", "close", "ret", "funding_day", "sig20")}
        rets = wide
        results, obs_keep, ports, cache, gaps = [], [], {}, {}, []
        specs = S.signals(p)
        for spec, r, obs in coinbase_premium(p, bars, cache):
            specs.append((spec, None))
            results.append(r)
            if len(obs):
                obs_keep.append(obs)
                ports[spec.name] = S.event_portfolio(obs, rets, spec.horizons[spec.primary_horizon])
                gaps.append({"signal": spec.name, "n_events": len(obs), **ports[spec.name].attrs})
        f1_obs = pd.DataFrame()
        for spec, ev in specs:
            if ev is None:
                continue
            print(f"-- {spec.name}: {len(ev)} raw events")
            r, obs = S.evaluate(spec, ev, p, bars, p_idx, cache)
            results.append(r)
            if len(obs):
                obs_keep.append(obs)
                ports[spec.name] = S.event_portfolio(obs, rets, spec.horizons[spec.primary_horizon])
                gaps.append({"signal": spec.name, "n_events": len(obs), **ports[spec.name].attrs})
                if spec.name.startswith("F1_"):
                    f1_obs = obs
        carry = carry_study(f1_obs, p_idx)
        if carry:
            results.append(carry)

        R = pd.DataFrame(results)
        pd.DataFrame(gaps).to_csv(OUT / "portfolio_data_gaps.csv", index=False)
        # BH family = alpha hypotheses only; the price-only controls are baselines, not hypotheses.
        hyp = R["family"] != "control"
        for part in ("IS", "OOS"):
            for col in ("p", "p_wild", "p_normal"):
                c = f"{part}_{col}"
                if c in R:
                    rej, adj = stats.bh_fdr(R[c].where(hyp), q=0.10)
                    R[f"{c}_bh"] = adj
                    if col == "p":
                        R[f"{part}_reject_bh10"] = rej
        R["bh_family_size_OOS"] = int((hyp & R.get("OOS_p", pd.Series(np.nan, index=R.index)).notna()).sum())
        R.to_csv(OUT / "signal_results.csv", index=False)

        keep_cols = ["ts", "asset", "direction", "signal", "family", "oos", "cluster", "beta60", "adv30",
                     "round_trip_cost", "alt_round_trip_cost", "hedge_round_trip_cost", "btc_regime", "vol_regime"]
        allobs = pd.concat(obs_keep, ignore_index=True)
        cols = keep_cols + [c for c in allobs.columns
                            if c.split("_")[0] in ("fwd", "hedged", "hedge", "net", "funding", "mae", "mfe")]
        cols = list(dict.fromkeys(cols))          # "hedge" prefix also matches hedge_round_trip_cost
        allobs[[c for c in cols if c in allobs]].to_parquet(OUT / "events.parquet", index=False)

        port = pd.DataFrame(ports).fillna(0.0)
        port.to_csv(OUT / "portfolios_daily.csv")
        rows = []
        for c in port.columns:
            first = allobs.loc[allobs.signal == c, "ts"].min()
            s = port.loc[first:, c]
            for part, x in (("ALL", s), ("IS", s[s.index < S.OOS_START]), ("OOS", s[s.index >= S.OOS_START])):
                if len(x) > 60:
                    rows.append({"signal": c, "period": part, "active_share": float((x != 0).mean()), **stats.perf_summary(x)})
        pd.DataFrame(rows).to_csv(OUT / "portfolio_stats.csv", index=False)

        # correlations to existing exposures
        ref = pd.DataFrame(index=port.index)
        ref["BTC"] = wide["ret"].get("BTCUSDT")
        g = REPO / "research" / "btc_confirmation_lag" / "results" / "tables" / "daily_returns_GERHARD_SMA120.csv"
        if g.exists():
            gd = pd.read_csv(g, index_col=0, parse_dates=True)
            ref["Gerhard_3close"] = gd["K3_close"].reindex(ref.index)
        mj = REPO / "reports" / "majors_alts" / "bt_daily_pnl.csv"
        if mj.exists():
            m = pd.read_csv(mj)
            dcol = [c for c in m.columns if "date" in c.lower()][0]
            vcol = [c for c in m.columns if c != dcol and pd.api.types.is_numeric_dtype(m[c])][0]
            ref["majors_alts_LS"] = m.set_index(pd.to_datetime(m[dcol]))[vcol].reindex(ref.index)
        corr = pd.concat([port.loc["2021-01-01":], ref.loc["2021-01-01":]], axis=1).corr()
        corr.to_csv(OUT / "correlations.csv")

        intraday_study().to_csv(OUT / "intraday_hour_dow.csv", index=False)
        run.result("n_signals", len(R))
        run.result("n_events", int(len(allobs)))
        run.result("has_coinglass", bool("long_liq_z" in p))
    print("Track B done")


if __name__ == "__main__":
    main()
