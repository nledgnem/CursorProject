"""Track A orchestration: trend-confirmation research across assets, systems and execution models.

    python -m track_a.run            (from research/quant_program)

Primary:      long/flat, spot-like (no funding), next-open execution, 10 bp per unit turnover.
Sensitivity:  long/short implemented on Binance perps with funding; costs 0/20 bp.
Validation:   chronological train <=2021 / validation 2022-23 / test 2024-26, plus anchored
              annual walk-forward for the fitted models (M4, M5, M6).
"""

from __future__ import annotations

import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
sys.path.insert(0, str(HERE.parents[2] / "btc_confirmation_lag"))

from backtest import block_bootstrap_diff  # noqa: E402
from qlib import checks, stats  # noqa: E402
from qlib.governance import RunLog  # noqa: E402
from qlib.paths import CACHE, TABLES  # noqa: E402
from track_a import economics as ECO  # noqa: E402
from track_a import events_build as EB  # noqa: E402
from track_a import models as M  # noqa: E402
from track_a.features import ASSETS, SYSTEMS, signed_frame  # noqa: E402
from signals import trend_signal  # noqa: E402

warnings.filterwarnings("ignore")
ASSETS_RUN = ["BTC", "ETH", "SOL"]
SYSTEMS_RUN = ["GERHARD_SMA120", "LL3_k0.30", "LL2_k0.30"]
WINDOWS = {"full": (None, None), "train_<=2021": (None, "2021-12-31"),
           "validation_2022-23": ("2022-01-01", "2023-12-31"), "test_2024+": ("2024-01-01", None)}
OUT = TABLES / "track_a"
OUT.mkdir(parents=True, exist_ok=True)


def wf_years(obs):
    first = obs["ts"].min().year + 2
    return list(range(max(2019, first), 2027))


def causal_probe(asset, system) -> checks.CheckResult:
    """Truncation test: the trend side and day-1 strength on truncated history must equal the full-history values."""
    from track_a.features import load_price
    px = load_price(asset)
    spec = SYSTEMS[system]

    def sig(df):
        side, line, up, dn = trend_signal(df, spec)
        r = np.log(df["close"] / df["close"].shift(1))
        z = r / r.rolling(20).std().shift(1)
        return side.fillna(9) * 1000 + z.fillna(0).round(9)

    return checks.causal(sig, px, probes=6, min_history=400)


def main():
    all_obs, metrics_rows, wf_rows, pick_rows, sens_rows, eco_rows, eco_bucket_rows, boot_rows, regime_rows = \
        [], [], [], [], [], [], [], [], []
    data_files = [p for p in (CACHE / "coinbase" / "daily").glob("*.parquet")] + \
                 [CACHE / "binance" / "funding" / f"{ASSETS[a]['perp']}.parquet" for a in ASSETS_RUN] + \
                 [CACHE / "binance" / "premium_1d" / f"{ASSETS[a]['perp']}.parquet" for a in ASSETS_RUN]
    with RunLog("track_a_trend_confirmation", "1.0",
                params={"K": 3, "cost_bps": M.COST_BPS, "thresh_grid": M.THRESH_GRID, "embargo_days": 35,
                        "systems": {s: SYSTEMS[s] for s in SYSTEMS_RUN}},
                universe=ASSETS_RUN, start=str(EB.EVAL_START.date()), end="latest", fees_bps=5, slippage_bps=5,
                data_files=data_files) as run:
        for asset in ASSETS_RUN:
            for system in SYSTEMS_RUN:
                print(f"== {asset} {system}")
                f, base, obs = EB.build(asset, system, "long_flat")
                if obs.empty:
                    continue
                side = f["side"]
                nf = SYSTEMS[system]["neutral_flips"]
                btc_ret = f["close"].pct_change().fillna(0)
                sf = {1: signed_frame(f, 1), -1: signed_frame(f, -1)}
                obs["asset"] = asset

                run.check(checks.duplicates(f.reset_index(), ["index"], f"dup_dates_{asset}"))
                run.check(checks.sorted_within(f.reset_index(), "index"))
                run.check(causal_probe(asset, system))

                for exposure, perp in (("long_flat", False), ("long_short", True)):
                    fixed = {"M0_3close": dict(K=3), "M1_1close": dict(K=1), "M2_2close": dict(K=2),
                             "M3_linear": dict(K=3, schedule_fn=lambda t, R, s: (1 / 3, 2 / 3))}
                    bts, ws = {}, {}
                    for name, kw in fixed.items():
                        mach, bt = M.run_model(f, side, exposure, nf, perp_funding=perp, **kw)
                        bts[name], ws[name] = bt, mach["w"]
                    run.check(checks.execution_lag(ws["M0_3close"].fillna(0), bts["M0_3close"]["pos"], 1))
                    run.check(checks.abnormal_turnover(bts["M1_1close"]["turnover"]))
                    eval_start = obs["ts"].min()
                    for name, bt in bts.items():
                        for wname, (a, b) in WINDOWS.items():
                            met = M.strategy_metrics(bt, btc_ret, a or eval_start, b)
                            if met:
                                metrics_rows.append({"asset": asset, "system": system, "exposure": exposure,
                                                     "model": name, "window": wname, **met})

                    # walk-forward fitted models
                    years = wf_years(obs)
                    if exposure == "long_flat":
                        for name in ("M1_1close", "M2_2close"):
                            obs[f"incr_{name[:2].lower()}"] = ECO.incremental_pnl(
                                obs, bts[name], bts["M0_3close"], ws[name], ws["M0_3close"])
                        obs["incr_k1"] = obs["incr_m1"]
                        obs["incr_m3"] = ECO.incremental_pnl(obs, bts["M3_linear"], bts["M0_3close"],
                                                             ws["M3_linear"], ws["M0_3close"])
                    oos, picks, _ = M.walk_forward(f, side, obs, exposure, nf, years, sf, btc_ret, perp)
                    picks = picks.assign(asset=asset, system=system, exposure=exposure)
                    pick_rows.append(picks)
                    for col in oos.columns:
                        met = M.strategy_metrics(pd.DataFrame({"ret": oos[col], "pos": np.nan, "turnover": np.nan})
                                                 .fillna({"pos": 0, "turnover": 0}), btc_ret)
                        # Trades/turnover are not meaningful for stitched series; keep return metrics
                        base_met = stats.perf_summary(oos[col])
                        wf_rows.append({"asset": asset, "system": system, "exposure": exposure, "model": col,
                                        "oos_start": str(oos.index.min().date()), "oos_end": str(oos.index.max().date()),
                                        **base_met})
                        if col != "M0_3close":
                            bb = block_bootstrap_diff(oos[col], oos["M0_3close"], 2000, 60, 20260915)
                            boot_rows.append({"asset": asset, "system": system, "exposure": exposure, "model": col,
                                              "window": "walk_forward_oos", **bb})
                    # fixed-model bootstraps on the untouched test window
                    for name in ("M1_1close", "M2_2close", "M3_linear"):
                        a = bts[name]["ret"].loc["2024-01-01":]
                        b = bts["M0_3close"]["ret"].loc["2024-01-01":]
                        boot_rows.append({"asset": asset, "system": system, "exposure": exposure, "model": name,
                                          "window": "test_2024+", **block_bootstrap_diff(a, b, 2000, 60, 20260915)})

                    if exposure == "long_flat":
                        s_tab = M.sensitivity(f, side, exposure, nf, sf, btc_ret,
                                              {k: (a or eval_start, b) for k, (a, b) in WINDOWS.items()}, perp)
                        m0 = {w: M.strategy_metrics(bts["M0_3close"], btc_ret, a or eval_start, b).get("Sharpe")
                              for w, (a, b) in WINDOWS.items()}
                        s_tab["M0_sharpe"] = s_tab["window"].map(m0)
                        sens_rows.append(s_tab.assign(asset=asset, system=system, exposure=exposure))

                # economics (long/flat)
                for col, label in (("incr_m1", "M1_1close"), ("incr_m2", "M2_2close"), ("incr_m3", "M3_linear")):
                    for kind, g in [("all", obs)] + list(obs.groupby("kind")):
                        eco_rows.append({"asset": asset, "system": system, "rule": label, "event_kind": kind,
                                         **ECO.summarize(g, col)})
                for feat in ("ret_z20", "brk_vol", "range_breakout_20", "trend_agree_count"):
                    bins, labels = ((EB.SIGMA_BINS, EB.SIGMA_LABELS) if feat != "trend_agree_count"
                                    else ([-0.5, 2.5, 4.5, 6.5, 8.5], ["0-2", "3-4", "5-6", "7-8"]))
                    eco_bucket_rows.append(ECO.by_bucket(obs, "incr_m1", feat, bins, labels)
                                           .assign(asset=asset, system=system, rule="M1_1close"))

                # regime breakdowns of event outcomes and M1-vs-M0 incremental P&L
                for dim in ("vol_regime", "bull_env", "liquidity_regime", "weekend", "macro_risk_on"):
                    if dim not in obs:
                        continue
                    for lvl, g in obs.groupby(dim, observed=True):
                        if len(g) < 5:
                            continue
                        regime_rows.append({"asset": asset, "system": system, "dimension": dim, "level": lvl,
                                            "n": len(g), "p_confirm": g["confirmed"].mean(),
                                            "p_valid_10d": g["valid_10d"].mean(), "fwd_10d_mean": g["fwd_10d"].mean(),
                                            "incr_m1_mean": g["incr_m1"].mean(), "incr_m1_sum": g["incr_m1"].sum()})
                all_obs.append(obs)

        obs = pd.concat(all_obs, ignore_index=True)
        obs.to_parquet(OUT / "events.parquet", index=False)
        obs.drop(columns=[c for c in obs.columns if obs[c].dtype == object and c not in
                          ("kind", "system", "asset", "exposure", "vol_regime", "bull_env", "liquidity_regime", "cluster")],
                 errors="ignore").to_csv(OUT / "events.csv", index=False)
        EB.conditional_tables(obs, ("system",)).to_csv(OUT / "conditional_by_system.csv", index=False)
        EB.conditional_tables(obs, ("system", "asset")).to_csv(OUT / "conditional_by_system_asset.csv", index=False)
        auc = EB.auc_table(obs, ("system",))
        auc.to_csv(OUT / "auc_by_system.csv", index=False)
        EB.logit_oos(obs).to_csv(OUT / "logit_oos.csv", index=False)
        EB.rarity(obs).to_csv(OUT / "rarity_aug2026_like.csv", index=False)
        pd.DataFrame(metrics_rows).to_csv(OUT / "strategy_metrics.csv", index=False)
        pd.DataFrame(wf_rows).to_csv(OUT / "walk_forward_oos_metrics.csv", index=False)
        pd.concat(pick_rows).to_csv(OUT / "walk_forward_picks.csv", index=False)
        pd.concat(sens_rows).to_csv(OUT / "sensitivity_thresholds.csv", index=False)
        pd.DataFrame(eco_rows).to_csv(OUT / "economics_summary.csv", index=False)
        pd.concat(eco_bucket_rows).to_csv(OUT / "economics_by_bucket.csv", index=False)
        pd.DataFrame(boot_rows).to_csv(OUT / "bootstrap_vs_M0.csv", index=False)
        pd.DataFrame(regime_rows).to_csv(OUT / "regime_event_outcomes.csv", index=False)

        run.result("n_events", int(len(obs)))
        run.result("outputs", sorted(p.name for p in OUT.iterdir()))
    print("Track A done")


if __name__ == "__main__":
    main()
