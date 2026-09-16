"""Pre-registered Track B signal definitions and a single evaluation pipeline.

Every signal is defined in STRUCTURAL_ALPHA_MAP.md BEFORE results were seen. Each returns
events [ts, asset, direction] using information known at the close of day ts. Evaluation:

  1. forward returns from the NEXT OPEN (qlib.events), declustered per asset;
  2. BTC-hedged: fwd - direction * beta60 * BTC fwd over the identical window;
  3. net of costs (liquidity-tiered round trip) and of funding paid/received over the hold;
  4. in-sample (< 2024) vs out-of-sample (>= 2024) with monthly-cluster CIs and cluster t;
  5. cost sensitivity (break-even round-trip bp) and regime splits;
  6. an event-portfolio daily return stream (equal weight over open positions) for portfolio tests.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd
from scipy.stats import norm

from qlib import events as E
from qlib import stats

OOS_START = pd.Timestamp("2024-01-01")
HORIZONS = {"1d": 1, "3d": 3, "5d": 5, "10d": 10}


@dataclass
class SignalSpec:
    name: str
    family: str
    hypothesis: str
    direction_note: str
    primary_horizon: str = "5d"
    min_sep: int = 5
    horizons: dict = field(default_factory=lambda: dict(HORIZONS))


BTC_HEDGE_RT = 0.0020     # BTC perp round trip for the hedge leg (majors tier)


def round_trip_cost(adv: pd.Series, asset: pd.Series) -> pd.Series:
    """Liquidity-tiered round trip (decimal): majors 20bp, ADV>$100m 40bp, >$20m 60bp, else 100bp."""
    majors = asset.isin(["BTCUSDT", "ETHUSDT"])
    c = np.where(adv >= 1e8, 0.0040, np.where(adv >= 2e7, 0.0060, 0.0100))
    return pd.Series(np.where(majors, 0.0020, c), index=adv.index)


# --------------------------------------------------------------------------
# Signal definitions (pre-registered)
# --------------------------------------------------------------------------
def _ev(mask: pd.Series, p: pd.DataFrame, direction) -> pd.DataFrame:
    d = p.loc[mask & p["eligible"], ["ts", "asset"]].copy()
    d["direction"] = direction if np.isscalar(direction) else direction[mask & p["eligible"]]
    return d


def signals(p: pd.DataFrame) -> list[tuple[SignalSpec, pd.DataFrame]]:
    out = []
    has_cg = "long_liq_z" in p
    add = lambda spec, ev: out.append((spec, ev))

    # B1 funding
    add(SignalSpec("F1_extreme_pos_funding_short", "funding",
                   "Top cross-sectional decile of 7d funding AND funding >= +2 sd vs own 90d: crowded levered longs "
                   "pay; expect weak forward returns", "short perp"),
        _ev((p["funding_xs_rank"] >= 0.9) & (p["funding_ts_z"] >= 2), p, -1))
    add(SignalSpec("F2_extreme_neg_funding_long", "funding",
                   "Bottom decile 7d funding AND funding <= -2 sd: crowded shorts pay; expect squeeze / rebound",
                   "long perp"),
        _ev((p["funding_xs_rank"] <= 0.1) & (p["funding_ts_z"] <= -2), p, 1))
    add(SignalSpec("F3_funding_despite_reversal_short", "funding",
                   "Top-decile funding persists while price fell >= 1 sd over 3d: trapped longs still paying; "
                   "expect continuation down", "short perp"),
        _ev((p["funding_xs_rank"] >= 0.9) & (p["mom_3d_z"] <= -1), p, -1))
    add(SignalSpec("F4_high_funding_ann_100pct_short", "funding",
                   "7d funding annualised >= 100%: extreme carry paid by longs", "short perp", min_sep=7),
        _ev(p["funding_7d_ann"] >= 1.0, p, -1))

    # B2 basis
    add(SignalSpec("P1_premium_z_high_short", "basis",
                   "Perp premium >= +2 sd vs own 90d: perp rich vs index; expect perp to cheapen", "short perp"),
        _ev(p["prem_z90"] >= 2, p, -1))
    add(SignalSpec("P2_premium_z_low_long", "basis",
                   "Perp premium <= -2 sd: perp cheap vs index; expect richening", "long perp"),
        _ev(p["prem_z90"] <= -2, p, 1))

    # B4 listings
    first = p.groupby("asset")["ts"].transform("min")
    lst = p[(p["ts"] == first) & (p["ts"] >= pd.Timestamp("2021-01-01"))][["ts", "asset"]].assign(direction=-1)
    add(SignalSpec("L1_new_perp_listing_short", "listings",
                   "First trading day of a new Binance USDT perp: attention-driven buying and supply overhang; "
                   "expect negative drift after day 1", "short perp", primary_horizon="10d", min_sep=0,
                   horizons={"5d": 5, "10d": 10, "20d": 20, "30d": 30}), lst)

    # B10 contract mechanics
    ic = p["interval_change"].fillna(False).astype(bool) & p["funding_day"].notna()
    add(SignalSpec("C1_funding_interval_shortened", "mechanics",
                   "Binance shortens the funding interval under extreme funding: payments accelerate for the "
                   "crowded side; trade against the side paying", "against funding sign", min_sep=10),
        _ev(ic, p, -np.sign(p["funding_day"].fillna(0)).replace(0, 1).astype(int)))

    if has_cg:
        # B6 liquidations
        add(SignalSpec("Q1_long_liq_flush_OI_reset_long", "liquidation",
                       "Long liquidations >= +2 sd of OI AND OI fell >= 1 sd: forced sellers exhausted; expect rebound",
                       "long perp"),
            _ev((p["long_liq_z"] >= 2) & (p["oi_chg_z"] <= -1), p, 1))
        add(SignalSpec("Q2_long_liq_OI_rebuild_short", "liquidation",
                       "Long liquidations >= +2 sd but OI rising: new shorts pressing, cascade fuel remains; "
                       "expect continuation down", "short perp"),
            _ev((p["long_liq_z"] >= 2) & (p["oi_chg_z"] > 0), p, -1))
        add(SignalSpec("Q3_short_liq_squeeze_OI_reset_short", "liquidation",
                       "Short liquidations >= +2 sd AND OI fell: squeeze fuel spent; expect fade", "short perp"),
            _ev((p["short_liq_z"] >= 2) & (p["oi_chg_z"] <= -1), p, -1))
        add(SignalSpec("Q4_short_liq_OI_rebuild_long", "liquidation",
                       "Short liquidations >= +2 sd with OI rising: new longs adding into squeeze; "
                       "expect continuation up", "long perp"),
            _ev((p["short_liq_z"] >= 2) & (p["oi_chg_z"] > 0), p, 1))
        # CONTROLS (not alpha claims): plain 1-sd price moves on the SAME CoinGlass-covered assets. The OI and
        # liquidation signals must beat these to show that positioning data adds information beyond momentum.
        cg_cov = p["oi_chg_z"].notna()
        add(SignalSpec("U1_control_price_up_1sd_long", "control",
                       "CONTROL: day return >= +1 sd on assets with OI data; measures plain momentum/reversal",
                       "long measured"), _ev(cg_cov & (p["ret_z"] >= 1), p, 1))
        add(SignalSpec("U2_control_price_down_1sd_long", "control",
                       "CONTROL: day return <= -1 sd on assets with OI data; measures plain reversal",
                       "long measured"), _ev(cg_cov & (p["ret_z"] <= -1), p, 1))
        # B7 OI / price quadrants (long-direction measurement)
        up, dn = p["ret_z"] >= 1, p["ret_z"] <= -1
        oiu, oid = p["oi_chg_z"] >= 1, p["oi_chg_z"] <= -1
        for nm, m, d, hyp in (("O1_price_up_OI_up", up & oiu, 1, "new longs chasing: fragile continuation"),
                              ("O2_price_up_OI_down", up & oid, 1, "short covering: weaker continuation"),
                              ("O3_price_down_OI_up", dn & oiu, 1, "new shorts: squeeze fuel, rebound"),
                              ("O4_price_down_OI_down", dn & oid, 1, "long capitulation: rebound")):
            add(SignalSpec(nm, "oi_price", hyp + " (measured as LONG; negative mean = short edge)", "long measured"),
                _ev(m, p, d))
        # B8 interactions (pre-registered only)
        add(SignalSpec("X1_crowded_long_reversal", "interaction",
                       "Top-decile funding + OI 7d build >= 1 sd + 3d price >= +2 sd: crowded late longs; expect reversal",
                       "short perp", min_sep=7),
            _ev((p["funding_xs_rank"] >= 0.9) & (p["oi_7d_z"] >= 1) & (p["mom_3d_z"] >= 2), p, -1))
        add(SignalSpec("X2_short_squeeze_continuation", "interaction",
                       "Negative funding + short liquidations >= 1 sd + OI falling: shorts forced out; short-horizon "
                       "continuation up", "long perp", primary_horizon="3d"),
            _ev((p["funding_day"] < 0) & (p["short_liq_z"] >= 1) & (p["oi_chg_z"] < 0), p, 1))
        add(SignalSpec("X3_capitulation_rebound", "interaction",
                       "Long liquidations >= 2 sd + OI reset <= -1 sd + funding <= 0: capitulation; expect rebound",
                       "long perp"),
            _ev((p["long_liq_z"] >= 2) & (p["oi_chg_z"] <= -1) & (p["funding_day"] <= 0), p, 1))
    add(SignalSpec("X4_funding_despite_reversal_OI", "interaction",
                   "Top-decile funding + 3d price <= -1 sd + OI not falling: trapped longs adding; continuation down",
                   "short perp"),
        _ev((p["funding_xs_rank"] >= 0.9) & (p["mom_3d_z"] <= -1) &
            ((p["oi_chg_z"] >= 0) if has_cg else True), p, -1))
    return out


# --------------------------------------------------------------------------
# Evaluation
# --------------------------------------------------------------------------
def _funding_over_hold(p_idx: pd.DataFrame, obs: pd.DataFrame, h: int) -> np.ndarray:
    """Sum of funding_day over days t+1..t+h (the holding window after next-open entry)."""
    out = np.full(len(obs), np.nan)
    for asset, g in obs.groupby("asset"):
        if asset not in p_idx.index.get_level_values(0):
            continue
        f = p_idx.loc[asset]["funding_day"].fillna(0.0)
        cs = f.cumsum()
        pos = np.searchsorted(f.index.to_numpy(), g["ts"].to_numpy())
        hi = np.minimum(pos + h, len(f) - 1)
        vals = cs.to_numpy()[hi] - cs.to_numpy()[np.minimum(pos, len(f) - 1)]
        out[g.index.to_numpy()] = vals
    return out


def evaluate(spec: SignalSpec, ev: pd.DataFrame, p: pd.DataFrame, bars: pd.DataFrame, p_idx: pd.DataFrame,
             btc_obs_cache: dict) -> tuple[dict, pd.DataFrame]:
    if ev.empty:
        return {"signal": spec.name, "family": spec.family, "n_events": 0}, pd.DataFrame()
    cfg = E.EventStudyConfig(horizons=spec.horizons, min_separation=spec.min_sep, baseline="none", reps=500)
    obs = E.compute_event_returns(bars, ev, cfg)
    if obs.empty:
        return {"signal": spec.name, "family": spec.family, "n_events": 0}, obs
    obs = obs.merge(p[["ts", "asset", "beta60", "adv30", "btc_regime", "vol_regime", "funding_day", "base"]],
                    on=["ts", "asset"], how="left")
    # BTC forward over identical windows
    key = tuple(spec.horizons.items())
    if key not in btc_obs_cache:
        b_ev = pd.DataFrame({"ts": bars.loc[bars.asset == "BTCUSDT", "ts"], "asset": "BTCUSDT", "direction": 1})
        bo = E.compute_event_returns(bars[bars.asset == "BTCUSDT"], b_ev, E.EventStudyConfig(horizons=spec.horizons,
                                                                                              baseline="none"))
        btc_obs_cache[key] = bo[["ts"] + [f"fwd_{h}" for h in spec.horizons]].rename(
            columns={f"fwd_{h}": f"btc_{h}" for h in spec.horizons})
    obs = obs.merge(btc_obs_cache[key], on="ts", how="left")
    # Hedge leg = BTC perp, size beta60, opposite side. It pays its own round trip and funding (review fix
    # 2026-09-15: v1 charged only the alt leg's costs and ignored BTC funding on the hedge).
    is_btc = (obs["asset"] == "BTCUSDT").to_numpy()
    alt_rt = round_trip_cost(obs["adv30"].fillna(0), obs["asset"])
    hedge_rt = pd.Series(np.where(is_btc, 0.0, obs["beta60"].abs() * BTC_HEDGE_RT), index=obs.index)
    rt = alt_rt + hedge_rt
    obs["alt_round_trip_cost"], obs["hedge_round_trip_cost"], obs["round_trip_cost"] = alt_rt, hedge_rt, rt
    for lab, h in spec.horizons.items():
        hedge = np.where(is_btc, 0.0, obs["direction"] * obs["beta60"] * obs[f"btc_{lab}"])
        obs[f"hedged_{lab}"] = obs[f"fwd_{lab}"] - hedge
        fund = _funding_over_hold(p_idx, obs, h)
        fund_btc = _funding_over_hold(p_idx, obs.assign(asset="BTCUSDT"), h)
        obs[f"hedge_funding_{lab}"] = np.where(is_btc, 0.0, obs["direction"] * obs["beta60"] * fund_btc)
        obs[f"funding_{lab}"] = -obs["direction"] * fund + obs[f"hedge_funding_{lab}"]   # longs pay positive funding
        obs[f"net_{lab}"] = obs[f"hedged_{lab}"] + obs[f"funding_{lab}"] - rt
    obs["signal"], obs["family"] = spec.name, spec.family
    obs["oos"] = obs["ts"] >= OOS_START

    lab = spec.primary_horizon
    res = {"signal": spec.name, "family": spec.family, "hypothesis": spec.hypothesis, "trade": spec.direction_note,
           "primary_horizon": lab, "n_events": len(obs), "n_assets": obs["asset"].nunique(),
           "first": str(obs["ts"].min().date()), "last": str(obs["ts"].max().date()),
           "median_round_trip_bp": float(obs["round_trip_cost"].median() * 1e4)}
    for part, g in (("IS", obs[~obs["oos"]]), ("OOS", obs[obs["oos"]]), ("ALL", obs)):
        v = g[[f"net_{lab}", f"fwd_{lab}", f"hedged_{lab}", f"funding_{lab}", "cluster"]].dropna(subset=[f"net_{lab}"])
        if len(v) < 5:
            res[f"{part}_n"] = len(v)
            continue
        m, lo, hi = stats.cluster_bootstrap_ci(v[f"net_{lab}"], v["cluster"], np.mean, 500)
        t = stats.cluster_t(v[f"net_{lab}"], v["cluster"])
        res.update({f"{part}_n": len(v), f"{part}_clusters": v["cluster"].nunique(),
                    f"{part}_gross_mean": v[f"fwd_{lab}"].mean(), f"{part}_hedged_mean": v[f"hedged_{lab}"].mean(),
                    f"{part}_funding_mean": v[f"funding_{lab}"].mean(), f"{part}_net_mean": m,
                    f"{part}_net_lo": lo, f"{part}_net_hi": hi, f"{part}_net_median": v[f"net_{lab}"].median(),
                    f"{part}_win": (v[f"net_{lab}"] > 0).mean(), f"{part}_t": t,
                    # primary p: t distribution with (clusters-1) df; wild-cluster bootstrap for few clusters;
                    # normal approximation kept only for comparison with v1
                    f"{part}_p": stats.cluster_p(v[f"net_{lab}"], v["cluster"]),
                    f"{part}_p_wild": stats.wild_cluster_p(v[f"net_{lab}"], v["cluster"]),
                    f"{part}_p_normal": float(2 * (1 - norm.cdf(abs(t)))) if np.isfinite(t) else np.nan,
                    f"{part}_mae_med": g[f"mae_{lab}"].median(), f"{part}_mfe_med": g[f"mfe_{lab}"].median()})
    res["breakeven_rt_bp_ALL"] = float((obs[f"hedged_{lab}"] + obs[f"funding_{lab}"]).mean() * 1e4)
    res["sign_consistent_IS_OOS"] = bool(np.sign(res.get("IS_net_mean", np.nan)) == np.sign(res.get("OOS_net_mean", np.nan)))
    by_year = obs.groupby(obs["ts"].dt.year)[f"net_{lab}"].mean()
    res["years_positive_share"] = float((by_year > 0).mean()) if len(by_year) else np.nan
    res["by_year"] = {int(k): round(float(v), 4) for k, v in by_year.items()}
    for dim in ("btc_regime", "vol_regime"):
        g = obs.groupby(dim, observed=True)[f"net_{lab}"].agg(["mean", "size"])
        res[f"by_{dim}"] = {str(k): (round(float(r["mean"]), 4), int(r["size"])) for k, r in g.iterrows()}
    return res, obs


def event_portfolio(obs: pd.DataFrame, wide: dict, hold: int, max_daily_vol: float = 0.02,
                    min_slots: int = 5) -> pd.Series:
    """Daily net return of a risk-budgeted portfolio of open event positions.

    wide: dict of wide DataFrames (index ts, columns asset): 'open', 'close', 'ret' (close-to-close),
          'funding_day', 'sig20' (prior daily vol).
    Each position:
      * enters at the OPEN of day t+1 (entry-day return = close/open - 1), held `hold` days;
      * is BTC-hedged with beta60 (known at t);
      * pays/receives funding for each held day (longs pay positive funding);
      * is scaled so its ex-ante daily vol is at most max_daily_vol (w = min(1, max_daily_vol / sig20));
      * pays half the round-trip cost (alt leg + BTC hedge leg) on entry and exit days;
      * the BTC hedge leg pays/receives BTC perp funding.
    Missing data (review fix 2026-09-15; v1 silently booked missing returns as 0%):
      * missing entry-day open/close for the asset or BTC -> the event is skipped;
      * a missing close while held -> the position is closed at the last valid close (truncated);
      * a day with no funding print books no funding (a settlement that did not happen is not a return).
    Counts are returned in Series.attrs.
    Capital is split across max(open positions, min_slots) slots, so a single position never carries the whole
    book and daily losses cannot exceed -100%.
    """
    if obs.empty:
        return pd.Series(dtype=float)
    idx = wide["close"].index
    pos_map = {t: i for i, t in enumerate(idx)}
    O, Cl, R = wide["open"], wide["close"], wide["ret"]
    FD, SG = wide["funding_day"], wide["sig20"]
    btc = R.get("BTCUSDT")
    s_sum, s_n = np.zeros(len(idx)), np.zeros(len(idx))
    skipped = truncated = 0
    for r in obs.itertuples():
        i0 = pos_map.get(r.ts)
        if i0 is None or r.asset not in R:
            continue
        days = np.arange(i0 + 1, min(i0 + 1 + hold, len(idx)))
        if not len(days):
            continue
        ra = R[r.asset].to_numpy()[days].astype(float)
        ra[0] = Cl[r.asset].iat[days[0]] / O[r.asset].iat[days[0]] - 1          # next-open entry
        hedged = btc is not None and r.asset != "BTCUSDT"
        rb = np.zeros(len(days))
        fb = np.zeros(len(days))
        if hedged:
            rb = btc.to_numpy()[days].astype(float)
            rb[0] = Cl["BTCUSDT"].iat[days[0]] / O["BTCUSDT"].iat[days[0]] - 1
            fb = FD["BTCUSDT"].to_numpy()[days].astype(float) if "BTCUSDT" in FD else fb
        bad = ~np.isfinite(ra) | ~np.isfinite(rb)
        if bad[0]:
            skipped += 1
            continue
        if bad.any():
            cut = int(np.argmax(bad))
            days, ra, rb, fb = days[:cut], ra[:cut], rb[:cut], fb[:cut]
            truncated += 1
        beta = r.beta60 if np.isfinite(r.beta60) else 1.0
        sig = SG[r.asset].iat[i0]
        w = min(1.0, max_daily_vol / sig) if np.isfinite(sig) and sig > 0 else 0.5
        fund = FD[r.asset].to_numpy()[days].astype(float)
        fund = np.where(np.isfinite(fund), fund, 0.0)
        fb = np.where(np.isfinite(fb), fb, 0.0)
        daily = w * (r.direction * (ra - beta * rb) - r.direction * fund + r.direction * beta * fb)
        daily[0] -= w * r.round_trip_cost / 2
        daily[-1] -= w * r.round_trip_cost / 2
        s_sum[days] += daily
        s_n[days] += 1
    slots = np.maximum(s_n, min_slots)
    out = pd.Series(np.where(s_n > 0, s_sum / slots, 0.0), index=idx)
    out.attrs.update(events_skipped_missing_entry=skipped, positions_truncated_missing_price=truncated)
    return out
