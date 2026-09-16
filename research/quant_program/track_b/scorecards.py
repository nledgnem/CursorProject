"""Build standard anomaly scorecards and Alpha Quality Scores from Track B results.

    python -m track_b.scorecards            (after track_b.run)

Mechanism text is taken from STRUCTURAL_ALPHA_MAP.md (written before results). Component
scores are rule-based from the result tables so the ranking is reproducible, not hand-tuned.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from qlib import scoring  # noqa: E402
from qlib.paths import TABLES  # noqa: E402

T = TABLES / "track_b"

FAMILY = {
    "funding": dict(
        mechanism="Leverage demand exceeds arbitrage capital; funding prices the imbalance and crowded payers "
                  "become marginal sellers (or buyers) when momentum stalls.",
        counterparty="Levered directional traders (mostly retail / trend followers) paying funding to hold exposure.",
        why_they_trade="Access to leverage and convexity matters more to them than carry cost.",
        why_exists="Delta-neutral capital is balance-sheet, venue and collateral constrained; the short-perp leg can be "
                   "squeezed before funding accrues.",
        why_persists="Carry trades are unpleasant (squeeze risk, exchange risk, capital lock-up across venues).",
        what_kills="Cheap institutional basis capital, lower leverage caps, funding caps, fee changes.",
        half_life="days to weeks", capacity="high for majors; limited by alt-perp liquidity", liquidity="perp-dependent",
        execution_difficulty="medium (daily rebalance, perp margin management)",
        venue_risk="single-exchange perp exposure (Binance); funding-cap rule changes",
        data_quality="good: exchange-native funding prints 2019-> (all perps incl. delisted)",
        prior_class="PERSISTENT RISK PREMIUM", mech_score=4, impl=4, cpty=3),
    "basis": dict(
        mechanism="Perp premium vs index is a real-time imbalance gauge; extremes mean-revert as arbitrage closes it.",
        counterparty="Aggressive perp takers paying up (or dumping) relative to spot index.",
        why_they_trade="Speed and leverage preference; perp is the default retail instrument.",
        why_exists="Spot-perp arbitrage needs spot inventory/borrow and two-venue capital.",
        why_persists="Capital friction and short-horizon squeeze risk.",
        what_kills="More arbitrage capital, index methodology changes.",
        half_life="hours to days", capacity="medium", liquidity="perp + spot needed for pure basis",
        execution_difficulty="medium", venue_risk="exchange + index methodology",
        data_quality="good: Binance premiumIndexKlines 2020->", prior_class="STRUCTURAL ALPHA", mech_score=3, impl=4, cpty=3),
    "listings": dict(
        mechanism="New listings draw attention-driven buying while early holders and market makers sell into new "
                  "liquidity; shorting only becomes possible once the perp exists.",
        counterparty="Attention-driven retail buyers and listing momentum chasers.",
        why_they_trade="Salience of new listings; FOMO; exchange promotion.",
        why_exists="Short supply is scarce at launch; unlock/airdrop holders need exit liquidity.",
        why_persists="Shorting new listings is dangerous (thin books, squeezes, funding spikes).",
        what_kills="Pre-listing price discovery elsewhere (DEX, other CEX perps), tighter listing standards.",
        half_life="weeks", capacity="low-medium (thin early books)", liquidity="thin in first days",
        execution_difficulty="high (day-1 volatility, funding spikes)", venue_risk="exchange + delisting risk",
        data_quality="medium: listing dates from first kline; no announcement timestamps",
        prior_class="BEHAVIORAL", mech_score=3, impl=3, cpty=3),
    "mechanics": dict(
        mechanism="Exchanges shorten funding intervals under extreme funding, accelerating payments by the crowded side.",
        counterparty="Crowded side forced to pay more frequently.",
        why_they_trade="Positions already on; switching costs.",
        why_exists="Rule change is mechanical and public but rarely monitored.",
        why_persists="Few participants track contract-spec changes systematically.",
        what_kills="Rule changes; wider adoption of spec monitoring.",
        half_life="days", capacity="low-medium", liquidity="alt perps", execution_difficulty="medium",
        venue_risk="exchange rule changes", data_quality="medium: interval changes inferred from print spacing",
        prior_class="STRUCTURAL ALPHA", mech_score=3, impl=4, cpty=3),
    "liquidation": dict(
        mechanism="Margin engines liquidate regardless of price: flushes overshoot when fuel is exhausted (OI reset) "
                  "and cascade when leverage keeps rebuilding.",
        counterparty="Liquidated leveraged traders and the insurance fund / ADL counterparties.",
        why_they_trade="Forced: margin calls, not choice.",
        why_exists="Liquidity providers widen during cascades; risk capital is scarce exactly when needed.",
        why_persists="Catching liquidation flushes is psychologically and operationally hard (gap risk).",
        what_kills="Lower leverage, better liquidation engines (partial liquidations), more market-making capital.",
        half_life="hours to days", capacity="medium (flush depth is large for majors)", liquidity="stressed at entry",
        execution_difficulty="high (enters during volatility)", venue_risk="exchange outages during cascades",
        data_quality="medium: CoinGlass aggregated daily, venue coverage spliced; pre-2021 zeros excluded",
        prior_class="FORCED FLOW", mech_score=4, impl=3, cpty=3),
    "oi_price": dict(
        mechanism="Price/OI quadrants separate new-leverage-driven moves from covering/capitulation.",
        counterparty="Late levered momentum traders or capitulating longs.",
        why_they_trade="Momentum chasing; margin pressure.", why_exists="Crowding is observable but slow to resolve.",
        why_persists="Signal is noisy and regime-dependent.", what_kills="Wider use of OI dashboards.",
        half_life="days", capacity="medium", liquidity="perp", execution_difficulty="medium",
        venue_risk="exchange", data_quality="medium: aggregated OI across venues (spliced)",
        prior_class="BEHAVIORAL", mech_score=2, impl=4, cpty=3),
    "interaction": dict(
        mechanism="Crowding (funding + OI) combined with fuel (liquidations / extension) creates asymmetric reversal "
                  "or continuation risk.",
        counterparty="The crowded, levered side.", why_they_trade="Late momentum / trapped positions.",
        why_exists="Requires combining several data sources few monitor jointly.",
        why_persists="Rare events; hard to size; painful to hold against crowded momentum.",
        what_kills="Wider multi-factor monitoring; lower leverage.",
        half_life="days", capacity="low-medium (rare events)", liquidity="perp", execution_difficulty="medium-high",
        venue_risk="exchange", data_quality="medium (inherits CoinGlass limitations)",
        prior_class="FORCED FLOW", mech_score=4, impl=3, cpty=3),
    "cross_venue": dict(
        mechanism="US USD spot demand (Coinbase) vs offshore USDT liquidity; premium indicates regional flow.",
        counterparty="Offshore liquidity providers slow to reprice US flow.",
        why_they_trade="Regional access constraints.", why_exists="Capital controls / USD rails / USDT FX.",
        why_persists="Transfer frictions.", what_kills="Faster cross-venue arbitrage; ETF plumbing.",
        half_life="hours to days", capacity="high (BTC)", liquidity="deep", execution_difficulty="low",
        venue_risk="low", data_quality="medium: daily closes cannot separate stale prints; USDT FX component",
        prior_class="STRUCTURAL ALPHA", mech_score=2, impl=5, cpty=4),
}
FAMILY["carry"] = dict(FAMILY["funding"], mechanism="Delta-neutral: short perp / long spot to collect extreme funding "
                       "minus premium change and four legs of costs.", execution_difficulty="high (two legs, spot inventory, "
                       "perp margin, squeeze risk)", prior_class="PERSISTENT RISK PREMIUM", mech_score=4, impl=2, cpty=3)


def _num(x, default=np.nan):
    try:
        v = float(x)
        return v if np.isfinite(v) else default
    except (TypeError, ValueError):
        return default


def evidence_gates(signal: str, family: str, g: dict, ctl: pd.DataFrame, port_oos: pd.Series) -> dict:
    """Skepticism gates added after the first scoring pass showed the rubric rewarding momentum and outliers.

    Each gate caps the recommendation; none changes the AQS, so the ranking stays comparable.
      * multiple testing: OOS BH-adjusted p >= 0.10            -> max PAPER TRADE
      * price-only control: >=50% of OOS events have a matched
        control and the excess-over-control CI includes zero   -> max WATCH
      * concentration: top-5 OOS events >= 50% of the P&L sum  -> max WATCH
      * implementation: event mean > 0 but OOS vol-scaled
        portfolio Sharpe <= 0                                   -> max WATCH
      * control signals are baselines                           -> REJECT (never traded)
    """
    gates = {}
    if family == "control":
        return {"control signal (baseline only)": "REJECT"}
    p_bh = _num(g.get("OOS_p_bh"))
    if not np.isfinite(p_bh) or p_bh >= 0.10:
        gates[f"OOS BH p={p_bh:.2f} (>=0.10)"] = "PAPER TRADE"
    if signal in ctl.index:
        c = ctl.loc[signal]
        lo = _num(c.get("excess_lo"))
        if _num(c.get("control_coverage"), 0) >= 0.5 and not lo > 0:
            label = ("too few events for a price-only control test" if not np.isfinite(lo) else
                     f"excess over price-only control {_num(c.get('excess_mean')):+.2%} "
                     f"[{lo:+.2%}, {_num(c.get('excess_hi')):+.2%}] not above 0")
            gates[label] = "WATCH"
        if _num(c.get("top5_share"), 0) >= 0.5:
            gates[f"top-5 events = {_num(c.get('top5_share')):.0%} of OOS P&L"] = "WATCH"
    sh = _num(port_oos.get(signal))
    if _num(g.get("OOS_net_mean")) > 0 and np.isfinite(sh) and sh <= 0:
        gates[f"positive event mean but OOS portfolio Sharpe {sh:.2f}"] = "WATCH"
    return gates


VERDICTS = ["CONTROL", "REJECT AS TRADE", "INSUFFICIENT EVIDENCE", "NO INCREMENTAL STRUCTURAL ALPHA",
            "FORWARD TEST ONLY", "WATCH"]


def verdict(family: str, g: dict, ctl: pd.DataFrame, signal: str, port_is: pd.Series, port_oos: pd.Series) -> str:
    """Rule-based research verdict (added after external review so a high AQS cannot override contradictory
    evidence). Rules are applied in order; the first that matches wins.
      CONTROL                          price-only baselines
      INSUFFICIENT EVIDENCE            fewer than 30 OOS events
      REJECT AS TRADE                  OOS net mean <= 0, or vol-scaled portfolio Sharpe <= 0 in BOTH IS and OOS
      NO INCREMENTAL STRUCTURAL ALPHA  >= 50% matched to the price control, the matched populations are
                                       comparable (|SMD| <= 0.5 on log ADV, beta and year), and the joint-bootstrap
                                       excess CI is not above zero (momentum/reversal conditioning; benchmark)
      FORWARD TEST ONLY                OOS net CI lower bound > 0 after all costs (still fails BH)
      WATCH                            hypothesis alive, evidence weak
    A control that is not like-for-like (e.g. C1 fires on illiquid alts, the controls live in the CoinGlass top-55)
    can neither confirm nor refute incremental alpha, so it does not trigger NO INCREMENTAL.
    """
    if family == "control":
        return "CONTROL"
    if _num(g.get("OOS_n"), 0) < 30:
        return "INSUFFICIENT EVIDENCE"
    s_is, s_oos = _num(port_is.get(signal)), _num(port_oos.get(signal))
    if not _num(g.get("OOS_net_mean")) > 0 or (np.isfinite(s_is) and np.isfinite(s_oos) and s_is <= 0 and s_oos <= 0):
        return "REJECT AS TRADE"
    if signal in ctl.index:
        c = ctl.loc[signal]
        smd = [abs(_num(c.get(k))) for k in ("smd_log_adv", "smd_beta", "smd_year")]
        comparable = all(np.isfinite(s) and s <= 0.5 for s in smd)
        if _num(c.get("control_coverage"), 0) >= 0.5 and comparable and not _num(c.get("excess_lo")) > 0:
            return "NO INCREMENTAL STRUCTURAL ALPHA"
    if _num(g.get("OOS_net_lo")) > 0:
        return "FORWARD TEST ONLY"
    return "WATCH"


VERDICT_CAP = {"CONTROL": "REJECT", "REJECT AS TRADE": "REJECT", "INSUFFICIENT EVIDENCE": "WATCH",
               "NO INCREMENTAL STRUCTURAL ALPHA": "WATCH"}


def build() -> pd.DataFrame:
    res = pd.read_csv(T / "signal_results.csv")
    ev = pd.read_parquet(T / "events.parquet") if (T / "events.parquet").exists() else pd.DataFrame()
    adv = ev.groupby("signal")["adv30"].median() if len(ev) else pd.Series(dtype=float)
    ctl_path = T / "control_matched_excess.csv"
    if not ctl_path.exists():
        from track_b import controls
        controls.build()
    ctl = pd.read_csv(ctl_path)
    ctl = ctl[ctl["window"] == "OOS"].set_index("signal")
    pst = pd.read_csv(T / "portfolio_stats.csv")
    port_oos = pst[pst["period"] == "OOS"].set_index("signal")["Sharpe"]
    port_is = pst[pst["period"] == "IS"].set_index("signal")["Sharpe"]
    cards, verdicts = [], {}
    for r in res.itertuples():
        fam_key = "carry" if str(r.signal).startswith("F1c") else r.family
        fam = FAMILY.get(fam_key, FAMILY["oi_price"])
        g = r._asdict()
        oos_t, is_t = _num(g.get("OOS_t")), _num(g.get("IS_t"))
        oos_m, is_m = _num(g.get("OOS_net_mean")), _num(g.get("IS_net_mean"))
        n_oos, n_all = _num(g.get("OOS_n"), 0), _num(g.get("n_events"), 0)
        consistent = np.isfinite(oos_m) and oos_m > 0 and (not np.isfinite(is_m) or is_m > 0)
        oos_score = scoring.score_oos(oos_t, consistent) if n_oos >= 20 else min(1, scoring.score_oos(oos_t, consistent))
        be, rt = _num(g.get("breakeven_rt_bp_ALL")), _num(g.get("median_round_trip_bp"), 20)
        ratio = be / rt if rt > 0 else np.nan
        cost = 0 if not np.isfinite(ratio) or ratio <= 0.5 else 1 if ratio < 1 else 2 if ratio < 1.5 else 3 if ratio < 2 else 4 if ratio < 3 else 5
        yps = _num(g.get("years_positive_share"))
        stab = 1 if not np.isfinite(yps) else 4 if yps >= 0.7 else 3 if yps >= 0.6 else 2 if yps >= 0.5 else 1
        reg = 2
        by_reg = g.get("by_btc_regime")
        if isinstance(by_reg, str) and by_reg.startswith("{"):
            d = ast.literal_eval(by_reg)
            signs = [v[0] > 0 for v in d.values() if v[1] >= 10]
            reg = 4 if signs and all(signs) else 2 if any(signs) else 0
        a = _num(adv.get(r.signal))
        cap = 1 if not np.isfinite(a) else 5 if a >= 5e8 else 4 if a >= 1e8 else 3 if a >= 3e7 else 2 if a >= 1e7 else 1
        if np.isfinite(is_m) and np.isfinite(oos_m):
            decay = 5 if is_m > 0 and oos_m >= is_m else 4 if is_m > 0 and oos_m >= 0.5 * is_m else 3 if oos_m > 0 else \
                1 if is_m > 0 else 0
        else:
            decay = 1
        comp = {"mechanism": fam["mech_score"], "oos_significance": oos_score, "regime_robust": reg,
                "param_stability": stab, "cost_resilience": cost, "capacity": cap,
                "sample_size": scoring.score_sample_size(int(n_all)), "decay": decay,
                "implementation": fam["impl"], "counterparty_risk": fam["cpty"]}
        noise = (not np.isfinite(oos_m) or oos_m <= 0) and (not np.isfinite(is_m) or is_m <= 0)
        conf = "HIGH" if oos_t >= 2.5 and consistent and n_all >= 100 else "MEDIUM" if oos_t >= 1.65 and consistent else "LOW"
        card = scoring.Scorecard(
            name=r.signal, family=r.family, mechanism=fam["mechanism"], counterparty=fam["counterparty"],
            why_they_trade=fam["why_they_trade"], why_opportunity_exists=fam["why_exists"],
            why_it_persists=fam["why_persists"], what_kills_it=fam["what_kills"], half_life=fam["half_life"],
            capacity=fam["capacity"] + (f"; median event ADV ${a/1e6:,.0f}m" if np.isfinite(a) else ""),
            liquidity=fam["liquidity"], execution_difficulty=fam["execution_difficulty"], venue_risk=fam["venue_risk"],
            data_quality=fam["data_quality"], classification="LIKELY NOISE" if noise else fam["prior_class"],
            stats={"n": int(n_all), "win_OOS": _num(g.get("OOS_win")), "mae_med_OOS": _num(g.get("OOS_mae_med")),
                   "mfe_med_OOS": _num(g.get("OOS_mfe_med")), "breakeven_rt_bp": be, "median_rt_cost_bp": rt},
            in_sample={"n": _num(g.get("IS_n"), 0), "net_mean": is_m, "median": _num(g.get("IS_net_median")), "t": is_t,
                       "p_bh": _num(g.get("IS_p_bh"))},
            out_of_sample={"n": n_oos, "net_mean": oos_m, "median": _num(g.get("OOS_net_median")), "t": oos_t,
                           "ci": f"[{_num(g.get('OOS_net_lo')):.4f}, {_num(g.get('OOS_net_hi')):.4f}]",
                           "p_bh": _num(g.get("OOS_p_bh"))},
            cost_sensitivity=f"break-even round trip {be:.0f} bp vs median modelled {rt:.0f} bp (ratio {ratio:.2f})"
            if np.isfinite(ratio) else "n/a",
            regime_dependence=str(g.get("by_btc_regime", "")) + " | years positive share " + (f"{yps:.2f}" if np.isfinite(yps) else "n/a"),
            component_scores=comp, confidence=conf)
        card.gates = evidence_gates(r.signal, r.family, g, ctl, port_oos)
        if r.family == "control":
            card.classification = "CONTROL (price-only baseline, not a strategy)"
        v = verdict(r.family, g, ctl, r.signal, port_is, port_oos)
        verdicts[r.signal] = v
        card.notes = f"**Research verdict: {v}**"
        if v in VERDICT_CAP:
            card.gates[f"verdict: {v}"] = VERDICT_CAP[v]
        cards.append(card)
    ranked = scoring.rank(cards)
    ranked["verdict"] = ranked["name"].map(verdicts)
    ranked.to_csv(T / "scorecards.csv", index=False)
    by_name = {c.name: c for c in cards}
    md = ["# Track B scorecards (auto-generated)", ""] + [by_name[n].to_markdown() + "\n" for n in ranked["name"]]
    (T / "scorecards.md").write_text("\n".join(md), encoding="utf-8")
    return ranked


if __name__ == "__main__":
    out = build()
    pd.set_option("display.width", 250)
    pd.set_option("display.max_colwidth", 120)
    print(out[["name", "AQS", "recommendation", "verdict", "gates"]].to_string())
