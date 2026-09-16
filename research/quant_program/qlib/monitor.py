"""Structural-alpha watchlist monitor (read-only research tool — it never places orders).

Run daily after the data refresh:
    python -m qlib.monitor                 (from research/quant_program)

For each pre-registered Track B signal it lists the assets triggering on the latest
settled day, with the signal's historical evidence and its scorecard recommendation.
Actions: IGNORE / WATCH / RESEARCH / PAPER TRADE / TRADE (TRADE = human decision only).
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from qlib.paths import RESULTS, TABLES  # noqa: E402

OUT = RESULTS / "monitor"
OUT.mkdir(parents=True, exist_ok=True)

ACTION = {"REJECT": "IGNORE", "WATCH": "WATCH", "PAPER TRADE": "PAPER TRADE",
          "SMALL LIVE TEST": "PAPER TRADE", "PRODUCTION CANDIDATE": "TRADE"}
STRENGTH_COLS = {"funding": ["funding_7d_ann", "funding_xs_rank", "funding_ts_z"], "basis": ["prem_close", "prem_z90"],
                 "liquidation": ["long_liq_z", "short_liq_z", "oi_chg_z"], "oi_price": ["ret_z", "oi_chg_z"],
                 "interaction": ["funding_xs_rank", "oi_7d_z", "mom_3d_z"], "listings": ["days_listed"],
                 "mechanics": ["interval_h", "funding_day"], "cross_venue": ["ret_z"]}
MAIN_RISK = {"funding": "squeeze while short / funding flips; exchange funding caps",
             "basis": "premium widens further in trend; index methodology",
             "liquidation": "cascade continues (catching a falling knife); data revisions",
             "oi_price": "regime dependence; OI data splicing across venues",
             "interaction": "small sample; crowding persists longer than capital",
             "listings": "listing pump continues; borrow/short availability", "mechanics": "rule changes",
             "cross_venue": "USD/USDT FX and stale-print artefacts"}


def build_watchlist(panel: pd.DataFrame | None = None, lookback_days: int = 1) -> pd.DataFrame:
    from track_b import studies as S
    from track_b.panel import build_panel
    p = panel if panel is not None else build_panel(save=False)
    latest = p["ts"].max()
    res_path, card_path = TABLES / "track_b" / "signal_results.csv", TABLES / "track_b" / "scorecards.csv"
    res = pd.read_csv(res_path).set_index("signal") if res_path.exists() else pd.DataFrame()
    cards = pd.read_csv(card_path).set_index("name") if card_path.exists() else pd.DataFrame()
    rows = []
    for spec, ev in S.signals(p):
        if spec.family == "control":        # price-only baselines are for evaluation, never for the watchlist
            continue
        live = ev[ev["ts"] > latest - pd.Timedelta(days=lookback_days)]
        if live.empty:
            continue
        info = res.loc[spec.name] if spec.name in res.index else pd.Series(dtype=object)
        card = cards.loc[spec.name] if spec.name in cards.index else pd.Series(dtype=object)
        rec = card.get("recommendation", "RESEARCH" if info.empty else "WATCH")
        # research verdict overrides: benchmarks, rejected trades and thin samples are not watchlist items
        if card.get("verdict") in ("REJECT AS TRADE", "INSUFFICIENT EVIDENCE", "NO INCREMENTAL STRUCTURAL ALPHA"):
            rec = "REJECT"
        for r in live.merge(p, on=["ts", "asset"], how="left", suffixes=("", "_p")).itertuples():
            strength = {c: round(float(getattr(r, c)), 4) for c in STRENGTH_COLS.get(spec.family, [])
                        if hasattr(r, c) and pd.notna(getattr(r, c))}
            rows.append({
                "date": latest.date(), "asset": r.asset, "venue": "Binance USD-M perp", "signal": spec.name,
                "direction": "LONG" if r.direction > 0 else "SHORT", "signal_strength": strength,
                "expected_horizon": spec.primary_horizon,
                "expected_edge_net": info.get("OOS_net_mean", info.get("ALL_net_mean", np.nan)),
                "mechanism_counterparty": spec.hypothesis,
                "historical_n": info.get("n_events", np.nan), "historical_oos_n": info.get("OOS_n", np.nan),
                "historical_win_rate_oos": info.get("OOS_win", np.nan),
                "current_regime": f"BTC {getattr(r, 'btc_regime', '?')}, vol {getattr(r, 'vol_regime', '?')}",
                "capacity_usd_1pct_adv": float(getattr(r, "adv30", np.nan)) * 0.01 if pd.notna(getattr(r, "adv30", np.nan)) else np.nan,
                "main_risk": MAIN_RISK.get(spec.family, ""),
                "aqs": card.get("AQS", np.nan),
                "recommended_action": ACTION.get(rec, rec),
            })
    return pd.DataFrame(rows)


def render_markdown(w: pd.DataFrame) -> str:
    if w.empty:
        return "# Structural Alpha Watchlist\n\nNo pre-registered signal triggered on the latest settled day.\n"
    lines = [f"# Structural Alpha Watchlist — {w['date'].iloc[0]}", "",
             "*Research monitor only. No orders are generated. TRADE requires an explicit human decision.*", "",
             "| Asset | Signal | Dir | Strength | Horizon | Hist. net edge | OOS n | Regime | Capacity (1% ADV) | Main risk | AQS | Action |",
             "|---|---|---|---|---|---:|---:|---|---:|---|---:|---|"]
    order = {"TRADE": 0, "PAPER TRADE": 1, "WATCH": 2, "RESEARCH": 3, "IGNORE": 4}
    for r in w.sort_values(by="recommended_action", key=lambda s: s.map(order).fillna(9)).itertuples():
        edge = f"{r.expected_edge_net:+.2%}" if pd.notna(r.expected_edge_net) else "—"
        cap = f"${r.capacity_usd_1pct_adv:,.0f}" if pd.notna(r.capacity_usd_1pct_adv) else "—"
        lines.append(f"| {r.asset} | {r.signal} | {r.direction} | {r.signal_strength} | {r.expected_horizon} | {edge} | "
                     f"{r.historical_oos_n} | {r.current_regime} | {cap} | {r.main_risk} | {r.aqs} | **{r.recommended_action}** |")
    return "\n".join(lines) + "\n"


def main():
    w = build_watchlist()
    if not w.empty:
        d = w["date"].iloc[0]
        w.to_csv(OUT / f"watchlist_{d}.csv", index=False)
        (OUT / f"watchlist_{d}.md").write_text(render_markdown(w), encoding="utf-8")
    print(render_markdown(w))


if __name__ == "__main__":
    main()
