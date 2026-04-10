from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.danlongshort.alert_ops import build_portfolio_snapshot_text
from src.danlongshort.config_loader import load_danlongshort_alerts_config
from src.danlongshort.funding import fetch_binance_usdm_funding_rates, estimate_daily_funding_pnl_usd
from src.danlongshort.portfolio import (
    Position,
    btc_rebalance_target_notional,
    compute_30d_betas,
    compute_portfolio_snapshot,
    fetch_30d_closes_usd,
    latest_prices_from_closes,
    load_positions_csv,
    load_symbol_to_coingecko_id,
)
from src.danlongshort.storage import (
    POSITIONS_FIELDS,
    append_position_row,
    ensure_positions_csv,
    read_positions_df,
    remove_positions_by_ticker,
    write_positions_df_atomic,
)


def _color_for_beta_imbalance(imbalance_usd: float) -> tuple[str, str]:
    a = abs(float(imbalance_usd))
    if a < 1_000:
        return "#16a34a", "Near-neutral"
    if a < 5_000:
        return "#f59e0b", "Moderate"
    return "#dc2626", "High"


def _side_dir(side: str) -> float:
    return 1.0 if str(side).strip().upper() == "LONG" else -1.0


def main() -> None:
    st.set_page_config(page_title="danlongshort", layout="wide")
    st.title("danlongshort")
    st.caption("Render-first beta-neutral L/S (independent). Uses live CoinGecko 30d betas + Binance funding via CCXT.")

    cfg = load_danlongshort_alerts_config(REPO_ROOT)
    ensure_positions_csv(cfg.positions_csv)
    symbol_to_cg = load_symbol_to_coingecko_id(cfg.allowlist_csv, override_yaml=cfg.symbol_map_yaml)

    st.divider()

    # ------------------------
    # Section 1: Beta calculator
    # ------------------------
    st.subheader("Beta calculator (read-only)")
    c1, c2, c3, c4 = st.columns([1.1, 1.0, 1.2, 0.9])
    with c1:
        tkr = st.text_input("Ticker", value="BTC").strip().upper()
    with c2:
        side = st.selectbox("Side", options=["LONG", "SHORT"], index=0)
    with c3:
        notional = st.number_input("Notional USD", min_value=0.0, value=10_000.0, step=500.0)
    with c4:
        calc = st.button("Calculate", use_container_width=True)

    if calc and tkr:
        closes = fetch_30d_closes_usd([tkr, "BTC"], symbol_to_cg=symbol_to_cg, enable_cache=True, cache_max_age_hours=12.0)
        betas = compute_30d_betas(closes, btc_ticker="BTC")
        beta = float(betas.get(tkr, float("nan")))
        exposure = float(notional) * _side_dir(side) * (beta if beta == beta else 0.0)
        btc_leg = -exposure
        btc_side = "LONG" if btc_leg >= 0 else "SHORT"
        st.metric("30d beta vs BTC", f"{beta:.2f}" if beta == beta else "n/a")
        st.info(f"To neutralize {side} ${notional:,.0f} {tkr} → {btc_side} ${abs(btc_leg):,.0f} BTC")

    st.divider()

    # ------------------------
    # Section 2: Portfolio manager
    # ------------------------
    st.subheader("Portfolio manager")
    left, right = st.columns([1.4, 1.0])
    with left:
        df = read_positions_df(cfg.positions_csv)
        st.caption(f"Editing: `{cfg.positions_csv}`")
        edited = st.data_editor(
            df,
            num_rows="dynamic",
            use_container_width=True,
            hide_index=True,
            column_config={
                "ticker": st.column_config.TextColumn(required=True),
                "side": st.column_config.SelectboxColumn(options=["LONG", "SHORT"], required=True),
                "notional_usd": st.column_config.NumberColumn(format="%.2f", required=True),
                "entry_price": st.column_config.NumberColumn(format="%.4f"),
                "entry_date": st.column_config.TextColumn(),
            },
        )
        if st.button("Save table to /data", type="primary"):
            try:
                out = pd.DataFrame(edited)
                out = out.loc[:, POSITIONS_FIELDS]
                write_positions_df_atomic(cfg.positions_csv, out)
                st.success("Saved.")
            except Exception as e:
                st.exception(e)

    with right:
        st.markdown("**Add position**")
        at = st.text_input("Add ticker", value="").strip().upper()
        aside = st.selectbox("Add side", options=["LONG", "SHORT"], index=0, key="add_side")
        an = st.number_input("Add notional USD", min_value=0.0, value=5_000.0, step=500.0, key="add_notional")
        ap = st.text_input("Entry price (optional)", value="", key="add_entry_price").strip()
        ad = st.date_input("Entry date", value=date.today(), key="add_entry_date")
        if st.button("Append row"):
            try:
                ep = float(ap) if ap else None
                append_position_row(cfg.positions_csv, ticker=at, side=aside, notional_usd=float(an), entry_price=ep, entry_date=ad.isoformat())
                st.success("Added.")
            except Exception as e:
                st.exception(e)

        st.markdown("**Remove by ticker**")
        rt = st.text_input("Remove ticker", value="").strip().upper()
        if st.button("Remove"):
            try:
                n = remove_positions_by_ticker(cfg.positions_csv, rt)
                st.success(f"Removed rows: {n}")
            except Exception as e:
                st.exception(e)

    st.divider()

    # ------------------------
    # Section 3: Portfolio snapshot (live)
    # ------------------------
    st.subheader("Portfolio snapshot (live)")
    positions = load_positions_csv(cfg.positions_csv)
    if not positions:
        st.info("No positions in CSV.")
        return

    tickers = [p.ticker for p in positions]
    closes = fetch_30d_closes_usd(tickers, symbol_to_cg=symbol_to_cg, enable_cache=True, cache_max_age_hours=12.0)
    betas = compute_30d_betas(closes, btc_ticker="BTC")
    latest = latest_prices_from_closes(closes)
    tbl, summ = compute_portfolio_snapshot(positions, betas, latest)

    funding_rates = fetch_binance_usdm_funding_rates([p.ticker for p in positions])
    tbl = tbl.copy()
    tbl["funding_rate_per_8h"] = tbl["ticker"].map(lambda t: funding_rates.get(str(t).upper()))
    tbl["est_daily_funding_usd"] = tbl.apply(
        lambda r: estimate_daily_funding_pnl_usd(
            notional_usd=float(r["notional_usd"]),
            direction=_side_dir(str(r["side"])),
            funding_rate_per_8h=(float(r["funding_rate_per_8h"]) if pd.notna(r["funding_rate_per_8h"]) else None),
        ),
        axis=1,
    )

    net_beta = float(summ.get("net_beta_exposure_usd") or 0.0)
    gross = float(summ.get("gross_notional_usd") or 0.0)
    net_notional = float(summ.get("net_notional_usd") or 0.0)
    lsr = float(summ.get("net_long_short_ratio") or float("nan"))
    adj = float(summ.get("btc_adjustment_usd_to_neutral") or 0.0)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Net beta exposure (USD)", f"{net_beta:,.0f}")
    c2.metric("Gross notional (USD)", f"{gross:,.0f}")
    c3.metric("Net notional (USD)", f"{net_notional:,.0f}")
    c4.metric("Net L/S %", f"{lsr*100:,.1f}%" if lsr == lsr else "n/a")
    col, label = _color_for_beta_imbalance(adj)
    c5.markdown(
        f"<div style='padding:10px;border-radius:12px;border:1px solid {col};'><b>Neutrality</b><br/>{label}<br/><span style='color:{col};font-weight:800;'>adj {adj:,.0f} USD</span></div>",
        unsafe_allow_html=True,
    )

    st.markdown("**Per-position**")
    st.dataframe(
        tbl.sort_values("notional_usd", key=lambda s: s.abs(), ascending=False),
        use_container_width=True,
        hide_index=True,
    )

    reb = btc_rebalance_target_notional(positions, betas)
    st.markdown("**BTC adjustment (adjust BTC only)**")
    st.write(f"Required BTC leg: {reb['required_btc_side']} ${float(reb['required_btc_notional_usd']):,.0f}")

    with st.expander("Telegram snapshot (HTML) preview"):
        snap = build_portfolio_snapshot_text(cfg) or "[danlongshort] Portfolio snapshot\nNo positions."
        st.code(snap, language="html")


if __name__ == "__main__":
    main()

