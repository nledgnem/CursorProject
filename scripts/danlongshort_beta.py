from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.danlongshort.funding import estimate_daily_funding_pnl_usd, fetch_binance_usdm_funding_rates
from src.danlongshort.portfolio import (
    btc_rebalance_target_notional,
    compute_30d_betas,
    compute_portfolio_snapshot,
    fetch_30d_closes_usd,
    latest_prices_from_closes,
    load_positions_csv,
    load_symbol_to_coingecko_id,
)


def main() -> None:
    p = argparse.ArgumentParser(description="danlongshort beta-neutral portfolio calculator")
    p.add_argument(
        "--positions",
        type=Path,
        default=Path("/data/danlongshort_positions.csv"),
        help="Positions CSV path (default: /data/danlongshort_positions.csv)",
    )
    p.add_argument(
        "--allowlist",
        type=Path,
        default=REPO_ROOT / "data" / "perp_allowlist.csv",
        help="Symbol->CoinGecko id mapping CSV (default: data/perp_allowlist.csv)",
    )
    p.add_argument(
        "--symbol-map",
        type=Path,
        default=REPO_ROOT / "configs" / "danlongshort_symbol_map.yaml",
        help="Optional symbol->CoinGecko id override YAML (default: configs/danlongshort_symbol_map.yaml)",
    )
    p.add_argument("--no-cache", action="store_true", help="Disable /data cache for CoinGecko closes")
    p.add_argument(
        "--rebalance",
        action="store_true",
        help="Output the exact BTC leg notional required to neutralize beta (adjust BTC only).",
    )
    args = p.parse_args()

    positions = load_positions_csv(args.positions)
    if not positions:
        print("[INFO] No positions found.")
        return

    symbol_to_cg = load_symbol_to_coingecko_id(args.allowlist, override_yaml=args.symbol_map)
    tickers = [p.ticker for p in positions]
    closes = fetch_30d_closes_usd(
        tickers=tickers,
        symbol_to_cg=symbol_to_cg,
        enable_cache=(not args.no_cache),
        cache_max_age_hours=12.0,
    )
    betas = compute_30d_betas(closes, btc_ticker="BTC")
    latest_prices = latest_prices_from_closes(closes)

    tbl, summ = compute_portfolio_snapshot(positions, betas, latest_prices)

    # Funding rates via CCXT (Binance USD-M perps)
    funding = fetch_binance_usdm_funding_rates([p.ticker for p in positions])
    funding_rows: list[dict] = []
    net_funding_daily = 0.0
    net_funding_known = False
    for pos in positions:
        r8h = funding.get(pos.ticker)
        pnl_d = estimate_daily_funding_pnl_usd(notional_usd=pos.notional_usd, direction=pos.direction, funding_rate_per_8h=r8h)
        if pnl_d is not None:
            net_funding_daily += pnl_d
            net_funding_known = True
        funding_rows.append({"ticker": pos.ticker, "funding_rate_per_8h": r8h if r8h is not None else pd.NA, "est_daily_funding_pnl_usd": pnl_d if pnl_d is not None else pd.NA})

    # Print per-position table
    show_cols = [
        "ticker",
        "side",
        "notional_usd",
        "current_price",
        "beta_30d",
        "beta_weighted_exposure_usd",
        "unrealized_pnl_usd",
    ]
    tbl_out = tbl.copy()
    tbl_out = tbl_out.sort_values("notional_usd", key=lambda s: s.abs(), ascending=False)
    print("\nPer-position:")
    if not tbl_out.empty:
        print(tbl_out[show_cols].to_string(index=False))
    else:
        print("(empty)")

    print("\nFunding (Binance USD-M, per 8h):")
    fdf = pd.DataFrame(funding_rows).sort_values("ticker")
    if not fdf.empty:
        print(fdf.to_string(index=False))
    else:
        print("(empty)")

    if net_funding_known:
        print(f"\nNet funding est (daily USD): {net_funding_daily:,.2f}")
    else:
        print("\nNet funding est (daily USD): n/a (missing funding rates)")

    # Portfolio summary + BTC adjustment
    net_beta = float(summ.get("net_beta_exposure_usd") or 0.0)
    gross = float(summ.get("gross_notional_usd") or 0.0)
    net_notional = float(summ.get("net_notional_usd") or 0.0)
    lsr = float(summ.get("net_long_short_ratio") or float("nan"))
    btc_adj = float(summ.get("btc_adjustment_usd_to_neutral") or 0.0)

    print("\nPortfolio:")
    print(f"- Net portfolio beta exposure (vs BTC): {net_beta:,.2f} USD")
    print(f"- Gross notional: {gross:,.2f} USD")
    print(f"- Net notional (longs - shorts): {net_notional:,.2f} USD")
    print(f"- Net long/short ratio: {lsr * 100:,.2f}%")
    print(f"- Suggested BTC leg adjustment to beta-neutral (USD notional): {btc_adj:,.2f}")

    if args.rebalance:
        reb = btc_rebalance_target_notional(positions, betas)
        print("\n--rebalance (adjust BTC only):")
        print(f"- Other legs beta exposure: {float(reb['other_exposure_usd']):,.2f} USD")
        print(f"- Current BTC beta exposure: {float(reb['current_btc_exposure_usd']):,.2f} USD")
        print(f"- Required BTC leg: {reb['required_btc_side']} {float(reb['required_btc_notional_usd']):,.2f} USD notional")


if __name__ == "__main__":
    main()

