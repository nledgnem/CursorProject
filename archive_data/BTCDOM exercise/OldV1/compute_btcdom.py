#!/usr/bin/env python3
"""
Reconstruct a BTCDOM-style index from the data lake using Binance's component info.

Methodology (Binance Price Component Info):
- Constituents and weights: Fixed list from Binance Futures BTCDOM index.
  Source: https://www.binance.com/en/futures/funding-history/perpetual/index
- Last Index Price (per constituent): Price_i(t) = BTC_close(t) / Constituent_i_close(t).
- Index: S(t) = sum_i (weight_pct_i/100) * Price_i(t); then base 1000 at reference date.

Outputs (all under BTCDOM exercise/):
- btcdom_daily.parquet, btcdom_daily.csv
- btcdom_chart.png
"""

from __future__ import annotations

import argparse
from pathlib import Path
from datetime import date

import polars as pl

# Binance BTCDOM Price Component Info (Weight % and Weight Quantity from Binance Futures)
# https://www.binance.com/en/futures/funding-history/perpetual/index
# (asset_id, weight_pct, weight_quantity) - Weight (Quantity) used for index level like Binance
BINANCE_BTCDOM_COMPONENTS = [
    ("ETH", 42.82, 64.55287400),
    ("XRP", 15.11, 0.01591236),
    ("BNB", 14.82, 6.80709263),
    ("SOL", 8.64, 0.55352779),
    ("TRX", 4.67, 0.00098125),
    ("DOGE", 2.94, 0.00021508),
    ("ADA", 1.85, 0.00039858),
    ("BCH", 1.70, 0.62672093),
    ("LINK", 1.13, 0.00765006),
    ("XLM", 0.92, 0.00011028),
    ("HBAR", 0.76, 0.00005645),
    ("LTC", 0.75, 0.03106884),
    ("AVAX", 0.71, 0.00484796),
    ("ZEC", 0.70, 0.12753188),
    ("SUI", 0.64, 0.00044956),
    ("DOT", 0.48, 0.00055799),
    ("UNI", 0.44, 0.00129526),
    ("TAO", 0.33, 0.04510682),
    ("AAVE", 0.31, 0.02645900),
    ("SKY", 0.28, 0.00001418),
]

# Default paths from repo root
DEFAULT_DATA_LAKE = Path("data/curated/data_lake")
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent
TOP_N = 20
REFERENCE_DATE_OPTIONAL = None  # None = use first date in series
BTC_ID = "BTC"


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def load_data(
    data_lake_dir: Path,
    start_date: date | None = None,
    end_date: date | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Load fact_price, fact_marketcap, dim_asset (read-only)."""
    data_lake_dir = Path(data_lake_dir)
    if not data_lake_dir.is_absolute():
        data_lake_dir = _project_root() / data_lake_dir

    price_path = data_lake_dir / "fact_price.parquet"
    mcap_path = data_lake_dir / "fact_marketcap.parquet"
    dim_path = data_lake_dir / "dim_asset.parquet"

    if not price_path.exists():
        raise FileNotFoundError(f"fact_price.parquet not found: {price_path}")
    if not mcap_path.exists():
        raise FileNotFoundError(f"fact_marketcap.parquet not found: {mcap_path}")
    if not dim_path.exists():
        raise FileNotFoundError(f"dim_asset.parquet not found: {dim_path}")

    prices = pl.read_parquet(price_path)
    mcap = pl.read_parquet(mcap_path)
    dim_asset = pl.read_parquet(dim_path)

    if start_date:
        sd = pl.date(start_date.year, start_date.month, start_date.day)
        prices = prices.filter(pl.col("date") >= sd)
        mcap = mcap.filter(pl.col("date") >= sd)
    if end_date:
        ed = pl.date(end_date.year, end_date.month, end_date.day)
        prices = prices.filter(pl.col("date") <= ed)
        mcap = mcap.filter(pl.col("date") <= ed)

    return prices, mcap, dim_asset


def verify_major_assets(
    prices: pl.DataFrame,
    mcap: pl.DataFrame,
    symbols: list[str] = ("BTC", "ETH", "BNB"),
) -> None:
    """Optional: print date-range coverage for BTC and a few major alts."""
    print("Verification (major assets):")
    for sym in symbols:
        p = prices.filter(pl.col("asset_id") == sym)
        m = mcap.filter(pl.col("asset_id") == sym)
        if p.height == 0 and m.height == 0:
            print(f"  {sym}: no data")
        else:
            dates_p = p["date"].min(), p["date"].max() if p.height else (None, None)
            dates_m = m["date"].min(), m["date"].max() if m.height else (None, None)
            print(f"  {sym}: price {dates_p[0]}..{dates_p[1]}, mcap {dates_m[0]}..{dates_m[1]}")


def compute_btcdom_series(
    prices: pl.DataFrame,
    mcap: pl.DataFrame | None,
    dim_asset: pl.DataFrame | None,
    reference_date: date | None = REFERENCE_DATE_OPTIONAL,
    use_quantity_weights: bool = True,
    match_date: date | None = None,
    match_value: float | None = None,
) -> pl.DataFrame:
    """
    Compute daily BTCDOM index using Binance Price Component Info.

    Last Index Price per constituent: Price_i(t) = BTC_close(t) / Constituent_i_close(t).
    If use_quantity_weights: raw S(t) = sum_i (weight_quantity_i * Price_i(t)) [Binance-style].
    Else: raw S(t) = sum_i (weight_pct_i/100) * Price_i(t) with renormalization when constituents missing.

    Divisor: if match_date and match_value set, divisor = raw_s(match_date)/match_value so index matches
    Binance scale; else divisor = raw_s(reference_date)/1000 (base 1000).
    """
    # BTC daily close
    btc_prices = prices.filter(pl.col("asset_id") == BTC_ID).select(
        pl.col("date").alias("date"),
        pl.col("close").alias("btc_close"),
    )
    all_dates = sorted(set(btc_prices["date"].to_list()) & set(prices["date"].to_list()))
    if not all_dates:
        return pl.DataFrame(
            schema={
                "date": pl.Date,
                "btcdom_index": pl.Float64,
                "constituent_count": pl.UInt32,
                "divisor": pl.Float64,
            }
        )

    constituent_ids = [c[0] for c in BINANCE_BTCDOM_COMPONENTS]
    weight_pcts = [c[1] for c in BINANCE_BTCDOM_COMPONENTS]
    weight_quantities = [c[2] for c in BINANCE_BTCDOM_COMPONENTS]

    rows = []
    for d in all_dates:
        btc_row = btc_prices.filter(pl.col("date") == d)
        if btc_row.height == 0:
            continue
        btc_close = float(btc_row["btc_close"][0])
        if btc_close <= 0 or not pl.Series([btc_close]).is_finite().all():
            continue

        p_d = prices.filter(pl.col("date") == d).filter(
            pl.col("asset_id").is_in(constituent_ids)
        ).select("asset_id", "close")
        p_d = p_d.filter(pl.col("close").is_finite() & (pl.col("close") > 0))
        if p_d.height == 0:
            continue

        available = set(p_d["asset_id"].to_list())
        weights_pct = []
        weights_qty = []
        prices_available = []
        sum_weight_pct = 0.0
        for aid, wp, wq in zip(constituent_ids, weight_pcts, weight_quantities):
            if aid not in available:
                continue
            row = p_d.filter(pl.col("asset_id") == aid)
            if row.height == 0:
                continue
            close_val = float(row["close"][0])
            weights_pct.append(wp)
            weights_qty.append(wq)
            prices_available.append(btc_close / close_val)
            sum_weight_pct += wp
        if sum_weight_pct <= 0:
            continue

        if use_quantity_weights:
            # Binance-style: raw = sum(weight_quantity_i * Last_Index_Price_i)
            s_t = sum(wq * px for wq, px in zip(weights_qty, prices_available))
        else:
            s_t = sum((wp / sum_weight_pct) * px for wp, px in zip(weights_pct, prices_available))
        if not (s_t > 0 and pl.Series([s_t]).is_finite().all()):
            continue

        rows.append(
            {
                "date": d,
                "raw_s": s_t,
                "constituent_count": len(weights_pct),
            }
        )

    if not rows:
        return pl.DataFrame(
            schema={
                "date": pl.Date,
                "btcdom_index": pl.Float64,
                "constituent_count": pl.UInt32,
                "divisor": pl.Float64,
            }
        )

    daily = pl.DataFrame(rows)

    # Divisor: match Binance scale (match_date + match_value) or base 1000
    if match_date is not None and match_value is not None and match_value > 0:
        ref_val = pl.date(match_date.year, match_date.month, match_date.day)
        ref_row = daily.filter(pl.col("date") == ref_val)
        if ref_row.height == 0:
            # use latest date we have as fallback
            ref_row = daily.filter(pl.col("date") == daily["date"].max())
        if ref_row.height > 0:
            divisor = float(ref_row["raw_s"][0]) / match_value
        else:
            divisor = 1.0
    else:
        ref = reference_date
        if ref is None:
            ref_val = daily["date"].min()
        else:
            ref_val = pl.date(ref.year, ref.month, ref.day)
        ref_row = daily.filter(pl.col("date") == ref_val)
        if ref_row.height == 0:
            ref_val = daily["date"].min()
            ref_row = daily.filter(pl.col("date") == ref_val)
        if ref_row.height == 0:
            divisor = 1.0
        else:
            divisor = float(ref_row["raw_s"][0]) / 1000.0
        if divisor <= 0 or not pl.Series([divisor]).is_finite().all():
            divisor = 1.0

    daily = daily.with_columns(
        (pl.col("raw_s") / divisor).alias("btcdom_index"),
        pl.lit(divisor).alias("divisor"),
    ).select("date", "btcdom_index", "constituent_count", "divisor")

    return daily


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reconstruct BTCDOM-style index from data lake."
    )
    parser.add_argument(
        "--data-path",
        type=Path,
        default=DEFAULT_DATA_LAKE,
        help="Path to data lake dir (default: data/curated/data_lake)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for outputs (default: script directory)",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=TOP_N,
        help="Ignored when using Binance component list (kept for compatibility)",
    )
    parser.add_argument(
        "--reference-date",
        type=str,
        default=None,
        help="Reference date for base 1000 (YYYY-MM-DD). Default: first date in series.",
    )
    parser.add_argument(
        "--no-verify",
        action="store_true",
        help="Skip verification of BTC/ETH/BNB coverage",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Start date for series and chart (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="End date for series and chart (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--match-binance-date",
        type=str,
        default=None,
        help="Date to align index to Binance scale (YYYY-MM-DD). Use with --match-binance-value.",
    )
    parser.add_argument(
        "--match-binance-value",
        type=float,
        default=None,
        help="Binance index value on match date (e.g. 5112.7). Sets divisor so our index equals this on that date.",
    )
    parser.add_argument(
        "--no-quantity-weights",
        action="store_true",
        help="Use weight %% instead of weight quantity (default: quantity, like Binance)",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    if not output_dir.is_absolute():
        output_dir = _project_root() / output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    ref_date = None
    if args.reference_date:
        parts = args.reference_date.split("-")
        if len(parts) == 3:
            ref_date = date(int(parts[0]), int(parts[1]), int(parts[2]))

    start_date = None
    if args.start_date:
        p = args.start_date.split("-")
        if len(p) == 3:
            start_date = date(int(p[0]), int(p[1]), int(p[2]))
    end_date = None
    if args.end_date:
        p = args.end_date.split("-")
        if len(p) == 3:
            end_date = date(int(p[0]), int(p[1]), int(p[2]))

    match_date = None
    if args.match_binance_date:
        p = args.match_binance_date.split("-")
        if len(p) == 3:
            match_date = date(int(p[0]), int(p[1]), int(p[2]))
    match_value = args.match_binance_value

    print("Loading data...")
    prices, mcap, dim_asset = load_data(args.data_path, start_date=start_date, end_date=end_date)
    print(f"  fact_price: {prices.shape[0]} rows")
    print(f"  fact_marketcap: {mcap.shape[0]} rows")
    print(f"  dim_asset: {dim_asset.shape[0]} assets")

    if not args.no_verify:
        verify_major_assets(prices, mcap)

    use_qty = not args.no_quantity_weights
    print("Computing BTCDOM series (Binance components, %s weights)..." % ("quantity" if use_qty else "pct"))
    daily = compute_btcdom_series(
        prices, mcap, dim_asset,
        reference_date=ref_date,
        use_quantity_weights=use_qty,
        match_date=match_date,
        match_value=match_value,
    )
    if daily.height == 0:
        print("No output: no valid dates after applying methodology.")
        return

    # Outputs
    daily_parquet = output_dir / "btcdom_daily.parquet"
    daily_csv = output_dir / "btcdom_daily.csv"
    daily.write_parquet(daily_parquet)
    daily.write_csv(daily_csv)
    print(f"Wrote {daily_parquet}")
    print(f"Wrote {daily_csv}")

    # Chart
    try:
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates

        fig, ax = plt.subplots(figsize=(12, 5))
        ax.plot(daily["date"].to_list(), daily["btcdom_index"].to_list(), color="#f0b90b")
        ax.set_xlabel("Date")
        ax.set_ylabel("BTCDOM index (base 1000)")
        title = "BTCDOM-style index (reconstructed from data lake)"
        if start_date or end_date:
            title += f" ({start_date or 'start'} to {end_date or 'end'})"
        ax.set_title(title)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
        plt.xticks(rotation=45)
        plt.tight_layout()
        if start_date or end_date:
            chart_path = output_dir / "btcdom_chart_2024_07_2026_01.png"
        else:
            chart_path = output_dir / "btcdom_chart.png"
        plt.savefig(chart_path, dpi=150)
        plt.close()
        print(f"Wrote {chart_path}")
    except Exception as e:
        print(f"Could not save chart: {e}")

    print("Done.")


if __name__ == "__main__":
    main()
