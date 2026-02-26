#!/usr/bin/env python3
"""
Binance basis-trade data pipeline.
Ranks USDT-margined PERPETUAL pairs using funding-rate history.
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

import pandas as pd

from binance import fetch_all_symbols_funding, fetch_exchange_info
from metrics import compute_metrics_for_windows

try:
    from charts import plot_apr_distribution, plot_symbol_series
    _charts_error: str | None = None
except ImportError as e:
    plot_apr_distribution = None  # type: ignore[misc, assignment]
    plot_symbol_series = None  # type: ignore[misc, assignment]
    _charts_error = str(e)
from scoring import rank_by_apr, rank_by_quality

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Rank Binance USDT-m perps for basis trades")
    p.add_argument("--days", type=int, default=365, help="Lookback days (max window)")
    p.add_argument(
        "--windows",
        type=int,
        nargs="+",
        default=[7, 14, 30, 365],
        help="Windows in days (e.g. 7 14 30 365)",
    )
    p.add_argument(
        "--max-symbols",
        type=int,
        default=0,
        help="Limit symbols for quick tests; 0 = all",
    )
    p.add_argument("--sleep-ms", type=int, default=300, help="Delay between API requests (ms)")
    p.add_argument("--out", type=str, default="./out", help="Output directory")
    p.add_argument(
        "--charts",
        action="store_true",
        help="Generate optional charts",
    )
    p.add_argument(
        "--chart-symbol",
        type=str,
        default="",
        help="Symbol for per-symbol time series chart",
    )
    p.add_argument(
        "--w-neg-frac",
        type=float,
        default=1.0,
        help="Weight for neg_frac in quality score",
    )
    p.add_argument(
        "--w-stdev",
        type=float,
        default=1.0,
        help="Weight for stdev in quality score",
    )
    p.add_argument(
        "--w-top10-share",
        type=float,
        default=1.0,
        help="Weight for top10_share in quality score",
    )
    return p.parse_args()


async def run(args: argparse.Namespace) -> None:
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    data_dir = out_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    import aiohttp

    async with aiohttp.ClientSession() as session:
        symbols_raw = await fetch_exchange_info(session, args.sleep_ms)
        symbols = [s["symbol"] for s in symbols_raw]
        logger.info("Found %d USDT-m PERPETUAL symbols", len(symbols))

    if args.max_symbols > 0:
        symbols = symbols[: args.max_symbols]
        logger.info("Limited to %d symbols", len(symbols))

    end_ms = pd.Timestamp.utcnow().value // 1_000_000
    start_ms = end_ms - args.days * 24 * 60 * 60 * 1000

    from tqdm import tqdm

    logger.info("Fetching funding history for %d symbols...", len(symbols))
    raw_data = await fetch_all_symbols_funding(
        symbols, start_ms, end_ms, sleep_ms=args.sleep_ms
    )

    all_metrics: list[pd.DataFrame] = []
    for symbol, rows in tqdm(raw_data.items(), desc="Computing metrics"):
        if not rows:
            continue
        df = pd.DataFrame(rows)
        # Save raw per-symbol data
        parquet_path = data_dir / f"{symbol}.parquet"
        try:
            df.to_parquet(parquet_path, index=False)
        except Exception as e:
            logger.warning("Could not save parquet for %s: %s", symbol, e)
            df.to_csv(data_dir / f"{symbol}.csv", index=False)

        m = compute_metrics_for_windows(df, args.windows)
        if m.empty:
            continue
        m["symbol"] = symbol
        all_metrics.append(m)

    if not all_metrics:
        logger.error("No metrics computed; exiting")
        return

    combined = pd.concat(all_metrics, ignore_index=True)

    ts = pd.Timestamp.utcnow().strftime("%Y%m%d_%H%M%S")
    rankings_path = out_dir / f"binance_funding_rankings_{ts}.csv"

    w_neg = args.w_neg_frac
    w_stdev = args.w_stdev
    w_top10 = args.w_top10_share

    for window in args.windows:
        subset = combined[combined["window_days"] == window].copy()
        if subset.empty:
            continue

        # Ranking A: apr
        ranked_apr = rank_by_apr(subset)
        # Ranking B: quality
        ranked_qual = rank_by_quality(subset, w_neg, w_stdev, w_top10)

        # Merge both rankings
        merged = ranked_apr[["symbol", "window_days", "funding_return", "apr_simple", "pos_frac", "neg_frac", "stdev", "top10_share", "max_drawdown", "n_prints", "rank_apr"]].merge(
            ranked_qual[["symbol", "quality_score", "rank_quality"]],
            on="symbol",
        )

        # Save top20 for this window
        top20_path = out_dir / f"top20_{window}d.csv"
        merged.head(20).to_csv(top20_path, index=False)
        logger.info("Wrote %s", top20_path)

        # Print top 20 to console
        print(f"\n--- Top 20 by APR (window={window}d) ---")
        display_cols = ["rank_apr", "symbol", "apr_simple", "funding_return", "neg_frac", "stdev", "top10_share"]
        avail = [c for c in display_cols if c in merged.columns]
        print(merged.head(20)[avail].to_string(index=False))

        print(f"\n--- Top 20 by Quality (window={window}d) ---")
        qcols = ["rank_quality", "symbol", "quality_score", "apr_simple", "neg_frac", "stdev", "top10_share"]
        qavail = [c for c in qcols if c in merged.columns]
        qsorted = merged.sort_values("quality_score", ascending=False).reset_index(drop=True)
        print(qsorted.head(20)[qavail].to_string(index=False))

    combined_ranked = combined.copy()
    combined_ranked["rank_apr"] = pd.NA
    combined_ranked["quality_score"] = pd.NA
    combined_ranked["rank_quality"] = pd.NA
    for window in args.windows:
        sub = combined[combined["window_days"] == window]
        if sub.empty:
            continue
        r_apr = rank_by_apr(sub)
        r_q = rank_by_quality(sub, w_neg, w_stdev, w_top10)
        for _, row in r_apr.iterrows():
            mask = (combined_ranked["symbol"] == row["symbol"]) & (combined_ranked["window_days"] == window)
            combined_ranked.loc[mask, "rank_apr"] = row["rank_apr"]
        for _, row in r_q.iterrows():
            mask = (combined_ranked["symbol"] == row["symbol"]) & (combined_ranked["window_days"] == window)
            combined_ranked.loc[mask, "quality_score"] = row["quality_score"]
            combined_ranked.loc[mask, "rank_quality"] = row["rank_quality"]

    combined_ranked.to_csv(rankings_path, index=False)
    logger.info("Wrote %s", rankings_path)

    if args.charts:
        if plot_apr_distribution is None:
            logger.warning("Charts disabled (matplotlib not available): %s", _charts_error or "")
        else:
            for window in args.windows:
                sub = combined[combined["window_days"] == window]
                if not sub.empty:
                    plot_apr_distribution(sub, out_dir, window)
            if args.chart_symbol and args.chart_symbol in raw_data:
                plot_symbol_series(raw_data[args.chart_symbol], args.chart_symbol, out_dir)


def main() -> None:
    args = parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
