"""
CLI entrypoint: python -m btcdom_recon --start YYYY-MM-DD --end YYYY-MM-DD --interval 1h --out outdir/
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from .config import MissingDataMode
from .reconstruct import reconstruct_btcdom
from .binance_api import fetch_binance_index_klines
from .validate import align_series, compute_metrics, save_validation_outputs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Reconstruct BTCDOM index from data lake and validate vs Binance index klines."
    )
    p.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    p.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    p.add_argument("--interval", default="1h", help="Candle interval (e.g. 1h, 1d). Default: 1h")
    p.add_argument("--out", default="outdir", type=Path, help="Output directory. Default: outdir/")
    p.add_argument("--data-lake", default=None, type=Path, help="Data lake dir (fact_price.parquet) or path to prices_daily.parquet. Default: auto-try data/curated/data_lake then data/curated/prices_daily.parquet")
    p.add_argument("--ffill-limit", type=int, default=2, help="Forward-fill limit for missing data")
    p.add_argument(
        "--missing-mode",
        choices=[m.value for m in MissingDataMode],
        default=MissingDataMode.RENORMALIZE.value,
        help="Missing data behavior: drop_row or renormalize",
    )
    p.add_argument("--no-plot", action="store_true", help="Skip overlay plot")
    p.add_argument("--demo", action="store_true", help="Use synthetic demo data (no data lake required)")
    p.add_argument("--no-quantity-weights", action="store_true", help="Use weight %% instead of weight quantity (index level then differs from Binance)")
    p.add_argument("--no-cap", action="store_true", help="Disable cap on aberrant last_index_price (can cause spikes from bad constituent data)")
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    start = args.start
    end = args.end
    interval = args.interval
    out_dir = Path(args.out)
    data_lake_path = args.data_lake
    ffill_limit = args.ffill_limit
    missing_mode = MissingDataMode(args.missing_mode)

    if args.demo:
        from .demo_data import generate_demo_fact_price
        demo_dir = Path(__file__).resolve().parent.parent / "demo_data"
        generate_demo_fact_price(start=start, end=end, out_dir=demo_dir)
        data_lake_path = demo_dir
        logger.info("Using demo data from %s", data_lake_path)

    # Map interval to pandas freq
    freq = "1h" if interval == "1h" else "1D" if interval in ("1d", "1D") else interval

    use_quantity_weights = not args.no_quantity_weights
    max_last_index_price = None if args.no_cap else 5_000_000.0
    logger.info("Loading data and reconstructing BTCDOM for %s to %s at %s (%s weights)", start, end, interval, "quantity" if use_quantity_weights else "pct")
    recon = reconstruct_btcdom(
        start,
        end,
        freq,
        ffill_limit=ffill_limit,
        missing_mode=missing_mode,
        use_quantity_weights=use_quantity_weights,
        max_last_index_price=max_last_index_price,
        data_lake_path=data_lake_path,
    )
    if recon.empty:
        logger.error("Reconstruction produced no rows. Check data lake and date range.")
        return 1

    logger.info("Fetching Binance index klines for BTCDOMUSDT %s", interval)
    start_ms = int(__import__("datetime").datetime.strptime(start, "%Y-%m-%d").timestamp() * 1000)
    end_ms = int(__import__("datetime").datetime.strptime(end, "%Y-%m-%d").timestamp() * 1000)
    # End of day
    end_ms += 86400 * 1000 - 1
    binance_df = fetch_binance_index_klines(
        symbol="BTCDOMUSDT",
        interval=interval,
        start_ms=start_ms,
        end_ms=end_ms,
    )

    if binance_df.empty:
        logger.warning("No Binance klines returned; metrics will be empty.")

    merged = align_series(recon, binance_df)
    metrics = compute_metrics(merged)
    metrics["interval"] = interval
    metrics["start"] = start
    metrics["end"] = end

    out_dir.mkdir(parents=True, exist_ok=True)
    save_validation_outputs(
        recon,
        binance_df,
        metrics,
        out_dir,
        plot=not args.no_plot,
    )

    logger.info("Metrics: MAE=%.4f, MAPE=%.2f%%, max_abs_error=%.4f, correlation=%.4f",
                metrics.get("mae") or 0,
                metrics.get("mape_pct") or 0,
                metrics.get("max_abs_error") or 0,
                metrics.get("correlation") or 0)
    logger.info("Outputs written to %s", out_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
