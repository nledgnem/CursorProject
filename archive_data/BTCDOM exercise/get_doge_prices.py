from __future__ import annotations

from datetime import date
from pathlib import Path
import argparse

from btcdom_recon.data import load_prices


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export daily prices for a symbol from the full dataset.")
    parser.add_argument("--symbol", default="DOGE", help="Asset symbol to export (default: DOGE).")
    parser.add_argument("--start", default="2024-01-01", help="Start date YYYY-MM-DD (default: 2024-01-01).")
    parser.add_argument(
        "--end",
        default=None,
        help="End date YYYY-MM-DD (default: today).",
    )
    parser.add_argument(
        "--freq",
        default="1D",
        help='Resample frequency (default: "1D").',
    )
    parser.add_argument(
        "--data-lake",
        dest="data_lake_path",
        default=None,
        help=(
            "Path to data lake directory (with fact_price.parquet) or to prices_daily.parquet. "
            "If omitted, uses the default configured in btcdom_recon."
        ),
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Output CSV path (default: <symbol>_prices_2024_onwards.csv).",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    symbol: str = args.symbol
    start: str = args.start
    end: str = args.end or date.today().isoformat()
    data_lake_path = args.data_lake_path
    freq: str = args.freq

    df = load_prices(
        symbol=symbol,
        start=start,
        end=end,
        freq=freq,
        data_lake_path=data_lake_path,
    )

    default_out_name = f"{symbol.lower()}_prices_2024_onwards.csv"
    out_path = Path(args.out) if args.out is not None else Path(default_out_name)
    df.to_csv(out_path, index=False)

    print(f"Retrieved {len(df)} rows for {symbol} from {start} to {end} at freq={freq}.")
    print(f"Saved to {out_path.resolve()}")


if __name__ == "__main__":
    main()

