from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.apathy_bleed.book import (
    append_book_row,
    book_summary,
    build_short_entry_row,
    format_book_summary_line,
    has_open_long_btc_for_cohort,
    has_open_short_duplicate,
    read_book_rows,
)
from src.apathy_bleed.config_loader import load_apathy_alerts_config


def _parse_date(s: str) -> date:
    return date.fromisoformat(s.strip())


def main() -> None:
    p = argparse.ArgumentParser(description="Log a new Apathy Bleed SHORT leg to the trade book.")
    p.add_argument("--cohort", required=True)
    p.add_argument("--ticker", required=True)
    p.add_argument("--entry-price", type=float, required=True)
    p.add_argument("--notional", type=float, required=True)
    p.add_argument("--entry-date", required=True, help="YYYY-MM-DD (UTC calendar day)")
    p.add_argument("--hold-days", type=int, default=None, help="Default from configs/apathy_alerts.yaml")
    p.add_argument("--notes", default="", help="Optional notes")
    args = p.parse_args()

    cfg = load_apathy_alerts_config(REPO_ROOT)
    hold = int(args.hold_days) if args.hold_days is not None else cfg.hold_days
    book_path = cfg.book_csv

    ticker = args.ticker.strip().upper()
    cohort = args.cohort.strip().upper()
    if not ticker:
        raise SystemExit("ticker must be non-empty")
    if args.entry_price <= 0:
        raise SystemExit("entry-price must be > 0")
    if args.notional <= 0:
        raise SystemExit("notional must be > 0")

    entry_date = _parse_date(args.entry_date)
    rows = read_book_rows(book_path)
    if has_open_short_duplicate(rows, cohort, ticker):
        raise SystemExit(f"Duplicate: OPEN SHORT already exists for {ticker} in {cohort}.")

    row = build_short_entry_row(
        cohort=cohort,
        ticker=ticker,
        entry_price=args.entry_price,
        notional=args.notional,
        entry_date=entry_date,
        hold_days=hold,
        notes=args.notes or None,
    )
    append_book_row(book_path, row)

    qty = float(row["quantity"])
    stop = float(row["stop_price_usd"])
    target = row["exit_date_target_utc"]
    print("Position logged.")
    print(f"  trade_id:           {row['trade_id']}")
    print(f"  cohort / ticker:    {cohort} / {ticker}")
    print(f"  entry_date_utc:     {row['entry_date_utc']}")
    print(f"  entry_price_usd:    {args.entry_price}")
    print(f"  notional_usd:       {args.notional}")
    print(f"  quantity:           {qty}")
    print(f"  stop_price_usd:     {stop} (entry * 1.60)")
    print(f"  exit_date_target:   {target}")
    print(f"  status:             OPEN")

    rows_after = read_book_rows(book_path)
    print(format_book_summary_line(book_summary(rows_after)))

    if has_open_long_btc_for_cohort(rows_after, cohort):
        print(f"BTC hedge already OPEN for {cohort}; skipping hedge prompt.")
        return

    ans = input("Also log the BTC hedge leg for this cohort? (y/n): ").strip().lower()
    if ans in ("y", "yes"):
        raw_px = input("Enter BTC USD mark for hedge leg: ").strip()
        try:
            btc_px = float(raw_px)
        except ValueError:
            raise SystemExit("Invalid BTC price")
        from src.apathy_bleed.trade_logging import log_btc_hedge

        try:
            log_btc_hedge(
                repo_root=REPO_ROOT,
                cohort=cohort,
                btc_price=btc_px,
                entry_date=entry_date,
                hold_days=hold,
                notes="",
            )
        except ValueError as e:
            raise SystemExit(str(e))
        print("BTC hedge row appended.")
        rows_final = read_book_rows(cfg.book_csv)
        print(format_book_summary_line(book_summary(rows_final)))


if __name__ == "__main__":
    main()
