from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.apathy_bleed.book import book_summary, format_book_summary_line, read_book_rows
from src.apathy_bleed.config_loader import load_apathy_alerts_config
from src.apathy_bleed.trade_logging import log_btc_hedge


def _parse_date(s: str) -> date:
    return date.fromisoformat(s.strip())


def main() -> None:
    p = argparse.ArgumentParser(description="Log LONG_BTC hedge for a cohort (notional = sum OPEN shorts).")
    p.add_argument("--cohort", required=True)
    p.add_argument("--btc-price", type=float, required=True)
    p.add_argument("--entry-date", required=True)
    p.add_argument("--hold-days", type=int, default=None)
    p.add_argument("--notes", default="")
    args = p.parse_args()

    cfg = load_apathy_alerts_config(REPO_ROOT)
    hold = int(args.hold_days) if args.hold_days is not None else cfg.hold_days
    entry_date = _parse_date(args.entry_date)

    try:
        row = log_btc_hedge(
            repo_root=REPO_ROOT,
            cohort=args.cohort,
            btc_price=args.btc_price,
            entry_date=entry_date,
            hold_days=hold,
            notes=args.notes,
        )
    except ValueError as e:
        raise SystemExit(str(e))

    notion = float(row["notional_usd"])
    print(f"BTC hedge for {row['cohort']}: ${notion:,.0f} notional @ ${args.btc_price:,.2f}")
    print(f"  trade_id: {row['trade_id']}")
    rows = read_book_rows(cfg.book_csv)
    print(format_book_summary_line(book_summary(rows)))


if __name__ == "__main__":
    main()
