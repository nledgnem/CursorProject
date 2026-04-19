from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.apathy_bleed.book import atomic_write_book, book_summary, format_book_summary_line, parse_iso_date, read_book_rows
from src.apathy_bleed.config_loader import load_apathy_alerts_config

REASON_STATUS = {
    "expiry": "CLOSED_EXPIRY",
    "stop": "CLOSED_STOP",
    "manual": "CLOSED_MANUAL",
}


def _utc_today() -> date:
    return datetime.now(timezone.utc).date()


def _pick_open_row(
    rows: list[dict[str, str]], *, ticker: str, cohort: str, side: str
) -> tuple[int, dict[str, str]] | None:
    ticker_u = ticker.strip().upper()
    cohort_u = cohort.strip().upper()
    side_u = side.strip().upper()
    candidates: list[tuple[int, dict[str, str], date]] = []
    for i, r in enumerate(rows):
        if (r.get("status") or "").strip().upper() != "OPEN":
            continue
        if (r.get("ticker") or "").strip().upper() != ticker_u:
            continue
        if (r.get("cohort") or "").strip().upper() != cohort_u:
            continue
        if (r.get("side") or "").strip().upper() != side_u:
            continue
        try:
            ed = parse_iso_date(r.get("entry_date_utc") or "")
        except ValueError:
            ed = date.min
        candidates.append((i, r, ed))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[2], reverse=True)
    idx, row, _ = candidates[0]
    return idx, row


def _compute_pnl(side: str, entry: float, exit_px: float, notional: float) -> tuple[float, float]:
    if side.upper() == "SHORT":
        pnl_pct = (entry - exit_px) / entry if entry else 0.0
    else:
        pnl_pct = (exit_px - entry) / entry if entry else 0.0
    pnl_usd = pnl_pct * notional
    return pnl_usd, pnl_pct


def main() -> None:
    p = argparse.ArgumentParser(description="Close an OPEN row in the Apathy Bleed book (rewrite row in place).")
    p.add_argument("--ticker", required=True)
    p.add_argument("--cohort", required=True)
    p.add_argument("--exit-price", type=float, required=True)
    p.add_argument("--reason", required=True, choices=sorted(REASON_STATUS.keys()))
    p.add_argument("--side", default="SHORT", help="SHORT or LONG_BTC")
    p.add_argument(
        "--exit-date",
        default="",
        help="Exit date in UTC as YYYY-MM-DD (default: today UTC).",
    )
    p.add_argument("--notes", default="", help="Optional free-text notes stored on the row.")
    args = p.parse_args()

    if args.exit_price <= 0:
        raise SystemExit("exit-price must be > 0")

    cfg = load_apathy_alerts_config(REPO_ROOT)
    book_path = cfg.book_csv
    rows = read_book_rows(book_path)
    picked = _pick_open_row(rows, ticker=args.ticker, cohort=args.cohort, side=args.side)
    if picked is None:
        raise SystemExit("No matching OPEN row found.")

    idx, target = picked
    entry = float(target["entry_price_usd"])
    notional = float(target["notional_usd"])
    side = (target.get("side") or "").strip().upper()
    pnl_usd, pnl_pct = _compute_pnl(side, entry, args.exit_price, notional)
    exit_raw = (args.exit_date or "").strip()
    if exit_raw:
        try:
            exit_day = date.fromisoformat(exit_raw)
        except ValueError as e:
            raise SystemExit(f"Invalid --exit-date (use YYYY-MM-DD): {exit_raw}") from e
    else:
        exit_day = _utc_today()
    status = REASON_STATUS[args.reason.strip().lower()]

    target["status"] = status
    target["exit_date_utc"] = exit_day.isoformat()
    target["exit_price_usd"] = f"{args.exit_price:.8f}".rstrip("0").rstrip(".")
    target["pnl_usd"] = f"{pnl_usd:.4f}".rstrip("0").rstrip(".")
    target["pnl_pct"] = f"{pnl_pct:.8f}".rstrip("0").rstrip(".")
    if (args.notes or "").strip():
        target["notes"] = args.notes.strip()

    rows[idx] = target
    atomic_write_book(book_path, rows)

    print("Trade closed.")
    print(f"  trade_id:        {target['trade_id']}")
    print(f"  ticker/cohort:   {target['ticker']} / {target['cohort']} ({side})")
    print(f"  status:          {status}")
    print(f"  exit_date_utc:   {exit_day.isoformat()}")
    print(f"  exit_price_usd:  {args.exit_price}")
    print(f"  pnl_usd:         {pnl_usd:,.2f}")
    print(f"  pnl_pct:         {pnl_pct * 100:.2f}%")
    print(format_book_summary_line(book_summary(read_book_rows(book_path))))


if __name__ == "__main__":
    main()
