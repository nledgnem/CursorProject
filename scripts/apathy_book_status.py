from __future__ import annotations

import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.apathy_bleed.book import (
    book_summary,
    max_open_entry_date,
    parse_iso_date,
    read_book_rows,
    short_adverse_move,
    unrealized_long_pct,
    unrealized_short_pct,
)
from src.apathy_bleed.config_loader import load_apathy_alerts_config
from src.apathy_bleed.variational_prices import fetch_variational_mark_prices


def _utc_today():
    return datetime.now(timezone.utc).date()


def main() -> None:
    cfg = load_apathy_alerts_config(REPO_ROOT)
    book_path = cfg.book_csv
    rows = read_book_rows(book_path)
    summ = book_summary(rows)
    print("=== Apathy Bleed book ===")
    print(f"OPEN rows: {summ.total_open_count} | OPEN short legs: {summ.open_short_count} | Short notional: ${summ.total_short_notional_usd:,.0f}")

    last_ent = max_open_entry_date(rows)
    if last_ent:
        days_since = (_utc_today() - last_ent).days
        print(f"Last OPEN entry_date (max): {last_ent.isoformat()} ({days_since} days ago)")
    else:
        print("Last OPEN entry_date: n/a")

    prices: dict[str, float] = {}
    try:
        prices = fetch_variational_mark_prices(
            cfg.variational_base_url,
            cfg.variational_stats_path,
            timeout_seconds=cfg.variational_timeout_seconds,
        )
    except Exception as e:
        print(f"Variational prices: unavailable ({e})")

    by_cohort: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        if (r.get("status") or "").upper() != "OPEN":
            continue
        by_cohort[(r.get("cohort") or "").strip().upper()].append(r)

    print("\n--- Per cohort ---")
    for c in sorted(by_cohort.keys()):
        legs = by_cohort[c]
        n_short = sum(1 for x in legs if (x.get("side") or "").upper() == "SHORT")
        n_btc = sum(1 for x in legs if (x.get("side") or "").upper() == "LONG_BTC")
        notion = sum(float(x.get("notional_usd") or 0) for x in legs if (x.get("side") or "").upper() == "SHORT")
        print(f"{c}: {len(legs)} open rows ({n_short} shorts, {n_btc} BTC) | short notional ${notion:,.0f}")

    print("\n--- Open positions (detail) ---")
    today = _utc_today()
    for r in rows:
        if (r.get("status") or "").upper() != "OPEN":
            continue
        tkr = (r.get("ticker") or "").strip().upper()
        co = (r.get("cohort") or "").strip().upper()
        side = (r.get("side") or "").strip().upper()
        tgt = None
        d_left = None
        try:
            entry = float(r.get("entry_price_usd") or 0)
            tgt = parse_iso_date(r.get("exit_date_target_utc") or "")
            d_left = (tgt - today).days
        except ValueError:
            entry = float(r.get("entry_price_usd") or 0) if (r.get("entry_price_usd") or "").strip() else 0.0
        mark = prices.get(tkr)
        extra = []
        if mark is not None and entry > 0:
            if side == "SHORT":
                adv = short_adverse_move(entry, mark) * 100.0
                ur = unrealized_short_pct(entry, mark) * 100.0
                try:
                    stop = float(r.get("stop_price_usd") or 0)
                except ValueError:
                    stop = 0.0
                dist_stop = ((stop - mark) / entry * 100.0) if stop and entry else float("nan")
                mk = f"{mark:,.2f}" if mark >= 1 else f"{mark:.6f}"
                extra.append(f"mark ${mk} | adverse {adv:+.1f}% vs entry | unrealized {ur:+.1f}%")
                if dist_stop == dist_stop:
                    extra.append(f"distance to stop (price): {dist_stop:+.1f}% of entry")
            elif side == "LONG_BTC":
                ur = unrealized_long_pct(entry, mark) * 100.0
                mk = f"{mark:,.2f}" if mark >= 1 else f"{mark:.6f}"
                extra.append(f"mark ${mk} | unrealized {ur:+.1f}%")
        else:
            extra.append("mark: N/A")
        tgt_s = f"{tgt.isoformat()} ({d_left}d left)" if tgt is not None and d_left is not None else "n/a"
        print(f"  {tkr} {co} {side} | exit target {tgt_s} | " + " | ".join(extra))


if __name__ == "__main__":
    main()
