#!/usr/bin/env python3
"""
Fetch Binance's OFFICIAL BTC Dominance Index -> data_lake/btcdom_binance_index.csv

WHAT THIS DATA IS
-----------------
The underlying index of Binance's ``BTCDOMUSDT`` USDS-M perpetual future.

It is NOT "BTC % of total crypto market cap" (the CoinMarketCap-style number).
It is a fixed-quantity, price-weighted basket of top alts priced in BTC terms,
constructed so that long BTCDOM is economically long-BTC / short-alts. The
repo's own assumption ledger (scripts/audit_btcdom_assumption_ledger.py)
documents the same construction, because our reconstruction was built to
REPLICATE this index -- it is anchored to
``base_index_level = 2448.02529635``, which that ledger records as the Binance
BTCDOM close on 2024-07-04.

So this is not a switch to a different measure. It is a switch from a home-made
approximation of Binance's index to Binance's index itself. See ADR 004.

WHY THE INDEX AND NOT THE PERP PRICE
------------------------------------
``/fapi/v1/klines?symbol=BTCDOMUSDT`` gives the PERPETUAL's traded price, which
is the index plus basis and funding pressure. ``/fapi/v1/indexPriceKlines`` gives
the underlying index itself. They are close (~0.04% apart at time of writing) but
the index is the actual dominance measure and carries no derivatives artefacts,
so that is what we store.

TIMESTAMP CONVENTION
--------------------
Daily UTC candles. Bucket open is 00:00:00 UTC; we take the CLOSE and stamp it
with the bucket's UTC calendar date, so the value on date t is known at
23:59:59 UTC on t. The still-open current UTC day is dropped -- an unsettled
close is not a close.

COVERAGE
--------
2021-06-21 onward (contract onboarding), i.e. ~5 years, versus the
reconstruction's 2024-07-04. No API key, no rate-limit concerns at daily
granularity.

    python scripts/data_ingestion/btcdom_binance_index.py [--out PATH]
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from repo_paths import data_lake_root  # noqa: E402

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

ENDPOINT = "https://fapi.binance.com/fapi/v1/indexPriceKlines"
PAIR = "BTCDOMUSDT"
OUT_NAME = "btcdom_binance_index.csv"

# Contract onboarding is 2021-06-17; the index series starts 2021-06-21.
HISTORY_START_MS = 0  # let the venue return its own earliest bar

# Sanity bounds. The index has traded roughly 1,000-6,000 over its life; these
# are deliberately loose -- they exist to catch a unit change or a garbage
# response, not to second-guess the market.
MIN_PLAUSIBLE = 100.0
MAX_PLAUSIBLE = 50_000.0


def fetch_index(session: requests.Session | None = None) -> pd.DataFrame:
    """Paginated daily index closes. Raises on unrecoverable fetch failure."""
    sess = session or requests.Session()
    rows: list[list] = []
    cur = HISTORY_START_MS
    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    while cur < end_ms:
        batch = None
        for attempt in range(5):
            try:
                r = sess.get(
                    ENDPOINT,
                    params={"pair": PAIR, "interval": "1d",
                            "startTime": cur, "limit": 1000},
                    timeout=30,
                )
                if r.status_code in (429, 418):
                    time.sleep(2.0 * (attempt + 1))
                    continue
                r.raise_for_status()
                batch = r.json()
                break
            except requests.RequestException as exc:
                if attempt == 4:
                    raise SystemExit(
                        f"BTCDOM index fetch failed after 5 attempts at startTime={cur}: {exc}"
                    ) from exc
                time.sleep(1.5 * (attempt + 1))
        if not batch:
            break
        rows.extend(batch)
        last_open = batch[-1][0]
        if last_open < cur:  # venue went backwards; refuse to loop forever
            break
        cur = last_open + 86_400_000
        time.sleep(0.15)

    if not rows:
        raise SystemExit("BTCDOM index fetch returned no rows.")

    df = pd.DataFrame(rows)
    out = pd.DataFrame(
        {
            "date": pd.to_datetime(df[0], unit="ms", utc=True)
            .dt.tz_localize(None)
            .dt.normalize(),
            "btcdom_index": df[4].astype(float),
        }
    )
    out = out.drop_duplicates(subset="date", keep="last").sort_values("date")

    # Drop the still-open current UTC day.
    today_utc = pd.Timestamp(datetime.now(timezone.utc).date())
    out = out[out["date"] < today_utc]
    return out.reset_index(drop=True)


def validate(df: pd.DataFrame) -> None:
    """Fail loudly rather than write a plausible-looking bad file."""
    if df.empty:
        raise SystemExit("BTCDOM index: empty frame after normalisation.")
    if df["btcdom_index"].isna().any():
        raise SystemExit(
            f"BTCDOM index: {int(df['btcdom_index'].isna().sum())} null value(s)."
        )
    bad = df[(df["btcdom_index"] <= MIN_PLAUSIBLE) | (df["btcdom_index"] >= MAX_PLAUSIBLE)]
    if not bad.empty:
        raise SystemExit(
            f"BTCDOM index: {len(bad)} value(s) outside [{MIN_PLAUSIBLE}, {MAX_PLAUSIBLE}]. "
            f"Possible unit change. First: {bad.iloc[0].to_dict()}"
        )
    if df["date"].duplicated().any():
        raise SystemExit("BTCDOM index: duplicate dates after dedupe.")
    if not df["date"].is_monotonic_increasing:
        raise SystemExit("BTCDOM index: dates not monotonically increasing.")

    # A calendar gap is not fatal (the venue can halt), but it must be visible.
    gaps = df["date"].diff().dt.days.dropna()
    n_gaps = int((gaps > 1).sum())
    if n_gaps:
        logger.warning(
            "BTCDOM index has %d calendar gap(s); largest %d days. "
            "Consumers merge as-of with a tolerance, so short gaps are absorbed.",
            n_gaps, int(gaps.max()),
        )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, default=None,
                    help="output CSV path (default: <data_lake>/btcdom_binance_index.csv)")
    args = ap.parse_args()

    out_path = args.out or (data_lake_root() / OUT_NAME)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info("Fetching Binance BTCDOM index (pair=%s, daily UTC closes)...", PAIR)
    df = fetch_index()
    validate(df)

    df.to_csv(out_path, index=False)
    drift = (datetime.now(timezone.utc).date() - df["date"].max().date()).days
    logger.info(
        "Wrote %s: %d rows, %s -> %s (drift=%dd, last=%.2f)",
        out_path, len(df), df["date"].min().date(), df["date"].max().date(),
        drift, float(df["btcdom_index"].iloc[-1]),
    )


if __name__ == "__main__":
    main()
