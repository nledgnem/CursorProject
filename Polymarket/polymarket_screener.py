"""
Polymarket Tail-Risk Screener — Phase 2 (Data Firehose) + Phase 3 (LLM Rule Screener).

Read-only pipeline: fetches active markets from Gamma API, filters by volume/expiry/price,
then screens with OpenAI for objective, non-insider, mathematically sound tail-risk bets.
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from dotenv import load_dotenv
from openai import OpenAI

# ---------------------------------------------------------------------------
# Config & env
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
load_dotenv(SCRIPT_DIR / ".env")

GAMMA_BASE = "https://gamma-api.polymarket.com"
EVENTS_URL = f"{GAMMA_BASE}/events"
MIN_VOLUME = 50_000
TAIL_PRICE_LOW = 0.02
TAIL_PRICE_HIGH = 0.08
EXPIRY_DAYS_MAX = 45
RATE_LIMIT_SLEEP = 0.5
# Cap event pages so Phase 2 completes in reasonable time (100 events per page)
MAX_EVENT_PAGES = 50


def _get_openai_client() -> OpenAI:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError(
            "OPENAI_API_KEY not set. Add it to Polymarket/.env (see .env template)."
        )
    return OpenAI(api_key=api_key)


# ---------------------------------------------------------------------------
# Phase 2: Data Firehose (Gamma API)
# ---------------------------------------------------------------------------


def _parse_outcome_prices(outcome_prices: Any) -> list[float]:
    """Parse outcomePrices from API (string or list) to list of floats."""
    if outcome_prices is None:
        return []
    if isinstance(outcome_prices, str):
        try:
            arr = json.loads(outcome_prices)
        except json.JSONDecodeError:
            return []
    else:
        arr = outcome_prices if isinstance(outcome_prices, list) else []
    return [float(x) for x in arr if x is not None]


def _has_tail_price_in_range(prices: list[float]) -> tuple[bool, float | None]:
    """True if any outcome price is in [TAIL_PRICE_LOW, TAIL_PRICE_HIGH]. Returns that price."""
    for p in prices:
        if TAIL_PRICE_LOW <= p <= TAIL_PRICE_HIGH:
            return True, p
    return False, None


def _parse_end_date(value: Any) -> datetime | None:
    """Parse end date from API (ISO string or None)."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=value.tzinfo or timezone.utc)
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def fetch_and_filter_markets() -> list[dict[str, Any]]:
    """
    Fetch active events from Gamma API and return a clean list of markets
    satisfying: volume >= 50k, expiry <= 45 days, at least one outcome in [0.02, 0.08],
    active and not closed.
    """
    now = datetime.now(timezone.utc)
    expiry_cap = now + timedelta(days=EXPIRY_DAYS_MAX)
    results: list[dict[str, Any]] = []
    limit = 100
    offset = 0
    pages_fetched = 0

    while True:
        if pages_fetched >= MAX_EVENT_PAGES:
            break
        time.sleep(RATE_LIMIT_SLEEP)
        resp = requests.get(
            EVENTS_URL,
            params={"limit": limit, "offset": offset},
            timeout=30,
        )
        resp.raise_for_status()
        events = resp.json()
        if not events:
            break

        for ev in events:
            vol = ev.get("volume")
            if vol is None:
                try:
                    vol = float(ev.get("volumeNum", 0) or 0)
                except (TypeError, ValueError):
                    vol = 0
            else:
                try:
                    vol = float(vol)
                except (TypeError, ValueError):
                    vol = 0
            if vol < MIN_VOLUME:
                continue
            if not ev.get("active", True) or ev.get("closed", False):
                continue

            end_dt = _parse_end_date(ev.get("endDate"))
            if end_dt is None:
                continue
            if end_dt < now:
                continue
            if end_dt > expiry_cap:
                continue

            markets = ev.get("markets") or []
            event_title = ev.get("title") or ""
            event_desc = ev.get("description") or ""
            event_resolution = ev.get("resolutionSource") or ""

            for m in markets:
                if not m.get("active", True) or m.get("closed", False):
                    continue
                prices = _parse_outcome_prices(m.get("outcomePrices"))
                ok, tail_price = _has_tail_price_in_range(prices)
                if not ok or tail_price is None:
                    continue
                m_end = _parse_end_date(m.get("endDate")) or end_dt
                title = m.get("question") or m.get("title") or event_title
                desc = m.get("description") or event_desc
                resolution = m.get("resolutionSource") or event_resolution
                resolution_rules = (desc.strip() + "\n\nResolution source: " + resolution).strip() if resolution else desc

                results.append({
                    "market_title": title,
                    "description": desc,
                    "resolution_rules": resolution_rules,
                    "token_price": round(tail_price, 4),
                    "expiry_date": m_end.isoformat(),
                })

        pages_fetched += 1
        if len(events) < limit:
            break
        offset += limit

    return results


# ---------------------------------------------------------------------------
# Phase 3: AI Screener (Hostile Quant)
# ---------------------------------------------------------------------------

SCREENER_SCHEMA = {
    "type": "object",
    "properties": {
        "is_objective": {
            "type": "boolean",
            "description": "True ONLY if resolution relies on strict math, a specific API, or hard data. False if it relies on vibes, UMA voter interpretation, or ambiguous wording.",
        },
        "has_insiders": {
            "type": "boolean",
            "description": "True if a single person/group could secretly know the outcome. False if it's macro/public data.",
        },
        "complexity_score": {
            "type": "integer",
            "description": "1 to 10 scale of how many nested conditions are in the rules.",
        },
        "verdict": {
            "type": "string",
            "enum": ["TRADE", "PASS"],
            "description": "TRADE if suitable as a tail-risk bet; PASS to discard.",
        },
    },
    "required": ["is_objective", "has_insiders", "complexity_score", "verdict"],
    "additionalProperties": False,
}


def screen_market_with_llm(client: OpenAI, market: dict[str, Any]) -> dict[str, Any] | None:
    """
    Run the Hostile Quant screener on one market. Returns enriched market dict if it
    passes the kill switch; otherwise None.
    """
    title = market.get("market_title", "")
    desc = market.get("description", "")
    rules = market.get("resolution_rules", "")

    prompt = f"""You are a hostile quant screening prediction markets for mathematically sound tail-risk bets. Only recommend TRADE when the market is objective, non-insider, and resolvable by hard data or strict rules.

Market title: {title}

Description:
{desc}

Resolution rules / source:
{rules}

Evaluate and respond with the exact JSON schema: is_objective (true only if resolution uses strict math, a specific API, or hard data; false for vibes/UMA/ambiguous), has_insiders (true if one party could secretly know the outcome), complexity_score (1-10), verdict (TRADE or PASS)."""

    time.sleep(RATE_LIMIT_SLEEP)
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_schema", "json_schema": {"name": "screener_result", "strict": True, "schema": SCREENER_SCHEMA}},
        )
    except Exception as e:
        print(f"  [LLM error for '{title[:50]}...']: {e}")
        return None

    text = (response.choices[0].message.content or "").strip()
    if not text:
        return None
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return None

    is_objective = data.get("is_objective", False)
    has_insiders = data.get("has_insiders", True)
    verdict = (data.get("verdict") or "PASS").strip().upper()

    # Kill switch
    if not is_objective or has_insiders or verdict != "TRADE":
        return None

    return {
        **market,
        "is_objective": is_objective,
        "has_insiders": has_insiders,
        "complexity_score": data.get("complexity_score", 0),
        "verdict": verdict,
        "screen_reason": f"Objective={is_objective}, no insiders, verdict=TRADE, complexity={data.get('complexity_score', 'N/A')}",
    }


# ---------------------------------------------------------------------------
# Main & output
# ---------------------------------------------------------------------------


def main() -> None:
    import sys
    print("Polymarket Tail-Risk Screener", flush=True)
    print("Phase 2: Fetching and filtering markets from Gamma API...", flush=True)
    sys.stdout.flush()
    filtered = fetch_and_filter_markets()
    print(f"  Filtered markets (volume>=${MIN_VOLUME:,}, expiry<={EXPIRY_DAYS_MAX}d, tail price in [{TAIL_PRICE_LOW},{TAIL_PRICE_HIGH}]): {len(filtered)}", flush=True)

    if not filtered:
        print("No markets passed Phase 2. Exiting.")
        return

    client = _get_openai_client()
    print("Phase 3: Screening with OpenAI (Hostile Quant)...")
    trades: list[dict[str, Any]] = []
    for i, m in enumerate(filtered, 1):
        print(f"  [{i}/{len(filtered)}] {m.get('market_title', '')[:60]}...")
        passed = screen_market_with_llm(client, m)
        if passed:
            trades.append(passed)

    # Output
    print("\n" + "=" * 70)
    print("TRADE TARGETS (passed AI screen)")
    print("=" * 70)
    if not trades:
        print("None. All candidates were discarded by the kill switch.")
        return
    for t in trades:
        print(f"\n  Title: {t.get('market_title', 'N/A')}")
        print(f"  Target price (tail): {t.get('token_price', 'N/A')}")
        print(f"  Expiry: {t.get('expiry_date', 'N/A')}")
        print(f"  Reason: {t.get('screen_reason', 'N/A')}")
    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
