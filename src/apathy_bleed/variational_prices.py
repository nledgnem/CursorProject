from __future__ import annotations

import logging

import requests

logger = logging.getLogger(__name__)


def fetch_variational_mark_prices(
    base_url: str,
    stats_path: str,
    *,
    timeout_seconds: float = 20.0,
) -> dict[str, float]:
    """
    GET public Variational stats; returns ticker -> mark_price (USD) uppercased.
    """
    url = f"{base_url.rstrip('/')}{stats_path}"
    try:
        resp = requests.get(url, timeout=timeout_seconds)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning("Variational stats fetch failed (non-fatal): %s", e)
        return {}

    listings = data.get("listings")
    if not isinstance(listings, list):
        logger.warning("Variational stats: missing `listings` array.")
        return {}

    out: dict[str, float] = {}
    for m in listings:
        if not isinstance(m, dict):
            continue
        tkr = m.get("ticker")
        if not tkr:
            continue
        price_raw = m.get("mark_price")
        if price_raw is None and "raw_metadata_json" not in m:
            # Some payloads nest mark_price only in expanded JSON in CSV snapshots; try string fields.
            pass
        if price_raw is None:
            continue
        try:
            px = float(price_raw)
        except (TypeError, ValueError):
            continue
        out[str(tkr).upper()] = px
    return out
