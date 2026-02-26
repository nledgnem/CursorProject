"""
Binance USDT-M futures REST API client.
- GET fapi/v1/exchangeInfo for symbol list
- GET fapi/v1/fundingRate for funding history with pagination
- Rate limiting, exponential backoff, retries for 429/418
"""

import asyncio
import logging
import random
import time
from typing import Any

import aiohttp

BASE_URL = "https://fapi.binance.com"

logger = logging.getLogger(__name__)


async def _request_with_retry(
    session: aiohttp.ClientSession,
    url: str,
    params: dict[str, Any] | None = None,
    sleep_ms: int = 300,
    max_retries: int = 5,
) -> dict | list:
    """Request with exponential backoff for 429/418 and jitter."""
    params = params or {}
    for attempt in range(max_retries):
        await asyncio.sleep(sleep_ms / 1000.0)
        try:
            async with session.get(url, params=params) as resp:
                if resp.status == 429 or resp.status == 418:
                    # Rate limited or IP banned
                    retry_after = int(resp.headers.get("Retry-After", 60))
                    wait = retry_after + (2**attempt) + random.uniform(0, 1)
                    logger.warning(
                        "Rate limited (status=%s), waiting %.1fs (attempt %d)",
                        resp.status,
                        wait,
                        attempt + 1,
                    )
                    await asyncio.sleep(wait)
                    continue
                resp.raise_for_status()
                return await resp.json()
        except aiohttp.ClientError as e:
            wait = (2**attempt) + random.uniform(0, 1)
            logger.warning("Request failed: %s, retrying in %.1fs (attempt %d)", e, wait, attempt + 1)
            await asyncio.sleep(wait)
    raise RuntimeError(f"Failed after {max_retries} retries: {url}")


async def fetch_exchange_info(session: aiohttp.ClientSession, sleep_ms: int = 300) -> list[dict]:
    """Fetch exchange info and return USDT-margined PERPETUAL symbols."""
    url = f"{BASE_URL}/fapi/v1/exchangeInfo"
    data = await _request_with_retry(session, url, sleep_ms=sleep_ms)
    symbols = []
    for s in data.get("symbols", []):
        if (
            s.get("contractType") == "PERPETUAL"
            and s.get("quoteAsset") == "USDT"
            and s.get("status") == "TRADING"
        ):
            symbols.append(s)
    return symbols


async def fetch_funding_rate_history(
    session: aiohttp.ClientSession,
    symbol: str,
    start_time: int | None = None,
    end_time: int | None = None,
    limit: int = 1000,
    sleep_ms: int = 300,
) -> list[dict]:
    """
    Fetch all funding rate history for a symbol with pagination.
    Paginates via startTime/endTime using last fundingTime + 1.
    """
    url = f"{BASE_URL}/fapi/v1/fundingRate"
    all_rows: list[dict] = []
    current_start = start_time
    max_fetch = 50_000  # safety cap
    fetches = 0

    while fetches < max_fetch:
        params: dict[str, Any] = {"symbol": symbol, "limit": limit}
        if current_start is not None:
            params["startTime"] = current_start
        if end_time is not None:
            params["endTime"] = end_time

        rows = await _request_with_retry(session, url, params=params, sleep_ms=sleep_ms)
        if not rows:
            break

        all_rows.extend(rows)
        fetches += 1

        if len(rows) < limit:
            break

        last_time = int(rows[-1]["fundingTime"])
        current_start = last_time + 1
        if end_time is not None and current_start >= end_time:
            break

    return all_rows


async def fetch_all_symbols_funding(
    symbols: list[str],
    start_time_ms: int,
    end_time_ms: int,
    sleep_ms: int = 300,
) -> dict[str, list[dict]]:
    """
    Fetch funding history for all symbols.
    Uses asyncio with concurrency limited to respect rate limits.
    """
    results: dict[str, list[dict]] = {}
    sem = asyncio.Semaphore(3)  # Limit concurrent requests

    async def fetch_one(session: aiohttp.ClientSession, sym: str) -> None:
        async with sem:
            try:
                rows = await fetch_funding_rate_history(
                    session, sym, start_time_ms, end_time_ms, sleep_ms=sleep_ms
                )
                results[sym] = rows
            except Exception as e:
                logger.error("Failed to fetch %s: %s", sym, e)
                results[sym] = []

    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            *[fetch_one(session, sym) for sym in symbols],
            return_exceptions=True,
        )

    return results
