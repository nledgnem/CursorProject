from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class FundingQuote:
    ticker: str
    funding_rate_per_8h: float | None  # decimal, e.g. 0.0001 == 1 bp per 8h


def _ccxt_symbol_for_usdt_perp(ticker: str) -> str:
    # CCXT Binance USD-M uses "BTC/USDT:USDT" style for perpetual swaps.
    t = str(ticker).strip().upper()
    return f"{t}/USDT:USDT"


def fetch_binance_usdm_funding_rates(tickers: list[str]) -> dict[str, float]:
    """
    Returns ticker -> current fundingRate (decimal per 8h) when available.
    Non-fatal: missing tickers are skipped.
    """
    try:
        import ccxt  # type: ignore
    except Exception as e:
        raise RuntimeError("ccxt is required for funding rates. Install ccxt and retry.") from e

    ex = ccxt.binanceusdm({"enableRateLimit": True})
    ex.load_markets()

    out: dict[str, float] = {}
    for t in sorted({str(x).strip().upper() for x in tickers if str(x).strip()}):
        sym = _ccxt_symbol_for_usdt_perp(t)
        try:
            fr = ex.fetch_funding_rate(sym)
        except Exception:
            continue
        rate = fr.get("fundingRate") if isinstance(fr, dict) else None
        try:
            rate_f = float(rate)
        except Exception:
            continue
        if math.isfinite(rate_f):
            out[t] = rate_f
    try:
        ex.close()
    except Exception:
        pass
    return out


def estimate_daily_funding_pnl_usd(
    *,
    notional_usd: float,
    direction: float,
    funding_rate_per_8h: float | None,
) -> float | None:
    """
    Binance convention (USDT-margined): positive funding => longs pay shorts.
    direction: LONG=+1, SHORT=-1
    Approx daily = 3 * per-8h rate.
    """
    if funding_rate_per_8h is None:
        return None
    try:
        r = float(funding_rate_per_8h)
    except Exception:
        return None
    if not math.isfinite(r):
        return None
    # LONG: pnl = -notional * r * 3; SHORT: pnl = +notional * r * 3
    return float(-direction * notional_usd * r * 3.0)

