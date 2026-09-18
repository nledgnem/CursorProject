"""Asset-identity checks for the CoinGecko-sourced lake tables.

The lake keys fact_price / fact_marketcap / fact_volume (and their silver copies) by
asset_id = upper-case ticker. The coin behind a ticker is whatever data/perp_allowlist.csv
names at fetch time, and nothing in the lake records that choice. These checks catch the
ways a ticker key ends up pointing at the wrong coin:

1. dim_asset.coingecko_id is a placeholder (lower-cased ticker), not the id actually fetched.
2. A ticker key equals the upper-cased CoinGecko slug of a *different* coin ('BITCOIN' holds
   a memecoin while real Bitcoin is 'BTC').
3. The price series under a ticker is not the coin trading under that ticker on Binance
   (currently, or in a historical segment -- a splice).
4. Market cap and price under one ticker come from different coins (implied supply =
   market_cap / close jumps by orders of magnitude, or disagrees with /coins/markets).

Pure functions only: no I/O, no network. scripts/verify_ingestion_integrity.py --mode
asset_identity wires them to the lake files and to Binance.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

LOG_10PCT = float(np.log(1.10))
BINANCE_MULTIPLIER_PREFIXES = (("1000000", 1e6), ("100000", 1e5), ("10000", 1e4), ("1000", 1e3), ("1M", 1e6))


def placeholder_coingecko_ids(dim_asset: pd.DataFrame, allowlist: pd.DataFrame) -> pd.DataFrame:
    """Allowlisted assets whose dim_asset.coingecko_id differs from the id the fetcher actually uses.

    allowlist columns: symbol, coingecko_id. Returns asset_id, dim_coingecko_id, fetched_coingecko_id.
    """
    a = allowlist.assign(asset_id=allowlist["symbol"].str.upper())[["asset_id", "coingecko_id"]]
    j = dim_asset[["asset_id", "coingecko_id"]].merge(a, on="asset_id", how="inner", suffixes=("_dim", "_fetched"))
    bad = j[j["coingecko_id_dim"] != j["coingecko_id_fetched"]]
    return bad.rename(columns={"coingecko_id_dim": "dim_coingecko_id",
                               "coingecko_id_fetched": "fetched_coingecko_id"}).reset_index(drop=True)


def slug_ticker_collisions(asset_ids, allowlist: pd.DataFrame) -> pd.DataFrame:
    """Ticker keys that spell another allowlisted coin's slug.

    Example: allowlist has BTC -> 'bitcoin' and BITCOIN -> 'harrypotterobamasonic10in'. A reader
    filtering asset_id == 'BITCOIN' (the dictionary once described asset_id as the slug) silently
    gets the memecoin. Returns asset_id, holds_coingecko_id, collides_with_coingecko_id,
    collides_with_ticker.
    """
    al = allowlist.assign(asset_id=allowlist["symbol"].str.upper())
    slug_owner = al[al["coingecko_id"].str.upper() != al["asset_id"]]
    slug_owner = slug_owner.assign(key=slug_owner["coingecko_id"].str.upper())
    ids = pd.DataFrame({"asset_id": sorted(set(asset_ids))})
    j = ids.merge(slug_owner[["key", "coingecko_id", "asset_id"]].rename(
        columns={"key": "asset_id", "coingecko_id": "collides_with_coingecko_id", "asset_id": "collides_with_ticker"}),
        on="asset_id", how="inner")
    j = j.merge(al[["asset_id", "coingecko_id"]].rename(columns={"coingecko_id": "holds_coingecko_id"}),
                on="asset_id", how="left")
    j = j[j["holds_coingecko_id"] != j["collides_with_coingecko_id"]]
    return j[["asset_id", "holds_coingecko_id", "collides_with_coingecko_id", "collides_with_ticker"]].reset_index(drop=True)


def binance_base_to_asset(base_asset: str) -> tuple[str, float]:
    """'1000PEPE' -> ('PEPE', 1000.0); '1MBABYDOGE' -> ('BABYDOGE', 1e6); '1INCH' -> ('1INCH', 1.0)."""
    for prefix, mult in BINANCE_MULTIPLIER_PREFIXES:
        rest = base_asset[len(prefix):]
        if base_asset.startswith(prefix) and rest[:1].isalpha():
            return rest, mult
    return base_asset, 1.0


def _runs(flag: pd.Series) -> list[tuple[pd.Timestamp, pd.Timestamp, int]]:
    out, start, prev = [], None, None
    for d, f in flag.items():
        if f and start is None:
            start = d
        if not f and start is not None:
            out.append((start, prev, len(flag[start:prev])))
            start = None
        prev = d
    if start is not None:
        out.append((start, prev, len(flag[start:prev])))
    return out


def price_identity(lake_close: pd.Series, binance_close: pd.Series, multiplier: float = 1.0,
                   lake_date_lag_days: int = 1, min_run_days: int = 14, recent_days: int = 30) -> dict:
    """Compare a lake close series with the Binance perp closes of the same ticker.

    lake_close: indexed by lake `date`. binance_close: indexed by Binance daily-bar open date (UTC).
    The lake stamps the UTC close of day d-1 on date d, hence lake_date_lag_days=1.
    Status:
      CURRENT_WRONG_COIN -- fewer than half of the last `recent_days` overlapping days within 10%
      SPLICED_HISTORY    -- currently right, but a run of >= min_run_days (7-day rolling median) is not
      OK                 -- neither
      NO_OVERLAP         -- fewer than 7 overlapping days
    """
    b = binance_close.copy()
    b.index = pd.DatetimeIndex(b.index) + pd.Timedelta(days=lake_date_lag_days)
    x = pd.concat([lake_close.rename("lake"), (b / multiplier).rename("binance")], axis=1).dropna()
    x = x[(x["lake"] > 0) & (x["binance"] > 0)].sort_index()
    if len(x) < 7:
        return {"status": "NO_OVERLAP", "overlap_days": len(x)}
    lr = np.abs(np.log(x["lake"] / x["binance"]))
    smooth = lr.rolling(7, center=True, min_periods=3).median()
    bad_runs = [r for r in _runs(smooth > LOG_10PCT) if r[2] >= min_run_days]
    recent = lr[lr.index > lr.index.max() - pd.Timedelta(days=recent_days)]
    recent_share = float((recent <= LOG_10PCT).mean())
    if recent_share < 0.5:
        status = "CURRENT_WRONG_COIN"
    elif bad_runs:
        status = "SPLICED_HISTORY"
    else:
        status = "OK"
    return {"status": status, "overlap_days": len(x), "med_abs_log": float(lr.median()),
            "share_within_10pct": float((lr <= LOG_10PCT).mean()), "recent_share_within_10pct": recent_share,
            "bad_runs": [(s.date(), e.date(), n) for s, e, n in bad_runs]}


def implied_supply_steps(lake: pd.DataFrame, factor: float = 3.0, since=None) -> pd.DataFrame:
    """Day-over-day jumps of implied supply (market_cap / close) by more than `factor` either way.

    A real coin's circulating supply does not triple or third overnight; when it appears to, the
    market cap or the price under that ticker switched coin. lake columns: asset_id, date, close,
    market cap column named `marketcap` or `market_cap`.
    """
    mc = "marketcap" if "marketcap" in lake.columns else "market_cap"
    d = lake[(lake["close"] > 0) & (lake[mc] > 0)].sort_values(["asset_id", "date"])
    lsup = np.log(d[mc] / d["close"])
    step = lsup.groupby(d["asset_id"]).diff()
    out = d.assign(log_step=step)[np.abs(step) > np.log(factor)]
    if since is not None:
        out = out[pd.to_datetime(out["date"]) >= pd.Timestamp(since)]
    return out[["asset_id", "date", "close", mc, "log_step"]].reset_index(drop=True)


def market_wide_splice_dates(lake: pd.DataFrame, column: str, factor: float = 3.0,
                             min_assets: int = 20) -> pd.Series:
    """Dates on which at least `min_assets` assets jump by more than `factor` overnight in `column`.

    Individual coins do move 3x in a day; dozens at once means the key -> coin binding changed
    for all of them (allowlist re-binding, or a stale vintage merged back). On the live lake
    2024-05..2026-09 the daily count has median 2 and 99th percentile 13; the three known mass
    splices score 61-87. Only consecutive calendar days are compared, so gaps are not jumps.
    """
    d = lake[lake[column] > 0].sort_values(["asset_id", "date"])
    dates = pd.to_datetime(d["date"])
    step = np.log(d[column]).groupby(d["asset_id"]).diff()
    gap = dates.groupby(d["asset_id"]).diff().dt.days
    counts = dates[(np.abs(step) > np.log(factor)) & (gap == 1)].value_counts().sort_index()
    return counts[counts >= min_assets]


def mcap_vs_snapshot(lake_day: pd.DataFrame, snapshot_day: pd.DataFrame, allowlist: pd.DataFrame,
                     price_tol: float = 0.05, mcap_tol: float = 0.10) -> pd.DataFrame:
    """Same-day comparison of lake price and market cap with /coins/markets for the fetched coin.

    lake_day: asset_id, close, marketcap (one date). snapshot_day: coingecko_id, current_price_usd,
    market_cap_usd (same date). Joined through the allowlist id, not the ticker, so ticker collisions
    inside the snapshot cannot mask a mismatch. Returns rows that fail either tolerance.
    """
    al = allowlist.assign(asset_id=allowlist["symbol"].str.upper())[["asset_id", "coingecko_id"]]
    j = lake_day.merge(al, on="asset_id").merge(snapshot_day, on="coingecko_id", how="inner")
    j = j[(j["close"] > 0) & (j["current_price_usd"] > 0)]
    j["price_log_diff"] = np.log(j["close"] / j["current_price_usd"])
    j["mcap_log_diff"] = np.log(j["marketcap"].where(j["marketcap"] > 0) / j["market_cap_usd"].where(j["market_cap_usd"] > 0))
    bad = (np.abs(j["price_log_diff"]) > np.log(1 + price_tol)) | (np.abs(j["mcap_log_diff"]) > np.log(1 + mcap_tol))
    return j[bad].reset_index(drop=True)
