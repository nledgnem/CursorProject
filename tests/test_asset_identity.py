"""Regression tests for src/data_lake/asset_identity.py.

Each fixture reproduces a defect found in the live lake on 2026-09-18:
placeholder dim_asset.coingecko_id, the BTC/BITCOIN slug collision, a spliced price history
(S held a sub-cent token before Sonic), and a market cap taken from a different coin than the
price (AVAX in lake vintage 599e5cb).
"""

import numpy as np
import pandas as pd

from src.data_lake.asset_identity import (
    binance_base_to_asset,
    implied_supply_steps,
    market_wide_splice_dates,
    mcap_vs_snapshot,
    placeholder_coingecko_ids,
    price_identity,
    slug_ticker_collisions,
)

ALLOW = pd.DataFrame({"symbol": ["BTC", "BITCOIN", "AVAX", "S"],
                      "coingecko_id": ["bitcoin", "harrypotterobamasonic10in", "avalanche-2", "sonic-3"]})


def test_placeholder_ids_flagged_and_real_ids_pass():
    dim = pd.DataFrame({"asset_id": ["BTC", "AVAX", "S"], "coingecko_id": ["bitcoin", "avax", "s"]})
    bad = placeholder_coingecko_ids(dim, ALLOW)
    assert sorted(bad.asset_id) == ["AVAX", "S"]
    assert bad.set_index("asset_id").loc["AVAX", "fetched_coingecko_id"] == "avalanche-2"


def test_bitcoin_slug_collision():
    hits = slug_ticker_collisions(["BTC", "BITCOIN", "AVAX"], ALLOW)
    assert hits.asset_id.tolist() == ["BITCOIN"]
    assert hits.iloc[0].collides_with_ticker == "BTC"


def test_no_collision_when_ticker_holds_its_own_slug():
    allow = pd.DataFrame({"symbol": ["BTC", "BITCOIN"], "coingecko_id": ["bitcoin", "bitcoin-wrapper"]})
    # 'BITCOIN' key spells the slug of BTC's coin -> still a collision
    assert slug_ticker_collisions(["BITCOIN"], allow).shape[0] == 1
    allow2 = pd.DataFrame({"symbol": ["ETH"], "coingecko_id": ["ethereum"]})
    assert slug_ticker_collisions(["ETH"], allow2).empty


def test_binance_multiplier_prefixes():
    assert binance_base_to_asset("1000PEPE") == ("PEPE", 1000.0)
    assert binance_base_to_asset("1000000MOG") == ("MOG", 1e6)
    assert binance_base_to_asset("1MBABYDOGE") == ("BABYDOGE", 1e6)
    assert binance_base_to_asset("1INCH") == ("1INCH", 1.0)
    assert binance_base_to_asset("BTC") == ("BTC", 1.0)


def _series(values, start="2025-01-01"):
    return pd.Series(values, index=pd.date_range(start, periods=len(values), freq="D"))


def test_price_identity_ok_with_one_day_lake_lag():
    rng = np.random.default_rng(0)
    px = 100 * np.exp(np.cumsum(rng.normal(0, 0.03, 200)))
    binance = _series(px)
    lake = binance.copy(); lake.index = lake.index + pd.Timedelta(days=1)
    r = price_identity(lake, binance)
    assert r["status"] == "OK"
    # a one-day misalignment would still pass the 10% tolerance on a random walk, so assert the
    # lag through the median error, which is exactly zero only when the dates line up
    assert r["med_abs_log"] < 1e-12
    assert price_identity(lake, binance, lake_date_lag_days=0)["med_abs_log"] > 1e-3


def test_price_identity_detects_splice_and_current_wrong_coin():
    binance = _series(np.full(200, 0.50))
    lake = binance.copy(); lake.index = lake.index + pd.Timedelta(days=1)
    spliced = lake.copy(); spliced.iloc[:60] = 3e-9            # another coin before listing
    r = price_identity(spliced, binance)
    assert r["status"] == "SPLICED_HISTORY" and r["bad_runs"][0][2] >= 55
    wrong_now = lake.copy(); wrong_now.iloc[-40:] = 0.004      # switched coin recently
    assert price_identity(wrong_now, binance)["status"] == "CURRENT_WRONG_COIN"


def test_implied_supply_step_catches_mcap_from_other_coin():
    d = pd.date_range("2026-03-01", periods=10, freq="D")
    lake = pd.DataFrame({"asset_id": "AVAX", "date": d, "close": 27.0,
                         "marketcap": [27.0 * 410e6] * 5 + [55e6] * 5})
    steps = implied_supply_steps(lake)
    assert steps.shape[0] == 1 and steps.iloc[0].date == d[5]


def test_market_wide_splice_dates_flags_mass_rebinding_only():
    d = pd.date_range("2026-03-01", periods=6, freq="D")
    rows = []
    for i in range(30):                                   # 30 assets re-bound on day 3
        caps = [1e9] * 3 + [2e6] * 3
        rows += [{"asset_id": f"A{i}", "date": x, "marketcap": c} for x, c in zip(d, caps)]
    rows += [{"asset_id": "LONE", "date": x, "marketcap": c} for x, c in zip(d, [1e6, 1e6, 5e6, 5e6, 5e6, 5e6])]
    lake = pd.DataFrame(rows)
    hits = market_wide_splice_dates(lake, "marketcap", min_assets=20)
    assert hits.index.tolist() == [d[3]] and hits.iloc[0] == 30
    # a single coin moving 5x is not a mass splice
    assert market_wide_splice_dates(lake[lake.asset_id == "LONE"], "marketcap", min_assets=1).index.tolist() == [d[2]]
    assert market_wide_splice_dates(lake[lake.asset_id == "LONE"], "marketcap", min_assets=2).empty


def test_mcap_vs_snapshot_joins_on_fetched_id_not_ticker():
    lake_day = pd.DataFrame({"asset_id": ["AVAX", "BTC"], "close": [27.0, 90_000.0],
                             "marketcap": [55e6, 1.8e12]})
    snap = pd.DataFrame({"coingecko_id": ["avalanche-2", "bitcoin", "harrypotterobamasonic10in"],
                         "current_price_usd": [27.1, 90_100.0, 0.08], "market_cap_usd": [11.1e9, 1.8e12, 8e7]})
    bad = mcap_vs_snapshot(lake_day, snap, ALLOW)
    assert bad.asset_id.tolist() == ["AVAX"]
