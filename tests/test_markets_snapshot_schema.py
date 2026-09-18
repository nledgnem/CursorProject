"""fact_markets_snapshot froze 2026-08-05..2026-09-18: CoinGecko returned fully_diluted_valuation
~1e24 for two coins, beyond Int64, and the inferred-schema frame build raised every day. The day's
frame is now built against the stored schema; unrepresentable values become null."""

from datetime import date

import polars as pl

from scripts.fetch_high_priority_data import SNAPSHOT_SCHEMA, build_snapshot_frame


def _rec(cg_id, fdv, mcap=4_000_000):
    return {"date": date(2026, 9, 18),
            "asset_id": cg_id.upper(), "coingecko_id": cg_id, "symbol": cg_id[:4].upper(), "name": cg_id,
            "current_price_usd": 0.01, "market_cap_usd": mcap, "market_cap_rank": 2131,
            "fully_diluted_valuation_usd": fdv, "total_volume_usd": 1e5, "circulating_supply": 4e8,
            "ath_date": None, "atl_date": None, "source": "coingecko"}


def test_oversized_fdv_becomes_null_and_frame_matches_stored_schema():
    df = build_snapshot_frame([_rec("linqai", 6857459302234878640128000), _rec("okcoin", 4_012_345)])
    assert df.schema == pl.Schema(SNAPSHOT_SCHEMA)
    fdv = dict(zip(df["coingecko_id"], df["fully_diluted_valuation_usd"]))
    assert fdv["linqai"] is None and fdv["okcoin"] == 4_012_345


def test_float_after_row_100_is_not_truncated_or_rejected():
    recs = [_rec(f"c{i}", 1_000_000 + i) for i in range(150)] + [_rec("late", 1.5e9, mcap=2.5e8)]
    df = build_snapshot_frame(recs)
    row = df.filter(pl.col("coingecko_id") == "late")
    assert row["fully_diluted_valuation_usd"][0] == 1_500_000_000 and row["market_cap_usd"][0] == 250_000_000


def test_concat_with_existing_file_schema():
    existing = build_snapshot_frame([_rec("okcoin", 1)])
    assert pl.concat([existing, build_snapshot_frame([_rec("linqai", 10**25)])]).height == 2
