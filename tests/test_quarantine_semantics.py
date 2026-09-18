"""A quarantine must stay missing downstream -- never become an implicit forward-fill.

End to end on a tiny lake: the real repair `apply` moves another coin's rows out of the fact
tables, then the real silver builder, the MSM loader, universe selection and returns run on it.

  AAA  quarantined 2026-03-05..03-10 (wrong coin mid-history)
  CCC  quarantined 2026-03-01..03-03 (rows before the coin existed; leading)
  BBB  ordinary gap on 2026-03-15 (not quarantined)
Closes are distinct per day (day-of-month), so any forward-fill is visible.
"""

import argparse
from datetime import date

import pandas as pd
import polars as pl
import pytest

import scripts.repair_asset_identity as rep
from majors_alts_monitor.msm_funding_v0.msm_data import MSMDataLoader
from majors_alts_monitor.msm_funding_v0.msm_returns import compute_alt_constituent_simple_returns, get_close_asof
from majors_alts_monitor.msm_funding_v0.msm_universe import select_top_n_alts
from scripts.data_ingestion.build_silver_layer import build_silver_fact_marketcap, build_silver_fact_price
from src.data_lake.quarantine import identity_quarantine_keys

D = pd.date_range("2026-03-01", "2026-03-20", freq="D")
Q_AAA = pd.date_range("2026-03-05", "2026-03-10", freq="D")
Q_CCC = pd.date_range("2026-03-01", "2026-03-03", freq="D")


@pytest.fixture()
def lake(tmp_path):
    lake = tmp_path / "lake"
    lake.mkdir()
    rows = []
    for d in D:
        for a, base in (("AAA", 100.0), ("BBB", 200.0), ("CCC", 300.0)):
            if a == "BBB" and d == pd.Timestamp("2026-03-15"):
                continue                                           # ordinary gap
            rows.append({"asset_id": a, "date": d.date(), "close": base + d.day,
                         "marketcap": (base + d.day) * 1e7, "volume": 1e6})
    df = pd.DataFrame(rows)
    for t, c in rep.FACT_TABLES.items():
        df[["asset_id", "date", c]].assign(source="coingecko").to_parquet(lake / f"{t}.parquet", index=False)
    pd.DataFrame({"asset_id": ["AAA", "BBB", "CCC"], "instrument_id": ["i1", "i2", "i3"], "date": [D[0].date()] * 3,
                  "funding_rate_raw_pct": [0.01] * 3}).to_parquet(lake / "silver_fact_funding.parquet", index=False)

    frames = {t: rep._read_fact(lake, t) for t in rep.FACT_TABLES}
    grouped = rep._by_asset(frames)
    man = tmp_path / "man"
    man.mkdir()
    segs, quar = [], []
    for uid, days in (("AAA", list(Q_AAA)), ("CCC", list(Q_CCC))):
        segs.append({"seg_id": uid, "asset_uid": uid, "class": "long_splice", "tables": "all",
                     "start": days[0].date(), "end": days[-1].date(), "apply_default": True,
                     "before_hash": rep._segment_hash(grouped, uid, days)})
        quar += [{"seg_id": uid, "asset_id": uid, "date": d, "table": t, "reason": "wrong coin"}
                 for d in days for t in rep.FACT_TABLES]
    pd.DataFrame(segs).to_csv(man / "manifest_segments.csv", index=False)
    pd.DataFrame(columns=["seg_id", "asset_id", "date", "table", "value"]).astype(
        {"date": "datetime64[ns]", "value": float}).to_parquet(man / "replacement_rows.parquet", index=False)
    pd.DataFrame(quar).to_parquet(man / "quarantine_keys.parquet", index=False)
    assert rep.cmd_apply(argparse.Namespace(lake_dir=str(lake), manifest_dir=str(man), classes=None,
                                            skip_changed=False, yes=True, include_dim=False,
                                            rekey_snapshot=False)) == 0
    silver_price, _ = build_silver_fact_price(lake)
    silver_price.to_parquet(lake / "silver_fact_price.parquet", index=False)
    silver_mcap, _ = build_silver_fact_marketcap(lake, silver_price)
    silver_mcap.to_parquet(lake / "silver_fact_marketcap.parquet", index=False)
    return lake


def _rows(df, asset, days):
    df = df.assign(date=pd.to_datetime(df["date"]))
    return df[(df["asset_id"] == asset) & df["date"].isin(days)].sort_values("date")


def test_quarantined_rows_are_missing_from_bronze(lake):
    for t in rep.FACT_TABLES:
        f = rep._read_fact(lake, t)
        assert _rows(f, "AAA", Q_AAA).empty and _rows(f, "CCC", Q_CCC).empty
    assert len(identity_quarantine_keys(lake, "fact_price")) == len(Q_AAA) + len(Q_CCC)


def test_silver_keeps_quarantine_missing_and_flagged_never_ffilled(lake):
    sp = pd.read_parquet(lake / "silver_fact_price.parquet")
    sm = pd.read_parquet(lake / "silver_fact_marketcap.parquet")
    for df, col in ((sp, "close"), (sm, "market_cap")):
        mid = _rows(df, "AAA", Q_AAA)
        assert len(mid) == len(Q_AAA) and mid[col].isna().all() and mid["is_identity_quarantined"].all()
        lead = _rows(df, "CCC", Q_CCC)                         # leading dates exist, NaN, flagged
        assert len(lead) == len(Q_CCC) and lead[col].isna().all() and lead["is_identity_quarantined"].all()
        after = _rows(df, "AAA", [pd.Timestamp("2026-03-11")])
        assert after[col].iloc[0] == pytest.approx((100 + 11) * (1e7 if col == "market_cap" else 1))
    assert not _rows(sp, "AAA", Q_AAA)["is_ffilled"].any()


def test_ordinary_gap_is_distinguishable_from_identity_quarantine(lake):
    sp = pd.read_parquet(lake / "silver_fact_price.parquet")
    gap = _rows(sp, "BBB", [pd.Timestamp("2026-03-15")])
    assert len(gap) == 1 and gap["close"].isna().all() and not gap["is_identity_quarantined"].any()
    assert int(sp["is_identity_quarantined"].sum()) == len(Q_AAA) + len(Q_CCC)


def test_msm_never_sees_quarantined_values(lake):
    data = MSMDataLoader(lake).load_datasets()
    px = data["prices"].with_columns(pl.col("date").cast(pl.Date))
    q = px.filter((pl.col("asset_id") == "AAA") & pl.col("date").is_in([d.date() for d in Q_AAA]))
    assert q.height == 0                                      # NaN rows dropped by the loader


def test_universe_does_not_substitute_a_stale_market_cap(lake):
    mc = pl.from_pandas(rep._read_fact(lake, "fact_marketcap")).with_columns(pl.col("date").cast(pl.Date))
    asof = date(2026, 3, 8)                                   # inside AAA's quarantine
    picked = select_top_n_alts(mc, asof, n=3, min_mcap_usd=0, max_mcap_age_days=3)
    assert "AAA" not in picked["asset_id"].to_list()
    # the unbounded lookup (pre-fix behaviour) would rank AAA on its 03-04 cap
    assert "AAA" in select_top_n_alts(mc, asof, n=3, min_mcap_usd=0)["asset_id"].to_list()


def test_returns_do_not_bridge_a_quarantine_gap(lake):
    px = MSMDataLoader(lake).load_datasets()["prices"].with_columns(pl.col("date").cast(pl.Date))
    assert get_close_asof(px, "AAA", date(2026, 3, 8)) is None
    assert get_close_asof(px, "AAA", date(2026, 3, 8), max_age_days=None)[0] == date(2026, 3, 4)   # old behaviour
    rets = compute_alt_constituent_simple_returns(px, ["AAA", "BBB"], date(2026, 3, 8), date(2026, 3, 12))
    assert "AAA" not in rets and "BBB" in rets
