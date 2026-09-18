"""Identity invariant (decision 2026-09-18): one asset_uid = one economic asset, for all time."""

import pandas as pd
import pyarrow.parquet as pq
import pytest

import scripts.repair_asset_identity as rep
from src.data_lake.asset_registry import coingecko_to_uid, load_policy, snapshot_asset_id

REG = pd.DataFrame({"asset_uid": ["BTC", "BITCOIN", "TON", "GRAM", "QAI", "QFI"],
                    "coingecko_id": ["bitcoin", "harrypotterobamasonic10in", "the-open-network", "gram-2",
                                     "quantixai", "quantixai"]})
POLICY = {"acknowledged_slug_collisions": {}, "aliases": {"QAI": {"alias_of": "QFI"}}}


def test_coin_to_uid_resolves_aliases_to_the_canonical_uid():
    m = coingecko_to_uid(REG, POLICY)
    assert m["bitcoin"] == "BTC" and m["harrypotterobamasonic10in"] == "BITCOIN" and m["quantixai"] == "QFI"


def test_a_coin_held_by_two_uids_without_a_declared_alias_is_refused():
    with pytest.raises(ValueError, match="quantixai"):
        coingecko_to_uid(REG, {"acknowledged_slug_collisions": {}, "aliases": {}})


def test_snapshot_ids_never_follow_the_ticker():
    m = coingecko_to_uid(REG, POLICY)
    assert snapshot_asset_id("the-open-network", m) == "TON"          # CoinGecko ticker is GRAM now
    assert snapshot_asset_id("tokamak-network", m) == "CG:tokamak-network"


def test_repo_policy_acknowledges_the_known_collisions_and_aliases():
    p = load_policy()
    assert p["acknowledged_slug_collisions"]["BITCOIN"]["holds"] == "harrypotterobamasonic10in"
    assert p["aliases"]["QAI"]["alias_of"] == "QFI" and p["aliases"]["AITECH"]["alias_of"] == "ACN"
    coingecko_to_uid(pd.read_csv(rep.REGISTRY_PATH))                    # repo registry honours the invariant


def test_rekey_snapshot_moves_real_bitcoin_off_the_memecoin_uid(tmp_path):
    snap = pd.DataFrame({"date": pd.to_datetime(["2026-08-04"] * 3).date, "asset_id": ["BITCOIN", "GRAM", "TON"],
                         "coingecko_id": ["bitcoin", "the-open-network", "tokamak-network"],
                         "market_cap_usd": [1_273_243_990_273, 3_000_000_000, 20_000_000]})
    path = tmp_path / "fact_markets_snapshot.parquet"
    snap.to_parquet(path, index=False)
    schema = pq.read_schema(path)
    stats = rep._rekey_snapshot(path, out_path=path)
    out = pd.read_parquet(path)
    assert dict(zip(out["coingecko_id"], out["asset_id"])) == {
        "bitcoin": "BTC", "the-open-network": "TON", "tokamak-network": "CG:tokamak-network"}
    assert stats["rows_rekeyed"] == 3 and pq.read_schema(path).equals(schema, check_metadata=False)
