"""Read-side access to the asset registry and identity policy (decision 2026-09-18).

data/asset_registry.csv: immutable asset_uid with effective-dated coingecko_id / binance_symbol.
configs/asset_identity_policy.yaml: acknowledged slug collisions and retired aliases.

Use these instead of matching tickers or lower-casing symbols: a ticker is an attribute of an
asset, not its identity (see reports/incidents/2026-09-18_asset_identity/README.md).
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import pandas as pd
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
REGISTRY_PATH = REPO_ROOT / "data" / "asset_registry.csv"
POLICY_PATH = REPO_ROOT / "configs" / "asset_identity_policy.yaml"


@lru_cache(maxsize=4)
def load_policy(path: Path = POLICY_PATH) -> dict:
    if not Path(path).exists():
        return {"acknowledged_slug_collisions": {}, "aliases": {}}
    p = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    return {"acknowledged_slug_collisions": p.get("acknowledged_slug_collisions") or {},
            "aliases": p.get("aliases") or {}}


def load_registry(path: Path = REGISTRY_PATH) -> pd.DataFrame:
    return pd.read_csv(path)


def coingecko_to_uid(registry: pd.DataFrame, policy: dict | None = None) -> dict[str, str]:
    """coingecko_id -> canonical asset_uid. A coin held by several uids resolves to the uid that is
    not a retired alias; ambiguity without a declared alias raises (the invariant is broken)."""
    aliases = (policy or load_policy())["aliases"]
    reg = registry.drop_duplicates("asset_uid")
    reg = reg[reg["coingecko_id"].notna() & ~reg["asset_uid"].isin(set(aliases))]
    dup = reg[reg["coingecko_id"].duplicated(keep=False)]
    if len(dup):
        raise ValueError(f"coins held by several uids without a declared alias: "
                         f"{dup.groupby('coingecko_id')['asset_uid'].apply(list).to_dict()}")
    return dict(zip(reg["coingecko_id"], reg["asset_uid"]))


def snapshot_asset_id(coingecko_id: str, cg_to_uid: dict[str, str]) -> str:
    """Key for a /coins/markets row: the registry uid, else a namespaced 'CG:<id>' that can never
    collide with a ticker uid and does not change when CoinGecko renames the ticker."""
    return cg_to_uid.get(coingecko_id) or f"CG:{coingecko_id}"
