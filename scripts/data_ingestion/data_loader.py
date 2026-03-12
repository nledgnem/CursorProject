from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable, List, Dict, Set

import pandas as pd


# Default: relative to cwd. Override via base_path when running from different working dirs.
DATA_LAKE_BASE = Path("data/curated/data_lake")


@dataclass
class AssetMetadata:
    asset_id: str
    symbol: str
    name: str | None
    chain: str | None
    is_stable: bool
    is_wrapped_stable: bool
    coingecko_id: str | None = None


class DataLoader:
    """
    Thin wrapper around the standardized data lake tables for BTCDOM.

    Responsibilities:
    - Load dim_asset / silver_fact_price / silver_fact_marketcap (clean Silver layer)
    - Provide utilities to:
        * Look up BTC/ETH asset_ids
        * Filter eligible universe on a given date (exclude BTC, stables, wrapped/staked)
        * Fetch daily close prices and market caps for a set of asset_ids over a date range
    """

    def __init__(self, base_path: Path | str | None = None) -> None:
        base = Path(base_path) if base_path is not None else DATA_LAKE_BASE
        self.base = base

        self.dim_asset = pd.read_parquet(self.base / "dim_asset.parquet")
        self.fact_price = pd.read_parquet(self.base / "silver_fact_price.parquet")
        self.fact_marketcap = pd.read_parquet(self.base / "silver_fact_marketcap.parquet")
        if "market_cap" in self.fact_marketcap.columns and "marketcap" not in self.fact_marketcap.columns:
            self.fact_marketcap = self.fact_marketcap.rename(columns={"market_cap": "marketcap"})

        # Normalize date columns
        for df in (self.fact_price, self.fact_marketcap):
            df["date"] = pd.to_datetime(df["date"]).dt.date

        # Cache of asset_id -> AssetMetadata
        self._asset_meta: Dict[str, AssetMetadata] = {}
        self._build_asset_metadata_cache()

        # BTC / ETH canonical IDs (can be overridden if needed)
        self.btc_asset_ids: Set[str] = self._infer_btc_ids()
        self.eth_asset_ids: Set[str] = self._infer_eth_ids()

    # ------------------------------------------------------------------
    # Metadata helpers
    # ------------------------------------------------------------------

    def _build_asset_metadata_cache(self) -> None:
        for _, row in self.dim_asset.iterrows():
            asset_id = str(row["asset_id"])
            self._asset_meta[asset_id] = AssetMetadata(
                asset_id=asset_id,
                symbol=str(row.get("symbol")) if row.get("symbol") is not None else asset_id,
                name=str(row.get("name")) if row.get("name") is not None else None,
                chain=str(row.get("chain")) if row.get("chain") is not None else None,
                is_stable=bool(row.get("is_stable", False)),
                is_wrapped_stable=bool(row.get("is_wrapped_stable", False)),
                coingecko_id=str(row.get("coingecko_id")) if row.get("coingecko_id") is not None else None,
            )

    def _infer_btc_ids(self) -> Set[str]:
        """
        Infer BTC asset_ids from dim_asset.
        We explicitly choose the canonical traded BTC asset_id (asset_id == 'BTC').
        """
        ids: Set[str] = set()
        for asset_id, meta in self._asset_meta.items():
            if asset_id == "BTC":
                ids.add(asset_id)
        return ids

    def _infer_eth_ids(self) -> Set[str]:
        ids: Set[str] = set()
        for asset_id, meta in self._asset_meta.items():
            sym = meta.symbol.upper()
            cg = (meta.coingecko_id or "").lower()
            if sym == "ETH" or cg in {"ethereum", "eth"}:
                ids.add(asset_id)
        return ids

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_asset_metadata(self, asset_id: str) -> AssetMetadata | None:
        return self._asset_meta.get(asset_id)

    def get_btc_asset_ids(self) -> Set[str]:
        return set(self.btc_asset_ids)

    def get_eth_asset_ids(self) -> Set[str]:
        return set(self.eth_asset_ids)

    @staticmethod
    def _wrapped_staked_denylist() -> Set[str]:
        return {
            "WBTC", "WETH", "STETH", "WSTETH", "BTCB", "RENBTC",
            "WBTC.E", "STKETH", "CBETH", "RSETH",
        }

    def get_eligible_universe_on_date(self, d: date) -> pd.DataFrame:
        mc_day = self.fact_marketcap[self.fact_marketcap["date"] == d]
        if mc_day.empty:
            return mc_day

        merged = mc_day.merge(
            self.dim_asset,
            on="asset_id",
            how="left",
            suffixes=("", "_asset"),
        )
        deny = self._wrapped_staked_denylist()

        def is_excluded(row: pd.Series) -> bool:
            asset_id = str(row["asset_id"])
            symbol = str(row.get("symbol", "") or "")
            upper_symbol = symbol.upper()
            if asset_id in self.btc_asset_ids:
                return True
            if bool(row.get("is_stable", False)) or bool(row.get("is_wrapped_stable", False)):
                return True
            if upper_symbol in deny:
                return True
            if any(sub in upper_symbol for sub in ["USD", "EUR", "GBP", "DAI"]):
                return True
            if upper_symbol != "BTC" and "BTC" in upper_symbol:
                return True
            if upper_symbol != "ETH" and "ETH" in upper_symbol:
                return True
            if any(sub in upper_symbol for sub in ["BULL", "BEAR", "UP", "DOWN"]):
                return True
            return False

        mask = ~merged.apply(is_excluded, axis=1)
        return merged.loc[mask].copy()

    def get_marketcaps(
        self,
        asset_ids: Iterable[str],
        start: date,
        end: date,
    ) -> pd.DataFrame:
        asset_ids = list(asset_ids)
        mc = self.fact_marketcap[
            (self.fact_marketcap["asset_id"].isin(asset_ids))
            & (self.fact_marketcap["date"] >= start)
            & (self.fact_marketcap["date"] <= end)
        ].copy()
        return mc

    def get_prices(
        self,
        asset_ids: Iterable[str],
        start: date,
        end: date,
    ) -> pd.DataFrame:
        asset_ids = list(asset_ids)
        pr = self.fact_price[
            (self.fact_price["asset_id"].isin(asset_ids))
            & (self.fact_price["date"] >= start)
            & (self.fact_price["date"] <= end)
        ].copy()
        return pr

    def iter_days(self, start: date, end: date) -> List[date]:
        days: List[date] = []
        cur = start
        while cur <= end:
            days.append(cur)
            cur += timedelta(days=1)
        return days
