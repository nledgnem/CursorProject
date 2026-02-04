"""Read-only data I/O layer for data lake access."""

import polars as pl
import pandas as pd
from pathlib import Path
from typing import Dict, Optional, List
from datetime import date, datetime
import duckdb
import logging

logger = logging.getLogger(__name__)


class ReadOnlyDataLoader:
    """Read-only data loader for data lake. Never writes to data/."""
    
    def __init__(
        self,
        data_lake_dir: Path,
        duckdb_path: Optional[Path] = None,
        universe_snapshots_path: Optional[Path] = None,
    ):
        """
        Initialize read-only data loader.
        
        Args:
            data_lake_dir: Path to data/curated/data_lake/
            duckdb_path: Optional path to DuckDB database (read-only)
            universe_snapshots_path: Optional path to universe_snapshots.parquet
        """
        self.data_lake_dir = Path(data_lake_dir)
        self.duckdb_path = Path(duckdb_path) if duckdb_path else None
        self.universe_snapshots_path = Path(universe_snapshots_path) if universe_snapshots_path else None
        
        # Validate paths exist
        if not self.data_lake_dir.exists():
            raise FileNotFoundError(f"Data lake directory not found: {self.data_lake_dir}")
        
        # Initialize DuckDB connection if available
        self.conn = None
        if self.duckdb_path and self.duckdb_path.exists():
            try:
                self.conn = duckdb.connect(str(self.duckdb_path), read_only=True)
                logger.info(f"Connected to DuckDB (read-only): {self.duckdb_path}")
            except Exception as e:
                logger.warning(f"Could not connect to DuckDB: {e}")
        
        # Load dimension tables
        self._load_dimensions()
    
    def _load_dimensions(self):
        """Load dimension tables (assets, instruments)."""
        dim_asset_path = self.data_lake_dir / "dim_asset.parquet"
        if dim_asset_path.exists():
            self.dim_asset = pl.read_parquet(dim_asset_path)
            logger.info(f"Loaded dim_asset: {len(self.dim_asset)} assets")
        else:
            self.dim_asset = None
            logger.warning("dim_asset.parquet not found")
        
        dim_instrument_path = self.data_lake_dir / "dim_instrument.parquet"
        if dim_instrument_path.exists():
            self.dim_instrument = pl.read_parquet(dim_instrument_path)
            logger.info(f"Loaded dim_instrument: {len(self.dim_instrument)} instruments")
        else:
            self.dim_instrument = None
    
    def _normalize_symbol(self, symbol: str) -> str:
        """Normalize symbol (strip PERP/-PERP, map SATS→1000SATS)."""
        s = symbol.upper().strip()
        # Strip perp suffixes
        for suffix in ["-PERP", "PERP", "-USDT", "-USD"]:
            if s.endswith(suffix):
                s = s[:-len(suffix)]
        # Map SATS
        if s == "SATS":
            s = "1000SATS"
        return s
    
    def _get_stablecoins(self) -> List[str]:
        """Get list of stablecoin asset_ids."""
        if self.dim_asset is None:
            return []
        
        # Get stablecoins from dim_asset
        stables = (
            self.dim_asset
            .filter(pl.col("is_stable") == True)
            .select("asset_id")
            .to_series()
            .to_list()
        )
        
        # Also check blacklist if available
        blacklist_path = self.data_lake_dir.parent.parent / "blacklist.csv"
        if blacklist_path.exists():
            blacklist_df = pl.read_csv(blacklist_path)
            if "asset_id" in blacklist_df.columns:
                stables.extend(blacklist_df["asset_id"].to_list())
            elif "symbol" in blacklist_df.columns:
                stables.extend(blacklist_df["symbol"].to_list())
        
        return list(set(stables))
    
    def load_dataset(
        self,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> Dict[str, pl.DataFrame]:
        """
        Load all datasets from data lake (read-only).
        
        Returns dict with keys:
        - prices: (asset_id, date, close)
        - marketcap: (asset_id, date, marketcap)
        - volume: (asset_id, date, volume)
        - funding: (asset_id, instrument_id, date, funding_rate) [if available]
        - open_interest: (asset_id, date, open_interest_usd) [if available]
        - universe_snapshots: (rebalance_date, asset_id, weight, ...) [if available]
        """
        result = {}
        
        # Load fact tables
        for table_name, value_col in [
            ("fact_price", "close"),
            ("fact_marketcap", "marketcap"),
            ("fact_volume", "volume"),
        ]:
            table_path = self.data_lake_dir / f"{table_name}.parquet"
            if table_path.exists():
                df = pl.read_parquet(table_path)
                
                # Filter by date range
                if start or end:
                    date_col = "date"
                    if start:
                        df = df.filter(pl.col(date_col) >= pl.date(start.year, start.month, start.day))
                    if end:
                        df = df.filter(pl.col(date_col) <= pl.date(end.year, end.month, end.day))
                
                result[table_name.replace("fact_", "")] = df
                logger.info(f"Loaded {table_name}: {len(df)} rows")
            else:
                logger.warning(f"{table_name}.parquet not found")
        
        # Load funding if available
        funding_path = self.data_lake_dir / "fact_funding.parquet"
        if funding_path.exists():
            df = pl.read_parquet(funding_path)
            if start or end:
                date_col = "date"
                if start:
                    df = df.filter(pl.col(date_col) >= pl.date(start.year, start.month, start.day))
                if end:
                    df = df.filter(pl.col(date_col) <= pl.date(end.year, end.month, end.day))
            result["funding"] = df
            logger.info(f"Loaded fact_funding: {len(df)} rows")
        
        # Load open interest if available
        oi_path = self.data_lake_dir / "fact_open_interest.parquet"
        if oi_path.exists():
            df = pl.read_parquet(oi_path)
            if start or end:
                date_col = "date"
                if start:
                    df = df.filter(pl.col(date_col) >= pl.date(start.year, start.month, start.day))
                if end:
                    df = df.filter(pl.col(date_col) <= pl.date(end.year, end.month, end.day))
            result["open_interest"] = df
            logger.info(f"Loaded fact_open_interest: {len(df)} rows")
        else:
            logger.warning("fact_open_interest.parquet not found - OI features will use marketcap proxy")
        
        # Load universe snapshots if available
        if self.universe_snapshots_path and self.universe_snapshots_path.exists():
            df = pl.read_parquet(self.universe_snapshots_path)
            if start or end:
                date_col = "rebalance_date"
                if start:
                    df = df.filter(pl.col(date_col) >= pl.date(start.year, start.month, start.day))
                if end:
                    df = df.filter(pl.col(date_col) <= pl.date(end.year, end.month, end.day))
            result["universe_snapshots"] = df
            logger.info(f"Loaded universe_snapshots: {len(df)} rows")
        
        return result
    
    def get_universe_at_date(
        self,
        asof_date: date,
        universe_snapshots: Optional[pl.DataFrame] = None,
    ) -> pl.DataFrame:
        """
        Get universe of eligible assets at a specific date (PIT-safe).
        
        If universe_snapshots is provided, use it. Otherwise, infer from fact tables.
        """
        if universe_snapshots is not None:
            # Use provided snapshots
            snapshots = universe_snapshots.filter(
                pl.col("rebalance_date") <= pl.date(asof_date.year, asof_date.month, asof_date.day)
            )
            if len(snapshots) == 0:
                return pl.DataFrame()
            
            # Get most recent snapshot
            latest_snapshot = (
                snapshots
                .sort("rebalance_date", descending=True)
                .head(1)
            )
            latest_date = latest_snapshot["rebalance_date"][0]
            
            # Get all assets in that snapshot
            universe = snapshots.filter(pl.col("rebalance_date") == latest_date)
            return universe
        
        # Fallback: infer from fact tables (less reliable)
        # This is a simplified version - in practice, you'd want more sophisticated logic
        logger.warning("Inferring universe from fact tables (not recommended for PIT)")
        
        # Load prices up to asof_date
        prices = pl.read_parquet(self.data_lake_dir / "fact_price.parquet")
        prices = prices.filter(
            (pl.col("date") <= pl.date(asof_date.year, asof_date.month, asof_date.day)) &
            (pl.col("date") >= pl.date(asof_date.year, asof_date.month, asof_date.day) - pl.duration(days=30))
        )
        
        # Get assets with recent data
        universe = (
            prices
            .group_by("asset_id")
            .agg(pl.count().alias("n_days"))
            .filter(pl.col("n_days") >= 20)
            .select("asset_id")
        )
        
        return universe
    
    def get_asset_metadata(self) -> pl.DataFrame:
        """Get asset metadata (symbol, name, etc.)."""
        if self.dim_asset is None:
            return pl.DataFrame()
        return self.dim_asset
    
    def close(self):
        """Close DuckDB connection if open."""
        if self.conn:
            self.conn.close()
            self.conn = None
