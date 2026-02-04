"""Data loading and sanity checks for MSM v0."""

import polars as pl
from pathlib import Path
from typing import Dict, Optional, Tuple
from datetime import date, datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class MSMDataLoader:
    """Read-only data loader for MSM v0 using existing data lake patterns."""
    
    def __init__(
        self,
        data_lake_dir: Path,
        duckdb_path: Optional[Path] = None,
    ):
        """
        Initialize data loader.
        
        Args:
            data_lake_dir: Path to data/curated/data_lake/
            duckdb_path: Optional path to DuckDB database (read-only)
        """
        self.data_lake_dir = Path(data_lake_dir)
        self.duckdb_path = Path(duckdb_path) if duckdb_path else None
        
        # Validate paths exist
        if not self.data_lake_dir.exists():
            raise FileNotFoundError(f"Data lake directory not found: {self.data_lake_dir}")
        
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
    
    def load_datasets(
        self,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> Dict[str, pl.DataFrame]:
        """
        Load required datasets from data lake.
        
        Returns dict with keys:
        - prices: (asset_id, date, close)
        - marketcap: (asset_id, date, marketcap)
        - funding: (asset_id, instrument_id, date, funding_rate)
        - dim_asset: (asset_id, symbol, is_stable, ...)
        """
        result = {}
        
        # Load prices
        prices_path = self.data_lake_dir / "fact_price.parquet"
        if prices_path.exists():
            df = pl.read_parquet(prices_path)
            if start or end:
                if start:
                    df = df.filter(pl.col("date") >= pl.date(start.year, start.month, start.day))
                if end:
                    df = df.filter(pl.col("date") <= pl.date(end.year, end.month, end.day))
            result["prices"] = df
            logger.info(f"Loaded fact_price: {len(df)} rows")
        else:
            raise FileNotFoundError(f"fact_price.parquet not found at {prices_path}")
        
        # Load marketcap
        marketcap_path = self.data_lake_dir / "fact_marketcap.parquet"
        if marketcap_path.exists():
            df = pl.read_parquet(marketcap_path)
            if start or end:
                if start:
                    df = df.filter(pl.col("date") >= pl.date(start.year, start.month, start.day))
                if end:
                    df = df.filter(pl.col("date") <= pl.date(end.year, end.month, end.day))
            result["marketcap"] = df
            logger.info(f"Loaded fact_marketcap: {len(df)} rows")
        else:
            raise FileNotFoundError(f"fact_marketcap.parquet not found at {marketcap_path}")
        
        # Load funding
        funding_path = self.data_lake_dir / "fact_funding.parquet"
        if funding_path.exists():
            df = pl.read_parquet(funding_path)
            if start or end:
                if start:
                    df = df.filter(pl.col("date") >= pl.date(start.year, start.month, start.day))
                if end:
                    df = df.filter(pl.col("date") <= pl.date(end.year, end.month, end.day))
            result["funding"] = df
            logger.info(f"Loaded fact_funding: {len(df)} rows")
        else:
            logger.warning("fact_funding.parquet not found - funding features will be unavailable")
            result["funding"] = pl.DataFrame()
        
        # Add dim_asset
        if self.dim_asset is not None:
            result["dim_asset"] = self.dim_asset
        
        return result
    
    def get_asset_metadata(self) -> pl.DataFrame:
        """Get asset metadata."""
        if self.dim_asset is None:
            return pl.DataFrame()
        return self.dim_asset


def data_sanity_check(
    data_lake_dir: Path,
    top_n: int = 30,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> Dict:
    """
    Perform data sanity check: print earliest/latest funding dates and estimated weekly coverage.
    
    Args:
        data_lake_dir: Path to data lake
        top_n: Number of top assets to check
        start_date: Optional start date for analysis
        end_date: Optional end date for analysis
    
    Returns:
        Dict with sanity check results
    """
    loader = MSMDataLoader(data_lake_dir)
    datasets = loader.load_datasets(start=start_date, end=end_date)
    
    funding = datasets.get("funding", pl.DataFrame())
    marketcap = datasets.get("marketcap", pl.DataFrame())
    prices = datasets.get("prices", pl.DataFrame())
    
    results = {}
    
    # Funding date range
    if len(funding) > 0:
        funding_dates = funding["date"].unique().sort()
        results["funding_earliest"] = funding_dates[0]
        results["funding_latest"] = funding_dates[-1]
        results["funding_n_days"] = len(funding_dates)
        print(f"Funding data: {results['funding_earliest']} to {results['funding_latest']} ({results['funding_n_days']} days)")
    else:
        results["funding_earliest"] = None
        results["funding_latest"] = None
        results["funding_n_days"] = 0
        print("WARNING: No funding data found")
    
    # Marketcap date range
    if len(marketcap) > 0:
        mcap_dates = marketcap["date"].unique().sort()
        results["marketcap_earliest"] = mcap_dates[0]
        results["marketcap_latest"] = mcap_dates[-1]
        results["marketcap_n_days"] = len(mcap_dates)
        print(f"Marketcap data: {results['marketcap_earliest']} to {results['marketcap_latest']} ({results['marketcap_n_days']} days)")
    
    # Price date range
    if len(prices) > 0:
        price_dates = prices["date"].unique().sort()
        results["price_earliest"] = price_dates[0]
        results["price_latest"] = price_dates[-1]
        results["price_n_days"] = len(price_dates)
        print(f"Price data: {results['price_earliest']} to {results['price_latest']} ({results['price_n_days']} days)")
    
    # Estimate weekly coverage for top N assets
    if len(funding) > 0 and len(marketcap) > 0:
        # Get top N assets by latest marketcap
        if len(marketcap) > 0:
            latest_date = marketcap["date"].max()
            latest_mcap = marketcap.filter(pl.col("date") == latest_date)
            top_assets = (
                latest_mcap
                .sort("marketcap", descending=True)
                .head(top_n)
                .select("asset_id")
                .to_series()
                .to_list()
            )
            
            # Check funding coverage for these assets
            funding_coverage = []
            for asset_id in top_assets:
                asset_funding = funding.filter(pl.col("asset_id") == asset_id)
                if len(asset_funding) > 0:
                    asset_dates = asset_funding["date"].unique().sort()
                    funding_coverage.append({
                        "asset_id": asset_id,
                        "n_days": len(asset_dates),
                        "earliest": asset_dates[0],
                        "latest": asset_dates[-1],
                    })
            
            results["top_n_coverage"] = {
                "n_assets_checked": len(top_assets),
                "n_assets_with_funding": len(funding_coverage),
                "coverage_pct": (len(funding_coverage) / len(top_assets) * 100) if top_assets else 0.0,
                "assets": funding_coverage,
            }
            
            print(f"\nTop {top_n} assets funding coverage:")
            print(f"  Assets with funding: {len(funding_coverage)}/{len(top_assets)} ({results['top_n_coverage']['coverage_pct']:.1f}%)")
            if len(funding_coverage) > 0:
                avg_days = sum(a["n_days"] for a in funding_coverage) / len(funding_coverage)
                print(f"  Average funding days per asset: {avg_days:.1f}")
    
    return results
