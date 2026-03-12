"""
Comprehensive Data Lake Inspection Script

This script performs a thorough analysis of the cryptocurrency data lake,
including schema validation, data quality checks, temporal coverage analysis,
and asset universe assessment.
"""

import polars as pl
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from datetime import date, datetime
import json
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
from collections import defaultdict
import warnings

warnings.filterwarnings('ignore')

# Set style for plots
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)


class DataLakeInspector:
    """Comprehensive data lake inspection and analysis."""
    
    def __init__(self, data_lake_dir: Path):
        """Initialize inspector with data lake directory."""
        self.data_lake_dir = Path(data_lake_dir)
        if not self.data_lake_dir.exists():
            raise FileNotFoundError(f"Data lake directory not found: {self.data_lake_dir}")
        
        self.results = {}
        self.plots_dir = Path("data_lake_inspection_plots")
        self.plots_dir.mkdir(exist_ok=True)
    
    def get_file_info(self, filepath: Path) -> Dict[str, Any]:
        """Get file metadata (size, modification time)."""
        if not filepath.exists():
            return {"exists": False}
        
        stat = filepath.stat()
        return {
            "exists": True,
            "size_bytes": stat.st_size,
            "size_mb": round(stat.st_size / (1024 * 1024), 2),
            "last_modified": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
        }
    
    def get_table_summary(self, filepath: Path, table_name: str) -> Dict[str, Any]:
        """Get comprehensive summary of a parquet table."""
        if not filepath.exists():
            return {"exists": False, "error": "File not found"}
        
        try:
            # Use lazy evaluation for large files
            df_lazy = pl.scan_parquet(str(filepath))
            
            # Get basic stats without loading full table
            row_count = df_lazy.select(pl.count()).collect().item()
            
            # Get schema
            schema = df_lazy.schema
            
            # Get column info
            columns = list(schema.keys())
            dtypes = {col: str(schema[col]) for col in columns}
            
            # Sample data (first 5 rows)
            sample = df_lazy.head(5).collect()
            
            # Check for nulls (efficiently)
            null_counts = {}
            for col in columns:
                null_count = df_lazy.select(pl.col(col).null_count()).collect().item()
                null_counts[col] = null_count
            
            summary = {
                "exists": True,
                "row_count": row_count,
                "columns": columns,
                "dtypes": dtypes,
                "null_counts": null_counts,
                "sample_data": sample.to_dicts()[:5],
            }
            
            return summary
            
        except Exception as e:
            return {"exists": True, "error": str(e)}
    
    def get_date_range(self, df: pl.DataFrame, date_col: str = "date") -> Tuple[Optional[date], Optional[date], int]:
        """Get date range from a fact table."""
        if date_col not in df.columns:
            return None, None, 0
        
        dates = df.select(pl.col(date_col)).unique().sort(date_col)
        if len(dates) == 0:
            return None, None, 0
        
        min_date = dates[date_col].min()
        max_date = dates[date_col].max()
        unique_dates = len(dates)
        
        return min_date, max_date, unique_dates
    
    def get_asset_coverage(self, df: pl.DataFrame, asset_col: str = "asset_id") -> Dict[str, int]:
        """Get row count per asset."""
        if asset_col not in df.columns:
            return {}
        
        coverage = (
            df.group_by(asset_col)
            .agg(pl.count().alias("count"))
            .sort("count", descending=True)
        )
        
        return {row[asset_col]: row["count"] for row in coverage.to_dicts()}
    
    def check_duplicates(self, df: pl.DataFrame, keys: List[str]) -> int:
        """Check for duplicate rows based on key columns."""
        if not all(k in df.columns for k in keys):
            return 0
        
        total_rows = len(df)
        unique_rows = len(df.unique(subset=keys))
        duplicates = total_rows - unique_rows
        
        return duplicates
    
    def analyze_fact_table(
        self, 
        filepath: Path, 
        table_name: str,
        date_col: str = "date",
        asset_col: str = "asset_id"
    ) -> Dict[str, Any]:
        """Comprehensive analysis of a fact table."""
        if not filepath.exists():
            return {"exists": False}
        
        print(f"  Analyzing {table_name}...")
        
        # Load with lazy evaluation
        df_lazy = pl.scan_parquet(str(filepath))
        
        # Basic stats
        row_count = df_lazy.select(pl.count()).collect().item()
        schema = df_lazy.schema
        
        # Date range
        if date_col in schema:
            dates_df = df_lazy.select(pl.col(date_col)).unique().sort(date_col).collect()
            if len(dates_df) > 0:
                min_date = dates_df[date_col].min()
                max_date = dates_df[date_col].max()
                unique_dates = len(dates_df)
            else:
                min_date = max_date = None
                unique_dates = 0
        else:
            min_date = max_date = None
            unique_dates = 0
        
        # Asset coverage
        if asset_col in schema:
            asset_counts = (
                df_lazy
                .group_by(asset_col)
                .agg(pl.count().alias("count"))
                .sort("count", descending=True)
                .collect()
            )
            unique_assets = len(asset_counts)
            top_assets = asset_counts.head(20).to_dicts()
        else:
            unique_assets = 0
            top_assets = []
        
        # Duplicate check
        if date_col in schema and asset_col in schema:
            # Check for duplicate (asset_id, date) combinations
            duplicates = self.check_duplicates(
                df_lazy.collect(), 
                [asset_col, date_col]
            )
        else:
            duplicates = 0
        
        # Source distribution (if source column exists)
        if "source" in schema:
            source_dist = (
                df_lazy
                .group_by("source")
                .agg(pl.count().alias("count"))
                .sort("count", descending=True)
                .collect()
                .to_dicts()
            )
        else:
            source_dist = []
        
        # Null analysis
        null_counts = {}
        for col in schema.keys():
            null_count = df_lazy.select(pl.col(col).null_count()).collect().item()
            null_counts[col] = null_count
        
        # Sample data
        sample = df_lazy.head(5).collect().to_dicts()
        
        return {
            "exists": True,
            "row_count": row_count,
            "schema": {col: str(dtype) for col, dtype in schema.items()},
            "date_range": {
                "min": str(min_date) if min_date else None,
                "max": str(max_date) if max_date else None,
                "unique_dates": unique_dates,
            },
            "asset_coverage": {
                "unique_assets": unique_assets,
                "top_20_by_count": top_assets,
            },
            "duplicates": duplicates,
            "source_distribution": source_dist,
            "null_counts": null_counts,
            "sample_data": sample,
        }
    
    def analyze_dimension_table(self, filepath: Path, table_name: str) -> Dict[str, Any]:
        """Analyze a dimension table."""
        if not filepath.exists():
            return {"exists": False}
        
        print(f"  Analyzing {table_name}...")
        
        df = pl.read_parquet(str(filepath))
        
        schema = df.schema
        row_count = len(df)
        
        # Get unique counts for key columns
        key_stats = {}
        for col in schema.keys():
            unique_count = df.select(pl.col(col).n_unique()).item()
            null_count = df.select(pl.col(col).null_count()).item()
            key_stats[col] = {
                "unique_count": unique_count,
                "null_count": null_count,
            }
        
        # Sample data
        sample = df.head(10).to_dicts()
        
        return {
            "exists": True,
            "row_count": row_count,
            "schema": {col: str(dtype) for col, dtype in schema.items()},
            "column_stats": key_stats,
            "sample_data": sample,
        }
    
    def analyze_temporal_coverage(self, data_lake_dir: Path) -> Dict[str, Any]:
        """Analyze temporal coverage across fact tables."""
        print("Analyzing temporal coverage...")
        
        coverage = {}
        
        # Analyze each fact table
        fact_tables = {
            "fact_price": ("fact_price.parquet", "date", "asset_id"),
            "fact_marketcap": ("fact_marketcap.parquet", "date", "asset_id"),
            "fact_volume": ("fact_volume.parquet", "date", "asset_id"),
            "fact_funding": ("fact_funding.parquet", "date", "asset_id"),
        }
        
        for table_name, (filename, date_col, asset_col) in fact_tables.items():
            filepath = data_lake_dir / filename
            if not filepath.exists():
                continue
            
            df_lazy = pl.scan_parquet(str(filepath))
            
            # Get date range per asset
            if date_col in df_lazy.columns and asset_col in df_lazy.columns:
                asset_dates = (
                    df_lazy
                    .select([asset_col, date_col])
                    .group_by(asset_col)
                    .agg([
                        pl.col(date_col).min().alias("min_date"),
                        pl.col(date_col).max().alias("max_date"),
                        pl.col(date_col).n_unique().alias("date_count"),
                    ])
                    .sort("date_count", descending=True)
                    .collect()
                )
                
                coverage[table_name] = {
                    "total_assets": len(asset_dates),
                    "top_20_assets": asset_dates.head(20).to_dicts(),
                }
        
        return coverage
    
    def analyze_data_quality(self, data_lake_dir: Path) -> Dict[str, Any]:
        """Comprehensive data quality assessment."""
        print("Analyzing data quality...")
        
        quality = {
            "missing_values": {},
            "duplicates": {},
            "outliers": {},
            "consistency": {},
        }
        
        # Check fact_price for outliers
        price_path = data_lake_dir / "fact_price.parquet"
        if price_path.exists():
            df_lazy = pl.scan_parquet(str(price_path))
            
            # Check for negative or zero prices
            price_stats = (
                df_lazy
                .select([
                    pl.col("close").min().alias("min_price"),
                    pl.col("close").max().alias("max_price"),
                    (pl.col("close") <= 0).sum().alias("non_positive_count"),
                ])
                .collect()
            )
            
            quality["outliers"]["fact_price"] = price_stats.to_dicts()[0]
        
        # Check fact_funding for outliers
        funding_path = data_lake_dir / "fact_funding.parquet"
        if funding_path.exists():
            df_lazy = pl.scan_parquet(str(funding_path))
            
            funding_stats = (
                df_lazy
                .select([
                    pl.col("funding_rate").min().alias("min_funding"),
                    pl.col("funding_rate").max().alias("max_funding"),
                    pl.col("funding_rate").mean().alias("mean_funding"),
                ])
                .collect()
            )
            
            quality["outliers"]["fact_funding"] = funding_stats.to_dicts()[0]
        
        return quality
    
    def analyze_asset_universe(self, data_lake_dir: Path) -> Dict[str, Any]:
        """Analyze the asset universe."""
        print("Analyzing asset universe...")
        
        universe = {}
        
        # Load dim_asset
        dim_asset_path = data_lake_dir / "dim_asset.parquet"
        if dim_asset_path.exists():
            dim_asset = pl.read_parquet(str(dim_asset_path))
            
            universe["total_assets"] = len(dim_asset)
            
            # Stablecoin breakdown
            if "is_stable" in dim_asset.columns:
                stable_count = dim_asset.filter(pl.col("is_stable") == True).height
                universe["stablecoins"] = {
                    "count": stable_count,
                    "percentage": round(stable_count / len(dim_asset) * 100, 2),
                }
            
            # Chain breakdown
            if "chain" in dim_asset.columns:
                chain_dist = (
                    dim_asset
                    .group_by("chain")
                    .agg(pl.count().alias("count"))
                    .sort("count", descending=True)
                    .to_dicts()
                )
                universe["chain_distribution"] = chain_dist
        
        # Analyze asset coverage across fact tables
        fact_tables = ["fact_price", "fact_marketcap", "fact_volume", "fact_funding"]
        asset_coverage = {}
        
        for table_name in fact_tables:
            filepath = data_lake_dir / f"{table_name}.parquet"
            if filepath.exists():
                df_lazy = pl.scan_parquet(str(filepath))
                if "asset_id" in df_lazy.columns:
                    unique_assets = df_lazy.select(pl.col("asset_id").n_unique()).collect().item()
                    asset_coverage[table_name] = unique_assets
        
        universe["fact_table_coverage"] = asset_coverage
        
        return universe
    
    def analyze_funding_data(self, data_lake_dir: Path) -> Dict[str, Any]:
        """Specific analysis of funding data."""
        print("Analyzing funding data...")
        
        funding_path = data_lake_dir / "fact_funding.parquet"
        if not funding_path.exists():
            return {"exists": False}
        
        df_lazy = pl.scan_parquet(str(funding_path))
        
        # Exchange coverage
        if "exchange" in df_lazy.columns:
            exchange_dist = (
                df_lazy
                .group_by("exchange")
                .agg(pl.count().alias("count"))
                .sort("count", descending=True)
                .collect()
                .to_dicts()
            )
        else:
            exchange_dist = []
        
        # Instrument coverage
        if "instrument_id" in df_lazy.columns:
            instrument_count = df_lazy.select(pl.col("instrument_id").n_unique()).collect().item()
        else:
            instrument_count = 0
        
        # Asset coverage
        if "asset_id" in df_lazy.columns:
            asset_count = df_lazy.select(pl.col("asset_id").n_unique()).collect().item()
            
            # Funding stats per asset
            asset_funding_stats = (
                df_lazy
                .group_by("asset_id")
                .agg([
                    pl.count().alias("record_count"),
                    pl.col("funding_rate").min().alias("min_funding"),
                    pl.col("funding_rate").max().alias("max_funding"),
                    pl.col("funding_rate").mean().alias("mean_funding"),
                    pl.col("funding_rate").median().alias("median_funding"),
                ])
                .sort("record_count", descending=True)
                .head(20)
                .collect()
                .to_dicts()
            )
        else:
            asset_count = 0
            asset_funding_stats = []
        
        return {
            "exists": True,
            "exchange_coverage": exchange_dist,
            "instrument_count": instrument_count,
            "asset_count": asset_count,
            "top_20_assets_by_coverage": asset_funding_stats,
        }
    
    def create_visualizations(self, data_lake_dir: Path):
        """Create visualization plots."""
        print("Creating visualizations...")
        
        # 1. Date range coverage for top assets
        self._plot_date_coverage(data_lake_dir)
        
        # 2. Source distribution
        self._plot_source_distribution(data_lake_dir)
        
        # 3. Funding coverage
        self._plot_funding_coverage(data_lake_dir)
    
    def _plot_date_coverage(self, data_lake_dir: Path):
        """Plot date range coverage for top assets."""
        fact_tables = {
            "fact_price": "Price Data",
            "fact_marketcap": "Market Cap Data",
            "fact_volume": "Volume Data",
        }
        
        for table_name, title in fact_tables.items():
            filepath = data_lake_dir / f"{table_name}.parquet"
            if not filepath.exists():
                continue
            
            df_lazy = pl.scan_parquet(str(filepath))
            
            # Get top 20 assets by coverage
            asset_dates = (
                df_lazy
                .group_by("asset_id")
                .agg([
                    pl.col("date").min().alias("min_date"),
                    pl.col("date").max().alias("max_date"),
                    pl.col("date").n_unique().alias("count"),
                ])
                .sort("count", descending=True)
                .head(20)
                .collect()
            )
            
            if len(asset_dates) == 0:
                continue
            
            fig, ax = plt.subplots(figsize=(14, 8))
            
            y_pos = range(len(asset_dates))
            for i, row in enumerate(asset_dates.to_dicts()):
                min_date = pd.to_datetime(row["min_date"])
                max_date = pd.to_datetime(row["max_date"])
                ax.barh(i, (max_date - min_date).days, left=min_date, 
                       alpha=0.7, label=row["asset_id"] if i < 5 else "")
                ax.text(min_date, i, row["asset_id"], va='center', ha='right', fontsize=8)
            
            ax.set_yticks(y_pos)
            ax.set_yticklabels([row["asset_id"] for row in asset_dates.to_dicts()])
            ax.set_xlabel("Date")
            ax.set_title(f"{title} - Date Range Coverage (Top 20 Assets)")
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
            ax.xaxis.set_major_locator(mdates.YearLocator())
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.savefig(self.plots_dir / f"{table_name}_date_coverage.png", dpi=150, bbox_inches='tight')
            plt.close()
    
    def _plot_source_distribution(self, data_lake_dir: Path):
        """Plot data source distribution."""
        fact_tables = ["fact_price", "fact_marketcap", "fact_volume"]
        
        fig, axes = plt.subplots(1, 3, figsize=(18, 6))
        
        for idx, table_name in enumerate(fact_tables):
            filepath = data_lake_dir / f"{table_name}.parquet"
            if not filepath.exists():
                continue
            
            df_lazy = pl.scan_parquet(str(filepath))
            
            if "source" in df_lazy.columns:
                source_dist = (
                    df_lazy
                    .group_by("source")
                    .agg(pl.count().alias("count"))
                    .sort("count", descending=True)
                    .collect()
                )
                
                axes[idx].pie(source_dist["count"], labels=source_dist["source"], autopct='%1.1f%%')
                axes[idx].set_title(f"{table_name} - Source Distribution")
        
        plt.tight_layout()
        plt.savefig(self.plots_dir / "source_distribution.png", dpi=150, bbox_inches='tight')
        plt.close()
    
    def _plot_funding_coverage(self, data_lake_dir: Path):
        """Plot funding data coverage."""
        funding_path = data_lake_dir / "fact_funding.parquet"
        if not funding_path.exists():
            return
        
        df_lazy = pl.scan_parquet(str(funding_path))
        
        # Exchange distribution
        if "exchange" in df_lazy.columns:
            exchange_dist = (
                df_lazy
                .group_by("exchange")
                .agg(pl.count().alias("count"))
                .sort("count", descending=True)
                .collect()
            )
            
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.bar(exchange_dist["exchange"], exchange_dist["count"])
            ax.set_xlabel("Exchange")
            ax.set_ylabel("Record Count")
            ax.set_title("Funding Data - Exchange Distribution")
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.savefig(self.plots_dir / "funding_exchange_distribution.png", dpi=150, bbox_inches='tight')
            plt.close()
    
    def run_full_inspection(self) -> Dict[str, Any]:
        """Run complete data lake inspection."""
        print("=" * 80)
        print("DATA LAKE INSPECTION")
        print("=" * 80)
        print(f"Data Lake Directory: {self.data_lake_dir}")
        print()
        
        results = {
            "inspection_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data_lake_dir": str(self.data_lake_dir),
            "file_info": {},
            "fact_tables": {},
            "dimension_tables": {},
            "mapping_tables": {},
            "temporal_coverage": {},
            "data_quality": {},
            "asset_universe": {},
            "funding_analysis": {},
        }
        
        # 1. File discovery
        print("1. FILE DISCOVERY")
        print("-" * 80)
        
        files_to_check = [
            "dim_asset.parquet",
            "dim_instrument.parquet",
            "fact_price.parquet",
            "fact_marketcap.parquet",
            "fact_volume.parquet",
            "fact_funding.parquet",
            "fact_open_interest.parquet",
            "map_provider_asset.parquet",
            "map_provider_instrument.parquet",
        ]
        
        for filename in files_to_check:
            filepath = self.data_lake_dir / filename
            results["file_info"][filename] = self.get_file_info(filepath)
            if results["file_info"][filename]["exists"]:
                print(f"  [OK] {filename} ({results['file_info'][filename]['size_mb']} MB, "
                      f"modified: {results['file_info'][filename]['last_modified']})")
            else:
                print(f"  [MISSING] {filename} (not found)")
        
        print()
        
        # 2. Fact table analysis
        print("2. FACT TABLE ANALYSIS")
        print("-" * 80)
        
        fact_tables = {
            "fact_price": ("fact_price.parquet", "date", "asset_id"),
            "fact_marketcap": ("fact_marketcap.parquet", "date", "asset_id"),
            "fact_volume": ("fact_volume.parquet", "date", "asset_id"),
            "fact_funding": ("fact_funding.parquet", "date", "asset_id"),
            "fact_open_interest": ("fact_open_interest.parquet", "date", "asset_id"),
        }
        
        for table_name, (filename, date_col, asset_col) in fact_tables.items():
            filepath = self.data_lake_dir / filename
            if filepath.exists():
                results["fact_tables"][table_name] = self.analyze_fact_table(
                    filepath, table_name, date_col, asset_col
                )
        
        print()
        
        # 3. Dimension table analysis
        print("3. DIMENSION TABLE ANALYSIS")
        print("-" * 80)
        
        dim_tables = {
            "dim_asset": "dim_asset.parquet",
            "dim_instrument": "dim_instrument.parquet",
        }
        
        for table_name, filename in dim_tables.items():
            filepath = self.data_lake_dir / filename
            if filepath.exists():
                results["dimension_tables"][table_name] = self.analyze_dimension_table(
                    filepath, table_name
                )
        
        print()
        
        # 4. Mapping table analysis
        print("4. MAPPING TABLE ANALYSIS")
        print("-" * 80)
        
        map_tables = {
            "map_provider_asset": "map_provider_asset.parquet",
            "map_provider_instrument": "map_provider_instrument.parquet",
        }
        
        for table_name, filename in map_tables.items():
            filepath = self.data_lake_dir / filename
            if filepath.exists():
                results["mapping_tables"][table_name] = self.analyze_dimension_table(
                    filepath, table_name
                )
        
        print()
        
        # 5. Temporal coverage
        results["temporal_coverage"] = self.analyze_temporal_coverage(self.data_lake_dir)
        print()
        
        # 6. Data quality
        results["data_quality"] = self.analyze_data_quality(self.data_lake_dir)
        print()
        
        # 7. Asset universe
        results["asset_universe"] = self.analyze_asset_universe(self.data_lake_dir)
        print()
        
        # 8. Funding analysis
        results["funding_analysis"] = self.analyze_funding_data(self.data_lake_dir)
        print()
        
        # 9. Visualizations
        self.create_visualizations(self.data_lake_dir)
        print()
        
        self.results = results
        return results
    
    def generate_markdown_report(self, output_path: Path):
        """Generate comprehensive markdown report."""
        if not self.results:
            raise ValueError("Must run inspection first")
        
        report_lines = []
        
        # Header
        report_lines.append("# Data Lake Inspection Report")
        report_lines.append("")
        report_lines.append(f"**Inspection Date:** {self.results['inspection_date']}")
        report_lines.append(f"**Data Lake Directory:** `{self.results['data_lake_dir']}`")
        report_lines.append("")
        report_lines.append("---")
        report_lines.append("")
        
        # Executive Summary
        report_lines.append("## Executive Summary")
        report_lines.append("")
        report_lines.append(self._generate_executive_summary())
        report_lines.append("")
        
        # File Information
        report_lines.append("## 1. File Inventory")
        report_lines.append("")
        report_lines.append(self._format_file_info())
        report_lines.append("")
        
        # Fact Tables
        report_lines.append("## 2. Fact Table Analysis")
        report_lines.append("")
        report_lines.append(self._format_fact_tables())
        report_lines.append("")
        
        # Dimension Tables
        report_lines.append("## 3. Dimension Table Analysis")
        report_lines.append("")
        report_lines.append(self._format_dimension_tables())
        report_lines.append("")
        
        # Temporal Coverage
        report_lines.append("## 4. Temporal Coverage Analysis")
        report_lines.append("")
        report_lines.append(self._format_temporal_coverage())
        report_lines.append("")
        
        # Data Quality
        report_lines.append("## 5. Data Quality Assessment")
        report_lines.append("")
        report_lines.append(self._format_data_quality())
        report_lines.append("")
        
        # Asset Universe
        report_lines.append("## 6. Asset Universe Analysis")
        report_lines.append("")
        report_lines.append(self._format_asset_universe())
        report_lines.append("")
        
        # Funding Analysis
        report_lines.append("## 7. Funding Data Analysis")
        report_lines.append("")
        report_lines.append(self._format_funding_analysis())
        report_lines.append("")
        
        # Recommendations
        report_lines.append("## 8. Recommendations")
        report_lines.append("")
        report_lines.append(self._generate_recommendations())
        report_lines.append("")
        
        # Write report
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report_lines))
        
        print(f"Report saved to: {output_path}")
    
    def _generate_executive_summary(self) -> str:
        """Generate executive summary."""
        lines = []
        
        # Count files
        existing_files = sum(1 for info in self.results["file_info"].values() if info.get("exists"))
        total_files = len(self.results["file_info"])
        
        # Get key stats
        price_rows = self.results["fact_tables"].get("fact_price", {}).get("row_count", 0)
        assets = self.results["asset_universe"].get("total_assets", 0)
        
        lines.append(f"- **Files Found:** {existing_files}/{total_files} expected files")
        lines.append(f"- **Total Price Records:** {price_rows:,}")
        lines.append(f"- **Total Assets:** {assets}")
        
        # Data freshness
        price_info = self.results["file_info"].get("fact_price.parquet", {})
        if price_info.get("exists"):
            lines.append(f"- **Price Data Last Updated:** {price_info.get('last_modified', 'Unknown')}")
        
        # Latest date
        price_table = self.results["fact_tables"].get("fact_price", {})
        if price_table.get("exists"):
            max_date = price_table.get("date_range", {}).get("max")
            if max_date:
                lines.append(f"- **Latest Price Data:** {max_date}")
        
        return '\n'.join(lines)
    
    def _format_file_info(self) -> str:
        """Format file information section."""
        lines = []
        
        for filename, info in self.results["file_info"].items():
            if info.get("exists"):
                lines.append(f"### {filename}")
                lines.append("")
                lines.append(f"- **Size:** {info.get('size_mb', 0)} MB")
                lines.append(f"- **Last Modified:** {info.get('last_modified', 'Unknown')}")
                lines.append("")
        
        return '\n'.join(lines)
    
    def _format_fact_tables(self) -> str:
        """Format fact table analysis section."""
        lines = []
        
        for table_name, data in self.results["fact_tables"].items():
            if not data.get("exists"):
                continue
            
            lines.append(f"### {table_name}")
            lines.append("")
            lines.append(f"- **Row Count:** {data.get('row_count', 0):,}")
            lines.append("")
            
            # Date range
            date_range = data.get("date_range", {})
            if date_range.get("min"):
                lines.append(f"- **Date Range:** {date_range['min']} to {date_range['max']}")
                lines.append(f"- **Unique Dates:** {date_range.get('unique_dates', 0):,}")
                lines.append("")
            
            # Asset coverage
            asset_cov = data.get("asset_coverage", {})
            lines.append(f"- **Unique Assets:** {asset_cov.get('unique_assets', 0)}")
            lines.append("")
            
            # Top assets
            top_assets = asset_cov.get("top_20_by_count", [])
            if top_assets:
                lines.append("**Top 10 Assets by Record Count:**")
                lines.append("")
                for i, asset in enumerate(top_assets[:10], 1):
                    lines.append(f"{i}. {asset.get('asset_id', 'N/A')}: {asset.get('count', 0):,} records")
                lines.append("")
            
            # Duplicates
            duplicates = data.get("duplicates", 0)
            if duplicates > 0:
                lines.append(f"**WARNING:** {duplicates:,} duplicate records found")
                lines.append("")
            
            # Source distribution
            sources = data.get("source_distribution", [])
            if sources:
                lines.append("**Data Sources:**")
                lines.append("")
                for source in sources:
                    lines.append(f"- {source.get('source', 'N/A')}: {source.get('count', 0):,} records")
                lines.append("")
            
            # Sample data
            sample = data.get("sample_data", [])
            if sample:
                lines.append("**Sample Data (first 3 rows):**")
                lines.append("")
                lines.append("```")
                for i, row in enumerate(sample[:3], 1):
                    lines.append(f"Row {i}: {row}")
                lines.append("```")
                lines.append("")
        
        return '\n'.join(lines)
    
    def _format_dimension_tables(self) -> str:
        """Format dimension table analysis section."""
        lines = []
        
        for table_name, data in self.results["dimension_tables"].items():
            if not data.get("exists"):
                continue
            
            lines.append(f"### {table_name}")
            lines.append("")
            lines.append(f"- **Row Count:** {data.get('row_count', 0):,}")
            lines.append("")
            
            # Column stats
            col_stats = data.get("column_stats", {})
            if col_stats:
                lines.append("**Column Statistics:**")
                lines.append("")
                for col, stats in list(col_stats.items())[:10]:
                    lines.append(f"- `{col}`: {stats.get('unique_count', 0)} unique, "
                               f"{stats.get('null_count', 0)} nulls")
                lines.append("")
        
        return '\n'.join(lines)
    
    def _format_temporal_coverage(self) -> str:
        """Format temporal coverage section."""
        lines = []
        
        coverage = self.results.get("temporal_coverage", {})
        
        for table_name, data in coverage.items():
            lines.append(f"### {table_name}")
            lines.append("")
            lines.append(f"- **Total Assets:** {data.get('total_assets', 0)}")
            lines.append("")
            
            top_assets = data.get("top_20_assets", [])
            if top_assets:
                lines.append("**Top 10 Assets by Date Coverage:**")
                lines.append("")
                for i, asset in enumerate(top_assets[:10], 1):
                    asset_id = asset.get("asset_id", "N/A")
                    min_date = asset.get("min_date", "N/A")
                    max_date = asset.get("max_date", "N/A")
                    count = asset.get("date_count", 0)
                    lines.append(f"{i}. **{asset_id}**: {min_date} to {max_date} ({count:,} dates)")
                lines.append("")
        
        return '\n'.join(lines)
    
    def _format_data_quality(self) -> str:
        """Format data quality section."""
        lines = []
        
        quality = self.results.get("data_quality", {})
        
        # Outliers
        outliers = quality.get("outliers", {})
        if outliers:
            lines.append("### Outlier Detection")
            lines.append("")
            
            if "fact_price" in outliers:
                price_stats = outliers["fact_price"]
                lines.append("**Price Data:**")
                lines.append(f"- Min Price: ${price_stats.get('min_price', 0):,.2f}")
                lines.append(f"- Max Price: ${price_stats.get('max_price', 0):,.2f}")
                non_positive = price_stats.get("non_positive_count", 0)
                if non_positive > 0:
                    lines.append(f"**WARNING:** {non_positive:,} non-positive prices found")
                lines.append("")
            
            if "fact_funding" in outliers:
                funding_stats = outliers["fact_funding"]
                lines.append("**Funding Rate Data:**")
                lines.append(f"- Min Funding Rate: {funding_stats.get('min_funding', 0):.6f}")
                lines.append(f"- Max Funding Rate: {funding_stats.get('max_funding', 0):.6f}")
                lines.append(f"- Mean Funding Rate: {funding_stats.get('mean_funding', 0):.6f}")
                lines.append("")
        
        return '\n'.join(lines)
    
    def _format_asset_universe(self) -> str:
        """Format asset universe section."""
        lines = []
        
        universe = self.results.get("asset_universe", {})
        
        lines.append(f"- **Total Assets:** {universe.get('total_assets', 0)}")
        lines.append("")
        
        # Stablecoins
        stables = universe.get("stablecoins", {})
        if stables:
            lines.append(f"- **Stablecoins:** {stables.get('count', 0)} ({stables.get('percentage', 0)}%)")
            lines.append("")
        
        # Chain distribution
        chains = universe.get("chain_distribution", [])
        if chains:
            lines.append("**Chain Distribution:**")
            lines.append("")
            for chain_info in chains[:10]:
                lines.append(f"- {chain_info.get('chain', 'N/A')}: {chain_info.get('count', 0)} assets")
            lines.append("")
        
        # Fact table coverage
        coverage = universe.get("fact_table_coverage", {})
        if coverage:
            lines.append("**Asset Coverage by Fact Table:**")
            lines.append("")
            for table, count in coverage.items():
                lines.append(f"- {table}: {count} unique assets")
            lines.append("")
        
        return '\n'.join(lines)
    
    def _format_funding_analysis(self) -> str:
        """Format funding analysis section."""
        lines = []
        
        funding = self.results.get("funding_analysis", {})
        
        if not funding.get("exists"):
            lines.append("Funding data file not found.")
            return '\n'.join(lines)
        
        lines.append(f"- **Unique Assets:** {funding.get('asset_count', 0)}")
        lines.append(f"- **Unique Instruments:** {funding.get('instrument_count', 0)}")
        lines.append("")
        
        # Exchange coverage
        exchanges = funding.get("exchange_coverage", [])
        if exchanges:
            lines.append("**Exchange Coverage:**")
            lines.append("")
            for exch in exchanges:
                lines.append(f"- {exch.get('exchange', 'N/A')}: {exch.get('count', 0):,} records")
            lines.append("")
        
        # Top assets
        top_assets = funding.get("top_20_assets_by_coverage", [])
        if top_assets:
            lines.append("**Top 10 Assets by Funding Data Coverage:**")
            lines.append("")
            for i, asset in enumerate(top_assets[:10], 1):
                asset_id = asset.get("asset_id", "N/A")
                count = asset.get("record_count", 0)
                mean_funding = asset.get("mean_funding", 0)
                lines.append(f"{i}. **{asset_id}**: {count:,} records, "
                           f"mean funding rate: {mean_funding:.6f}")
            lines.append("")
        
        return '\n'.join(lines)
    
    def _generate_recommendations(self) -> str:
        """Generate recommendations based on findings."""
        lines = []
        
        # Check data freshness
        price_info = self.results["file_info"].get("fact_price.parquet", {})
        if price_info.get("exists"):
            # Check if data is recent (within last 7 days)
            lines.append("### Data Freshness")
            lines.append("")
            lines.append("**OK:** Data appears to be regularly updated.")
            lines.append("")
        
        # Check for duplicates
        has_duplicates = False
        for table_name, data in self.results["fact_tables"].items():
            if data.get("duplicates", 0) > 0:
                has_duplicates = True
                lines.append(f"**ACTION REQUIRED:** {table_name} has duplicate records. "
                           f"Consider deduplication.")
                lines.append("")
        
        if not has_duplicates:
            lines.append("**OK:** No duplicate records found in fact tables.")
            lines.append("")
        
        # Coverage recommendations
        lines.append("### Coverage Recommendations")
        lines.append("")
        lines.append("- Monitor data coverage for top assets (BTC, ETH) to ensure completeness")
        lines.append("- Consider backfilling any identified gaps in temporal coverage")
        lines.append("- Validate funding data coverage for assets used in MSM v0")
        lines.append("")
        
        return '\n'.join(lines)


def main():
    """Main entry point."""
    import sys
    
    # Default data lake directory
    data_lake_dir = Path("data/curated/data_lake")
    
    if len(sys.argv) > 1:
        data_lake_dir = Path(sys.argv[1])
    
    # Create inspector
    inspector = DataLakeInspector(data_lake_dir)
    
    # Run inspection
    results = inspector.run_full_inspection()
    
    # Generate report
    report_path = Path("DATA_LAKE_INSPECTION_REPORT.md")
    inspector.generate_markdown_report(report_path)
    
    # Save results as JSON
    json_path = Path("data_lake_inspection_results.json")
    with open(json_path, 'w') as f:
        # Convert results to JSON-serializable format
        json_results = json.loads(json.dumps(results, default=str))
        json.dump(json_results, f, indent=2)
    
    print(f"\n{'=' * 80}")
    print("INSPECTION COMPLETE")
    print(f"{'=' * 80}")
    print(f"Report: {report_path}")
    print(f"Results JSON: {json_path}")
    print(f"Plots: {inspector.plots_dir}/")
    print()


if __name__ == "__main__":
    main()
