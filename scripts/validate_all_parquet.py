#!/usr/bin/env python3
"""Comprehensive validation of all parquet datasets in the data lake."""

import sys
import argparse
import io
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd
from datetime import date, datetime

# Windows encoding fix
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.data_lake.schema import (
    DIM_ASSET_SCHEMA,
    DIM_INSTRUMENT_SCHEMA,
    MAP_PROVIDER_ASSET_SCHEMA,
    MAP_PROVIDER_INSTRUMENT_SCHEMA,
    FACT_PRICE_SCHEMA,
    FACT_MARKETCAP_SCHEMA,
    FACT_VOLUME_SCHEMA,
    FACT_FUNDING_SCHEMA,
    UNIVERSE_ELIGIBILITY_SCHEMA,
    BASKET_SNAPSHOTS_SCHEMA,
)


class ParquetValidator:
    """Validates all parquet files in the data lake."""
    
    def __init__(self, data_lake_dir: Path, curated_dir: Optional[Path] = None):
        self.data_lake_dir = data_lake_dir
        self.curated_dir = curated_dir or data_lake_dir.parent
        self.errors = []
        self.warnings = []
        self.stats = {}
    
    def validate_file_exists(self, filepath: Path, table_name: str) -> bool:
        """Check if file exists."""
        if not filepath.exists():
            self.errors.append(f"[ERROR] {table_name}: File not found at {filepath}")
            return False
        return True
    
    def validate_schema(self, df: pd.DataFrame, schema: Dict, table_name: str) -> bool:
        """Validate DataFrame against expected schema."""
        valid = True
        
        # Check required columns
        for col in schema.keys():
            if col not in df.columns:
                self.errors.append(f"[ERROR] {table_name}: Missing required column '{col}'")
                valid = False
        
        # Check for unexpected columns (warn only)
        for col in df.columns:
            if col not in schema:
                self.warnings.append(f"[WARN] {table_name}: Unexpected column '{col}' (not in schema)")
        
        return valid
    
    def validate_data_types(self, df: pd.DataFrame, schema: Dict, table_name: str) -> bool:
        """Validate data types match schema."""
        valid = True
        
        for col, expected_type in schema.items():
            if col not in df.columns:
                continue
            
            if df[col].empty:
                continue
            
            # Check date columns
            if expected_type == date:
                if not pd.api.types.is_datetime64_any_dtype(df[col]) and not pd.api.types.is_object_dtype(df[col]):
                    self.errors.append(f"[ERROR] {table_name}.{col}: Expected date type, got {df[col].dtype}")
                    valid = False
            
            # Check string columns
            elif expected_type == str or expected_type == Optional[str]:
                if not pd.api.types.is_object_dtype(df[col]):
                    self.warnings.append(f"[WARN] {table_name}.{col}: Expected string type, got {df[col].dtype}")
            
            # Check float columns
            elif expected_type == float or expected_type == Optional[float]:
                if not pd.api.types.is_numeric_dtype(df[col]):
                    self.errors.append(f"[ERROR] {table_name}.{col}: Expected numeric type, got {df[col].dtype}")
                    valid = False
            
            # Check bool columns
            elif expected_type == bool:
                if not pd.api.types.is_bool_dtype(df[col]):
                    self.warnings.append(f"[WARN] {table_name}.{col}: Expected bool type, got {df[col].dtype}")
        
        return valid
    
    def validate_dim_asset(self) -> bool:
        """Validate dim_asset.parquet"""
        filepath = self.data_lake_dir / "dim_asset.parquet"
        if not self.validate_file_exists(filepath, "dim_asset"):
            return False
        
        try:
            df = pd.read_parquet(filepath)
            self.stats["dim_asset"] = {"rows": len(df), "columns": len(df.columns)}
            
            if len(df) == 0:
                self.errors.append("[ERROR] dim_asset: Empty table")
                return False
            
            # Schema validation
            if not self.validate_schema(df, DIM_ASSET_SCHEMA, "dim_asset"):
                return False
            
            # Data type validation
            if not self.validate_data_types(df, DIM_ASSET_SCHEMA, "dim_asset"):
                return False
            
            # Business logic checks
            if df["asset_id"].isna().any():
                self.errors.append("[ERROR] dim_asset: asset_id cannot be null")
                return False
            
            if df["asset_id"].duplicated().any():
                self.errors.append("[ERROR] dim_asset: Duplicate asset_id found")
                return False
            
            if df["symbol"].isna().any():
                self.errors.append("[ERROR] dim_asset: symbol cannot be null")
                return False
            
            print(f"[OK] dim_asset: {len(df):,} rows, {df['asset_id'].nunique()} unique assets")
            return True
            
        except Exception as e:
            self.errors.append(f"[ERROR] dim_asset: Error reading file: {e}")
            return False
    
    def validate_dim_instrument(self) -> bool:
        """Validate dim_instrument.parquet"""
        filepath = self.data_lake_dir / "dim_instrument.parquet"
        if not self.validate_file_exists(filepath, "dim_instrument"):
            return False
        
        try:
            df = pd.read_parquet(filepath)
            self.stats["dim_instrument"] = {"rows": len(df), "columns": len(df.columns)}
            
            if len(df) == 0:
                self.warnings.append("[WARN] dim_instrument: Empty table (may be expected)")
                return True  # Empty is OK for instruments
            
            if not self.validate_schema(df, DIM_INSTRUMENT_SCHEMA, "dim_instrument"):
                return False
            
            if not self.validate_data_types(df, DIM_INSTRUMENT_SCHEMA, "dim_instrument"):
                return False
            
            if df["instrument_id"].isna().any():
                self.errors.append("[ERROR] dim_instrument: instrument_id cannot be null")
                return False
            
            if df["instrument_id"].duplicated().any():
                self.errors.append("[ERROR] dim_instrument: Duplicate instrument_id found")
                return False
            
            print(f"[OK] dim_instrument: {len(df):,} rows, {df['instrument_id'].nunique()} unique instruments")
            return True
            
        except Exception as e:
            self.errors.append(f"[ERROR] dim_instrument: Error reading file: {e}")
            return False
    
    def validate_fact_price(self) -> bool:
        """Validate fact_price.parquet"""
        filepath = self.data_lake_dir / "fact_price.parquet"
        if not self.validate_file_exists(filepath, "fact_price"):
            return False
        
        try:
            df = pd.read_parquet(filepath)
            self.stats["fact_price"] = {"rows": len(df), "columns": len(df.columns)}
            
            if len(df) == 0:
                self.errors.append("[ERROR] fact_price: Empty table")
                return False
            
            if not self.validate_schema(df, FACT_PRICE_SCHEMA, "fact_price"):
                return False
            
            if not self.validate_data_types(df, FACT_PRICE_SCHEMA, "fact_price"):
                return False
            
            # Business logic checks
            if df["asset_id"].isna().any():
                self.errors.append("[ERROR] fact_price: asset_id cannot be null")
                return False
            
            if df["close"].isna().any():
                self.warnings.append("[WARN] fact_price: Some close prices are null (may be expected)")
            
            if (df["close"] <= 0).any():
                self.errors.append("[ERROR] fact_price: Found non-positive close prices")
                return False
            
            if df["source"].isna().any():
                self.errors.append("[ERROR] fact_price: source cannot be null")
                return False
            
            # Check date range
            if "date" in df.columns:
                date_col = pd.to_datetime(df["date"])
                self.stats["fact_price"]["date_range"] = {
                    "min": str(date_col.min().date()),
                    "max": str(date_col.max().date()),
                }
            
            print(f"[OK] fact_price: {len(df):,} rows, {df['asset_id'].nunique()} assets, "
                  f"date range: {self.stats['fact_price'].get('date_range', {}).get('min', 'N/A')} to "
                  f"{self.stats['fact_price'].get('date_range', {}).get('max', 'N/A')}")
            return True
            
        except Exception as e:
            self.errors.append(f"[ERROR] fact_price: Error reading file: {e}")
            return False
    
    def validate_fact_marketcap(self) -> bool:
        """Validate fact_marketcap.parquet"""
        filepath = self.data_lake_dir / "fact_marketcap.parquet"
        if not self.validate_file_exists(filepath, "fact_marketcap"):
            return False
        
        try:
            df = pd.read_parquet(filepath)
            self.stats["fact_marketcap"] = {"rows": len(df), "columns": len(df.columns)}
            
            if len(df) == 0:
                self.errors.append("[ERROR] fact_marketcap: Empty table")
                return False
            
            if not self.validate_schema(df, FACT_MARKETCAP_SCHEMA, "fact_marketcap"):
                return False
            
            if df["asset_id"].isna().any():
                self.errors.append("[ERROR] fact_marketcap: asset_id cannot be null")
                return False
            
            if (df["marketcap"] < 0).any():
                self.errors.append("[ERROR] fact_marketcap: Found negative marketcap values")
                return False
            
            print(f"[OK] fact_marketcap: {len(df):,} rows, {df['asset_id'].nunique()} assets")
            return True
            
        except Exception as e:
            self.errors.append(f"[ERROR] fact_marketcap: Error reading file: {e}")
            return False
    
    def validate_fact_volume(self) -> bool:
        """Validate fact_volume.parquet"""
        filepath = self.data_lake_dir / "fact_volume.parquet"
        if not self.validate_file_exists(filepath, "fact_volume"):
            return False
        
        try:
            df = pd.read_parquet(filepath)
            self.stats["fact_volume"] = {"rows": len(df), "columns": len(df.columns)}
            
            if len(df) == 0:
                self.errors.append("[ERROR] fact_volume: Empty table")
                return False
            
            if not self.validate_schema(df, FACT_VOLUME_SCHEMA, "fact_volume"):
                return False
            
            if df["asset_id"].isna().any():
                self.errors.append("[ERROR] fact_volume: asset_id cannot be null")
                return False
            
            if (df["volume"] < 0).any():
                self.errors.append("[ERROR] fact_volume: Found negative volume values")
                return False
            
            print(f"[OK] fact_volume: {len(df):,} rows, {df['asset_id'].nunique()} assets")
            return True
            
        except Exception as e:
            self.errors.append(f"[ERROR] fact_volume: Error reading file: {e}")
            return False
    
    def validate_fact_funding(self) -> bool:
        """Validate fact_funding.parquet"""
        filepath = self.data_lake_dir / "fact_funding.parquet"
        if not self.validate_file_exists(filepath, "fact_funding"):
            return False
        
        try:
            df = pd.read_parquet(filepath)
            self.stats["fact_funding"] = {"rows": len(df), "columns": len(df.columns)}
            
            if len(df) == 0:
                self.warnings.append("[WARN] fact_funding: Empty table (may be expected if funding not fetched)")
                return True  # Empty is OK for funding
            
            if not self.validate_schema(df, FACT_FUNDING_SCHEMA, "fact_funding"):
                return False
            
            if df["asset_id"].isna().any():
                self.errors.append("[ERROR] fact_funding: asset_id cannot be null")
                return False
            
            if df["source"].isna().any():
                self.errors.append("[ERROR] fact_funding: source cannot be null")
                return False
            
            mapped_pct = (df["instrument_id"].notna().sum() / len(df) * 100) if len(df) > 0 else 0
            print(f"[OK] fact_funding: {len(df):,} rows, {df['asset_id'].nunique()} assets, "
                  f"{mapped_pct:.1f}% mapped to instrument_id")
            return True
            
        except Exception as e:
            self.errors.append(f"[ERROR] fact_funding: Error reading file: {e}")
            return False
    
    def validate_mapping_tables(self) -> bool:
        """Validate mapping tables"""
        valid = True
        
        # map_provider_asset
        filepath = self.data_lake_dir / "map_provider_asset.parquet"
        if filepath.exists():
            try:
                df = pd.read_parquet(filepath)
                self.stats["map_provider_asset"] = {"rows": len(df), "columns": len(df.columns)}
                
                if not self.validate_schema(df, MAP_PROVIDER_ASSET_SCHEMA, "map_provider_asset"):
                    valid = False
                
                # Check uniqueness: one provider_asset_id should map to one asset_id per valid window
                duplicates = df.groupby(["provider", "provider_asset_id"]).size()
                if (duplicates > 1).any():
                    self.warnings.append("[WARN] map_provider_asset: Some provider_asset_id have multiple mappings (check valid_from/valid_to)")
                
                print(f"[OK] map_provider_asset: {len(df):,} rows")
                
            except Exception as e:
                self.errors.append(f"[ERROR] map_provider_asset: Error reading file: {e}")
                valid = False
        else:
            self.warnings.append("[WARN] map_provider_asset: File not found")
        
        # map_provider_instrument
        filepath = self.data_lake_dir / "map_provider_instrument.parquet"
        if filepath.exists():
            try:
                df = pd.read_parquet(filepath)
                self.stats["map_provider_instrument"] = {"rows": len(df), "columns": len(df.columns)}
                
                if not self.validate_schema(df, MAP_PROVIDER_INSTRUMENT_SCHEMA, "map_provider_instrument"):
                    valid = False
                
                print(f"[OK] map_provider_instrument: {len(df):,} rows")
                
            except Exception as e:
                self.errors.append(f"[ERROR] map_provider_instrument: Error reading file: {e}")
                valid = False
        else:
            self.warnings.append("[WARN] map_provider_instrument: File not found")
        
        return valid
    
    def validate_joins(self) -> bool:
        """Validate that joins work correctly"""
        valid = True
        
        try:
            # Load required tables
            dim_asset = pd.read_parquet(self.data_lake_dir / "dim_asset.parquet")
            fact_price = pd.read_parquet(self.data_lake_dir / "fact_price.parquet")
            map_provider_asset = pd.read_parquet(self.data_lake_dir / "map_provider_asset.parquet")
            
            # Test join: fact_price -> dim_asset
            price_assets = set(fact_price["asset_id"].unique())
            dim_assets = set(dim_asset["asset_id"].unique())
            orphaned_price = price_assets - dim_assets
            
            if orphaned_price:
                self.errors.append(f"[ERROR] Join validation: {len(orphaned_price)} asset_ids in fact_price not in dim_asset")
                valid = False
            else:
                print("[OK] Join validation: All fact_price asset_ids exist in dim_asset")
            
            # Test join: map_provider_asset -> dim_asset
            map_assets = set(map_provider_asset["asset_id"].unique())
            orphaned_map = map_assets - dim_assets
            
            if orphaned_map:
                self.errors.append(f"[ERROR] Join validation: {len(orphaned_map)} asset_ids in map_provider_asset not in dim_asset")
                valid = False
            else:
                print("[OK] Join validation: All map_provider_asset asset_ids exist in dim_asset")
            
            # Test fact_funding -> dim_instrument if both exist
            if (self.data_lake_dir / "fact_funding.parquet").exists() and (self.data_lake_dir / "dim_instrument.parquet").exists():
                fact_funding = pd.read_parquet(self.data_lake_dir / "fact_funding.parquet")
                dim_instrument = pd.read_parquet(self.data_lake_dir / "dim_instrument.parquet")
                
                if len(fact_funding) > 0:
                    funding_instruments = set(fact_funding["instrument_id"].dropna().unique())
                    dim_instruments = set(dim_instrument["instrument_id"].unique())
                    orphaned_funding = funding_instruments - dim_instruments
                    
                    if orphaned_funding:
                        self.warnings.append(f"[WARN] Join validation: {len(orphaned_funding)} instrument_ids in fact_funding not in dim_instrument")
                    else:
                        print("[OK] Join validation: All fact_funding instrument_ids exist in dim_instrument")
            
        except Exception as e:
            self.errors.append(f"[ERROR] Join validation: Error: {e}")
            valid = False
        
        return valid
    
    def validate_universe_eligibility(self) -> bool:
        """Validate universe_eligibility.parquet"""
        filepath = self.curated_dir / "universe_eligibility.parquet"
        if not filepath.exists():
            self.warnings.append("[WARN] universe_eligibility: File not found (may not be generated yet)")
            return True
        
        try:
            df = pd.read_parquet(filepath)
            self.stats["universe_eligibility"] = {"rows": len(df), "columns": len(df.columns)}
            
            if len(df) == 0:
                self.warnings.append("[WARN] universe_eligibility: Empty table")
                return True
            
            # Check for required columns (flexible: accept either asset_id or symbol)
            required_cols = ["rebalance_date", "symbol"]  # symbol is used instead of asset_id
            missing = [col for col in required_cols if col not in df.columns]
            if missing:
                self.errors.append(f"[ERROR] universe_eligibility: Missing required columns: {missing}")
                return False
            
            # Check for other important columns (warn if missing)
            important_cols = ["snapshot_date", "exclusion_reason", "source"]
            for col in important_cols:
                if col not in df.columns:
                    self.warnings.append(f"[WARN] universe_eligibility: Missing expected column '{col}'")
            
            # Schema validation (warn about extra columns, but don't fail)
            for col in df.columns:
                if col not in UNIVERSE_ELIGIBILITY_SCHEMA and col != "symbol":  # symbol is OK
                    self.warnings.append(f"[WARN] universe_eligibility: Extra column '{col}' (not in schema)")
            
            print(f"[OK] universe_eligibility: {len(df):,} rows, {df['symbol'].nunique()} unique symbols")
            return True
            
        except Exception as e:
            self.errors.append(f"[ERROR] universe_eligibility: Error reading file: {e}")
            return False
    
    def validate_basket_snapshots(self) -> bool:
        """Validate basket_snapshots.parquet"""
        filepath = self.curated_dir / "universe_snapshots.parquet"
        if not filepath.exists():
            self.warnings.append("[WARN] basket_snapshots: File not found (may not be generated yet)")
            return True
        
        try:
            df = pd.read_parquet(filepath)
            self.stats["basket_snapshots"] = {"rows": len(df), "columns": len(df.columns)}
            
            if len(df) == 0:
                self.warnings.append("[WARN] basket_snapshots: Empty table")
                return True
            
            # Check for required columns (flexible: accept either asset_id or symbol)
            required_cols = ["rebalance_date", "symbol", "weight", "rank"]  # symbol is used instead of asset_id
            missing = [col for col in required_cols if col not in df.columns]
            if missing:
                self.errors.append(f"[ERROR] basket_snapshots: Missing required columns: {missing}")
                return False
            
            # Check weights sum to ~1.0 per rebalance_date
            if "rebalance_date" in df.columns and "weight" in df.columns:
                weight_sums = df.groupby("rebalance_date")["weight"].sum()
                if (weight_sums < 0.99).any() or (weight_sums > 1.01).any():
                    self.warnings.append("[WARN] basket_snapshots: Some rebalance dates have weights not summing to ~1.0")
            
            # Schema validation (warn about extra columns, but don't fail)
            for col in df.columns:
                if col not in BASKET_SNAPSHOTS_SCHEMA and col != "symbol":  # symbol is OK
                    self.warnings.append(f"[WARN] basket_snapshots: Extra column '{col}' (not in schema)")
            
            print(f"[OK] basket_snapshots: {len(df):,} rows, {df['symbol'].nunique()} unique symbols")
            return True
            
        except Exception as e:
            self.errors.append(f"[ERROR] basket_snapshots: Error reading file: {e}")
            return False
    
    def run_all_validations(self) -> bool:
        """Run all validation checks."""
        print("=" * 70)
        print("PARQUET DATASET VALIDATION")
        print("=" * 70)
        print(f"\nData Lake Directory: {self.data_lake_dir}")
        print(f"Curated Directory: {self.curated_dir}\n")
        
        results = []
        
        # Dimension tables
        print("\n[1] Dimension Tables")
        print("-" * 70)
        results.append(("dim_asset", self.validate_dim_asset()))
        results.append(("dim_instrument", self.validate_dim_instrument()))
        
        # Fact tables
        print("\n[2] Fact Tables")
        print("-" * 70)
        results.append(("fact_price", self.validate_fact_price()))
        results.append(("fact_marketcap", self.validate_fact_marketcap()))
        results.append(("fact_volume", self.validate_fact_volume()))
        results.append(("fact_funding", self.validate_fact_funding()))
        
        # Mapping tables
        print("\n[3] Mapping Tables")
        print("-" * 70)
        results.append(("mapping_tables", self.validate_mapping_tables()))
        
        # Join validation
        print("\n[4] Join Validation")
        print("-" * 70)
        results.append(("joins", self.validate_joins()))
        
        # Pipeline outputs
        print("\n[5] Pipeline Outputs")
        print("-" * 70)
        results.append(("universe_eligibility", self.validate_universe_eligibility()))
        results.append(("basket_snapshots", self.validate_basket_snapshots()))
        
        # Summary
        print("\n" + "=" * 70)
        print("VALIDATION SUMMARY")
        print("=" * 70)
        
        passed = sum(1 for _, result in results if result)
        total = len(results)
        
        print(f"\n[OK] Passed: {passed}/{total} checks")
        
        if self.warnings:
            print(f"\n[WARN] Warnings ({len(self.warnings)}):")
            for warning in self.warnings:
                print(f"  {warning}")
        
        if self.errors:
            print(f"\n[ERROR] Errors ({len(self.errors)}):")
            for error in self.errors:
                print(f"  {error}")
            print("\n[FAIL] VALIDATION FAILED")
            return False
        else:
            print("\n[PASS] ALL VALIDATIONS PASSED")
            return True


def main():
    parser = argparse.ArgumentParser(
        description="Validate all parquet datasets in the data lake",
    )
    parser.add_argument(
        "--data-lake-dir",
        type=Path,
        default=Path("data/curated/data_lake"),
        help="Data lake directory (default: data/curated/data_lake)",
    )
    parser.add_argument(
        "--curated-dir",
        type=Path,
        default=Path("data/curated"),
        help="Curated data directory (default: data/curated)",
    )
    
    args = parser.parse_args()
    
    repo_root = Path(__file__).parent.parent
    data_lake_dir = (repo_root / args.data_lake_dir).resolve() if not args.data_lake_dir.is_absolute() else args.data_lake_dir
    curated_dir = (repo_root / args.curated_dir).resolve() if not args.curated_dir.is_absolute() else args.curated_dir
    
    validator = ParquetValidator(data_lake_dir, curated_dir)
    success = validator.run_all_validations()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
