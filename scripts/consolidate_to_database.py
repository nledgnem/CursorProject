#!/usr/bin/env python3
"""
Optional: Consolidate all parquet files into a single DuckDB database.

WARNING: This creates a LARGE database file by copying all data.
The current view-based approach is more efficient. Only use this if you
specifically need all data in a single database file.
"""

import argparse
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    import duckdb
except ImportError:
    print("ERROR: DuckDB not installed. Install with: pip install duckdb")
    sys.exit(1)


def consolidate_parquet_to_database(
    curated_dir: Path,
    snapshots_dir: Path,
    outputs_dir: Path,
    output_db: Path,
    overwrite: bool = False,
) -> None:
    """
    Import all parquet files as tables into a single DuckDB database.
    
    This COPIES all data into the database (unlike views which just reference files).
    """
    
    if output_db.exists() and not overwrite:
        print(f"ERROR: Database already exists: {output_db}")
        print("Use --overwrite to replace it.")
        sys.exit(1)
    
    # Remove existing database if overwriting
    if output_db.exists() and overwrite:
        output_db.unlink()
    
    conn = duckdb.connect(str(output_db))
    
    print("=" * 70)
    print("CONSOLIDATING PARQUET FILES INTO DUCKDB DATABASE")
    print("=" * 70)
    print(f"\nOutput database: {output_db}")
    print(f"\n⚠️  WARNING: This will COPY all data into the database.")
    print(f"   The database file will be large (potentially GB).")
    print(f"   Consider using views instead (current approach).\n")
    
    imported_tables = []
    
    # Try data lake format first, fallback to wide format
    data_lake_dir = curated_dir / "data_lake"
    
    # Import fact tables (data lake format)
    fact_tables = {
        "fact_price": "fact_price.parquet",
        "fact_marketcap": "fact_marketcap.parquet",
        "fact_volume": "fact_volume.parquet",
    }
    
    if data_lake_dir.exists():
        print(f"\n[Using Data Lake Format] {data_lake_dir}")
        for table_name, filename in fact_tables.items():
            file_path = data_lake_dir / filename
            if file_path.exists():
                print(f"\n[Importing] {table_name} from {file_path}...")
                file_path_posix = file_path.resolve().as_posix()
                
                # Import as table (copies data)
                conn.execute(f"""
                    CREATE TABLE {table_name} AS
                    SELECT * FROM read_parquet('{file_path_posix}')
                """)
                
                # Get row count
                result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                print(f"  ✓ Imported {result[0]:,} rows")
                imported_tables.append(table_name)
            else:
                print(f"  [SKIP] {filename} not found")
        
        # Also import dimension and mapping tables
        for table_name in ["dim_asset", "dim_instrument", "map_provider_asset", "map_provider_instrument"]:
            file_path = data_lake_dir / f"{table_name}.parquet"
            if file_path.exists():
                print(f"\n[Importing] {table_name} from {file_path}...")
                file_path_posix = file_path.resolve().as_posix()
                conn.execute(f"""
                    CREATE TABLE {table_name} AS
                    SELECT * FROM read_parquet('{file_path_posix}')
                """)
                result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                print(f"  ✓ Imported {result[0]:,} rows")
                imported_tables.append(table_name)
    
    # Fallback to wide format if data lake not available
    if not data_lake_dir.exists() or not any((data_lake_dir / f).exists() for f in fact_tables.values()):
        print(f"\n[Using Wide Format] {curated_dir}")
        for name, filename in [
            ("prices_daily", "prices_daily.parquet"),
            ("marketcap_daily", "marketcap_daily.parquet"),
            ("volume_daily", "volume_daily.parquet"),
        ]:
            file_path = curated_dir / filename
            if file_path.exists():
                print(f"\n[Importing] {name} from {file_path}...")
                file_path_posix = file_path.resolve().as_posix()
                
                # Import as table (copies data)
                conn.execute(f"""
                    CREATE TABLE {name} AS
                    SELECT * FROM read_parquet('{file_path_posix}')
                """)
                
                # Get row count
                result = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()
                print(f"  ✓ Imported {result[0]:,} rows")
                imported_tables.append(name)
            else:
                print(f"  [SKIP] {filename} not found")
    
    # Import universe snapshots
    snapshots_path = snapshots_dir / "universe_snapshots.parquet"
    if snapshots_path.exists():
        print(f"\n[Importing] universe_snapshots from {snapshots_path}...")
        snapshots_path_posix = snapshots_path.resolve().as_posix()
        conn.execute(f"""
            CREATE TABLE universe_snapshots AS
            SELECT * FROM read_parquet('{snapshots_path_posix}')
        """)
        conn.execute("""
            CREATE TABLE basket_snapshots AS
            SELECT * FROM universe_snapshots
        """)
        result = conn.execute("SELECT COUNT(*) FROM universe_snapshots").fetchone()
        print(f"  ✓ Imported {result[0]:,} rows")
        imported_tables.extend(["universe_snapshots", "basket_snapshots"])
    
    # Import universe eligibility
    universe_eligibility_path = snapshots_dir / "universe_eligibility.parquet"
    if universe_eligibility_path.exists():
        print(f"\n[Importing] universe_eligibility from {universe_eligibility_path}...")
        universe_path_posix = universe_eligibility_path.resolve().as_posix()
        conn.execute(f"""
            CREATE TABLE universe_eligibility AS
            SELECT * FROM read_parquet('{universe_path_posix}')
        """)
        result = conn.execute("SELECT COUNT(*) FROM universe_eligibility").fetchone()
        print(f"  ✓ Imported {result[0]:,} rows")
        imported_tables.append("universe_eligibility")
    
    # Import perp listings
    perp_listings_path = curated_dir / "perp_listings_binance.parquet"
    if not perp_listings_path.exists():
        perp_listings_path = Path(curated_dir.parent) / "raw" / "perp_listings_binance.parquet"
    
    if perp_listings_path.exists():
        print(f"\n[Importing] perp_listings from {perp_listings_path}...")
        perp_path_posix = perp_listings_path.resolve().as_posix()
        conn.execute(f"""
            CREATE TABLE perp_listings_binance AS
            SELECT * FROM read_parquet('{perp_path_posix}')
        """)
        result = conn.execute("SELECT COUNT(*) FROM perp_listings_binance").fetchone()
        print(f"  ✓ Imported {result[0]:,} rows")
        imported_tables.append("perp_listings_binance")
    
    # Import repair log if exists
    repair_log_path = outputs_dir / "repair_log.parquet"
    if repair_log_path.exists():
        print(f"\n[Importing] repair_log from {repair_log_path}...")
        repair_path_posix = repair_log_path.resolve().as_posix()
        conn.execute(f"""
            CREATE TABLE repair_log AS
            SELECT * FROM read_parquet('{repair_path_posix}')
        """)
        result = conn.execute("SELECT COUNT(*) FROM repair_log").fetchone()
        print(f"  ✓ Imported {result[0]:,} rows")
        imported_tables.append("repair_log")
    
    # Import backtest results if exists
    results_path = outputs_dir / "backtest_results.csv"
    if results_path.exists():
        print(f"\n[Importing] backtest_results from {results_path}...")
        results_path_posix = results_path.resolve().as_posix()
        conn.execute(f"""
            CREATE TABLE backtest_results AS
            SELECT * FROM read_csv_auto('{results_path_posix}')
        """)
        result = conn.execute("SELECT COUNT(*) FROM backtest_results").fetchone()
        print(f"  ✓ Imported {result[0]:,} rows")
        imported_tables.append("backtest_results")
    
    # Get database size
    db_size = output_db.stat().st_size / (1024 * 1024)  # MB
    
    conn.close()
    
    print("\n" + "=" * 70)
    print("CONSOLIDATION COMPLETE")
    print("=" * 70)
    print(f"\n✓ Database: {output_db}")
    print(f"✓ Size: {db_size:.2f} MB")
    print(f"✓ Tables: {', '.join(imported_tables)}")
    print(f"\n⚠️  NOTE: Original parquet files are unchanged.")
    print(f"   You now have data in BOTH parquet files AND the database.")
    print(f"\n💡 Query the database:")
    print(f"   python scripts/query_duckdb.py --db {output_db}")


def main():
    parser = argparse.ArgumentParser(
        description="Consolidate all parquet files into a single DuckDB database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Consolidate to default location
  python scripts/consolidate_to_database.py

  # Consolidate to custom location
  python scripts/consolidate_to_database.py --output outputs/research_full.duckdb

  # Overwrite existing database
  python scripts/consolidate_to_database.py --overwrite
        """
    )
    
    parser.add_argument(
        "--curated-dir",
        type=Path,
        default=Path("data/curated"),
        help="Directory with curated parquet files (default: data/curated)",
    )
    parser.add_argument(
        "--snapshots-dir",
        type=Path,
        default=Path("data/curated"),
        help="Directory with snapshot parquet files (default: data/curated)",
    )
    parser.add_argument(
        "--outputs-dir",
        type=Path,
        default=Path("outputs"),
        help="Directory with output files (default: outputs)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("outputs/research_full.duckdb"),
        help="Output database path (default: outputs/research_full.duckdb)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing database if it exists",
    )
    
    args = parser.parse_args()
    
    repo_root = Path(__file__).parent.parent
    curated_dir = (repo_root / args.curated_dir).resolve()
    snapshots_dir = (repo_root / args.snapshots_dir).resolve()
    outputs_dir = (repo_root / args.outputs_dir).resolve()
    output_db = (repo_root / args.output).resolve()
    
    if not curated_dir.exists():
        print(f"ERROR: Curated directory not found: {curated_dir}")
        sys.exit(1)
    
    consolidate_parquet_to_database(
        curated_dir=curated_dir,
        snapshots_dir=snapshots_dir,
        outputs_dir=outputs_dir,
        output_db=output_db,
        overwrite=args.overwrite,
    )


if __name__ == "__main__":
    main()

