#!/usr/bin/env python3
"""Query crypto backtest data using DuckDB."""

import sys
import argparse
from pathlib import Path
from typing import Optional

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb


def resolve_path(path: Path, repo_root: Path) -> Path:
    """Resolve a path relative to repo_root if not absolute."""
    if path.is_absolute():
        return path
    return repo_root / path


def create_views(conn: duckdb.DuckDBPyConnection, data_dir: Path, 
                 snapshots_path: Path, outputs_dir: Path) -> None:
    """Create DuckDB views for all parquet files."""
    
    # Try data lake format first, fallback to wide format
    data_lake_dir = data_dir.parent / "data_lake" if data_dir.name != "curated" else data_dir / "data_lake"
    
    if data_lake_dir.exists() and (data_lake_dir / "fact_price.parquet").exists():
        # Use data lake format - create views directly
        print(f"  Using data lake format from {data_lake_dir}")
        
        # Dimension tables
        for table_name in ["dim_asset", "dim_instrument"]:
            parquet_path = data_lake_dir / f"{table_name}.parquet"
            if parquet_path.exists():
                parquet_path_posix = parquet_path.resolve().as_posix()
                conn.execute(f"""
                    CREATE OR REPLACE VIEW {table_name} AS
                    SELECT * FROM read_parquet('{parquet_path_posix}')
                """)
                print(f"  Created view: {table_name}")
        
        # Mapping tables
        for table_name in ["map_provider_asset", "map_provider_instrument"]:
            parquet_path = data_lake_dir / f"{table_name}.parquet"
            if parquet_path.exists():
                parquet_path_posix = parquet_path.resolve().as_posix()
                conn.execute(f"""
                    CREATE OR REPLACE VIEW {table_name} AS
                    SELECT * FROM read_parquet('{parquet_path_posix}')
                """)
                print(f"  Created view: {table_name}")
        
        # Fact tables
        for table_name in ["fact_price", "fact_marketcap", "fact_volume", "fact_funding"]:
            parquet_path = data_lake_dir / f"{table_name}.parquet"
            if parquet_path.exists():
                parquet_path_posix = parquet_path.resolve().as_posix()
                conn.execute(f"""
                    CREATE OR REPLACE VIEW {table_name} AS
                    SELECT * FROM read_parquet('{parquet_path_posix}')
                """)
                print(f"  Created view: {table_name}")
        
        # Also create legacy views for backward compatibility if wide format exists
        prices_path = data_dir / "prices_daily.parquet"
        if prices_path.exists():
            prices_path_posix = prices_path.resolve().as_posix()
            conn.execute(f"""
                CREATE OR REPLACE VIEW prices_daily AS
                SELECT * FROM read_parquet('{prices_path_posix}')
            """)
            print(f"  Created legacy view: prices_daily (for backward compatibility)")
    else:
        # Fallback to wide format
        # Prices
        prices_path = data_dir / "prices_daily.parquet"
        if prices_path.exists():
            prices_path_posix = prices_path.resolve().as_posix()
            conn.execute(f"""
                CREATE OR REPLACE VIEW prices_daily AS
                SELECT * FROM read_parquet('{prices_path_posix}')
            """)
            print(f"  Created view: prices_daily ({prices_path})")
        
        # Market cap
        mcap_path = data_dir / "marketcap_daily.parquet"
        if mcap_path.exists():
            mcap_path_posix = mcap_path.resolve().as_posix()
            conn.execute(f"""
                CREATE OR REPLACE VIEW marketcap_daily AS
                SELECT * FROM read_parquet('{mcap_path_posix}')
            """)
            print(f"  Created view: marketcap_daily ({mcap_path})")
        
        # Volume
        volume_path = data_dir / "volume_daily.parquet"
        if volume_path.exists():
            volume_path_posix = volume_path.resolve().as_posix()
            conn.execute(f"""
                CREATE OR REPLACE VIEW volume_daily AS
                SELECT * FROM read_parquet('{volume_path_posix}')
            """)
            print(f"  Created view: volume_daily ({volume_path})")
    
    # Basket snapshots (universe_snapshots.parquet - selected top-N)
    if snapshots_path.exists():
        snapshots_path_posix = snapshots_path.resolve().as_posix()
        conn.execute(f"""
            CREATE OR REPLACE VIEW universe_snapshots AS
            SELECT * FROM read_parquet('{snapshots_path_posix}')
        """)
        print(f"  Created view: universe_snapshots ({snapshots_path})")
        
        # Also create semantic alias for clarity
        conn.execute("""
            CREATE OR REPLACE VIEW basket_snapshots AS
            SELECT * FROM universe_snapshots
        """)
        print(f"  Created view: basket_snapshots (alias for universe_snapshots)")
    
    # Universe eligibility (universe_eligibility.parquet - all candidates)
    universe_eligibility_path = data_dir / "universe_eligibility.parquet"
    if universe_eligibility_path.exists():
        universe_eligibility_path_posix = universe_eligibility_path.resolve().as_posix()
        conn.execute(f"""
            CREATE OR REPLACE VIEW universe_eligibility AS
            SELECT * FROM read_parquet('{universe_eligibility_path_posix}')
        """)
        print(f"  Created view: universe_eligibility ({universe_eligibility_path})")
    
    # Repair log (optional)
    repair_log_path = outputs_dir / "repair_log.parquet"
    if repair_log_path.exists():
        repair_log_path_posix = repair_log_path.resolve().as_posix()
        conn.execute(f"""
            CREATE OR REPLACE VIEW repair_log AS
            SELECT * FROM read_parquet('{repair_log_path_posix}')
        """)
        print(f"  Created view: repair_log ({repair_log_path})")


def main():
    parser = argparse.ArgumentParser(
        description="Query crypto backtest data using DuckDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run a single SQL query
  python scripts/query_duckdb.py --sql "SELECT COUNT(*) FROM universe_snapshots"

  # Run SQL from file
  python scripts/query_duckdb.py --sql-file queries/my_query.sql

  # Interactive mode (repl)
  python scripts/query_duckdb.py

  # Custom data directories
  python scripts/query_duckdb.py --data-dir data/curated --snapshots data/curated/universe_snapshots.parquet --sql "SELECT ..."
        """,
    )
    
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="Directory containing curated parquet files (default: data/curated)",
    )
    parser.add_argument(
        "--snapshots",
        type=Path,
        default=None,
        help="Path to universe_snapshots.parquet (default: data/curated/universe_snapshots.parquet)",
    )
    parser.add_argument(
        "--outputs-dir",
        type=Path,
        default=None,
        help="Directory containing outputs (default: outputs)",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help="Path to DuckDB database file (default: outputs/research.duckdb)",
    )
    parser.add_argument(
        "--sql",
        type=str,
        default=None,
        help="SQL query to execute (single query)",
    )
    parser.add_argument(
        "--sql-file",
        type=Path,
        default=None,
        help="Path to SQL file to execute",
    )
    parser.add_argument(
        "--list-views",
        action="store_true",
        help="List all available views and exit",
    )
    
    args = parser.parse_args()
    
    repo_root = Path(__file__).parent.parent
    
    # Resolve paths
    data_dir = resolve_path(args.data_dir, repo_root) if args.data_dir else repo_root / "data" / "curated"
    snapshots_path = resolve_path(args.snapshots, repo_root) if args.snapshots else repo_root / "data" / "curated" / "universe_snapshots.parquet"
    outputs_dir = resolve_path(args.outputs_dir, repo_root) if args.outputs_dir else repo_root / "outputs"
    db_path = resolve_path(args.db, repo_root) if args.db else repo_root / "outputs" / "research.duckdb"
    
    # Ensure parent directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Connect to DuckDB
    conn = duckdb.connect(str(db_path))
    
    print("=" * 60)
    print("DuckDB Query Interface")
    print("=" * 60)
    print(f"Database: {db_path}")
    print(f"Data directory: {data_dir}")
    print(f"Snapshots: {snapshots_path}")
    print(f"Outputs directory: {outputs_dir}")
    print("-" * 60)
    
    # Create views
    print("\nCreating views...")
    create_views(conn, data_dir, snapshots_path, outputs_dir)
    
    # List views if requested
    if args.list_views:
        print("\nAvailable views:")
        result = conn.execute("SHOW TABLES").fetchall()
        for row in result:
            print(f"  - {row[0]}")
        conn.close()
        return
    
    # Execute SQL
    if args.sql:
        print(f"\nExecuting SQL query:")
        print(f"  {args.sql}")
        print("-" * 60)
        try:
            result = conn.execute(args.sql).fetchdf()
            print(result.to_string())
        except Exception as e:
            print(f"[ERROR] Query failed: {e}")
            conn.close()
            sys.exit(1)
    
    elif args.sql_file:
        sql_file_path = resolve_path(args.sql_file, repo_root)
        if not sql_file_path.exists():
            print(f"[ERROR] SQL file not found: {sql_file_path}")
            conn.close()
            sys.exit(1)
        
        print(f"\nExecuting SQL from file: {sql_file_path}")
        print("-" * 60)
        try:
            with open(sql_file_path) as f:
                sql = f.read()
            result = conn.execute(sql).fetchdf()
            print(result.to_string())
        except Exception as e:
            print(f"[ERROR] Query failed: {e}")
            conn.close()
            sys.exit(1)
    
    else:
        # Interactive mode
        print("\nEntering interactive mode. Type SQL queries (or 'exit' to quit):")
        print("-" * 60)
        try:
            while True:
                query = input("\nSQL> ").strip()
                if not query or query.lower() in ['exit', 'quit', 'q']:
                    break
                
                if query.lower() == 'help':
                    print("\nAvailable commands:")
                    print("  help       - Show this help")
                    print("  views      - List all available views")
                    print("  exit/quit  - Exit interactive mode")
                    print("\nOr enter any SQL query to execute")
                    continue
                
                if query.lower() == 'views':
                    result = conn.execute("SHOW TABLES").fetchall()
                    print("\nAvailable views:")
                    for row in result:
                        print(f"  - {row[0]}")
                    continue
                
                try:
                    result = conn.execute(query).fetchdf()
                    if len(result) > 0:
                        print(result.to_string())
                    else:
                        print("(No rows returned)")
                except Exception as e:
                    print(f"[ERROR] {e}")
        except KeyboardInterrupt:
            print("\n\nExiting...")
    
    conn.close()


if __name__ == "__main__":
    main()
