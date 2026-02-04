"""Build DuckDB database with views for all data lake tables."""

from pathlib import Path
from typing import Optional
import duckdb


def build_duckdb_views(
    data_lake_dir: Path,
    output_db: Path,
    snapshots_dir: Optional[Path] = None,
    outputs_dir: Optional[Path] = None,
) -> None:
    """
    Create DuckDB database with views for all data lake tables.
    
    Args:
        data_lake_dir: Directory containing parquet files (dim/map/fact tables)
        output_db: Path to output DuckDB database file
        snapshots_dir: Directory with universe_eligibility and basket_snapshots
        outputs_dir: Directory with backtest_results and other outputs
    """
    conn = duckdb.connect(str(output_db))
    
    print("\n[Setup] Creating DuckDB views for data lake...")
    
    # Dimension tables
    dim_tables = ["dim_asset", "dim_instrument"]
    for table_name in dim_tables:
        parquet_path = data_lake_dir / f"{table_name}.parquet"
        if parquet_path.exists():
            parquet_path_posix = parquet_path.resolve().as_posix()
            conn.execute(f"""
                CREATE OR REPLACE VIEW {table_name} AS
                SELECT * FROM read_parquet('{parquet_path_posix}')
            """)
            print(f"  Created view: {table_name}")
    
    # Mapping tables
    map_tables = ["map_provider_asset", "map_provider_instrument"]
    for table_name in map_tables:
        parquet_path = data_lake_dir / f"{table_name}.parquet"
        if parquet_path.exists():
            parquet_path_posix = parquet_path.resolve().as_posix()
            conn.execute(f"""
                CREATE OR REPLACE VIEW {table_name} AS
                SELECT * FROM read_parquet('{parquet_path_posix}')
            """)
            print(f"  Created view: {table_name}")
    
    # Fact tables
    fact_tables = ["fact_price", "fact_marketcap", "fact_volume", "fact_funding"]
    for table_name in fact_tables:
        parquet_path = data_lake_dir / f"{table_name}.parquet"
        if parquet_path.exists():
            parquet_path_posix = parquet_path.resolve().as_posix()
            conn.execute(f"""
                CREATE OR REPLACE VIEW {table_name} AS
                SELECT * FROM read_parquet('{parquet_path_posix}')
            """)
            print(f"  Created view: {table_name}")
    
    # Universe eligibility
    if snapshots_dir:
        universe_path = snapshots_dir / "universe_eligibility.parquet"
        if universe_path.exists():
            universe_path_posix = universe_path.resolve().as_posix()
            conn.execute(f"""
                CREATE OR REPLACE VIEW universe_eligibility AS
                SELECT * FROM read_parquet('{universe_path_posix}')
            """)
            print(f"  Created view: universe_eligibility")
    
    # Basket snapshots
    if snapshots_dir:
        basket_path = snapshots_dir / "universe_snapshots.parquet"
        if basket_path.exists():
            basket_path_posix = basket_path.resolve().as_posix()
            conn.execute(f"""
                CREATE OR REPLACE VIEW basket_snapshots AS
                SELECT * FROM read_parquet('{basket_path_posix}')
            """)
            conn.execute("""
                CREATE OR REPLACE VIEW universe_snapshots AS
                SELECT * FROM basket_snapshots
            """)
            print(f"  Created view: basket_snapshots (and universe_snapshots)")
    
    # Backtest results
    if outputs_dir:
        results_path = outputs_dir / "backtest_results.csv"
        if results_path.exists():
            results_path_posix = results_path.resolve().as_posix()
            conn.execute(f"""
                CREATE OR REPLACE VIEW backtest_results AS
                SELECT * FROM read_csv_auto('{results_path_posix}')
            """)
            print(f"  Created view: backtest_results")
    
    # Alignment audit
    if outputs_dir:
        audit_path = outputs_dir / "alignment_audit.json"
        if audit_path.exists():
            # DuckDB can read JSON, but for simplicity, we'll create a view if it's CSV
            # For now, skip JSON - can add later if needed
            pass
    
    conn.close()
    print(f"  Database saved to: {output_db}")
