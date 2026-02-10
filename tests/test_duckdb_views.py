"""Test DuckDB views creation and querying."""

import pytest
import pandas as pd
import numpy as np
from datetime import date, timedelta
from pathlib import Path
import tempfile
import sys
import subprocess

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

import duckdb
from query_duckdb import create_views


def test_duckdb_views_create_and_query():
    """Test that DuckDB views can be created and queried."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        
        # Create synthetic parquet files
        data_dir = tmp_path / "data"
        outputs_dir = tmp_path / "outputs"
        data_dir.mkdir()
        outputs_dir.mkdir()
        
        # Create prices panel
        dates = pd.date_range("2024-01-01", periods=10, freq="D")
        prices = pd.DataFrame({
            "BTC": [50000.0 + i * 100 for i in range(10)],
            "ETH": [3000.0 + i * 10 for i in range(10)],
        }, index=dates)
        prices.index.name = "date"
        prices.to_parquet(data_dir / "prices_daily.parquet")
        
        # Create market cap panel
        mcaps = pd.DataFrame({
            "BTC": [1e12 + i * 1e9 for i in range(10)],
            "ETH": [3e11 + i * 1e8 for i in range(10)],
        }, index=dates)
        mcaps.index.name = "date"
        mcaps.to_parquet(data_dir / "marketcap_daily.parquet")
        
        # Create volume panel
        volumes = pd.DataFrame({
            "BTC": [1e9 + i * 1e6 for i in range(10)],
            "ETH": [5e8 + i * 1e5 for i in range(10)],
        }, index=dates)
        volumes.index.name = "date"
        volumes.to_parquet(data_dir / "volume_daily.parquet")
        
        # Create universe snapshots
        snapshots_data = {
            "rebalance_date": ["2024-01-01", "2024-01-01"],
            "snapshot_date": ["2024-01-01", "2024-01-01"],
            "symbol": ["BTC", "ETH"],
            "coingecko_id": ["bitcoin", "ethereum"],
            "venue": ["BINANCE", "BINANCE"],
            "basket_name": ["benchmark_ls_TOP30", "benchmark_ls_TOP30"],
            "selection_version": ["v1", "v1"],
            "rank": [1, 2],
            "weight": [0.5, 0.5],
            "marketcap": [1e12, 3e11],
            "volume_14d": [1e9, 5e8],
        }
        snapshots_df = pd.DataFrame(snapshots_data)
        snapshots_path = data_dir / "universe_snapshots.parquet"
        snapshots_df.to_parquet(snapshots_path, index=False)
        
        # Connect to DuckDB
        db_path = outputs_dir / "test.duckdb"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = duckdb.connect(str(db_path))
        
        # Create views using the actual function from query_duckdb.py
        create_views(conn, data_dir, snapshots_path, outputs_dir)
        
        # Test query: count rows in each view
        result_prices = conn.execute("SELECT COUNT(*) AS cnt FROM prices_daily").fetchdf()
        assert result_prices.iloc[0]['cnt'] == 10  # 10 dates
        
        result_mcap = conn.execute("SELECT COUNT(*) AS cnt FROM marketcap_daily").fetchdf()
        assert result_mcap.iloc[0]['cnt'] == 10
        
        result_snapshots = conn.execute("SELECT COUNT(*) AS cnt FROM universe_snapshots").fetchdf()
        assert result_snapshots.iloc[0]['cnt'] == 2  # 2 symbols
        
        # Test query: get symbols from snapshots
        result_symbols = conn.execute("SELECT DISTINCT symbol FROM universe_snapshots ORDER BY symbol").fetchdf()
        assert len(result_symbols) == 2
        assert list(result_symbols['symbol']) == ['BTC', 'ETH']
        
        # Test query: join prices and snapshots
        result_join = conn.execute("""
            SELECT 
                s.symbol,
                s.weight,
                p.BTC AS btc_price
            FROM universe_snapshots s
            CROSS JOIN prices_daily p
            WHERE s.symbol = 'BTC'
              AND p.date = '2024-01-01'
        """).fetchdf()
        
        assert len(result_join) > 0
        assert result_join.iloc[0]['symbol'] == 'BTC'
        assert result_join.iloc[0]['weight'] == 0.5
        
        conn.close()


def test_query_duckdb_script_invocation():
    """Test that query_duckdb.py script can be invoked (smoke test)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        
        # Create minimal data
        data_dir = tmp_path / "data"
        outputs_dir = tmp_path / "outputs"
        data_dir.mkdir()
        outputs_dir.mkdir()
        
        # Create a minimal prices file
        dates = pd.date_range("2024-01-01", periods=5, freq="D")
        prices = pd.DataFrame({"BTC": [50000.0] * 5}, index=dates)
        prices.index.name = "date"
        prices.to_parquet(data_dir / "prices_daily.parquet")
        
        # Create minimal snapshots
        snapshots_df = pd.DataFrame({
            "rebalance_date": ["2024-01-01"],
            "snapshot_date": ["2024-01-01"],
            "symbol": ["BTC"],
            "coingecko_id": ["bitcoin"],
            "venue": ["BINANCE"],
            "basket_name": ["test"],
            "selection_version": ["v1"],
            "rank": [1],
            "weight": [1.0],
            "marketcap": [1e12],
            "volume_14d": [1e9],
        })
        snapshots_path = data_dir / "universe_snapshots.parquet"
        snapshots_df.to_parquet(snapshots_path, index=False)
        
        # Test script invocation with --list-views
        script_path = Path(__file__).parent.parent / "scripts" / "query_duckdb.py"
        
        result = subprocess.run(
            [
                sys.executable,
                str(script_path),
                "--data-dir", str(data_dir),
                "--snapshots", str(snapshots_path),
                "--outputs-dir", str(outputs_dir),
                "--list-views",
            ],
            capture_output=True,
            text=True,
            cwd=str(Path(__file__).parent.parent),
        )
        
        # Should succeed and list views
        assert result.returncode == 0
        assert "universe_snapshots" in result.stdout or "Creating views" in result.stdout
        # Verify that the script actually created the view
        assert "Created view" in result.stdout or "universe_snapshots" in result.stdout


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
