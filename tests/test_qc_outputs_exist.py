"""Test that QC pipeline produces all required output files."""

import pytest
import pandas as pd
import numpy as np
from datetime import date, timedelta
from pathlib import Path
import tempfile
import sys
import json

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from qc_curate import run_qc_pipeline, QC_CONFIG


def test_qc_outputs_exist():
    """Test that QC pipeline produces all required outputs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        
        # Create directory structure
        raw_dir = tmp_path / "raw"
        curated_dir = tmp_path / "curated"
        outputs_dir = tmp_path / "outputs"
        raw_dir.mkdir()
        
        # Create minimal synthetic raw data
        dates = pd.date_range("2024-01-01", periods=30, freq="D")
        
        # Prices: simple series with one spike
        prices = pd.DataFrame({
            "BTC": [50000.0 + i * 100 for i in range(30)],
            "ETH": [3000.0 + i * 10 for i in range(30)],
        }, index=dates)
        prices.loc[dates[15], "BTC"] = 1000000.0  # Add a spike
        
        # Market cap: similar structure
        mcaps = pd.DataFrame({
            "BTC": [1e12 + i * 1e9 for i in range(30)],
            "ETH": [3e11 + i * 1e8 for i in range(30)],
        }, index=dates)
        
        # Volume: similar structure
        volumes = pd.DataFrame({
            "BTC": [1e9 + i * 1e6 for i in range(30)],
            "ETH": [5e8 + i * 1e5 for i in range(30)],
        }, index=dates)
        
        # Write raw parquet files
        prices.to_parquet(raw_dir / "prices_daily.parquet")
        mcaps.to_parquet(raw_dir / "marketcap_daily.parquet")
        volumes.to_parquet(raw_dir / "volume_daily.parquet")
        
        # Run QC pipeline
        run_qc_pipeline(
            raw_dir=raw_dir,
            out_dir=curated_dir,
            outputs_dir=outputs_dir,
            config=QC_CONFIG,
            repo_root=tmp_path,
        )
        
        # Check that curated parquet files exist
        assert (curated_dir / "prices_daily.parquet").exists()
        assert (curated_dir / "marketcap_daily.parquet").exists()
        assert (curated_dir / "volume_daily.parquet").exists()
        
        # Check that outputs exist
        assert (outputs_dir / "qc_report.md").exists()
        assert (outputs_dir / "repair_log.parquet").exists()
        assert (outputs_dir / "run_metadata_qc.json").exists()
        
        # Verify curated data was modified (spike should be NA)
        curated_prices = pd.read_parquet(curated_dir / "prices_daily.parquet")
        assert pd.isna(curated_prices.loc[dates[15], "BTC"])  # Spike should be NA
        
        # Verify repair log exists and has entries
        repair_log = pd.read_parquet(outputs_dir / "repair_log.parquet")
        assert len(repair_log) > 0
        assert "return_spike" in repair_log["rule"].values  # Should have spike entry
        
        # Verify metadata JSON structure
        with open(outputs_dir / "run_metadata_qc.json") as f:
            metadata = json.load(f)
        
        assert metadata["script_name"] == "qc_curate.py"
        assert "run_timestamp" in metadata
        assert "config_hash" in metadata
        assert "input_files" in metadata
        assert "output_files" in metadata
        assert "repair_stats" in metadata
        assert metadata["repair_stats"]["total_edits"] > 0
        
        # Verify input/output file hashes exist
        assert "prices" in metadata["input_files"]
        assert metadata["input_files"]["prices"]["hash"] is not None
        assert "prices" in metadata["output_files"]
        assert metadata["output_files"]["prices"]["hash"] is not None
        
        # Verify QC report exists and contains expected content
        with open(outputs_dir / "qc_report.md") as f:
            report_content = f.read()
        
        assert "# QC Curation Report" in report_content
        assert "PRICES" in report_content
        assert "MARKETCAP" in report_content
        assert "VOLUME" in report_content
        assert "Repair Summary" in report_content


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
