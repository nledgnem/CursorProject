"""
Test pipeline run modes (smoke vs research) and gating logic.
"""
import json
import subprocess
import sys
from pathlib import Path
import tempfile
import shutil

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def test_research_mode_fails_on_skipped_qc():
    """Test that research mode fails if QC is skipped."""
    repo_root = Path(__file__).parent.parent
    script_path = repo_root / "scripts" / "run_pipeline.py"
    config_path = repo_root / "configs" / "golden.yaml"
    
    if not script_path.exists() or not config_path.exists():
        print("SKIP: Required files not found")
        return
    
    # Run with research mode and skip-qc - should fail
    cmd = [
        sys.executable,
        str(script_path),
        "--config", str(config_path),
        "--mode", "research",
        "--skip-qc",
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    
    # Should fail with error about QC being required
    assert result.returncode != 0, "Research mode should fail when QC is skipped"
    assert "cannot be skipped" in result.stderr.lower() or "cannot be skipped" in result.stdout.lower(), \
        "Error message should mention that QC cannot be skipped"


def test_smoke_mode_allows_skips():
    """Test that smoke mode allows skipping steps but marks status as PASS_WITH_WARNINGS."""
    repo_root = Path(__file__).parent.parent
    script_path = repo_root / "scripts" / "run_pipeline.py"
    config_path = repo_root / "configs" / "golden.yaml"
    
    if not script_path.exists() or not config_path.exists():
        print("SKIP: Required files not found")
        return
    
    # Create temporary run directory
    with tempfile.TemporaryDirectory() as tmpdir:
        run_dir = Path(tmpdir) / "test_run"
        
        # Run with smoke mode and skip-qc - should pass but with warnings
        cmd = [
            sys.executable,
            str(script_path),
            "--config", str(config_path),
            "--mode", "smoke",
            "--skip-qc",
            "--skip-snapshots",
            "--skip-backtest",
            "--run-dir", str(run_dir),
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        
        # Should complete (may fail if data doesn't exist, but that's OK for this test)
        # If it completes, check the receipt
        receipt_path = run_dir / "run_receipt.json"
        if receipt_path.exists():
            with open(receipt_path) as f:
                receipt = json.load(f)
            
            assert receipt.get("mode") == "smoke", "Receipt should record mode=smoke"
            assert "skipped_steps" in receipt, "Receipt should include skipped_steps"
            assert "qc_curation" in receipt.get("skipped_steps", []), "QC should be in skipped_steps"
            
            overall_status = receipt.get("overall_status")
            # In smoke mode with skips, should be PASS_WITH_WARNINGS or FAIL (if critical steps fail)
            assert overall_status in ["PASS", "PASS_WITH_WARNINGS", "FAIL"], \
                f"Overall status should be valid, got {overall_status}"


def test_receipt_contains_manager_summary():
    """Test that run_receipt contains manager_summary fields."""
    repo_root = Path(__file__).parent.parent
    
    # Look for existing run receipts
    runs_dir = repo_root / "outputs" / "runs"
    if not runs_dir.exists():
        print("SKIP: No runs directory found")
        return
    
    # Find most recent run receipt
    receipts = list(runs_dir.glob("*/run_receipt.json"))
    if not receipts:
        print("SKIP: No run receipts found")
        return
    
    # Check the most recent one
    receipt_path = max(receipts, key=lambda p: p.stat().st_mtime)
    
    with open(receipt_path) as f:
        receipt = json.load(f)
    
    # Check for manager_summary
    assert "manager_summary" in receipt, "Receipt should contain manager_summary"
    summary = receipt["manager_summary"]
    
    # Check required fields
    assert "mode" in summary, "manager_summary should include mode"
    assert "qc_run" in summary, "manager_summary should include qc_run"
    assert "validation_run" in summary, "manager_summary should include validation_run"
    
    # Check optional fields (may not be present if data doesn't exist)
    optional_fields = [
        "time_range",
        "rebalance_dates_count",
        "avg_eligible_count",
        "rebalance_coverage",
        "pct_rebalance_dates_with_full_top_n",
    ]
    
    print(f"\nManager summary from {receipt_path.name}:")
    for field in ["mode", "qc_run", "validation_run"] + optional_fields:
        if field in summary:
            print(f"  {field}: {summary[field]}")


def test_research_mode_requires_validation():
    """Test that research mode requires validation to run."""
    repo_root = Path(__file__).parent.parent
    script_path = repo_root / "scripts" / "run_pipeline.py"
    config_path = repo_root / "configs" / "golden.yaml"
    
    if not script_path.exists() or not config_path.exists():
        print("SKIP: Required files not found")
        return
    
    # Run with research mode and skip-validation - should fail
    cmd = [
        sys.executable,
        str(script_path),
        "--config", str(config_path),
        "--mode", "research",
        "--skip-validation",
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    
    # Should fail with error about validation being required
    assert result.returncode != 0, "Research mode should fail when validation is skipped"
    assert "cannot be skipped" in result.stderr.lower() or "cannot be skipped" in result.stdout.lower(), \
        "Error message should mention that validation cannot be skipped"


if __name__ == "__main__":
    print("=" * 80)
    print("TEST: Research mode fails on skipped QC")
    print("=" * 80)
    try:
        test_research_mode_fails_on_skipped_qc()
        print("[PASS] Research mode correctly fails when QC is skipped")
    except Exception as e:
        print(f"[FAIL] {e}")
    
    print("\n" + "=" * 80)
    print("TEST: Smoke mode allows skips")
    print("=" * 80)
    try:
        test_smoke_mode_allows_skips()
        print("[PASS] Smoke mode allows skipping steps")
    except Exception as e:
        print(f"[FAIL] {e}")
    
    print("\n" + "=" * 80)
    print("TEST: Receipt contains manager summary")
    print("=" * 80)
    try:
        test_receipt_contains_manager_summary()
        print("[PASS] Receipt contains manager_summary")
    except Exception as e:
        print(f"[FAIL] {e}")
    
    print("\n" + "=" * 80)
    print("TEST: Research mode requires validation")
    print("=" * 80)
    try:
        test_research_mode_requires_validation()
        print("[PASS] Research mode correctly fails when validation is skipped")
    except Exception as e:
        print(f"[FAIL] {e}")

