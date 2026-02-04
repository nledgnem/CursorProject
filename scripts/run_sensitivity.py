#!/usr/bin/env python3
"""Run sensitivity analysis with multiple config variants."""

import sys
import argparse
import subprocess
from pathlib import Path
import pandas as pd
import yaml

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def run_backtest_with_config(config_path: Path, variant_name: str, repo_root: Path, output_dir: Path) -> dict:
    """
    Run backtest with a specific config variant and return paths to all artifacts.
    
    Returns dict with keys: results_path, report_path, metadata_path, turnover_path
    """
    script_dir = repo_root / "scripts"
    
    # Use variant-specific output directory to avoid overwriting artifacts
    variant_output_dir = output_dir / variant_name
    variant_output_dir.mkdir(parents=True, exist_ok=True)
    
    cmd = [sys.executable, str(script_dir / "run_backtest.py"), "--config", str(config_path), "--output-dir", str(variant_output_dir)]
    print(f"\nRunning backtest variant: {variant_name}")
    print(f"  Command: {' '.join(cmd)}")
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"[ERROR] Backtest failed for {variant_name}:")
        print(result.stderr)
        return None
    
    # All artifacts are in variant_output_dir
    artifacts = {
        "results_path": variant_output_dir / "backtest_results.csv",
        "report_path": variant_output_dir / "report.md",
        "metadata_path": variant_output_dir / "run_metadata_backtest.json",
        "turnover_path": variant_output_dir / "rebalance_turnover.csv",
    }
    
    # Verify results file exists
    if not artifacts["results_path"].exists():
        print(f"[WARN] Results file not found: {artifacts['results_path']}")
        return None
    
    return artifacts


def create_variant_configs(base_config_path: Path, output_dir: Path, variants: list) -> dict:
    """
    Create variant config files.
    
    Args:
        base_config_path: Path to base config YAML
        output_dir: Directory to save variant configs
        variants: List of dicts with variant_name and config_overrides
    
    Returns:
        Dict mapping variant_name -> variant_config_path
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    with open(base_config_path) as f:
        base_config = yaml.safe_load(f)
    
    variant_paths = {}
    
    for variant in variants:
        variant_name = variant["variant_name"]
        overrides = variant["config_overrides"]
        
        # Deep copy base config and apply overrides
        variant_config = yaml.safe_load(yaml.dump(base_config))  # Deep copy via serialization
        
        # Apply overrides (nested dict update)
        for key, value in overrides.items():
            if isinstance(value, dict) and key in variant_config and isinstance(variant_config[key], dict):
                variant_config[key].update(value)
            else:
                variant_config[key] = value
        
        # Save variant config
        variant_path = output_dir / f"config_{variant_name}.yaml"
        with open(variant_path, "w") as f:
            yaml.dump(variant_config, f, default_flow_style=False, sort_keys=False)
        
        variant_paths[variant_name] = variant_path
        print(f"Created variant config: {variant_path}")
    
    return variant_paths


def compare_results(artifacts_dict: dict, output_path: Path):
    """Compare backtest results across variants and write summary table."""
    summary_rows = []
    
    for variant_name, artifacts in artifacts_dict.items():
        if artifacts is None or not artifacts["results_path"].exists():
            continue
        
        results_path = artifacts["results_path"]
        
        df = pd.read_csv(results_path)
        
        returns = df["r_ls_net"].dropna()
        if len(returns) == 0:
            continue
        
        total_return = (1.0 + returns).prod() - 1.0
        annualized_return = (1.0 + total_return) ** (252 / len(returns)) - 1.0
        
        if returns.std() > 0:
            sharpe = (252 ** 0.5) * returns.mean() / returns.std()
        else:
            sharpe = 0.0
        
        equity = (1.0 + df["r_ls_net"].fillna(0.0)).cumprod()
        running_max = equity.cummax()
        drawdown = (equity / running_max) - 1.0
        max_dd = drawdown.min()
        
        summary_rows.append({
            "variant": variant_name,
            "total_return_pct": total_return * 100,
            "annualized_return_pct": annualized_return * 100,
            "sharpe": sharpe,
            "max_drawdown_pct": max_dd * 100,
            "trading_days": len(returns),
        })
    
    if not summary_rows:
        print("[WARN] No valid results to compare")
        return
    
    summary_df = pd.DataFrame(summary_rows)
    
    # Write summary table as markdown
    with open(output_path, "w") as f:
        f.write("# Sensitivity Analysis Summary\n\n")
        f.write("Comparison of backtest results across config variants.\n\n")
        f.write("## Results Table\n\n")
        f.write("| Variant | Total Return | Annualized Return | Sharpe | Max DD | Trading Days |\n")
        f.write("|---------|--------------|-------------------|--------|--------|-------------|\n")
        
        for _, row in summary_df.iterrows():
            f.write(f"| {row['variant']} | {row['total_return_pct']:.2f}% | {row['annualized_return_pct']:.2f}% | {row['sharpe']:.2f} | {row['max_drawdown_pct']:.2f}% | {row['trading_days']} |\n")
        
        f.write("\n")
        f.write("## Notes\n\n")
        f.write("- All returns are net of costs (fees + slippage)\n")
        f.write("- Sharpe ratio assumes 252 trading days per year\n")
        f.write("- Max DD = Maximum Drawdown\n\n")
    
    print(f"\n[SUCCESS] Sensitivity analysis summary saved to {output_path}")
    print("\nSummary:")
    print(summary_df.to_string(index=False))


def main():
    parser = argparse.ArgumentParser(
        description="Run sensitivity analysis with multiple config variants",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run top-20 vs top-30 vs top-50 comparison
  python scripts/run_sensitivity.py --config configs/strategy.yaml \\
    --variants top20,top30,top50 \\
    --override-snapshots top_n:20 top_n:30 top_n:50

  # Run monthly vs quarterly rebalancing
  python scripts/run_sensitivity.py --config configs/strategy.yaml \\
    --variants monthly,quarterly \\
    --override-snapshots rebalance_frequency:monthly rebalance_frequency:quarterly
        """,
    )
    
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to base strategy config YAML",
    )
    parser.add_argument(
        "--variants",
        type=str,
        required=True,
        help="Comma-separated list of variant names",
    )
    parser.add_argument(
        "--override-snapshots",
        nargs="+",
        help="Config overrides for snapshots (format: key:value, supports nested keys like 'snapshots.top_n:30')",
    )
    parser.add_argument(
        "--override-backtest",
        nargs="+",
        help="Config overrides for backtest (format: key:value)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory for variant configs and summary (default: outputs/sensitivity)",
    )
    
    args = parser.parse_args()
    repo_root = Path(__file__).parent.parent
    
    # Resolve config path
    config_path = args.config if args.config.is_absolute() else repo_root / args.config
    
    if not config_path.exists():
        print(f"[ERROR] Config file not found: {config_path}")
        sys.exit(1)
    
    # Parse variants
    variant_names = [v.strip() for v in args.variants.split(",")]
    
    if len(variant_names) > 5:
        print(f"[WARN] Running {len(variant_names)} variants - this may take a while")
    
    # Parse overrides
    def parse_override(s: str):
        key, value = s.split(":", 1)
        # Try to convert value to int/float/bool
        try:
            if value.lower() == "true":
                value = True
            elif value.lower() == "false":
                value = False
            elif "." in value and value.replace(".", "").isdigit():
                value = float(value)
            elif value.isdigit():
                value = int(value)
        except:
            pass
        return key, value
    
    # Build variant configs
    variants = []
    override_snapshots = args.override_snapshots or []
    override_backtest = args.override_backtest or []
    
    # Simple approach: each variant gets one set of overrides
    # More sophisticated parsing can be added later
    if len(override_snapshots) != len(variant_names):
        print(f"[ERROR] Number of snapshot overrides ({len(override_snapshots)}) must match number of variants ({len(variant_names)})")
        sys.exit(1)
    
    for variant_name, override_str in zip(variant_names, override_snapshots):
        key, value = parse_override(override_str)
        # Support nested keys like "snapshots.top_n"
        if "." in key:
            parts = key.split(".")
            overrides = {}
            current = overrides
            for part in parts[:-1]:
                current[part] = {}
                current = current[part]
            current[parts[-1]] = value
        else:
            overrides = {key: value}
        
        # Add backtest overrides if provided
        if override_backtest:
            for override_str_bt in override_backtest:
                key_bt, value_bt = parse_override(override_str_bt)
                if "." in key_bt:
                    parts = key_bt.split(".")
                    current = overrides
                    for part in parts[:-1]:
                        if part not in current:
                            current[part] = {}
                        current = current[part]
                    current[parts[-1]] = value_bt
                else:
                    overrides[key_bt] = value_bt
        
        variants.append({
            "variant_name": variant_name,
            "config_overrides": overrides,
        })
    
    # Create output directory
    output_dir = args.output_dir if args.output_dir else repo_root / "outputs" / "sensitivity"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Create variant configs
    variant_configs = create_variant_configs(config_path, output_dir, variants)
    
    # Run backtests (each variant gets its own subdirectory to avoid overwriting artifacts)
    artifacts_dict = {}
    for variant_name, variant_config_path in variant_configs.items():
        artifacts = run_backtest_with_config(variant_config_path, variant_name, repo_root, output_dir)
        artifacts_dict[variant_name] = artifacts
    
    # Compare results
    summary_path = output_dir / "sensitivity_summary.md"
    compare_results(artifacts_dict, summary_path)
    
    print(f"\n[SUCCESS] Sensitivity analysis complete!")
    print(f"  Summary: {summary_path}")


if __name__ == "__main__":
    main()
