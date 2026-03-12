"""
Join BTCDOM (recon.csv) to MSM v0 timeseries by decision_date (asof merge).
Optionally apply gating (filter rows by BTCDOM threshold) and write gated CSV.
Read-only from reports/; all outputs under BTCDOM exercise/msm_btcdom/.
"""

import argparse
import logging
from pathlib import Path
from typing import Optional

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def load_config(config_path: Optional[Path] = None) -> dict:
    """Load optional config.yaml from msm_btcdom dir."""
    try:
        import yaml
    except ImportError:
        return {}
    if config_path is None:
        config_path = Path(__file__).resolve().parent / "config.yaml"
    if not config_path.exists():
        return {}
    with open(config_path) as f:
        return yaml.safe_load(f) or {}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Join BTCDOM to MSM timeseries (asof on decision_date).")
    p.add_argument(
        "--timeseries-csv",
        required=True,
        type=Path,
        help="Path to msm_timeseries.csv (e.g. reports/msm_funding_v0/<run>/msm_timeseries.csv)",
    )
    p.add_argument(
        "--recon-csv",
        type=Path,
        default=None,
        help="Path to recon.csv (default: ../recon.csv relative to script)",
    )
    p.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Output directory for joined CSV (default: out/ under msm_btcdom)",
    )
    p.add_argument(
        "--gate",
        action="store_true",
        help="Apply BTCDOM gating and write msm_timeseries_gated.csv",
    )
    p.add_argument(
        "--mode",
        choices=["above", "below", "between"],
        default="above",
        help="Gate mode: include only when btcdom is above/below threshold or between low-high",
    )
    p.add_argument(
        "--threshold",
        type=float,
        default=None,
        help="Single threshold for mode above/below (e.g. 4000)",
    )
    p.add_argument(
        "--low",
        type=float,
        default=None,
        help="Lower bound for mode between",
    )
    p.add_argument(
        "--high",
        type=float,
        default=None,
        help="Upper bound for mode between",
    )
    p.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to config.yaml (default: config.yaml in script dir)",
    )
    return p.parse_args()


def resolve_paths(args: argparse.Namespace) -> tuple[Path, Path, Path]:
    script_dir = Path(__file__).resolve().parent
    config = load_config(args.config or script_dir / "config.yaml")

    timeseries_path = Path(args.timeseries_csv)
    if not timeseries_path.is_absolute():
        timeseries_path = (Path.cwd() / timeseries_path).resolve()
    if not timeseries_path.exists():
        raise FileNotFoundError(f"Timeseries CSV not found: {timeseries_path}")

    recon_path = args.recon_csv
    if recon_path is None:
        recon_path = script_dir / ".." / "recon.csv"
    recon_path = Path(recon_path).resolve()
    if not recon_path.exists():
        raise FileNotFoundError(f"Recon CSV not found: {recon_path}")

    out_dir = args.out_dir
    if out_dir is None:
        out_dir = script_dir / config.get("out_dir", "out")
    out_dir = Path(out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    return timeseries_path, recon_path, out_dir


def asof_join_btcdom(msm: pd.DataFrame, recon: pd.DataFrame) -> pd.DataFrame:
    """
    Join btcdom_recon (and n_constituents_used) to MSM by decision_date.
    For each decision_date use the last recon row with timestamp <= decision_date.
    """
    msm = msm.copy()
    msm["decision_date"] = pd.to_datetime(msm["decision_date"]).dt.normalize()
    recon = recon.copy()
    recon["_recon_date"] = pd.to_datetime(recon["timestamp"]).dt.normalize()
    recon = recon.sort_values("_recon_date")
    extra = [c for c in ["n_constituents_used"] if c in recon.columns]
    recon = recon[["_recon_date", "btcdom_recon"] + extra].drop_duplicates(subset=["_recon_date"], keep="last")

    merged = pd.merge_asof(
        msm.sort_values("decision_date"),
        recon,
        left_on="decision_date",
        right_on="_recon_date",
        direction="backward",
    )
    if "_recon_date" in merged.columns:
        merged = merged.drop(columns=["_recon_date"])
    return merged


def apply_gate(df: pd.DataFrame, mode: str, threshold: Optional[float], low: Optional[float], high: Optional[float]) -> pd.DataFrame:
    """Filter rows by BTCDOM gate. Requires column btcdom_recon."""
    if "btcdom_recon" not in df.columns or df["btcdom_recon"].isna().all():
        logger.warning("No btcdom_recon column or all NaN; gate has no effect.")
        return df
    b = df["btcdom_recon"]
    if mode == "above":
        t = threshold if threshold is not None else 4000.0
        return df[b >= t]
    if mode == "below":
        t = threshold if threshold is not None else 5000.0
        return df[b <= t]
    if mode == "between":
        lo = low if low is not None else 3500.0
        hi = high if high is not None else 5500.0
        return df[(b >= lo) & (b <= hi)]
    return df


def main() -> None:
    args = parse_args()
    timeseries_path, recon_path, out_dir = resolve_paths(args)

    logger.info("Loading MSM timeseries: %s", timeseries_path)
    msm = pd.read_csv(timeseries_path)
    logger.info("Loading recon: %s", recon_path)
    recon = pd.read_csv(recon_path)

    required = ["decision_date", "F_tk", "y"]
    for c in required:
        if c not in msm.columns:
            raise ValueError(f"Timeseries missing required column: {c}")
    if "timestamp" not in recon.columns or "btcdom_recon" not in recon.columns:
        raise ValueError("Recon CSV must have timestamp and btcdom_recon")

    out = asof_join_btcdom(msm, recon)
    out_path = out_dir / "msm_timeseries_with_btcdom.csv"
    out.to_csv(out_path, index=False)
    logger.info("Wrote %s (%d rows)", out_path, len(out))

    if args.gate:
        threshold = args.threshold
        low, high = args.low, args.high
        gated = apply_gate(out, args.mode, threshold, low, high)
        gated_path = out_dir / "msm_timeseries_gated.csv"
        gated.to_csv(gated_path, index=False)
        logger.info("Gated (%s): %d of %d weeks -> %s", args.mode, len(gated), len(out), gated_path)

    return None


if __name__ == "__main__":
    main()
