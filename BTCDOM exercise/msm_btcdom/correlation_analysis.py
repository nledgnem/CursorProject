"""
Correlation analysis: funding (F_tk) vs forward LS return (y), overall and by BTCDOM regime.
Reads joined timeseries CSV (with btcdom_recon). Writes report and optional plots to out dir.
"""

import argparse
import json
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    _HAS_MPL = True
except ImportError:
    _HAS_MPL = False


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Correlation F_tk vs y, overall and by BTCDOM regime.")
    p.add_argument(
        "--timeseries-csv",
        required=True,
        type=Path,
        help="Path to msm_timeseries_with_btcdom.csv (or any CSV with F_tk, y, optionally btcdom_recon)",
    )
    p.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Directory for report and plots (default: same as timeseries CSV)",
    )
    p.add_argument(
        "--format",
        choices=["txt", "json", "both"],
        default="both",
        help="Report format",
    )
    p.add_argument(
        "--no-plot",
        action="store_true",
        help="Skip generating scatter/rolling correlation plots",
    )
    p.add_argument(
        "--regime-quantiles",
        type=int,
        default=4,
        help="Number of quantiles for BTCDOM regime (default 4 = quartiles)",
    )
    return p.parse_args()


def compute_correlations(df: pd.DataFrame, regime_col: Optional[str] = "btcdom_regime") -> dict:
    """Compute overall and (if regime_col present) per-regime correlations and stats."""
    df = df.dropna(subset=["F_tk", "y"])
    if len(df) == 0:
        return {"error": "No valid F_tk, y pairs after dropna"}

    out = {
        "n_weeks": int(len(df)),
        "overall": {
            "pearson": float(df["F_tk"].corr(df["y"])),
            "spearman": float(df["F_tk"].corr(df["y"], method="spearman")),
            "mean_y": float(df["y"].mean()),
            "hit_rate_negative_y": float((df["y"] < 0).mean()),
        },
    }

    if regime_col in df.columns and df[regime_col].notna().any():
        regimes = sorted(df[regime_col].dropna().unique())
        out["by_regime"] = {}
        for r in regimes:
            sub = df[df[regime_col] == r]
            if len(sub) < 3:
                continue
            out["by_regime"][str(r)] = {
                "n": int(len(sub)),
                "pearson": float(sub["F_tk"].corr(sub["y"])),
                "spearman": float(sub["F_tk"].corr(sub["y"], method="spearman")),
                "mean_y": float(sub["y"].mean()),
                "hit_rate_negative_y": float((sub["y"] < 0).mean()),
            }
    else:
        out["by_regime"] = None

    return out


def add_regime(df: pd.DataFrame, col: str = "btcdom_recon", n_quantiles: int = 4) -> pd.DataFrame:
    """Add btcdom_regime as quantile labels (1..n_quantiles)."""
    df = df.copy()
    if col not in df.columns or df[col].isna().all():
        df["btcdom_regime"] = np.nan
        return df
    q = pd.qcut(df[col].rank(method="first"), n_quantiles, labels=range(1, n_quantiles + 1))
    df["btcdom_regime"] = q
    return df


def write_report(results: dict, out_dir: Path, fmt: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    report_txt = out_dir / "funding_return_correlation_report.txt"
    report_json = out_dir / "funding_return_correlation_report.json"

    def to_lines() -> list:
        lines = [
            "Funding (F_tk) vs forward LS return (y)",
            "=" * 50,
            f"Weeks: {results.get('n_weeks', 'N/A')}",
            "",
            "Overall",
            "  Pearson:  {:.4f}".format(results["overall"]["pearson"]),
            "  Spearman: {:.4f}".format(results["overall"]["spearman"]),
            "  mean(y):  {:.4f}".format(results["overall"]["mean_y"]),
            "  hit_rate(y<0): {:.2%}".format(results["overall"]["hit_rate_negative_y"]),
            "",
        ]
        if results.get("by_regime"):
            lines.append("By BTCDOM regime (quantiles)")
            for regime, stats in results["by_regime"].items():
                lines.append(f"  Regime {regime}: n={stats['n']} pearson={stats['pearson']:.4f} spearman={stats['spearman']:.4f} mean_y={stats['mean_y']:.4f}")
            lines.append("")
        return lines

    if fmt in ("txt", "both"):
        with open(report_txt, "w") as f:
            f.write("\n".join(to_lines()))
        logger.info("Wrote %s", report_txt)
    if fmt in ("json", "both"):
        with open(report_json, "w") as f:
            json.dump(results, f, indent=2)
        logger.info("Wrote %s", report_json)


def plot_scatter(df: pd.DataFrame, out_dir: Path, regime_col: Optional[str] = "btcdom_regime") -> None:
    if not _HAS_MPL:
        return
    out_dir.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots()
    df = df.dropna(subset=["F_tk", "y"])
    if regime_col in df.columns and df[regime_col].notna().any():
        for r in sorted(df[regime_col].dropna().unique()):
            sub = df[df[regime_col] == r]
            ax.scatter(sub["F_tk"], sub["y"], alpha=0.6, label=f"Regime {r}", s=20)
        ax.legend()
    else:
        ax.scatter(df["F_tk"], df["y"], alpha=0.6, s=20)
    ax.axhline(0, color="gray", linestyle="--")
    ax.set_xlabel("F_tk (7d mean funding)")
    ax.set_ylabel("y (forward LS return)")
    ax.set_title("Funding vs forward LS return")
    fig.savefig(out_dir / "funding_vs_return_scatter.png", dpi=120, bbox_inches="tight")
    plt.close(fig)
    logger.info("Wrote %s", out_dir / "funding_vs_return_scatter.png")


def plot_rolling_correlation(df: pd.DataFrame, window: int, out_dir: Path) -> None:
    if not _HAS_MPL:
        return
    df = df.dropna(subset=["F_tk", "y"]).sort_values("decision_date")
    if len(df) < window:
        return
    df = df.copy()
    df["roll_corr"] = df["F_tk"].rolling(window).corr(df["y"])
    out_dir.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots()
    ax.plot(pd.to_datetime(df["decision_date"]), df["roll_corr"], label=f"Rolling corr (w={window})")
    ax.axhline(0, color="gray", linestyle="--")
    ax.set_xlabel("decision_date")
    ax.set_ylabel("Pearson(F_tk, y)")
    ax.set_title("Rolling correlation (F_tk vs y)")
    ax.legend()
    fig.savefig(out_dir / "funding_return_rolling_corr.png", dpi=120, bbox_inches="tight")
    plt.close(fig)
    logger.info("Wrote %s", out_dir / "funding_return_rolling_corr.png")


def main() -> None:
    args = parse_args()
    csv_path = Path(args.timeseries_csv).resolve()
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)
    out_dir = args.out_dir
    if out_dir is None:
        out_dir = csv_path.parent
    out_dir = Path(out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Loading %s", csv_path)
    df = pd.read_csv(csv_path)
    for c in ["F_tk", "y"]:
        if c not in df.columns:
            raise ValueError(f"CSV missing column: {c}")

    n_quantiles = args.regime_quantiles
    if "btcdom_recon" in df.columns and df["btcdom_recon"].notna().any():
        df = add_regime(df, col="btcdom_recon", n_quantiles=n_quantiles)
    else:
        df["btcdom_regime"] = np.nan
        logger.warning("No btcdom_recon or all NaN; skipping regime breakdown.")

    results = compute_correlations(df, regime_col="btcdom_regime")
    if "error" in results:
        logger.error(results["error"])
        return
    write_report(results, out_dir, args.format)

    if not args.no_plot and _HAS_MPL:
        plot_scatter(df, out_dir)
        plot_rolling_correlation(df, window=max(4, len(df) // 10), out_dir=out_dir)


if __name__ == "__main__":
    main()
