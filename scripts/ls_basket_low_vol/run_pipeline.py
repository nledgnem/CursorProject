"""
End-to-end pipeline: universe QC, Method A & B, parameter sweep, deliverables.
"""

import json
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import date
import sys

# Add repo root
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def load_config(config_path: Optional[Path] = None) -> Dict:
    """Load JSON config, merged with defaults and strategy_benchmark.yaml."""
    import yaml
    default_path = Path(__file__).parent / "config_default.json"
    with open(default_path) as f:
        cfg = json.load(f)
    # Merge strategy_benchmark.yaml for dates and cost_model
    bench_path = REPO_ROOT / "configs" / "strategy_benchmark.yaml"
    if bench_path.exists():
        with open(bench_path) as f:
            bench = yaml.safe_load(f)
        if bench:
            cfg.setdefault("start_date", bench.get("start_date", cfg.get("start_date")))
            cfg.setdefault("end_date", bench.get("end_date", cfg.get("end_date")))
            if "cost_model" in bench:
                cfg.setdefault("cost_model", {}).update(bench["cost_model"])
    if config_path and Path(config_path).exists():
        with open(config_path) as f:
            user = json.load(f)
        for k, v in user.items():
            if isinstance(v, dict) and k in cfg and isinstance(cfg[k], dict):
                cfg[k].update(v)
            else:
                cfg[k] = v
    return cfg


def run_sweep(
    prices: pd.DataFrame,
    marketcap: pd.DataFrame,
    volume: pd.DataFrame,
    config: Dict,
) -> List[Dict]:
    """Run parameter sweep for Method A and Method B."""
    from .universe import run_universe_qc
    from .method_a import run_method_a
    from .method_b import run_method_b
    from .backtest_basket import run_backtest

    start = date.fromisoformat(config["start_date"])
    end = date.fromisoformat(config["end_date"])
    cost = config.get("cost_model", {})
    fee_bps = cost.get("fee_bps", 5)
    slippage_bps = cost.get("slippage_bps", 5)
    qc = config.get("universe_qc", {})
    min_mcap = qc.get("min_mcap_usd", 10e6)
    min_vol = qc.get("min_volume_usd_14d_avg", 1e6)

    prices_u, mcap_u, vol_u, ureport = run_universe_qc(
        prices, marketcap, volume, start, end, min_mcap, min_vol
    )

    sweep_cfg = config.get("sweep", {})
    G_list = sweep_cfg.get("G", [1.0])
    alpha_list = sweep_cfg.get("alpha_cvar", [0.5])
    beta_list = sweep_cfg.get("beta_turnover", [0.1])
    K_list = sweep_cfg.get("K", [10])
    max_w_list = sweep_cfg.get("max_w_abs", [0.10])

    results = []

    # Method A sweep
    ma_cfg = config.get("method_a", {}).copy()
    for G in G_list:
        for alpha in alpha_list:
            for beta in beta_list:
                for max_w in max_w_list:
                    ma_cfg["G"] = G
                    ma_cfg["alpha_cvar"] = alpha
                    ma_cfg["beta_turnover"] = beta
                    ma_cfg["max_w_abs"] = max_w
                    ma_cfg["fee_bps"] = fee_bps
                    ma_cfg["slippage_bps"] = slippage_bps
                    try:
                        snapshots, meta = run_method_a(prices_u, mcap_u, vol_u, start, end, ma_cfg)
                        if not snapshots:
                            continue
                        pnl_df, metrics = run_backtest(snapshots, prices_u, fee_bps, slippage_bps)
                        if metrics.get("error"):
                            continue
                        results.append({
                            "method": "A",
                            "params": {"G": G, "alpha": alpha, "beta": beta, "max_w_abs": max_w},
                            "snapshots": snapshots,
                            "pnl_df": pnl_df,
                            "metrics": metrics,
                            "meta": meta,
                        })
                    except Exception as e:
                        print(f"Method A G={G} alpha={alpha} beta={beta} max_w={max_w} failed: {e}")
                        continue

    # Method B sweep
    mb_cfg = config.get("method_b", {}).copy()
    for K in K_list:
        for max_w in max_w_list:
            mb_cfg["K"] = K
            mb_cfg["max_w_abs"] = max_w
            mb_cfg["fee_bps"] = fee_bps
            mb_cfg["slippage_bps"] = slippage_bps
            try:
                snapshots, meta = run_method_b(prices_u, mcap_u, vol_u, start, end, mb_cfg)
                if not snapshots:
                    continue
                pnl_df, metrics = run_backtest(snapshots, prices_u, fee_bps, slippage_bps)
                if metrics.get("error"):
                    continue
                results.append({
                    "method": "B",
                    "params": {"K": K, "max_w_abs": max_w},
                    "snapshots": snapshots,
                    "pnl_df": pnl_df,
                    "metrics": metrics,
                    "meta": meta,
                })
            except Exception as e:
                print(f"Method B K={K} max_w={max_w} failed: {e}")
                continue

    return results, ureport


def select_top_candidates(
    results: List[Dict],
    max_avg_turnover: float = 0.20,
    min_ls_corr: float = 0.80,
) -> List[Dict]:
    """Filter by constraints and rank by volatility."""
    filtered = []
    for r in results:
        m = r["metrics"]
        turn = m.get("avg_monthly_turnover") or m.get("avg_turnover") or 0
        corr = m.get("long_short_corr")
        if corr is None:
            corr = 0.0
        if turn <= max_avg_turnover and corr >= min_ls_corr:
            filtered.append(r)
    if not filtered:
        return sorted(results, key=lambda x: x["metrics"].get("realized_vol_ann", float("inf")))
    return sorted(filtered, key=lambda x: x["metrics"].get("realized_vol_ann", float("inf")))


def write_deliverables(
    results: List[Dict],
    universe_report: Dict,
    config: Dict,
    output_dir: Path,
    min_ls_corr: float = 0.80,
) -> None:
    """Write CSVs, charts, summary."""
    output_dir = Path(output_dir)
    runs_dir = output_dir / "runs"
    reports_dir = output_dir / "reports"
    configs_dir = output_dir / "configs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)
    configs_dir.mkdir(parents=True, exist_ok=True)

    with open(configs_dir / "params_used.json", "w") as f:
        json.dump(config, f, indent=2)

    top3 = results[:3]
    for rank, r in enumerate(top3, 1):
        prefix = f"rank{rank}_{r['method']}"
        snaps = r["snapshots"]
        pnl_df = r["pnl_df"]
        metrics = r["metrics"]

        # Latest weights
        if snaps:
            last = snaps[-1]
            w = last["weights"]
            mcap = last.get("marketcap", {})
            adv = last.get("adv_30d", {})
            rows = []
            for sym, weight in sorted(w.items(), key=lambda x: -abs(x[1])):
                side = "long" if weight > 0 else "short"
                rows.append({
                    "symbol": sym,
                    "weight": weight,
                    "side": side,
                    "marketcap": mcap.get(sym, np.nan),
                    "adv_30d": adv.get(sym, np.nan),
                })
            pd.DataFrame(rows).to_csv(runs_dir / f"{prefix}_weights.csv", index=False)

        # Time series
        if not pnl_df.empty:
            pnl_df.to_csv(runs_dir / f"{prefix}_daily_pnl.csv", index=False)

        # Summary table
        summary = {
            "realized_vol_ann": metrics.get("realized_vol_ann"),
            "kurtosis": metrics.get("kurtosis"),
            "cvar95": metrics.get("cvar95"),
            "cvar99": metrics.get("cvar99"),
            "max_drawdown": metrics.get("max_drawdown"),
            "avg_turnover": metrics.get("avg_turnover"),
            "avg_long_short_corr": metrics.get("long_short_corr"),
            "gross_exposure": metrics.get("avg_gross_exposure"),
        }
        pd.DataFrame([summary]).to_csv(runs_dir / f"{prefix}_summary.csv", index=False)

    # Diagnostic plots
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        for rank, r in enumerate(top3, 1):
            prefix = f"rank{rank}_{r['method']}"
            pnl_df = r["pnl_df"]
            if pnl_df.empty or "equity" not in pnl_df.columns:
                continue

            pnl_df = pnl_df.copy()
            pnl_df["date"] = pd.to_datetime(pnl_df["date"])
            pnl_df = pnl_df.set_index("date")

            fig, axes = plt.subplots(2, 2, figsize=(12, 10))
            # Equity curve
            axes[0, 0].plot(pnl_df.index, pnl_df["equity"])
            axes[0, 0].set_title("Equity Curve")
            axes[0, 0].set_xlabel("Date")
            # Rolling vol
            ret = pnl_df["pnl"].dropna()
            roll_vol = ret.rolling(90).std() * np.sqrt(252) * 100
            axes[0, 1].plot(roll_vol.index, roll_vol)
            axes[0, 1].set_title("Rolling 90d Volatility (%)")
            axes[0, 1].set_xlabel("Date")
            # Turnover histogram
            axes[1, 0].hist(pnl_df["turnover"].dropna(), bins=30)
            axes[1, 0].set_title("Turnover Histogram")
            axes[1, 0].set_xlabel("Turnover")
            # Gross exposure
            axes[1, 1].plot(pnl_df.index, pnl_df["gross_exposure"])
            axes[1, 1].set_title("Daily Gross Exposure")
            axes[1, 1].set_xlabel("Date")
            plt.tight_layout()
            plt.savefig(reports_dir / f"{prefix}_diagnostics.png", dpi=150)
            plt.close()
    except ImportError:
        pass

    # Markdown report
    lines = [
        "# LS Basket Low-Vol Report",
        "",
        "## Universe QC",
        f"- Period: {universe_report.get('start_date')} to {universe_report.get('end_date')}",
        f"- Universe size: {universe_report.get('universe_size')}",
        f"- Min mcap USD: {universe_report.get('min_mcap_usd', 0):,.0f}",
        f"- Min 14d avg volume USD: {universe_report.get('min_volume_usd_14d_avg', 0):,.0f}",
        "",
        "## Top 3 Baskets",
        "",
    ]
    for rank, r in enumerate(top3, 1):
        m = r["metrics"]
        lines.append(f"### Rank {rank}: Method {r['method']} (params: {r['params']})")
        lines.append(f"- Realized Vol (ann): {m.get('realized_vol_ann', 0):.2%}")
        lines.append(f"- Kurtosis: {m.get('kurtosis', 0):.2f}")
        lines.append(f"- CVaR 95%: {m.get('cvar95', 0):.2%}")
        lines.append(f"- CVaR 99%: {m.get('cvar99', 0):.2%}")
        lines.append(f"- Max Drawdown: {m.get('max_drawdown', 0):.2%}")
        lines.append(f"- Avg Turnover: {m.get('avg_turnover', 0):.2%}")
        lines.append(f"- Long/Short Corr: {m.get('long_short_corr', 0):.2f}")
        lines.append("")
    lines.append("## Recommendation")
    if top3:
        best = top3[0]
        m = best["metrics"]
        corr = m.get("long_short_corr")
        meets_corr = corr is not None and corr >= min_ls_corr
        lines.append(f"**Recommended: Method {best['method']}** with params {best['params']}. ")
        if meets_corr:
            lines.append("Select this basket for the lowest realized volatility among candidates meeting turnover and long/short correlation constraints.")
        else:
            lines.append(f"*Note: Long/short correlation ({corr:.2f}) is below target (>={min_ls_corr}). No candidate met all constraints; this is the lowest-volatility basket. Consider relaxing constraints or adjusting method parameters.*")
    with open(reports_dir / "summary.md", "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", type=Path, help="JSON config path")
    ap.add_argument("--output-dir", type=Path, default=None)
    ap.add_argument("--quick", action="store_true", help="Minimal sweep for testing")
    args = ap.parse_args()

    config = load_config(args.config)
    if args.quick:
        config["sweep"] = {"G": [1.0], "alpha_cvar": [0.5], "beta_turnover": [0.1], "K": [10], "max_w_abs": [0.10]}
    data_lake = Path(config["data_lake_dir"])
    if not data_lake.is_absolute():
        data_lake = REPO_ROOT / data_lake

    from .utils import load_data_lake
    data = load_data_lake(str(data_lake))
    prices = data["prices"]
    marketcap = data["marketcap"]
    volume = data["volume"]

    start = date.fromisoformat(config["start_date"])
    end = date.fromisoformat(config["end_date"])
    prices = prices[(prices.index >= start) & (prices.index <= end)]
    marketcap = marketcap.reindex(prices.index).ffill().bfill()
    volume = volume.reindex(prices.index).ffill().bfill()

    print("Running universe QC and parameter sweep...")
    results, ureport = run_sweep(prices, marketcap, volume, config)

    max_turn = config.get("constraints", {}).get("max_avg_turnover", 0.20)
    min_corr = config.get("constraints", {}).get("min_long_short_corr", 0.80)
    ranked = select_top_candidates(results, max_turn, min_corr)

    out_dir = args.output_dir or (REPO_ROOT / config["output_dir"])
    write_deliverables(ranked, ureport, config, out_dir, min_ls_corr=min_corr)

    run_meta = {
        "timestamp": pd.Timestamp.now().isoformat(),
        "universe_report": ureport,
        "num_candidates": len(results),
        "top_3": [
            {"method": r["method"], "params": r["params"], "vol": r["metrics"].get("realized_vol_ann")}
            for r in ranked[:3]
        ],
    }
    meta_path = REPO_ROOT / "outputs" / "run_metadata_ls_low_vol.json"
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    with open(meta_path, "w") as f:
        json.dump(run_meta, f, indent=2)

    print("\n=== Executive Summary ===")
    print(f"Universe size: {ureport.get('universe_size')}")
    print(f"Candidates evaluated: {len(results)}")
    for i, r in enumerate(ranked[:3], 1):
        print(f"Rank {i}: Method {r['method']} vol={r['metrics'].get('realized_vol_ann', 0):.2%}")
    print(f"Outputs written to {out_dir}")


if __name__ == "__main__":
    main()
