#!/usr/bin/env python
"""Run 15+15 long/short baskets: 5 equal-weight + 5 optimized, ranked by volatility."""

import sys
import json
import pandas as pd
from pathlib import Path
from datetime import date

REPO = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO))

from scripts.ls_basket_low_vol.utils import load_data_lake
from scripts.ls_basket_low_vol.universe import run_universe_qc
from scripts.ls_basket_low_vol.basket_15x15 import run_baskets_15x15
from scripts.ls_basket_low_vol.backtest_basket import run_backtest


def main():
    config_path = REPO / "scripts" / "ls_basket_low_vol" / "config_default.json"
    with open(config_path) as f:
        config = json.load(f)

    start = date.fromisoformat(config["start_date"])
    end = date.fromisoformat(config["end_date"])
    data = load_data_lake(str(REPO / config["data_lake_dir"]))
    prices = data["prices"][(data["prices"].index >= start) & (data["prices"].index <= end)]
    mcap = data["marketcap"].reindex(prices.index).ffill().bfill()
    vol = data["volume"].reindex(prices.index).ffill().bfill()

    prices_u, mcap_u, vol_u, ureport = run_universe_qc(
        prices, mcap, vol, start, end,
        config["universe_qc"]["min_mcap_usd"],
        config["universe_qc"]["min_volume_usd_14d_avg"],
    )

    fee_bps = config["cost_model"]["fee_bps"]
    slippage_bps = config["cost_model"]["slippage_bps"]

    print("Building 10 baskets (5 equal-weight + 5 optimized)...")
    baskets = run_baskets_15x15(prices_u, start, end, lookback=90)

    results = []
    for b in baskets:
        pnl_df, metrics = run_backtest(b["snapshots"], prices_u, fee_bps, slippage_bps)
        if metrics.get("error"):
            continue
        results.append({
            **b,
            "pnl_df": pnl_df,
            "metrics": metrics,
        })

    results.sort(key=lambda x: x["metrics"].get("realized_vol_ann", float("inf")))

    out_dir = REPO / "outputs" / "ls_basket_low_vol" / "baskets_15x15"
    out_dir.mkdir(parents=True, exist_ok=True)
    runs_dir = out_dir / "runs"
    runs_dir.mkdir(exist_ok=True)

    lines = [
        "# 15+15 Long/Short Baskets — Ranked by Realized Volatility",
        "",
        f"Universe: {ureport['universe_size']} assets",
        f"Period: {start} to {end}",
        "",
        "| Rank | Type | Strategy | Vol (ann) | Max DD | Turnover |",
        "|------|------|----------|-----------|--------|----------|",
    ]

    for i, r in enumerate(results[:10], 1):
        m = r["metrics"]
        vol_pct = m.get("realized_vol_ann", 0) * 100
        dd = m.get("max_drawdown", 0) * 100
        turn = m.get("avg_turnover", 0) * 100
        lines.append(f"| {i} | {r['basket_type']} | {r['strategy']} | {vol_pct:.2f}% | {dd:.1f}% | {turn:.2f}% |")

        prefix = f"rank{i}_{r['basket_type']}_{r['strategy']}"
        last = r["snapshots"][-1] if r["snapshots"] else {}
        longs = last.get("longs", [])
        shorts = last.get("shorts", [])
        w = last.get("weights", {})

        rows = []
        for a in longs + shorts:
            wt = w.get(a, 1/30 if a in longs else -1/30)
            rows.append({"symbol": a, "side": "long" if wt > 0 else "short", "weight": wt})
        pd.DataFrame(rows).to_csv(runs_dir / f"{prefix}_weights.csv", index=False)

        if not r["pnl_df"].empty:
            r["pnl_df"].to_csv(runs_dir / f"{prefix}_daily_pnl.csv", index=False)

        pd.DataFrame([m]).to_csv(runs_dir / f"{prefix}_summary.csv", index=False)

    with open(out_dir / "summary.md", "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    meta = {
        "n_baskets": len(results),
        "top_5": [
            {"rank": i, "type": r["basket_type"], "strategy": r["strategy"], "vol": r["metrics"].get("realized_vol_ann")}
            for i, r in enumerate(results[:5], 1)
        ],
    }
    with open(out_dir / "metadata.json", "w") as f:
        json.dump(meta, f, indent=2)

    print("\n=== Results ===")
    for i, r in enumerate(results[:10], 1):
        v = r["metrics"].get("realized_vol_ann", 0) * 100
        print(f"  {i}. {r['basket_type']} / {r['strategy']}: vol={v:.2f}%")
    print(f"\nOutputs: {out_dir}")

    try:
        from scripts.ls_basket_low_vol.generate_basket_charts import main as gen_charts
        gen_charts()
        print("Charts generated.")
    except Exception as e:
        print(f"Charts: {e}")


if __name__ == "__main__":
    main()
