"""
Strategy Simulator v1 – Majors/Alts Momentum with Trend Gate
=============================================================

Merges cross-sectional momentum rank (cs_rank) with the macro
funding sensor (w_risk), applies the RBMA trend gate, and simulates
a daily-rebalanced **winner-take-all** strategy on a $1 M stablecoin
reserve.

Allocation rule
---------------
  For each day T (using features computed at the 00:00 UTC close of T):
    1. Filter to assets where trend_gate == 1.
    2. If no asset passes the gate → weight = 0 for all (park in stables).
    3. Otherwise the asset with the highest cs_rank receives a weight
       equal to that day's w_risk.  Ties are broken by column order
       (BTC → ETH → SOL → BNB).
    4. target_weight is .shift(1) so the weight traded on day T is
       derived exclusively from signals available at close of T-1
       (Look-Ahead Ban).

Usage
-----
    python scripts/strategy_simulator.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
TICKERS = ["BTC", "ETH", "SOL", "BNB"]


# ─── data loading ───────────────────────────────────────────────────


def load_features() -> pd.DataFrame:
    """Load the long-format feature table (cs_rank, trend_gate, etc.)."""
    path = REPO_ROOT / "data" / "features" / "cross_sectional_rank.parquet"
    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    return df


def load_macro_sensor() -> pd.DataFrame:
    """
    Load the macro funding sensor (w_risk in [0, 1]).

    If the file does not exist, every day is mocked as w_risk = 1.0
    (fully risk-on) and a loud warning is printed.
    """
    path = REPO_ROOT / "data" / "features" / "macro_sensor.parquet"
    if path.exists():
        df = pd.read_parquet(path)
        df["date"] = pd.to_datetime(df["date"]).dt.normalize()
        if "w_risk" not in df.columns:
            raise ValueError("macro_sensor.parquet must contain a 'w_risk' column")
        return df[["date", "w_risk"]]

    print(
        "WARNING: data/features/macro_sensor.parquet not found. "
        "Mocking w_risk = 1.0 (fully risk-on) for all dates. "
        "Replace with the real sensor before live deployment.",
        file=sys.stderr,
    )
    feat = load_features()
    dates = feat["date"].drop_duplicates().sort_values().reset_index(drop=True)
    return pd.DataFrame({"date": dates, "w_risk": 1.0})


def load_prices(tickers: list[str]) -> pd.DataFrame:
    """Load wide prices panel and return only the requested tickers."""
    path = REPO_ROOT / "data" / "curated" / "prices_daily.parquet"
    df = pd.read_parquet(path)
    missing = [t for t in tickers if t not in df.columns]
    if missing:
        raise ValueError(f"Missing tickers in prices_daily.parquet: {missing}")
    out = df[tickers].copy()
    out.index = pd.to_datetime(out.index).normalize()
    out.index.name = "date"
    return out


# ─── allocation engine ──────────────────────────────────────────────


def compute_weights(
    features: pd.DataFrame,
    macro: pd.DataFrame,
    tickers: list[str],
) -> pd.DataFrame:
    """
    Winner-take-all allocation gated by trend_gate and scaled by w_risk.

    Returns
    -------
    shifted_weight : pd.DataFrame
        Wide DataFrame (date × ticker) of portfolio weights already
        shifted forward by 1 day (Look-Ahead Ban applied).
    """
    cs_rank_wide = features.pivot(
        index="date", columns="ticker", values="cs_rank",
    )[tickers]

    trend_gate_wide = features.pivot(
        index="date", columns="ticker", values="trend_gate",
    )[tickers]

    # Align w_risk onto the feature date index;
    # missing macro data → w_risk = 0.0 (fail-safe to stables)
    w_risk = (
        macro.set_index("date")["w_risk"]
        .reindex(cs_rank_wide.index)
        .fillna(0.0)
    )

    # Mask out assets that fail the trend gate
    eligible_rank = cs_rank_wide.where(trend_gate_wide == 1)

    # Winner = column with the highest eligible cs_rank per row.
    # Only call idxmax on rows that have at least one non-NaN value
    # to avoid the deprecated idxmax-on-all-NA behaviour.
    all_nan = eligible_rank.isna().all(axis=1)
    has_eligible = ~all_nan
    winner = pd.Series(np.nan, index=eligible_rank.index, dtype=object)
    if has_eligible.any():
        winner.loc[has_eligible] = eligible_rank.loc[has_eligible].idxmax(axis=1)

    raw_weight = pd.DataFrame(0.0, index=cs_rank_wide.index, columns=tickers)
    for ticker in tickers:
        raw_weight[ticker] = (winner == ticker).astype(float)

    # Rows with no eligible asset already have all-zero weights
    # because NaN != ticker evaluates to False.

    # Scale the winner's allocation by w_risk
    target_weight = raw_weight.multiply(w_risk, axis=0)

    # LOOK-AHEAD BAN: signal from T-1 trades on T
    shifted_weight = target_weight.shift(1)

    return shifted_weight


# ─── return calculations ────────────────────────────────────────────


def compute_returns(
    prices: pd.DataFrame,
    weights: pd.DataFrame,
    tickers: list[str],
) -> pd.DataFrame:
    """
    Compute daily strategy returns, BTC buy-and-hold returns, and
    cumulative compound curves for both.
    """
    daily_ret = prices[tickers].pct_change(fill_method=None)

    common_idx = weights.index.intersection(daily_ret.index)
    w = weights.reindex(common_idx)
    r = daily_ret.reindex(common_idx)

    strat_daily = (w * r).sum(axis=1)
    strat_daily.name = "strategy_return"

    btc_daily = r["BTC"].copy()
    btc_daily.name = "btc_return"

    strat_cum = (1 + strat_daily).cumprod()
    btc_cum = (1 + btc_daily).cumprod()

    log = pd.DataFrame({
        "strategy_return": strat_daily,
        "btc_return": btc_daily,
        "strategy_cumulative": strat_cum,
        "btc_cumulative": btc_cum,
    })

    for t in tickers:
        log[f"w_{t}"] = w[t]

    log.index.name = "date"
    return log


# ─── metrics ────────────────────────────────────────────────────────


def max_drawdown(cum_series: pd.Series) -> float:
    """Peak-to-trough drawdown on a cumulative-return series."""
    running_max = cum_series.cummax()
    dd = (cum_series - running_max) / running_max
    return float(dd.min())


# ─── plotting ───────────────────────────────────────────────────────


def plot_equity_curve(log: pd.DataFrame, out_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(14, 6))

    ax.plot(
        log.index, log["strategy_cumulative"],
        linewidth=1.4, color="steelblue", label="Strategy v1",
    )
    ax.plot(
        log.index, log["btc_cumulative"],
        linewidth=1.0, color="grey", alpha=0.7,
        linestyle="--", label="Buy & Hold BTC",
    )

    ax.axhline(1.0, color="black", linewidth=0.5, linestyle=":")
    ax.set_ylabel("Cumulative Return (1.0 = start)")
    ax.set_title("Strategy v1  vs  Buy & Hold BTC")
    ax.legend(loc="upper left")
    ax.grid(True, alpha=0.25)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    fig.autofmt_xdate(rotation=30)
    fig.tight_layout()
    fig.savefig(out_path, dpi=180)
    plt.close(fig)
    print(f"Saved chart: {out_path}")


# ─── main ───────────────────────────────────────────────────────────


def main() -> int:
    # ── Step 1: load & merge ─────────────────────────────────────────
    features = load_features()
    macro = load_macro_sensor()
    prices = load_prices(TICKERS)

    # ── Step 2: allocation with shift(1) ─────────────────────────────
    shifted_weights = compute_weights(features, macro, TICKERS)

    # ── Step 3: strategy returns ─────────────────────────────────────
    log = compute_returns(prices, shifted_weights, TICKERS)

    # Drop leading NaN rows produced by shift(1) + pct_change()
    log = log.dropna(subset=["strategy_return", "btc_return"])

    # ── Step 4a: persist audit log ───────────────────────────────────
    bt_dir = REPO_ROOT / "data" / "backtests"
    bt_dir.mkdir(parents=True, exist_ok=True)
    csv_path = bt_dir / "strategy_v1_log.csv"

    log_out = log.reset_index()
    log_out["date"] = log_out["date"].dt.strftime("%Y-%m-%d")
    log_out.to_csv(csv_path, index=False)
    print(f"Saved: {csv_path}  ({len(log_out)} rows)")

    # ── Step 4b: summary metrics ─────────────────────────────────────
    strat_total = log["strategy_cumulative"].iloc[-1] - 1.0
    btc_total = log["btc_cumulative"].iloc[-1] - 1.0
    strat_mdd = max_drawdown(log["strategy_cumulative"])
    btc_mdd = max_drawdown(log["btc_cumulative"])

    print()
    print("=" * 52)
    print("  Strategy v1  vs  Buy & Hold BTC  –  Summary")
    print("=" * 52)
    print(f"  Strategy  cumulative return : {strat_total:+.4f}  ({strat_total * 100:+.2f}%)")
    print(f"  Strategy  max drawdown      : {strat_mdd:.4f}  ({strat_mdd * 100:.2f}%)")
    print(f"  BTC B&H   cumulative return : {btc_total:+.4f}  ({btc_total * 100:+.2f}%)")
    print(f"  BTC B&H   max drawdown      : {btc_mdd:.4f}  ({btc_mdd * 100:.2f}%)")
    print("=" * 52)
    print()

    # ── Step 4c: equity curve chart ──────────────────────────────────
    chart_path = REPO_ROOT / "validation_output" / "strategy_v1_equity_curve.png"
    chart_path.parent.mkdir(parents=True, exist_ok=True)
    plot_equity_curve(log, chart_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
