#!/usr/bin/env python3
"""
Phase 9 research: regime-conditional forward returns audit.

Outputs:
- validation_output/regime_fwd_returns.png
- Console summary table: median 7D forward return + win rate by regime for BTC and SOL
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns


REGIME_ORDER = [
    "Cold Flush",
    "Recovery Ramp",
    "Golden Pocket",
    "Leverage Exhaustion",
]

TARGET_TICKERS = ["BTC", "SOL"]


def _latest_macro_default() -> Path:
    candidates = sorted(
        Path("reports").glob("msm_funding_v0/*/macro_audit/master_macro_features.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if candidates:
        return candidates[0]
    return Path("reports/msm_funding_v0/20260325_112659/macro_audit/master_macro_features.csv")


def _normalize_date_utc(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, utc=True, errors="coerce")
    return dt.dt.floor("D")


def load_prices(price_path: Path, tickers: Iterable[str]) -> pd.DataFrame:
    if not price_path.exists():
        raise FileNotFoundError(f"Price file not found: {price_path}")

    if price_path.suffix.lower() == ".csv":
        raw = pd.read_csv(price_path)
    elif price_path.suffix.lower() == ".parquet":
        raw = pd.read_parquet(price_path)
    else:
        raise ValueError(f"Unsupported file extension for prices: {price_path.suffix}")

    cols_lower = {str(c).lower(): c for c in raw.columns}

    # Long format expected: date/ticker/close.
    if {"date", "ticker", "close"}.issubset(set(cols_lower.keys())):
        df = raw.rename(
            columns={
                cols_lower["date"]: "date",
                cols_lower["ticker"]: "ticker",
                cols_lower["close"]: "close",
            }
        )[["date", "ticker", "close"]]
    elif {"date", "asset_id", "close"}.issubset(set(cols_lower.keys())):
        df = raw.rename(
            columns={
                cols_lower["date"]: "date",
                cols_lower["asset_id"]: "ticker",
                cols_lower["close"]: "close",
            }
        )[["date", "ticker", "close"]]
    else:
        # Wide fallback: assume index/date column + ticker columns.
        date_col = None
        for candidate in ("date", "datetime", "timestamp", "time"):
            if candidate in cols_lower:
                date_col = cols_lower[candidate]
                break
        if date_col is None:
            raw = raw.reset_index()
            date_col = raw.columns[0]

        available = [t for t in tickers if t in raw.columns]
        if len(available) == 0:
            raise ValueError(
                "Could not parse prices as long format and no BTC/SOL columns found in wide format."
            )

        df = raw[[date_col] + available].rename(columns={date_col: "date"})
        df = df.melt(id_vars=["date"], var_name="ticker", value_name="close")

    df["date"] = _normalize_date_utc(df["date"])
    df["ticker"] = df["ticker"].astype(str).str.upper()
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df[df["ticker"].isin(list(tickers))].dropna(subset=["date", "close"]).copy()
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

    if df.empty:
        raise ValueError("No BTC/SOL price rows after parsing and filtering.")

    return df


def load_prices_with_fallback(
    primary_path: Path, tickers: Iterable[str], reference_dates: pd.Series | None = None
) -> tuple[pd.DataFrame, Path]:
    candidates = [
        primary_path,
        Path("sol_eth_bnb_prices.csv"),
        Path("data/curated/prices_daily.parquet"),
        Path("data/curated/data_lake/fact_price.parquet"),
    ]
    seen = set()
    for path in candidates:
        key = str(path.resolve()) if path.exists() else str(path)
        if key in seen or not path.exists():
            continue
        seen.add(key)
        try:
            df = load_prices(path, tickers)
            available = set(df["ticker"].unique())
            if not set(tickers).issubset(available):
                continue

            # Guard against malformed wide-format date parsing.
            if df["date"].min() < pd.Timestamp("2010-01-01", tz="UTC"):
                continue

            if reference_dates is not None:
                overlap = df["date"].isin(reference_dates).sum()
                if overlap == 0:
                    continue

                return df, path
        except Exception:
            continue

    raise ValueError(
        "Unable to load prices with both BTC and SOL from provided/fallback paths."
    )


def load_macro(macro_path: Path) -> pd.DataFrame:
    if not macro_path.exists():
        raise FileNotFoundError(f"Macro file not found: {macro_path}")

    df = pd.read_csv(macro_path)
    if "decision_date" not in df.columns or "Environment_APR" not in df.columns:
        raise ValueError("Macro CSV must contain decision_date and Environment_APR columns.")

    df["decision_date"] = pd.to_datetime(df["decision_date"], utc=True, errors="coerce")
    df = df.dropna(subset=["decision_date", "Environment_APR"]).copy()
    df["Environment_APR"] = pd.to_numeric(df["Environment_APR"], errors="coerce")
    df = df.dropna(subset=["Environment_APR"]).copy()

    # Strict temporal alignment gate: keep exactly midnight UTC observations.
    df = df[
        (df["decision_date"].dt.hour == 0)
        & (df["decision_date"].dt.minute == 0)
        & (df["decision_date"].dt.second == 0)
    ].copy()
    df["decision_date"] = df["decision_date"].dt.floor("D")
    return df


def map_regime(environment_apr: pd.Series) -> pd.Categorical:
    conds = [
        environment_apr < 2.0,
        (environment_apr >= 2.0) & (environment_apr < 5.0),
        (environment_apr >= 5.0) & (environment_apr <= 15.0),
        environment_apr > 15.0,
    ]
    labels = REGIME_ORDER
    mapped = np.select(conds, labels, default="Unknown")
    return pd.Categorical(mapped, categories=REGIME_ORDER, ordered=True)


def add_forward_returns(price_df: pd.DataFrame) -> pd.DataFrame:
    out = price_df.copy()
    out["fwd_ret_1d"] = out.groupby("ticker")["close"].transform(lambda s: s.pct_change(1).shift(-1))
    out["fwd_ret_3d"] = out.groupby("ticker")["close"].transform(lambda s: s.pct_change(3).shift(-3))
    out["fwd_ret_7d"] = out.groupby("ticker")["close"].transform(lambda s: s.pct_change(7).shift(-7))
    out = out.dropna(subset=["fwd_ret_1d", "fwd_ret_3d", "fwd_ret_7d"]).copy()
    return out


def build_plot_df(price_df: pd.DataFrame, macro_df: pd.DataFrame) -> pd.DataFrame:
    merged = price_df.merge(
        macro_df[["decision_date", "Environment_APR"]].copy(),
        left_on="date",
        right_on="decision_date",
        how="inner",
        validate="many_to_one",
    )

    merged["Regime"] = map_regime(merged["Environment_APR"])
    merged = merged[merged["Regime"].isin(REGIME_ORDER)].copy()
    return merged


def save_visual(df: pd.DataFrame, output_path: Path) -> None:
    sns.set_theme(style="whitegrid")
    fig, axes = plt.subplots(2, 2, figsize=(16, 10), sharex=False, sharey=False)

    palette = {
        "Cold Flush": "#1f77b4",
        "Recovery Ramp": "#2ca02c",
        "Golden Pocket": "#ffbf00",
        "Leverage Exhaustion": "#d62728",
    }

    for row_idx, ticker in enumerate(TARGET_TICKERS):
        sub = df[df["ticker"] == ticker].copy()

        sns.boxplot(
            data=sub,
            x="Regime",
            y="fwd_ret_7d",
            hue="Regime",
            order=REGIME_ORDER,
            ax=axes[row_idx, 0],
            palette=palette,
            dodge=False,
            legend=False,
        )
        axes[row_idx, 0].axhline(0.0, color="black", linewidth=0.8, alpha=0.8)
        axes[row_idx, 0].set_title(f"{ticker}: 7D Forward Return by Regime")
        axes[row_idx, 0].set_xlabel("Regime")
        axes[row_idx, 0].set_ylabel("7D Forward Return")
        axes[row_idx, 0].tick_params(axis="x", rotation=20)

        sns.scatterplot(
            data=sub,
            x="Environment_APR",
            y="fwd_ret_7d",
            hue="Regime",
            hue_order=REGIME_ORDER,
            palette=palette,
            alpha=0.8,
            s=50,
            ax=axes[row_idx, 1],
        )
        axes[row_idx, 1].axhline(0.0, color="black", linewidth=0.8, alpha=0.8)
        axes[row_idx, 1].set_title(f"{ticker}: Environment_APR vs 7D Forward Return")
        axes[row_idx, 1].set_xlabel("Environment_APR (%)")
        axes[row_idx, 1].set_ylabel("7D Forward Return")
        handles, labels = axes[row_idx, 1].get_legend_handles_labels()
        if handles:
            axes[row_idx, 1].legend(title="Regime", loc="best")

    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def summary_table(df: pd.DataFrame) -> pd.DataFrame:
    agg = (
        df.groupby(["ticker", "Regime"], observed=True)["fwd_ret_7d"]
        .agg(
            median_7d_forward_return="median",
            win_rate=lambda s: (s > 0).mean(),
            sample_size="count",
        )
        .reset_index()
    )
    agg["win_rate"] = agg["win_rate"] * 100.0
    return agg


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Regime-conditional forward return audit for BTC and SOL."
    )
    parser.add_argument(
        "--prices-path",
        type=Path,
        default=Path("sol_eth_bnb_prices.csv"),
        help="Price dataset path (CSV or Parquet).",
    )
    parser.add_argument(
        "--macro-path",
        type=Path,
        default=_latest_macro_default(),
        help="Path to master_macro_features.csv.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("validation_output/regime_fwd_returns.png"),
        help="Output path for 2x2 regime forward return figure.",
    )
    args = parser.parse_args()

    macro_df = load_macro(args.macro_path)
    price_df, used_price_path = load_prices_with_fallback(
        args.prices_path, TARGET_TICKERS, reference_dates=macro_df["decision_date"]
    )
    price_df = add_forward_returns(price_df)
    plot_df = build_plot_df(price_df, macro_df)

    if plot_df.empty:
        raise ValueError(
            "No aligned rows after merge. Check date coverage and whether macro decision_date matches price date."
        )

    save_visual(plot_df, args.output)
    summary = summary_table(plot_df)

    pd.set_option("display.width", 140)
    pd.set_option("display.max_columns", None)
    print(f"Saved visual artifact: {args.output}")
    print(f"Price source used: {used_price_path}")
    print(f"Macro source used: {args.macro_path}")
    print("\nMedian 7D forward return and win rate (%) by regime:")
    print(
        summary.sort_values(["ticker", "Regime"]).to_string(
            index=False,
            float_format=lambda x: f"{x:,.4f}",
        )
    )


if __name__ == "__main__":
    main()
