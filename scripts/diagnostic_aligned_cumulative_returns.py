"""
Aligned cumulative return diagnostic:

- Reuses strict 7-day temporal alignment logic from diagnostic_true_correlation.py.
- Builds a clean panel of 81 weekly observations with:
    * y: Original L/S strategy weekly log return.
    * y_pure: Pure BTC minus cap-weighted Top 30 ALT weekly log spread (log).
    * btcdom_7d_ret: True 7-day BTCDOM log return over [decision_date, next_date].
- Converts each to cumulative percentage returns and visualizes:
    * Chart 1: Original L/S vs 7-day BTCDOM.
    * Chart 2: Pure-BTC/Cap-Weighted L/S vs 7-day BTCDOM.
"""

from pathlib import Path
from typing import List, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_LAKE = PROJECT_ROOT / "data" / "curated" / "data_lake"

SILVER_PRICE_PATH = DATA_LAKE / "silver_fact_price.parquet"
SILVER_MCAP_PATH = DATA_LAKE / "silver_fact_marketcap.parquet"
BTC_DOM_PATH = DATA_LAKE / "btcdom_reconstructed.csv"

MSM_TS_PATH = (
    PROJECT_ROOT
    / "reports"
    / "msm_funding_v0"
    / "silver_router_variance_shield"
    / "msm_timeseries.csv"
)


def load_silver_price() -> pd.DataFrame:
    df = pd.read_parquet(SILVER_PRICE_PATH, columns=["asset_id", "date", "close"])
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df = df.sort_values(["asset_id", "date"]).reset_index(drop=True)
    return df


def load_silver_mcap() -> pd.DataFrame:
    df = pd.read_parquet(SILVER_MCAP_PATH)
    rename_map = {}
    if "marketcap_usd" in df.columns:
        rename_map["marketcap_usd"] = "marketcap"
    elif "marketcap" in df.columns:
        rename_map["marketcap"] = "marketcap"
    elif "mcap_usd" in df.columns:
        rename_map["mcap_usd"] = "marketcap"
    elif "market_cap" in df.columns:
        rename_map["market_cap"] = "marketcap"
    if rename_map:
        df = df.rename(columns=rename_map)
    cols = ["asset_id", "date", "marketcap"]
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in silver_fact_marketcap.parquet: {missing}")
    df = df[cols].copy()
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df = df.sort_values(["asset_id", "date"]).reset_index(drop=True)
    return df


def rebuild_weekly_top30_capweighted(
    price_df: pd.DataFrame,
    mcap_df: pd.DataFrame,
    decision_dates: List[pd.Timestamp],
    next_dates: List[pd.Timestamp],
    top_n: int = 30,
) -> pd.DataFrame:
    price_df = price_df.sort_values(["asset_id", "date"]).reset_index(drop=True)
    mcap_df = mcap_df.sort_values(["asset_id", "date"]).reset_index(drop=True)

    price_by_asset = {
        aid: sub.sort_values("date").reset_index(drop=True)
        for aid, sub in price_df.groupby("asset_id")
    }
    mcap_by_asset = {
        aid: sub.sort_values("date").reset_index(drop=True)
        for aid, sub in mcap_df.groupby("asset_id")
    }

    def asof_close(asset_df: pd.DataFrame, asof: pd.Timestamp) -> float | None:
        dates = asset_df["date"].values
        idx = np.searchsorted(dates, asof.to_datetime64(), side="right") - 1
        if idx < 0:
            return None
        return float(asset_df["close"].iloc[idx])

    def asof_mcap(asset_df: pd.DataFrame, asof: pd.Timestamp) -> float | None:
        dates = asset_df["date"].values
        idx = np.searchsorted(dates, asof.to_datetime64(), side="right") - 1
        if idx < 0:
            return None
        return float(asset_df["marketcap"].iloc[idx])

    all_assets = sorted(set(price_df["asset_id"]) & set(mcap_df["asset_id"]))

    rows: List[dict] = []

    for d0, d1 in zip(decision_dates, next_dates):
        d0 = pd.to_datetime(d0).normalize()
        d1 = pd.to_datetime(d1).normalize()

        mcap_records: List[Tuple[str, float]] = []
        for aid in all_assets:
            if aid == "BTC":
                continue
            mdf = mcap_by_asset.get(aid)
            if mdf is None:
                continue
            m = asof_mcap(mdf, d0)
            if m is None or not np.isfinite(m) or m <= 0.0:
                continue
            mcap_records.append((aid, m))

        if not mcap_records:
            continue

        mcap_df_week = pd.DataFrame(mcap_records, columns=["asset_id", "mcap"])
        mcap_df_week = mcap_df_week.sort_values("mcap", ascending=False).head(top_n)

        alt_ids = list(mcap_df_week["asset_id"].values)
        total_mcap = float(mcap_df_week["mcap"].sum())
        if total_mcap <= 0.0:
            continue

        weights = {
            row["asset_id"]: row["mcap"] / total_mcap for _, row in mcap_df_week.iterrows()
        }

        alt_rets: List[float] = []
        alt_weights: List[float] = []
        for aid in alt_ids:
            pdf = price_by_asset.get(aid)
            if pdf is None:
                continue
            prev_close = asof_close(pdf, d0)
            curr_close = asof_close(pdf, d1)
            if prev_close is None or curr_close is None or prev_close <= 0.0:
                continue
            r = (curr_close / prev_close) - 1.0
            if not np.isfinite(r):
                continue
            alt_rets.append(r)
            alt_weights.append(weights[aid])

        if not alt_rets or not alt_weights:
            continue

        w = np.array(alt_weights, dtype=float)
        w = w / w.sum()
        r = np.array(alt_rets, dtype=float)
        r_alts_cap = float(np.dot(w, r))

        rows.append(
            {
                "decision_date": d0,
                "next_date": d1,
                "R_alts_cap": r_alts_cap,
            }
        )

    return pd.DataFrame(rows).sort_values("decision_date").reset_index(drop=True)


def build_pure_btc_vs_capweighted(
    decision_dates: List[pd.Timestamp],
    next_dates: List[pd.Timestamp],
) -> pd.DataFrame:
    price_df = load_silver_price()
    mcap_df = load_silver_mcap()

    cap_df = rebuild_weekly_top30_capweighted(
        price_df,
        mcap_df,
        decision_dates=decision_dates,
        next_dates=next_dates,
        top_n=30,
    )
    if cap_df.empty:
        raise RuntimeError("Cap-weighted ALT reconstruction produced empty dataframe.")

    btc_prices = price_df[price_df["asset_id"] == "BTC"].copy()
    if btc_prices.empty:
        raise RuntimeError("BTC not found in silver_fact_price.parquet.")
    btc_prices = btc_prices.sort_values("date").reset_index(drop=True)

    def asof_close_btc(asof: pd.Timestamp) -> float | None:
        dates = btc_prices["date"].values
        idx = np.searchsorted(dates, asof.to_datetime64(), side="right") - 1
        if idx < 0:
            return None
        return float(btc_prices["close"].iloc[idx])

    btc_rows: List[dict] = []
    for d0, d1 in zip(decision_dates, next_dates):
        d0 = pd.to_datetime(d0).normalize()
        d1 = pd.to_datetime(d1).normalize()
        prev_close = asof_close_btc(d0)
        curr_close = asof_close_btc(d1)
        if prev_close is None or curr_close is None or prev_close <= 0.0:
            continue
        r_btc = (curr_close / prev_close) - 1.0
        btc_rows.append({"decision_date": d0, "next_date": d1, "R_btc": r_btc})

    btc_df = pd.DataFrame(btc_rows)

    merged = (
        cap_df.merge(
            btc_df,
            on=["decision_date", "next_date"],
            how="inner",
        )
        .sort_values("decision_date")
        .reset_index(drop=True)
    )

    merged["Y_pure"] = merged["R_btc"] - merged["R_alts_cap"]
    merged["y_pure"] = np.log1p(merged["Y_pure"])

    return merged[["decision_date", "next_date", "y_pure"]].copy()


def build_strictly_aligned_panel() -> pd.DataFrame:
    """
    Reproduce the strict alignment from diagnostic_true_correlation.py:
    - Exact BTCDOM prices on decision_date and next_date.
    - 7-day BTCDOM log return.
    - Original y and y_pure, merged on the same week grid.
    - No NaNs in any of the three series.
    """
    msm = pd.read_csv(
        MSM_TS_PATH,
        parse_dates=["decision_date", "next_date"],
    )
    msm["decision_date"] = msm["decision_date"].dt.normalize()
    msm["next_date"] = msm["next_date"].dt.normalize()

    btcdom = pd.read_csv(
        BTC_DOM_PATH,
        usecols=["date", "reconstructed_index_value"],
        parse_dates=["date"],
    )
    btcdom["date"] = btcdom["date"].dt.normalize()
    btcdom = btcdom.rename(columns={"reconstructed_index_value": "btcdom_price"})

    btc_start = btcdom.rename(
        columns={"date": "decision_date", "btcdom_price": "btcdom_price_start"}
    )
    btc_end = btcdom.rename(
        columns={"date": "next_date", "btcdom_price": "btcdom_price_end"}
    )

    df = msm.merge(btc_start, on="decision_date", how="left")
    df = df.merge(btc_end, on="next_date", how="left")

    df["btcdom_7d_ret"] = np.log(df["btcdom_price_end"] / df["btcdom_price_start"])

    # Build pure BTC vs cap-weighted ALTs over the MSM week grid
    pure_df = build_pure_btc_vs_capweighted(
        decision_dates=list(msm["decision_date"].values),
        next_dates=list(msm["next_date"].values),
    )

    df = df.merge(
        pure_df,
        on=["decision_date", "next_date"],
        how="inner",
    )

    # Strictly drop any rows with missing values in the three key series
    df = df.dropna(subset=["y", "y_pure", "btcdom_7d_ret"])
    df = df.sort_values("decision_date").reset_index(drop=True)

    return df[["decision_date", "y", "y_pure", "btcdom_7d_ret"]].copy()


def main() -> None:
    aligned = build_strictly_aligned_panel()

    # Cumulative log returns
    aligned["cum_y"] = aligned["y"].cumsum()
    aligned["cum_y_pure"] = aligned["y_pure"].cumsum()
    aligned["cum_btcdom"] = aligned["btcdom_7d_ret"].cumsum()

    # Convert to cumulative % returns (start at 0% by construction)
    aligned["cum_y_pct"] = (np.exp(aligned["cum_y"]) - 1.0) * 100.0
    aligned["cum_y_pure_pct"] = (np.exp(aligned["cum_y_pure"]) - 1.0) * 100.0
    aligned["cum_btcdom_pct"] = (np.exp(aligned["cum_btcdom"]) - 1.0) * 100.0

    # --- Chart 1: Original L/S vs 7-Day BTCDOM ---
    fig1, ax1 = plt.subplots(figsize=(10, 5))
    ax1.plot(
        aligned["decision_date"],
        aligned["cum_y_pct"],
        color="blue",
        linewidth=2,
        label="Original L/S Strategy",
    )
    ax1.plot(
        aligned["decision_date"],
        aligned["cum_btcdom_pct"],
        color="black",
        linestyle="--",
        linewidth=2,
        label="True 7-Day BTCDOM",
    )
    ax1.set_title("Chart 1: Cumulative Return (Original L/S vs True 7-Day BTCDOM)")
    ax1.set_ylabel("Cumulative Return (%)")
    ax1.set_xlabel("decision_date")
    ax1.grid(True, alpha=0.3)
    ax1.legend()
    plt.tight_layout()
    fig1.savefig(
        PROJECT_ROOT / "scripts" / "chart_aligned_1_original_vs_btcdom.png",
        dpi=150,
        bbox_inches="tight",
    )
    plt.close(fig1)

    # --- Chart 2: Pure-BTC/Cap-Weighted vs 7-Day BTCDOM ---
    fig2, ax2 = plt.subplots(figsize=(10, 5))
    ax2.plot(
        aligned["decision_date"],
        aligned["cum_y_pure_pct"],
        color="green",
        linewidth=2,
        label="Pure-BTC / Cap-Weighted L/S",
    )
    ax2.plot(
        aligned["decision_date"],
        aligned["cum_btcdom_pct"],
        color="black",
        linestyle="--",
        linewidth=2,
        label="True 7-Day BTCDOM",
    )
    ax2.set_title(
        "Chart 2: Cumulative Return (Pure-BTC/Cap-Weighted L/S vs True 7-Day BTCDOM)"
    )
    ax2.set_ylabel("Cumulative Return (%)")
    ax2.set_xlabel("decision_date")
    ax2.grid(True, alpha=0.3)
    ax2.legend()
    plt.tight_layout()
    fig2.savefig(
        PROJECT_ROOT / "scripts" / "chart_aligned_2_pure_vs_btcdom.png",
        dpi=150,
        bbox_inches="tight",
    )
    plt.close(fig2)

    # Terminal report: final cumulative percentage returns over the aligned window
    final_row = aligned.iloc[-1]
    final_btcdom = float(final_row["cum_btcdom_pct"])
    final_y = float(final_row["cum_y_pct"])
    final_y_pure = float(final_row["cum_y_pure_pct"])

    print(f"Final Return - True 7-Day BTCDOM: {final_btcdom:.4f}%")
    print(f"Final Return - Original L/S Strategy: {final_y:.4f}%")
    print(f"Final Return - Pure-BTC/Cap-Weighted L/S Strategy: {final_y_pure:.4f}%")


if __name__ == "__main__":
    main()

