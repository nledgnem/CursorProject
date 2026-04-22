"""
Outlier diagnostics: impact of cross-sectional pumps/dumps on L/S vs BTCDOM correlation.
Rebuilds weekly Top 30 ALT basket from silver_fact_price and recomputes mean/median/trimmed
L/S spreads, then compares correlation vs BTCDOM.
"""

from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd
from scipy import stats


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TIMESERIES_PATH = (
    PROJECT_ROOT
    / "reports"
    / "msm_funding_v0"
    / "silver_router_variance_shield"
    / "msm_timeseries.csv"
)
DATA_LAKE = PROJECT_ROOT / "data" / "curated" / "data_lake"
BTCDOM_PATH = DATA_LAKE / "btcdom_reconstructed.csv"
SILVER_PRICE_PATH = DATA_LAKE / "silver_fact_price.parquet"


def load_price_panel() -> pd.DataFrame:
    """Load silver_fact_price.parquet into a tidy pandas panel."""
    df = pd.read_parquet(SILVER_PRICE_PATH, columns=["asset_id", "date", "close"])
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    # Silver layer has already removed explicit NaNs and non-finite closes at build time.
    df = df.sort_values(["asset_id", "date"]).reset_index(drop=True)
    return df


def build_asset_index(price_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Group price panel by asset_id for fast asof lookups."""
    grouped: Dict[str, pd.DataFrame] = {}
    for asset_id, sub in price_df.groupby("asset_id"):
        grouped[asset_id] = sub.sort_values("date").reset_index(drop=True)
    return grouped


def get_asof_close(asset_df: pd.DataFrame, asof_date: pd.Timestamp) -> float | None:
    """Get last available close on or before asof_date for a single asset."""
    dates = asset_df["date"].values
    idx = np.searchsorted(dates, asof_date.to_datetime64(), side="right") - 1
    if idx < 0:
        return None
    return float(asset_df["close"].iloc[idx])


def compute_weekly_spreads(
    ts: pd.DataFrame,
    price_by_asset: Dict[str, pd.DataFrame],
    majors: List[str],
    majors_weights: List[float],
) -> pd.DataFrame:
    """
    Recompute weekly ALT and MAJ simple-return baskets three ways:
    - mean (original)
    - median
    - 10% trimmed mean
    Applies the same variance shield: if fewer than 20 valid ALT returns, force Y*=0.
    """
    rows = []

    for _, row in ts.iterrows():
        decision_date = pd.to_datetime(row["decision_date"]).normalize()
        next_date = pd.to_datetime(row["next_date"]).normalize()

        # ALT universe from engine output
        basket_members = str(row["basket_members"])
        if not basket_members:
            continue
        alt_ids = [a.strip() for a in basket_members.split(",") if a.strip()]

        alt_rets: List[float] = []
        for asset_id in alt_ids:
            asset_df = price_by_asset.get(asset_id)
            if asset_df is None:
                continue
            prev_close = get_asof_close(asset_df, decision_date)
            curr_close = get_asof_close(asset_df, next_date)
            if prev_close is None or curr_close is None or prev_close <= 0.0:
                continue
            r = (curr_close / prev_close) - 1.0
            alt_rets.append(r)

        n_valid_alts = len(alt_rets)

        # Majors basket: BTC/ETH benchmark using same asof logic
        maj_rets: List[float] = []
        for major in majors:
            asset_df = price_by_asset.get(major)
            if asset_df is None:
                maj_rets.append(np.nan)
                continue
            prev_close = get_asof_close(asset_df, decision_date)
            curr_close = get_asof_close(asset_df, next_date)
            if prev_close is None or curr_close is None or prev_close <= 0.0:
                maj_rets.append(np.nan)
                continue
            r_major = (curr_close / prev_close) - 1.0
            maj_rets.append(r_major)

        if len(maj_rets) != len(majors):
            # Should not happen but guard against mismatch
            continue

        r_maj = 0.0
        all_maj_valid = True
        for r_m, w in zip(maj_rets, majors_weights):
            if np.isnan(r_m):
                all_maj_valid = False
            else:
                r_maj += w * r_m
        if not all_maj_valid:
            r_maj = np.nan

        if n_valid_alts == 0 or np.isnan(r_maj):
            # No useful week here
            continue

        # Outlier-robust ALT basket estimators
        r_alts_mean = float(np.nanmean(alt_rets)) if n_valid_alts > 0 else np.nan
        r_alts_median = float(np.nanmedian(alt_rets)) if n_valid_alts > 0 else np.nan
        r_alts_trimmed = float(
            stats.trim_mean(alt_rets, proportiontocut=0.1)
        ) if n_valid_alts > 0 else np.nan

        # Variance shield: align with engine semantics (Y=0 when <20 ALTs)
        if n_valid_alts < 20:
            Y_mean = 0.0
            Y_median = 0.0
            Y_trimmed = 0.0
        else:
            Y_mean = r_maj - r_alts_mean
            Y_median = r_maj - r_alts_median
            Y_trimmed = r_maj - r_alts_trimmed

        y_mean = float(np.log1p(Y_mean)) if np.isfinite(Y_mean) else np.nan
        y_median = float(np.log1p(Y_median)) if np.isfinite(Y_median) else np.nan
        y_trimmed = float(np.log1p(Y_trimmed)) if np.isfinite(Y_trimmed) else np.nan

        rows.append(
            {
                "decision_date": decision_date,
                "next_date": next_date,
                "y_mean": y_mean,
                "y_median": y_median,
                "y_trimmed": y_trimmed,
            }
        )

    return pd.DataFrame(rows).sort_values("decision_date").reset_index(drop=True)


def main() -> None:
    # Load engine timeseries for decision dates, funding, and universe members
    ts = pd.read_csv(
        TIMESERIES_PATH,
        parse_dates=["decision_date", "next_date"],
    )

    # Keep only what we need
    ts = ts[["decision_date", "next_date", "basket_members"]].copy()

    # Load price panel and index by asset
    price_df = load_price_panel()
    price_by_asset = build_asset_index(price_df)

    # Engine-major leg parameters (fixed BTC/ETH benchmark)
    majors = ["BTC", "ETH"]
    majors_weights = [0.7, 0.3]

    spreads_df = compute_weekly_spreads(ts, price_by_asset, majors, majors_weights)

    if spreads_df.empty:
        print("No spreads computed; check silver_fact_price or universe membership.")
        return

    # Strict 7-day BTCDOM: price at decision_date and next_date, then btcdom_7d_ret = log(price_end/price_start)
    btcdom = pd.read_csv(
        BTCDOM_PATH,
        usecols=["date", "reconstructed_index_value"],
    )
    btcdom["date"] = pd.to_datetime(btcdom["date"]).dt.normalize()
    btcdom = btcdom.rename(columns={"reconstructed_index_value": "btcdom_price"})
    btc_start = btcdom.rename(columns={"date": "decision_date", "btcdom_price": "btcdom_price_start"})
    btc_end = btcdom.rename(columns={"date": "next_date", "btcdom_price": "btcdom_price_end"})
    df = spreads_df.merge(btc_start, on="decision_date", how="left").merge(btc_end, on="next_date", how="left")
    df["btcdom_7d_ret"] = np.log(df["btcdom_price_end"].astype(float) / df["btcdom_price_start"].astype(float))
    df = df.dropna(subset=["btcdom_7d_ret"]).drop(columns=["btcdom_price_start", "btcdom_price_end"], errors="ignore")
    df = df.sort_values("decision_date").reset_index(drop=True)

    # Last 2 years
    cutoff = pd.Timestamp.now().normalize() - pd.DateOffset(years=2)
    df_2y = df.loc[df["decision_date"] >= cutoff].copy()

    # Drop NaNs pairwise for correlations
    def corr_pair(a: pd.Series, b: pd.Series):
        mask = a.notna() & b.notna()
        if mask.sum() < 3:
            return np.nan, np.nan
        pearson_r, _ = stats.pearsonr(a[mask], b[mask])
        spearman_r, _ = stats.spearmanr(a[mask], b[mask])
        return pearson_r, spearman_r

    pearson_mean, spearman_mean = corr_pair(df_2y["y_mean"], df_2y["btcdom_7d_ret"])
    pearson_trim, spearman_trim = corr_pair(df_2y["y_trimmed"], df_2y["btcdom_7d_ret"])
    pearson_med, spearman_med = corr_pair(df_2y["y_median"], df_2y["btcdom_7d_ret"])

    # --- Chart 1: correlation bar chart (Pearson only) ---
    import matplotlib.pyplot as plt

    labels = ["Original Mean", "10% Trimmed Mean", "Robust Median"]
    pearsons = [pearson_mean, pearson_trim, pearson_med]

    fig1, ax1 = plt.subplots(figsize=(7, 5))
    colors = ["#c0392b", "#27ae60", "#2980b9"]
    bars = ax1.bar(labels, pearsons, color=colors)
    ax1.axhline(0, color="black", linewidth=1)
    ax1.set_ylabel("Pearson correlation vs 7-day BTCDOM")
    ax1.set_title("Impact of Outlier Removal on BTCDOM Correlation")
    for bar, val in zip(bars, pearsons):
        ax1.text(
            bar.get_x() + bar.get_width() / 2.0,
            val + (0.01 if val >= 0 else -0.01),
            f"{val:.3f}",
            ha="center",
            va="bottom" if val >= 0 else "top",
            fontsize=10,
        )
    plt.tight_layout()
    fig1.savefig(PROJECT_ROOT / "scripts" / "chart_outlier_1_bar.png", dpi=150, bbox_inches="tight")
    plt.close(fig1)

    # --- Chart 2: rolling correlation comparison (12-week Pearson) ---
    roll = 12
    roll_corr_mean = (
        df_2y["y_mean"].rolling(roll, min_periods=roll).corr(df_2y["btcdom_7d_ret"])
    )
    roll_corr_med = (
        df_2y["y_median"].rolling(roll, min_periods=roll).corr(df_2y["btcdom_7d_ret"])
    )

    fig2, ax2 = plt.subplots(figsize=(10, 5))
    ax2.plot(
        df_2y["decision_date"],
        roll_corr_mean,
        color="red",
        linestyle="--",
        linewidth=2,
        label="Original Mean",
    )
    ax2.plot(
        df_2y["decision_date"],
        roll_corr_med,
        color="blue",
        linestyle="-",
        linewidth=2,
        label="Robust Median",
    )
    ax2.axhline(0.0, color="black", linewidth=1)
    ax2.set_ylabel("12-week rolling Pearson correlation vs 7-day BTCDOM")
    ax2.set_xlabel("decision_date")
    ax2.set_title("12-Week Rolling Correlation: Original Mean vs. Robust Median")
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    plt.tight_layout()
    fig2.savefig(PROJECT_ROOT / "scripts" / "chart_outlier_2_rolling.png", dpi=150, bbox_inches="tight")
    plt.close(fig2)

    # --- Validation output (Pearson baseline + trimmed + median) ---
    print(f"Correlation baseline (Original Mean): {pearson_mean:.6f}")
    print(f"Correlation (10% Trimmed Mean): {pearson_trim:.6f}")
    print(f"Correlation (Robust Median): {pearson_med:.6f}")


if __name__ == "__main__":
    main()

