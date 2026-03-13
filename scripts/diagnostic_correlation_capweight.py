"""
Cap-weighted counterfactual diagnostic:
- Rebuild weekly Top 30 ALT universe from Silver price + market cap.
- Compute cap-weighted ALT weekly return R_alts_cap.
- Compute pure BTC leg return R_BTC over same weeks.
- Define y_pure = log(1 + (R_BTC - R_alts_cap)).
- Merge with BTC DOM weekly log returns on decision_date.
- Report Pearson correlations:
    * Original strategy vs BTCDOM (from msm_timeseries.csv, column y)
    * Theoretical cap-weighted y_pure vs BTCDOM.
"""

from pathlib import Path
from typing import List, Tuple

import numpy as np
import pandas as pd
from scipy import stats


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
    """Load silver_fact_price.parquet (asset_id, date, close)."""
    df = pd.read_parquet(SILVER_PRICE_PATH, columns=["asset_id", "date", "close"])
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df = df.sort_values(["asset_id", "date"]).reset_index(drop=True)
    return df


def load_silver_mcap() -> pd.DataFrame:
    """Load silver_fact_marketcap.parquet (asset_id, date, marketcap_usd)."""
    # Try common column names; fall back to generic.
    df = pd.read_parquet(SILVER_MCAP_PATH)
    # Standardize column names we need.
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
    """
    Rebuild weekly Top N ALT universe and compute cap-weighted weekly return.

    We:
    - For each decision_date, take the latest available market cap snapshot on/before decision_date.
    - Exclude BTC from the ALT set (pure alts).
    - Rank by market cap and take Top N.
    - For each asset and week, compute simple return from decision_date to next_date via asof pricing.
    - Cap-weight at decision_date market caps (fixed for that week).
    """
    # Helper: asof for price
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

        # Universe: assets with valid mcap asof d0, excluding BTC
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

        weights = {row["asset_id"]: row["mcap"] / total_mcap for _, row in mcap_df_week.iterrows()}

        # Compute asset-level simple returns
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

        # Normalize weights over valid names only
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


def build_pure_btc_vs_capweighted() -> pd.DataFrame:
    """Build dataframe with y_pure and btcdom_ret aligned on decision_date."""
    # Load silver price & mcap
    price_df = load_silver_price()
    mcap_df = load_silver_mcap()

    # Load engine timeseries for decision dates
    ts = pd.read_csv(
        MSM_TS_PATH,
        parse_dates=["decision_date", "next_date"],
    )
    ts["decision_date"] = ts["decision_date"].dt.normalize()
    ts["next_date"] = ts["next_date"].dt.normalize()

    # Rebuild cap-weighted ALT leg
    cap_df = rebuild_weekly_top30_capweighted(
        price_df,
        mcap_df,
        decision_dates=list(ts["decision_date"].values),
        next_dates=list(ts["next_date"].values),
        top_n=30,
    )

    if cap_df.empty:
        raise RuntimeError("Cap-weighted ALT reconstruction produced empty dataframe.")

    # Compute BTC weekly simple returns using same asof logic
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
    for d0, d1 in zip(ts["decision_date"].values, ts["next_date"].values):
        d0 = pd.to_datetime(d0).normalize()
        d1 = pd.to_datetime(d1).normalize()
        prev_close = asof_close_btc(d0)
        curr_close = asof_close_btc(d1)
        if prev_close is None or curr_close is None or prev_close <= 0.0:
            continue
        r_btc = (curr_close / prev_close) - 1.0
        btc_rows.append({"decision_date": d0, "next_date": d1, "R_btc": r_btc})

    btc_df = pd.DataFrame(btc_rows)

    # Merge cap-weighted alts and BTC leg
    merged = (
        cap_df.merge(
            btc_df,
            on=["decision_date", "next_date"],
            how="inner",
        )
        .sort_values("decision_date")
        .reset_index(drop=True)
    )

    # Pure BTC spread and log return
    merged["Y_pure"] = merged["R_btc"] - merged["R_alts_cap"]
    merged["y_pure"] = np.log1p(merged["Y_pure"])

    # Strict 7-day BTCDOM: look up price at decision_date and next_date, then btcdom_7d_ret = log(price_end/price_start)
    btcdom = pd.read_csv(
        BTC_DOM_PATH,
        usecols=["date", "reconstructed_index_value"],
    )
    btcdom["date"] = pd.to_datetime(btcdom["date"]).dt.normalize()
    btcdom = btcdom.rename(columns={"reconstructed_index_value": "btcdom_price"})
    btc_start = btcdom.rename(columns={"date": "decision_date", "btcdom_price": "btcdom_price_start"})
    btc_end = btcdom.rename(columns={"date": "next_date", "btcdom_price": "btcdom_price_end"})
    out = merged.merge(btc_start, on="decision_date", how="left")
    out = out.merge(btc_end, on="next_date", how="left")
    out["btcdom_7d_ret"] = np.log(out["btcdom_price_end"].astype(float) / out["btcdom_price_start"].astype(float))
    out = out.dropna(subset=["btcdom_7d_ret"]).drop(columns=["btcdom_price_start", "btcdom_price_end"], errors="ignore")

    return out.sort_values("decision_date").reset_index(drop=True)


def pearson_corr_pair(a: pd.Series, b: pd.Series) -> float:
    """Pairwise-drop NaNs and return Pearson r (or NaN if <3 points)."""
    mask = a.notna() & b.notna()
    if mask.sum() < 3:
        return float("nan")
    r, _ = stats.pearsonr(a[mask], b[mask])
    return float(r)


def main() -> None:
    # Step 1: Majors basket constituents (from config.yaml, already printed elsewhere)
    # We still log them here for completeness.
    import yaml

    with open(PROJECT_ROOT / "majors_alts_monitor" / "config.yaml", "r") as f:
        cfg = yaml.safe_load(f)
    majors = cfg["universe"]["majors"]
    print(f"Audit: Majors Basket Constituents: {majors}")

    # Step 2: Timestamp alignment check between MSM and BTCDOM
    msm = pd.read_csv(
        MSM_TS_PATH,
        parse_dates=["decision_date", "next_date"],
    )
    btcdom = pd.read_csv(
        BTC_DOM_PATH,
        usecols=["date", "reconstructed_index_value"],
        parse_dates=["date"],
    )
    msm["decision_date"] = msm["decision_date"].dt.normalize()
    msm["next_date"] = msm["next_date"].dt.normalize()
    btcdom["date"] = btcdom["date"].dt.normalize()

    joined = msm.merge(
        btcdom[["date"]],
        left_on="decision_date",
        right_on="date",
        how="inner",
    )
    sample_rows = joined[["decision_date", "next_date", "date"]].head(3)

    msm_dates = set(msm["decision_date"].unique())
    btcdom_dates = set(btcdom["date"].unique())
    missing = sorted(d for d in msm_dates if d not in btcdom_dates)
    extra = sorted(d for d in btcdom_dates if d not in msm_dates)
    alignment_pass = (not missing) and (not extra)

    status = "PASS" if alignment_pass else "FAIL"
    evidence = (
        f"Sample joined rows:\n{sample_rows.to_string(index=False)}\n"
        f"MSM-only dates (first 3): {missing[:3]}\n"
        f"BTCDOM-only dates (first 3): {extra[:3]}"
    )

    # Step 3: Original strategy correlation vs BTCDOM (strict 7-day alignment)
    btcdom = pd.read_csv(BTC_DOM_PATH, usecols=["date", "reconstructed_index_value"], parse_dates=["date"])
    btcdom["date"] = btcdom["date"].dt.normalize()
    btcdom = btcdom.rename(columns={"reconstructed_index_value": "btcdom_price"})
    btc_start = btcdom.rename(columns={"date": "decision_date", "btcdom_price": "btcdom_price_start"})
    btc_end = btcdom.rename(columns={"date": "next_date", "btcdom_price": "btcdom_price_end"})
    msm_corr_df = msm.merge(btc_start, on="decision_date", how="left").merge(btc_end, on="next_date", how="left")
    msm_corr_df["btcdom_7d_ret"] = np.log(msm_corr_df["btcdom_price_end"].astype(float) / msm_corr_df["btcdom_price_start"].astype(float))
    msm_corr_df = msm_corr_df.dropna(subset=["y", "btcdom_7d_ret"])
    corr_original = pearson_corr_pair(msm_corr_df["y"], msm_corr_df["btcdom_7d_ret"])

    # Step 4: Cap-weighted counterfactual and correlation vs BTCDOM (strict 7d)
    pure_df = build_pure_btc_vs_capweighted()
    corr_capweighted = pearson_corr_pair(pure_df["y_pure"], pure_df["btcdom_7d_ret"])

    # Step 5: Terminal diagnostic report
    print(
        f"Audit: Timestamp Alignment Check: {status} - "
        f"{'decision_date set matches exactly' if alignment_pass else 'see evidence below'}"
    )
    print(evidence)
    print(f"Correlation: Original Strategy vs BTCDOM: {corr_original:.6f}")
    print(
        "Correlation: Theoretical Cap-Weighted (Pure BTC vs Cap-Weighted Alts) "
        f"vs BTCDOM: {corr_capweighted:.6f}"
    )


if __name__ == "__main__":
    main()

