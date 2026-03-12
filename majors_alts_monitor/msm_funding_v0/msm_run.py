"""MSM v0 - Funding-only module: Main CLI entrypoint."""

import argparse
import yaml
import polars as pl
import pandas as pd
import numpy as np
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import date, datetime, timedelta
import logging
import sys

try:
    from .msm_data import MSMDataLoader, data_sanity_check
    from .msm_universe import get_excluded_assets, select_top_n_alts, get_universe_hash
    from .msm_feature import compute_feature_for_week
    from .msm_label import compute_labels_v0_0, compute_labels_v0_1
    from .msm_returns import compute_returns_for_week
    from .msm_outputs import generate_outputs
except ImportError:
    # Fallback for direct script execution
    from msm_data import MSMDataLoader, data_sanity_check
    from msm_universe import get_excluded_assets, select_top_n_alts, get_universe_hash
    from msm_feature import compute_feature_for_week
    from msm_label import compute_labels_v0_0, compute_labels_v0_1
    from msm_returns import compute_returns_for_week
    from msm_outputs import generate_outputs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_weekly_decision_dates(
    start_date: date,
    end_date: date,
    anchor_day: str = "monday",
    anchor_hour: int = 0,
    anchor_minute: int = 0,
) -> List[date]:
    """
    Generate weekly decision dates anchored to Monday 00:00 UTC.
    
    Convention: Monday 00:00 UTC means decision at start of Monday UTC week.
    Decision dates are the start of each Monday UTC week.
    
    Args:
        start_date: Start date for date range
        end_date: End date for date range
        anchor_day: Day of week to anchor (default "monday")
        anchor_hour: Hour (UTC) for anchor (default 0)
        anchor_minute: Minute (UTC) for anchor (default 0)
    
    Returns:
        List of decision dates (Mondays)
    """
    # Map day name to weekday number (Monday = 0)
    day_map = {
        "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
        "friday": 4, "saturday": 5, "sunday": 6
    }
    target_weekday = day_map.get(anchor_day.lower(), 0)
    
    dates = []
    current = start_date
    
    # Find first Monday on or after start_date
    while current.weekday() != target_weekday:
        current += timedelta(days=1)
    
    # Generate weekly dates
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=7)
    
    return dates


def run_msm_v0(
    config_path: Path,
    start_date: date,
    end_date: date,
    run_id: Optional[str] = None,
    sanity_check_only: bool = False,
) -> None:
    """
    Run MSM v0 analysis over a date range.
    
    Args:
        config_path: Path to config YAML file
        start_date: Start date for analysis
        end_date: End date for analysis
        run_id: Optional run identifier (default: timestamp)
        sanity_check_only: If True, only run data sanity check
    """
    # Load config
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    
    # Generate run_id if not provided
    if run_id is None:
        run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    
    # Initialize data loader
    data_lake_dir = Path(config["data"]["data_lake_dir"])
    loader = MSMDataLoader(data_lake_dir)
    
    # Run sanity check
    logger.info("Running data sanity check...")
    sanity_results = data_sanity_check(
        data_lake_dir,
        top_n=config["universe"]["basket_size"],
        start_date=start_date,
        end_date=end_date,
    )
    
    if sanity_check_only:
        logger.info("Sanity check complete. Exiting.")
        return
    
    # Load datasets
    logger.info("Loading datasets...")
    datasets = loader.load_datasets(start=start_date, end=end_date)
    
    prices = datasets["prices"]
    marketcap = datasets["marketcap"]
    funding = datasets.get("funding", pl.DataFrame())
    dim_asset = datasets.get("dim_asset")
    
    # Get excluded assets
    exclude_categories = config["universe"].get("exclude_categories", [])
    excluded_assets = get_excluded_assets(dim_asset, exclude_categories)
    
    # Get decision dates
    decision_dates = get_weekly_decision_dates(
        start_date,
        end_date,
        anchor_day=config["decision"]["anchor_day"],
        anchor_hour=config["decision"]["anchor_hour"],
        anchor_minute=config["decision"]["anchor_minute"],
    )
    
    logger.info(f"Found {len(decision_dates)} decision dates from {start_date} to {end_date}")
    
    # Process each week
    timeseries_data = []
    feature_values = []  # For v0.0 labeling
    
    # Track rejection reasons for debugging
    rejection_counts = {
        "skipped_no_universe": 0,
        "skipped_funding_coverage": 0,
        "skipped_price_coverage": 0,
        "skipped_returns_computation": 0,
    }
    
    for i, decision_date in enumerate(decision_dates):
        # Get next decision date
        if i + 1 < len(decision_dates):
            next_date = decision_dates[i + 1]
        else:
            # Last week: use end_date or decision_date + 7 days
            next_date = min(end_date, decision_date + timedelta(days=7))
        
        # Select top N ALTs
        top_alts = select_top_n_alts(
            marketcap,
            decision_date,
            n=config["universe"]["basket_size"],
            min_mcap_usd=config["universe"]["min_mcap_usd"],
            excluded_assets=excluded_assets,
        )
        
        if len(top_alts) == 0:
            logger.warning(f"No ALTs selected for {decision_date} - skipping")
            rejection_counts["skipped_no_universe"] += 1
            continue
        
        asset_ids = top_alts["asset_id"].to_list()
        basket_hash = get_universe_hash(asset_ids)
        
        # Compute feature
        feature_result = compute_feature_for_week(
            funding,
            asset_ids,
            decision_date,
            lookback_days=config["feature"]["lookback_days"],
            min_coverage_pct=config["feature"]["min_coverage_pct"],
        )
        
        feature_value, coverage_pct, n_valid = feature_result
        
        # Coverage check: skip if coverage < 60%
        if feature_value is None:
            logger.info(f"Week {decision_date}: Funding coverage {coverage_pct:.1f}% < {config['feature']['min_coverage_pct']}% - skipping")
            rejection_counts["skipped_funding_coverage"] += 1
            continue
        
        feature_values.append(feature_value)
        
        # Compute returns (with asof pricing)
        returns, rejection_reason = compute_returns_for_week(
            prices,
            marketcap,
            asset_ids,
            config["universe"]["majors"],
            config["universe"]["majors_weights"],
            decision_date,
            next_date,
            min_price_coverage_pct=config["feature"]["min_price_coverage_pct"],
        )
        
        if returns is None:
            if rejection_reason:
                rejection_counts[rejection_reason] = rejection_counts.get(rejection_reason, 0) + 1
                logger.warning(f"Week {decision_date}: {rejection_reason} - skipping")
            else:
                rejection_counts["skipped_returns_computation"] += 1
                logger.warning(f"Week {decision_date}: Could not compute returns - skipping")
            continue
        
        r_alts, r_majors_dict, r_maj_weighted, y = returns
        
        # Extract individual major returns for output
        # Store as dynamic columns based on config majors
        major_returns_dict = {}
        for i, major_id in enumerate(config["universe"]["majors"]):
            major_returns_dict[f"r_{major_id.lower()}"] = r_majors_dict.get(major_id, float('nan'))
        
        # Also keep r_btc/r_eth for backward compatibility if they exist in config
        r_btc = r_majors_dict.get("BTC", float('nan'))
        r_eth = r_majors_dict.get("ETH", float('nan'))
        
        # Store data (labels will be computed later)
        row_data = {
            "decision_date": decision_date,
            "next_date": next_date,
            "basket_hash": basket_hash,
            "basket_members": ",".join(asset_ids),
            "n_valid": n_valid,  # Number of assets with valid funding
            "coverage": coverage_pct,  # Coverage percentage
            "F_tk": feature_value,
            "label_v0_0": None,  # Will be filled later
            "label_v0_1": None,  # Will be filled later
            "p_v0_0": None,  # Percentile rank v0.0 (will be filled later)
            "p_v0_1": None,  # Percentile rank v0.1 (will be filled later)
            "r_alts": r_alts,
            "r_maj_weighted": r_maj_weighted,  # Weighted major return
            "y": y,
        }
        
        # Add individual major returns (dynamic based on config)
        row_data.update(major_returns_dict)
        
        # Add backward-compatible BTC/ETH columns if they exist
        if "BTC" in config["universe"]["majors"]:
            row_data["r_btc"] = r_btc
        if "ETH" in config["universe"]["majors"]:
            row_data["r_eth"] = r_eth
        
        timeseries_data.append(row_data)
    
    logger.info(f"Processed {len(timeseries_data)} valid weeks out of {len(decision_dates)} decision dates")
    logger.info(f"Rejection summary: {rejection_counts}")
    
    # Compute labels
    if len(timeseries_data) > 0:
        # Extract feature values for labeling
        feature_list = [d["F_tk"] for d in timeseries_data]
        
        # v0.0: FULL_SAMPLE
        labels_v0_0, percentile_ranks_v0_0 = compute_labels_v0_0(
            feature_list,
            bin_ranges=config["label"]["bin_ranges"],
            bin_names=config["label"]["bin_names"],
        )
        
        # v0.1: ROLLING_PAST_52W
        labels_v0_1, percentile_ranks_v0_1 = compute_labels_v0_1(
            feature_list,
            window_weeks=config["label"]["v0_1"]["window_weeks"],
            bin_ranges=config["label"]["bin_ranges"],
            bin_names=config["label"]["bin_names"],
        )
        
        # Assign labels and percentile ranks
        for i, (label_0, label_1, p_0, p_1) in enumerate(
            zip(labels_v0_0, labels_v0_1, percentile_ranks_v0_0, percentile_ranks_v0_1)
        ):
            timeseries_data[i]["label_v0_0"] = label_0
            timeseries_data[i]["label_v0_1"] = label_1
            timeseries_data[i]["p_v0_0"] = p_0
            timeseries_data[i]["p_v0_1"] = p_1
    
    # Generate outputs
    output_dir = Path(config["outputs"]["reports_dir"]) / run_id
    output_dir.mkdir(parents=True, exist_ok=True)
    
    metadata = {
        "run_id": run_id,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "n_weeks": len(timeseries_data),
        "n_decision_dates": len(decision_dates),
        "rejection_counts": rejection_counts,
        "rejection_rate": {
            "total_decision_dates": len(decision_dates),
            "valid_weeks": len(timeseries_data),
            "rejection_rate_pct": ((len(decision_dates) - len(timeseries_data)) / len(decision_dates) * 100.0) if len(decision_dates) > 0 else 0.0,
        },
        "sanity_check": sanity_results,
    }
    
    generate_outputs(
        timeseries_data,
        config,
        metadata,
        output_dir,
    )
    
    logger.info(f"MSM v0 run complete. Outputs in: {output_dir}")

    # ------------------------------------------------------------------
    # Persist historical regime columns and gated return; then live terminal status
    # ------------------------------------------------------------------
    try:
        if len(timeseries_data) == 0:
            logger.info("No valid weeks; skipping regime persistence and live status.")
            return

        timeseries_csv_path = output_dir / "msm_timeseries.csv"
        if not timeseries_csv_path.exists():
            logger.warning(f"Timeseries CSV not found at {timeseries_csv_path}; skipping regime persistence.")
            return

        df = pd.read_csv(timeseries_csv_path, parse_dates=["decision_date", "next_date"])
        if df.empty or "F_tk" not in df.columns or "y" not in df.columns:
            logger.warning("Timeseries CSV empty or missing F_tk/y; skipping regime persistence.")
            return

        df = df.sort_values("decision_date").reset_index(drop=True)

        # Funding regime: 52-week rolling percentile of F_tk -> quartiles
        df["funding_pct_rank"] = df["F_tk"].rolling(window=52, min_periods=26).apply(
            lambda x: pd.Series(x).rank(pct=True).iloc[-1], raw=False
        )
        df["funding_regime"] = pd.cut(
            df["funding_pct_rank"],
            bins=[0.0, 0.25, 0.50, 0.75, 1.0],
            labels=["Q1: Negative/Low", "Q2: Weak", "Q3: Neutral", "Q4: High"],
            include_lowest=True,
        )

        # BTCDOM trend: 30d SMA of reconstructed BTCDOM, Rising/Falling
        data_lake_dir = Path(config["data"]["data_lake_dir"])
        btcdom_path = data_lake_dir / "btcdom_reconstructed.csv"
        if btcdom_path.exists():
            recon = pd.read_csv(btcdom_path, parse_dates=["date"]).sort_values("date")
            if "reconstructed_index_value" in recon.columns:
                recon["sma_30"] = recon["reconstructed_index_value"].rolling(window=30, min_periods=30).mean()
                trend_df = recon[["date", "reconstructed_index_value", "sma_30"]].rename(
                    columns={"date": "decision_date", "reconstructed_index_value": "btcd_index_decision"}
                )
                df = df.merge(trend_df, on="decision_date", how="left")
                df["BTCDOM_Trend"] = np.where(df["btcd_index_decision"] > df["sma_30"], "Rising", "Falling")
            else:
                logger.warning("btcdom_reconstructed.csv missing column reconstructed_index_value; BTCDOM_Trend not set.")
        else:
            logger.warning(f"btcdom_reconstructed.csv not found at {btcdom_path}; BTCDOM_Trend not set.")

        # Gate status and gated return (y_filtered = y when gate ON, else 0; y_gated alias for PM)
        if "BTCDOM_Trend" in df.columns:
            gate = (df["funding_regime"] == "Q2: Weak") & (df["BTCDOM_Trend"] == "Rising")
        else:
            gate = pd.Series(False, index=df.index)
        df["is_mrf_active"] = gate.astype(bool)
        df["y_filtered"] = np.where(df["is_mrf_active"], df["y"], 0.0)
        df["y_gated"] = df["y_filtered"]

        # Overwrite msm_timeseries.csv with full historical track record (including regime columns)
        df.to_csv(timeseries_csv_path, index=False)
        logger.info(f"Persisted historical regimes and y_gated to {timeseries_csv_path}")

        # Live terminal status for latest week
        latest_df = df.dropna(subset=["funding_regime"])
        if latest_df.empty:
            return
        latest_row = latest_df.iloc[-1]
        latest_date = latest_row["decision_date"].date()
        funding_label = str(latest_row["funding_regime"])
        btcd_label = str(latest_row.get("BTCDOM_Trend", "Unknown")) if pd.notna(latest_row.get("BTCDOM_Trend")) else "Unknown"
        gate_on = bool(latest_row.get("is_mrf_active", False))
        gate_label = "ACTIVE - DEPLOY L/S BASKET" if gate_on else "INACTIVE - HOLD 100% CASH"
        status = (
            "\n=========================================\n"
            f"LIVE MARKET REGIME STATUS (As of {latest_date})\n"
            f"Funding Regime: {funding_label}\n"
            f"BTCDOM Trend: {btcd_label}\n"
            f"MRF GATE: {gate_label}\n"
            "=========================================\n"
        )
        logger.info(status)

    except Exception as e:
        logger.warning(f"Regime persistence or live status failed: {e}", exc_info=True)


def main():
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(
        description="MSM v0 - Funding-only module for majors vs alts monitoring"
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).parent / "msm_config.yaml",
        help="Path to config YAML file",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        required=True,
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        required=True,
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--run-id",
        type=str,
        default=None,
        help="Run identifier (default: timestamp)",
    )
    parser.add_argument(
        "--sanity-check-only",
        action="store_true",
        help="Only run data sanity check",
    )
    
    args = parser.parse_args()
    
    # Parse dates
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    
    # Run
    try:
        run_msm_v0(
            args.config,
            start_date,
            end_date,
            run_id=args.run_id,
            sanity_check_only=args.sanity_check_only,
        )
    except Exception as e:
        logger.error(f"Error running MSM v0: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
