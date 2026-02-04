"""CLI command: generate daily signals."""

import argparse
import yaml
import json
from pathlib import Path
from datetime import date, datetime
import logging

import polars as pl
from .data_io import ReadOnlyDataLoader
from .features import FeatureLibrary
from .beta_neutral import DualBetaNeutralLS
from .regime import RegimeModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Generate daily signals")
    parser.add_argument("--asof", type=str, required=True, help="As-of date (YYYY-MM-DD)")
    parser.add_argument("--config", type=str, default="majors_alts_monitor/config.yaml", help="Config file path")
    
    args = parser.parse_args()
    
    # Load config
    config_path = Path(args.config)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    # Parse date
    asof_date = date.fromisoformat(args.asof)
    
    logger.info(f"Generating signals as-of {asof_date}")
    
    # Initialize data loader (read-only)
    data_loader = ReadOnlyDataLoader(
        data_lake_dir=Path(config["data"]["data_lake_dir"]),
        duckdb_path=Path(config["data"]["duckdb_path"]) if config["data"].get("duckdb_path") else None,
        universe_snapshots_path=Path(config["data"]["universe_snapshots_path"]) if config["data"].get("universe_snapshots_path") else None,
    )
    
    try:
        # Load datasets (need some history for features)
        lookback_days = config["features"]["lookback_days"]
        start_date = date(asof_date.year, asof_date.month, asof_date.day)
        # Go back enough days for feature computation
        from datetime import timedelta
        start_date = start_date - timedelta(days=lookback_days + 60)
        
        datasets = data_loader.load_dataset(start=start_date, end=asof_date)
        
        prices = datasets.get("price")
        marketcap = datasets.get("marketcap")
        volume = datasets.get("volume")
        funding = datasets.get("funding")
        
        if prices is None or marketcap is None or volume is None:
            raise ValueError("Missing required datasets")
        
        # Get stablecoins for exclusion
        stablecoins = data_loader._get_stablecoins()
        exclude_assets = stablecoins + config["universe"].get("exclude_assets", [])
        
        # Compute features
        feature_lib = FeatureLibrary(
            burn_in_days=config["features"]["burn_in_days"],
            lookback_days=config["features"]["lookback_days"],
        )
        
        features = feature_lib.compute_features(
            prices, marketcap, volume, funding,
            majors=config["universe"]["majors"],
            exclude_assets=exclude_assets,
        )
        
        # Get latest features
        latest_features = features.filter(pl.col("date") == pl.date(asof_date.year, asof_date.month, asof_date.day))
        if len(latest_features) == 0:
            # Use most recent available
            latest_features = features.tail(1)
        
        # Compute regime
        regime_model = RegimeModel(
            mode=config["regime"]["mode"],
            default_weights=config["regime"]["composite"]["default_weights"],
        )
        
        regime_result = regime_model.compute_composite_score(latest_features)
        
        if len(regime_result) == 0:
            raise ValueError(f"No regime data for {asof_date}")
        
        regime_row = regime_result.row(0, named=True)
        
        # Build ALT basket
        beta_neutral = DualBetaNeutralLS(majors=config["universe"]["majors"])
        alt_basket = beta_neutral.build_alt_basket(
            prices, marketcap, volume, asof_date,
            basket_size=config["universe"]["basket_size"],
            min_mcap_usd=config["universe"]["min_mcap_usd"],
            min_volume_usd=config["universe"]["min_volume_usd"],
            per_name_cap=config["universe"]["per_name_cap"],
            exclude_assets=exclude_assets,
        )
        
        # Estimate betas
        alt_betas = {}
        for alt_id in alt_basket.keys():
            alt_betas[alt_id] = beta_neutral.estimate_betas(prices, alt_id, asof_date)
        
        # Size majors (simplified)
        major_weights = {"BTC": 0.5, "ETH": 0.5}  # Placeholder
        
        # Compute expected funding (if available)
        expected_funding = 0.0
        if funding is not None:
            funding_today = funding.filter(pl.col("date") == pl.date(asof_date.year, asof_date.month, asof_date.day))
            if len(funding_today) > 0:
                expected_funding = funding_today["funding_rate"].mean()
        
        # Generate signal
        signal = {
            "asof_date": asof_date.isoformat(),
            "regime": regime_row["regime"],
            "score": float(regime_row["score"]),
            "suggested_gross": sum(abs(w) for w in alt_basket.values()) + sum(abs(w) for w in major_weights.values()),
            "leg_weights": {
                "majors": major_weights,
                "alts": alt_basket,
            },
            "expected_funding_daily": float(expected_funding * 3.0) if config["costs"]["funding_8h_rate"] else float(expected_funding),
            "key_features": {
                "alt_breadth": float(latest_features["raw_alt_breadth_pct_up"][0]) if "raw_alt_breadth_pct_up" in latest_features.columns else None,
                "btc_dominance": float(latest_features["raw_btc_dominance"][0]) if "raw_btc_dominance" in latest_features.columns else None,
            },
        }
        
        # Write signal
        reports_dir = Path(config["outputs"]["reports_dir"])
        reports_dir.mkdir(parents=True, exist_ok=True)
        
        signal_path = reports_dir / "signals_today.json"
        with open(signal_path, "w") as f:
            json.dump(signal, f, indent=2)
        
        logger.info(f"Signal written to {signal_path}")
        print(json.dumps(signal, indent=2))
        
    finally:
        data_loader.close()


if __name__ == "__main__":
    main()
