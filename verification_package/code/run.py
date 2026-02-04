"""Main CLI command: run backtest."""

import argparse
import yaml
from pathlib import Path
from datetime import date, datetime
import logging
import polars as pl

from .data_io import ReadOnlyDataLoader
from .features import FeatureLibrary
from .beta_neutral import DualBetaNeutralLS
from .regime import RegimeModel
from .backtest import BacktestEngine
from .outputs import OutputGenerator
from .experiment_manager import ExperimentManager, generate_run_id
from .regime_evaluation import evaluate_regime_edges, compute_target_returns, format_regime_evaluation_results

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Run majors vs alts regime monitor backtest")
    parser.add_argument("--start", type=str, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--config", type=str, default="majors_alts_monitor/config.yaml", help="Config file path")
    parser.add_argument("--experiment", type=str, help="Experiment YAML file path (overrides config)")
    
    args = parser.parse_args()
    
    # Load base config
    config_path = Path(args.config)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    # Load experiment spec if provided
    experiment_spec = None
    if args.experiment:
        experiment_path = Path(args.experiment)
        if not experiment_path.exists():
            raise FileNotFoundError(f"Experiment file not found: {experiment_path}")
        with open(experiment_path) as f:
            experiment_spec = yaml.safe_load(f)
        
        # Merge experiment spec into config (experiment overrides)
        # TODO: More sophisticated merging logic
        if "target" in experiment_spec:
            # Update universe config from experiment
            if "short_leg" in experiment_spec["target"]:
                config["universe"]["basket_size"] = experiment_spec["target"]["short_leg"].get("n", 20)
        if "state_mapping" in experiment_spec:
            # Update regime config from experiment
            if "n_regimes" in experiment_spec["state_mapping"]:
                config["regime"]["n_regimes"] = experiment_spec["state_mapping"]["n_regimes"]
            if "thresholds" in experiment_spec["state_mapping"]:
                thresholds = experiment_spec["state_mapping"]["thresholds"]
                config["regime"]["composite"]["threshold_low"] = thresholds.get("low", -0.5)
                config["regime"]["composite"]["threshold_high"] = thresholds.get("high", 0.5)
                config["regime"]["composite"]["threshold_strong_low"] = thresholds.get("strong_low", -1.5)
                config["regime"]["composite"]["threshold_strong_high"] = thresholds.get("strong_high", 1.5)
        if "backtest" in experiment_spec:
            # Update backtest config from experiment
            for key, value in experiment_spec["backtest"].items():
                if key in config["backtest"]:
                    if isinstance(value, dict) and isinstance(config["backtest"][key], dict):
                        config["backtest"][key].update(value)
                    else:
                        config["backtest"][key] = value
    
    # Parse dates
    start_date = date.fromisoformat(args.start)
    end_date = date.fromisoformat(args.end)
    
    logger.info(f"Running backtest: {start_date} to {end_date}")
    
    # Initialize data loader (read-only)
    data_loader = ReadOnlyDataLoader(
        data_lake_dir=Path(config["data"]["data_lake_dir"]),
        duckdb_path=Path(config["data"]["duckdb_path"]) if config["data"].get("duckdb_path") else None,
        universe_snapshots_path=Path(config["data"]["universe_snapshots_path"]) if config["data"].get("universe_snapshots_path") else None,
    )
    
    try:
        # Load datasets
        datasets = data_loader.load_dataset(start=start_date, end=end_date)
        
        prices = datasets.get("price")
        marketcap = datasets.get("marketcap")
        volume = datasets.get("volume")
        funding = datasets.get("funding")
        open_interest = datasets.get("open_interest")
        universe_snapshots = datasets.get("universe_snapshots")
        
        if prices is None or marketcap is None or volume is None:
            raise ValueError("Missing required datasets (prices, marketcap, volume)")
        
        # Get stablecoins for exclusion
        stablecoins = data_loader._get_stablecoins()
        exclude_assets = stablecoins + config["universe"].get("exclude_assets", [])
        
        # Initialize components
        feature_lib = FeatureLibrary(
            burn_in_days=config["features"]["burn_in_days"],
            lookback_days=config["features"]["lookback_days"],
        )
        
        beta_neutral = DualBetaNeutralLS(
            majors=config["universe"]["majors"],
            lookback_days=config["beta"]["lookback_days"],
            ridge_alpha=config["beta"]["ridge_alpha"],
            winsorize_pct=config["beta"]["winsorize_pct"],
            beta_clamp=tuple(config["beta"]["beta_clamp"]),
            default_beta=config["beta"]["default_beta"],
        )
        
        # Compute features
        logger.info("Computing features...")
        features = feature_lib.compute_features(
            prices, marketcap, volume, funding,
            open_interest=open_interest,
            majors=config["universe"]["majors"],
            exclude_assets=exclude_assets,
        )
        
        # Regime modeling
        logger.info("Computing regime series...")
        n_regimes = config["regime"].get("n_regimes", 3)
        regime_model = RegimeModel(
            mode=config["regime"]["mode"],
            default_weights=config["regime"]["composite"]["default_weights"],
            threshold_low=config["regime"]["composite"].get("threshold_low", -0.5),
            threshold_high=config["regime"]["composite"].get("threshold_high", 0.5),
            threshold_strong_low=config["regime"]["composite"].get("threshold_strong_low", -1.5),
            threshold_strong_high=config["regime"]["composite"].get("threshold_strong_high", 1.5),
            hysteresis_low=config["regime"]["composite"]["hysteresis"]["low_band"],
            hysteresis_high=config["regime"]["composite"]["hysteresis"]["high_band"],
            n_regimes=n_regimes,
        )
        
        # Walk-forward grid search if enabled
        # Note: Grid search requires LS returns which we don't have yet
        # It will be run during backtest evaluation phase
        if config["regime"]["composite"]["grid_search"]["enabled"]:
            logger.info("Grid search will be performed during backtest evaluation")
        
        # Compute regime series (pass prices for high-vol gate)
        regime_series = regime_model.compute_composite_score(features, prices=prices)
        
        # Backtest
        logger.info("Running backtest...")
        backtest_engine = BacktestEngine(
            maker_fee_bps=config["costs"]["maker_fee_bps"],
            taker_fee_bps=config["costs"]["taker_fee_bps"],
            slippage_bps=config["costs"]["slippage_bps"],
            slippage_adv_multiplier=config["costs"]["slippage_adv_multiplier"],
            funding_enabled=config["costs"]["funding_enabled"],
            funding_8h_rate=config["costs"]["funding_8h_rate"],
            vol_target=config["universe"].get("vol_target"),
            regime_position_scaling=config["backtest"].get("regime_position_scaling", {}),
            risk_management=config["backtest"].get("risk_management", {}),
        )
        
        # Detect MSM mode from experiment spec
        is_msm_mode = False
        msm_config = None
        if experiment_spec and experiment_spec.get("category_path") == "msm":
            is_msm_mode = True
            msm_config = experiment_spec.get("target", {})
            logger.info("MSM mode detected: using market cap-based basket selection")
        
        # Build ALT basket function (MSM or regular)
        if is_msm_mode:
            # Pure MSM: market cap-based, no enhanced filters
            short_leg_config = msm_config.get("short_leg", {})
            def build_alt_basket(asof_date):
                return beta_neutral.build_msm_basket(
                    prices, marketcap, volume, asof_date,
                    n=short_leg_config.get("n", 20),
                    min_mcap_usd=config["universe"]["min_mcap_usd"],
                    min_volume_usd=short_leg_config.get("min_volume_usd", 1_000),  # Light liquidity check
                    exclude_assets=exclude_assets,
                    weighting=short_leg_config.get("weighting", "equal"),
                )
        else:
            # Regular mode: volume-based with optional enhanced filters
            alt_selection_config = config["universe"].get("alt_selection", {})
            # Enable enhanced selection if any filter is configured
            if alt_selection_config:
                alt_selection_config["enabled"] = (
                    alt_selection_config.get("max_volatility") is not None or
                    alt_selection_config.get("min_correlation") is not None or
                    alt_selection_config.get("max_momentum") is not None or
                    alt_selection_config.get("min_momentum") is not None or
                    alt_selection_config.get("weight_by_inverse_vol", False)
                )
            
            def build_alt_basket(asof_date):
                return beta_neutral.build_alt_basket(
                    prices, marketcap, volume, asof_date,
                    basket_size=config["universe"]["basket_size"],
                    min_mcap_usd=config["universe"]["min_mcap_usd"],
                    min_volume_usd=config["universe"]["min_volume_usd"],
                    per_name_cap=config["universe"]["per_name_cap"],
                    exclude_assets=exclude_assets,
                    alt_selection_config=alt_selection_config if alt_selection_config.get("enabled", False) else None,
                )
        
        # Estimate beta function
        def estimate_beta(asset_id, asof_date):
            return beta_neutral.estimate_betas(
                prices, asset_id, asof_date,
                tracker_betas=None,  # Could load from data lake if available
            )
        
        # Create neutrality solver function
        # Returns combined dict with both ALT and major weights
        if is_msm_mode:
            # MSM mode: use fixed major weights (BTC-only or BTC+ETH fixed)
            long_leg_config = msm_config.get("long_leg", {})
            major_weights_fixed = long_leg_config.get("weights", {"BTC": 1.0})
            
            def solve_neutrality(alt_weights_new, alt_betas_new):
                # MSM: use fixed major weights, scale alts to 50% gross
                alt_total = sum(abs(w) for w in alt_weights_new.values())
                if alt_total == 0:
                    return {}
                
                # Scale alts to 50% gross (short)
                alt_scale = 0.5 / alt_total if alt_total > 0 else 1.0
                scaled_alt_weights = {k: -abs(v) * alt_scale for k, v in alt_weights_new.items()}
                
                # Use fixed major weights (normalized to 50% gross, long)
                major_total = sum(abs(w) for w in major_weights_fixed.values())
                major_scale = 0.5 / major_total if major_total > 0 else 1.0
                scaled_major_weights = {k: v * major_scale for k, v in major_weights_fixed.items()}
                
                return {**scaled_alt_weights, **scaled_major_weights}
        else:
            # Regular mode: beta-neutral or dollar-neutral
            neutrality_mode = config["universe"].get("neutrality_mode", "dollar_neutral")
            def solve_neutrality(alt_weights_new, alt_betas_new):
                return beta_neutral.solve_neutrality(
                    alt_weights_new, alt_betas_new,
                    major_weights={"BTC": 0.0, "ETH": 0.0},
                    gross_cap=config["universe"]["gross_cap"],
                    neutrality_mode=neutrality_mode,
                )
        
        # Helper to separate ALT and major weights from combined dict
        def separate_weights(combined_weights, majors_list=["BTC", "ETH"]):
            alt_weights = {k: v for k, v in combined_weights.items() if k not in majors_list}
            major_weights = {k: v for k, v in combined_weights.items() if k in majors_list}
            return alt_weights, major_weights
        
        # Run backtest
        backtest_results = backtest_engine.run_backtest(
            prices, marketcap, volume, funding,
            features, regime_series,
            build_alt_basket, estimate_beta, solve_neutrality,
            start_date, end_date,
            walk_forward=config["backtest"]["walk_forward"],
            lookback_window_days=config["backtest"]["lookback_window_days"],
            test_window_days=config["backtest"]["test_window_days"],
        )
        
        # Initialize experiment manager if experiment spec provided
        experiment_manager = None
        run_id = None
        if experiment_spec:
            experiment_manager = ExperimentManager()
            run_id = generate_run_id(experiment_spec.get("experiment_id", "experiment"))
            
            # Get data snapshot dates
            data_snapshot_dates = {
                "price": datasets.get("price", pl.DataFrame()).select("date").max().item() if "price" in datasets and len(datasets["price"]) > 0 else None,
                "funding": datasets.get("funding", pl.DataFrame()).select("date").max().item() if "funding" in datasets and len(datasets.get("funding", pl.DataFrame())) > 0 else None,
                "open_interest": datasets.get("open_interest", pl.DataFrame()).select("date").max().item() if "open_interest" in datasets and len(datasets.get("open_interest", pl.DataFrame())) > 0 else None,
            }
            
            # Write manifest
            experiment_manager.write_manifest(
                run_id=run_id,
                experiment_spec=experiment_spec,
                resolved_config=config,
                data_snapshot_dates=data_snapshot_dates,
            )
        
        # Generate outputs
        logger.info("Generating outputs...")
        output_gen = OutputGenerator(
            reports_dir=Path(config["outputs"]["reports_dir"]),
            artifacts_dir=Path(config["outputs"]["artifacts_dir"]),
            experiment_manager=experiment_manager,
        )
        
        output_gen.generate_outputs(
            regime_series, features, backtest_results,
            start_date, end_date,
        )
        
        # Compute KPIs and stability metrics
        kpis = output_gen._compute_kpis(backtest_results)
        stability_metrics = output_gen.compute_stability_metrics(regime_series)
        
        # Compute regime-conditional forward returns for MSM (auto) or if requested
        regime_evaluation_results = None
        if is_msm_mode or (experiment_spec and experiment_spec.get("backtest", {}).get("with_regime_eval", False)):
            logger.info("Computing regime-conditional forward returns...")
            
            # Use backtest returns as target (r_ls_net represents the strategy returns)
            # For MSM, we evaluate: mean(r_ls_net | regime)
            if len(backtest_results) > 0 and "r_ls_net" in backtest_results.columns and "date" in backtest_results.columns:
                # Use backtest returns as target
                target_returns = backtest_results.select(["date", pl.col("r_ls_net").alias("return")]).drop_nulls()
                
                if len(target_returns) > 0 and len(regime_series) > 0:
                    # Evaluate regime edges
                    horizons = experiment_spec.get("target", {}).get("horizon_days", [1, 5, 10, 20]) if experiment_spec else [1, 5, 10, 20]
                    try:
                        regime_evaluation_results = evaluate_regime_edges(
                            target_returns=target_returns,
                            regime_series=regime_series,
                            horizons_days=horizons,
                        )
                    except Exception as e:
                        logger.warning(f"Failed to compute regime evaluation: {e}", exc_info=True)
                        regime_evaluation_results = None
                else:
                    logger.warning("Insufficient data for regime evaluation")
            else:
                logger.warning("Backtest results missing required columns for regime evaluation")
                
                # Print formatted results
                if regime_evaluation_results:
                    print("\n" + format_regime_evaluation_results(regime_evaluation_results))
                    
                    # Save to file if experiment manager active
                    if experiment_manager and run_id:
                        eval_path = experiment_manager.create_run_directory(run_id) / "regime_evaluation.json"
                        import json
                        with open(eval_path, "w") as f:
                            json.dump(regime_evaluation_results, f, indent=2, default=str)
                        logger.info(f"Written regime evaluation: {eval_path}")
        
        # Write experiment outputs if experiment manager is active
        if experiment_manager and run_id:
            # Write metrics (include regime evaluation if available)
            all_metrics = {**kpis, **stability_metrics}
            if regime_evaluation_results:
                all_metrics["regime_evaluation"] = regime_evaluation_results
            experiment_manager.write_metrics(run_id, all_metrics)
            
            # Write timeseries
            returns_df = backtest_results.select(["date", "r_ls_net", "pnl", "cost", "funding"])
            experiment_manager.write_timeseries(
                run_id,
                regime_timeseries=regime_series,
                returns=returns_df,
            )
            
            # Update catalog
            experiment_manager.update_catalog(
                run_id=run_id,
                experiment_spec=experiment_spec,
                metrics=kpis,
                stability_metrics=stability_metrics,
            )
            
            logger.info(f"Experiment run complete: {run_id}")
        
        logger.info("Backtest complete!")
        
    finally:
        data_loader.close()


if __name__ == "__main__":
    main()
