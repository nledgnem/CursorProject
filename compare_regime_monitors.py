"""Compare the two regime monitors and create a backtest adapter for the existing one."""

import polars as pl
import numpy as np
from pathlib import Path
from datetime import date, datetime
from typing import Dict, List, Optional
import sys

# Add OwnScripts to path
sys.path.insert(0, str(Path(__file__).parent / "OwnScripts" / "regime_backtest"))

# Import the existing regime monitor logic
from regime_monitor import (
    compute_regime,
    compute_heating_and_funding_risk_from_series,
    ALT_SYMBOLS,
    H_SHORT,
    H_LONG,
    H_LOW,
    H_HIGH,
    W_FUNDING,
)

# Import our current monitor components
from majors_alts_monitor.data_io import load_dataset
from majors_alts_monitor.features import FeatureLibrary
from majors_alts_monitor.regime import RegimeModel
from majors_alts_monitor.beta_neutral import DualBetaNeutralLS
from majors_alts_monitor.backtest import BacktestEngine
from majors_alts_monitor.outputs import OutputGenerator
import yaml
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def convert_data_lake_to_legacy_format(
    prices: pl.DataFrame,
    marketcap: pl.DataFrame,
    volume: pl.DataFrame,
    funding: Optional[pl.DataFrame],
    date: date,
) -> Dict[str, Dict[str, float]]:
    """Convert data lake format to legacy regime_monitor format."""
    # Filter to date
    prices_date = prices.filter(pl.col("date") == pl.date(date.year, date.month, date.day))
    
    # Get BTC and ETH prices
    btc_price = prices_date.filter(pl.col("asset_id") == "BTC")
    eth_price = prices_date.filter(pl.col("asset_id") == "ETH")
    
    # Compute 1d and 7d returns
    prices_sorted = prices.sort(["asset_id", "date"])
    prices_with_ret = prices_sorted.with_columns([
        (pl.col("close") / pl.col("close").shift(1).over("asset_id") - 1.0).alias("ret_1d"),
        (pl.col("close") / pl.col("close").shift(7).over("asset_id") - 1.0).alias("ret_7d"),
    ])
    
    # Filter to date and convert to dict
    prices_date_ret = prices_with_ret.filter(
        pl.col("date") == pl.date(date.year, date.month, date.day)
    )
    
    result = {}
    for row in prices_date_ret.iter_rows(named=True):
        asset_id = row["asset_id"]
        ret_1d = row.get("ret_1d")
        ret_7d = row.get("ret_7d")
        price = row.get("close")
        
        if ret_1d is None or ret_7d is None or price is None:
            continue
        
        result[asset_id] = {
            "price_usd": float(price),
            "return_1d": float(ret_1d) * 100.0,  # Convert to percentage
            "return_7d": float(ret_7d) * 100.0,
        }
    
    return result


def compute_legacy_regime_series(
    prices: pl.DataFrame,
    marketcap: pl.DataFrame,
    volume: pl.DataFrame,
    funding: Optional[pl.DataFrame],
    dates: List[date],
) -> pl.DataFrame:
    """Compute regime series using legacy regime_monitor logic."""
    regimes = []
    
    # Build funding series for heating calculation
    f_alt_series = []
    f_btc_series = []
    
    # Precompute funding by date
    funding_by_date: Dict[date, Dict[str, float]] = {}
    if funding is not None:
        for d in dates:
            funding_date = funding.filter(
                pl.col("date") == pl.date(d.year, d.month, d.day)
            )
            
            # Get BTC funding
            btc_funding = funding_date.filter(pl.col("asset_id") == "BTC")
            f_btc = 0.0
            if len(btc_funding) > 0:
                f_btc = float(btc_funding["funding_rate"][0]) if "funding_rate" in btc_funding.columns else 0.0
            
            # Get ALT funding average
            alt_funding = funding_date.filter(
                pl.col("asset_id").is_in(ALT_SYMBOLS)
            )
            alt_vals = []
            if len(alt_funding) > 0 and "funding_rate" in alt_funding.columns:
                alt_vals = alt_funding["funding_rate"].to_list()
            
            f_alt = float(np.mean(alt_vals)) if alt_vals else 0.0
            
            funding_by_date[d] = {"f_alt": f_alt, "f_btc": f_btc}
            f_alt_series.append(f_alt)
            f_btc_series.append(f_btc)
    
    # Compute OI (simplified - use marketcap as proxy or set to 0)
    # The legacy monitor uses CoinGlass OI, which we don't have in data lake
    # We'll use a simplified version
    btc_oi_by_date: Dict[date, Dict[str, float]] = {}
    for d in dates:
        mcap_date = marketcap.filter(
            pl.col("date") == pl.date(d.year, d.month, d.day)
        )
        btc_mcap = mcap_date.filter(pl.col("asset_id") == "BTC")
        oi_usd = float(btc_mcap["marketcap"][0]) if len(btc_mcap) > 0 else 0.0
        
        # Compute 3d change (simplified)
        oi_change_3d = 0.0
        if len(dates) > 3 and dates.index(d) >= 3:
            prev_d = dates[dates.index(d) - 3]
            prev_mcap = marketcap.filter(
                pl.col("date") == pl.date(prev_d.year, prev_d.month, prev_d.day)
            )
            prev_btc_mcap = prev_mcap.filter(pl.col("asset_id") == "BTC")
            prev_oi = float(prev_btc_mcap["marketcap"][0]) if len(prev_btc_mcap) > 0 else 0.0
            if prev_oi > 0:
                oi_change_3d = (oi_usd / prev_oi - 1.0) * 100.0
        
        btc_oi_by_date[d] = {
            "oi_usd_all": oi_usd,
            "oi_change_3d_pct": oi_change_3d,
        }
    
    # Compute regimes
    for idx, d in enumerate(dates):
        # Convert data to legacy format
        prices_dict = convert_data_lake_to_legacy_format(
            prices, marketcap, volume, funding, d
        )
        
        if "BTC" not in prices_dict:
            regimes.append({
                "date": d,
                "regime": "BALANCED",
                "score": 50.0,
                "regime_score": 50.0,
            })
            continue
        
        # Get funding for heating calculation
        f_alt_today = funding_by_date.get(d, {}).get("f_alt", 0.0)
        f_btc_today = funding_by_date.get(d, {}).get("f_btc", 0.0)
        
        # Build funding series up to this point
        f_alt_series_upto = f_alt_series[:idx+1] if idx < len(f_alt_series) else []
        f_btc_series_upto = f_btc_series[:idx+1] if idx < len(f_btc_series) else []
        
        # Compute funding risk and heating
        funding_risk, heating = compute_heating_and_funding_risk_from_series(
            f_alt_series_upto,
            f_btc_series_upto,
            default_risk=0.5,
        )
        
        # Get OI
        btc_oi = btc_oi_by_date.get(d, {"oi_usd_all": 0.0, "oi_change_3d_pct": 0.0})
        
        # Compute regime
        regime = compute_regime(
            prices=prices_dict,
            btc_oi=btc_oi,
            funding_risk=funding_risk,
            f_alt=f_alt_today,
            f_btc=f_btc_today,
            heating=heating,
        )
        
        # Convert bucket to our regime format
        bucket = regime.get("bucket", "YELLOW")
        if bucket == "GREEN":
            regime_name = "RISK_ON_MAJORS"
        elif bucket == "YELLOWGREEN":
            regime_name = "RISK_ON_MAJORS"  # Weak risk on majors
        elif bucket == "YELLOW":
            regime_name = "BALANCED"
        elif bucket == "ORANGE":
            regime_name = "RISK_ON_ALTS"  # Weak risk on alts
        else:  # RED
            regime_name = "RISK_ON_ALTS"
        
        regimes.append({
            "date": d,
            "regime": regime_name,
            "score": regime.get("regime_score", 50.0),
            "regime_score": regime.get("regime_score", 50.0),
        })
    
    return pl.DataFrame(regimes)


def run_comparison_backtest():
    """Run backtest with both regime monitors and compare."""
    print("=" * 80)
    print("COMPARING REGIME MONITORS")
    print("=" * 80)
    
    # Load config
    config_path = Path("majors_alts_monitor/config.yaml")
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Load data
    logger.info("Loading data...")
    data = load_dataset(
        data_lake_dir=Path(config["data"]["data_lake_dir"]),
        duckdb_path=Path(config["data"]["duckdb_path"]) if config["data"].get("duckdb_path") else None,
        universe_snapshots_path=Path(config["data"]["universe_snapshots_path"]) if config["data"].get("universe_snapshots_path") else None,
        start_date="2024-01-01",
        end_date="2025-12-31",
    )
    
    prices = data["fact_price"]
    marketcap = data["fact_marketcap"]
    volume = data["fact_volume"]
    funding = data.get("fact_funding")
    
    # Get date range
    dates = sorted(prices["date"].unique().to_list())
    dates = [d for d in dates if d >= date(2024, 1, 1) and d <= date(2025, 12, 31)]
    
    print(f"\nLoaded {len(dates)} trading days")
    
    # ===== CURRENT MONITOR =====
    print("\n" + "=" * 80)
    print("CURRENT MONITOR (majors_alts_monitor)")
    print("=" * 80)
    
    # Compute features
    feature_lib = FeatureLibrary(
        burn_in_days=config["features"]["burn_in_days"],
        lookback_days=config["features"]["lookback_days"],
    )
    
    features = feature_lib.compute_features(
        prices, marketcap, volume, funding,
        majors=config["universe"]["majors"],
        exclude_assets=config["universe"].get("exclude_assets", []),
    )
    
    # Compute regime series
    regime_model = RegimeModel(
        mode=config["regime"]["mode"],
        n_regimes=config["regime"]["n_regimes"],
        default_weights=config["regime"]["composite"]["default_weights"],
        threshold_low=config["regime"]["composite"].get("threshold_low", -0.5),
        threshold_high=config["regime"]["composite"].get("threshold_high", 0.5),
        threshold_strong_low=config["regime"]["composite"].get("threshold_strong_low", -1.5),
        threshold_strong_high=config["regime"]["composite"].get("threshold_strong_high", 1.5),
        hysteresis_low=config["regime"]["composite"]["hysteresis"]["low_band"],
        hysteresis_high=config["regime"]["composite"]["hysteresis"]["high_band"],
    )
    
    regime_series_current = regime_model.compute_composite_score(features)
    
    # Run backtest
    beta_neutral = DualBetaNeutralLS(
        majors=config["universe"]["majors"],
        lookback_days=config["beta"]["lookback_days"],
        ridge_alpha=config["beta"]["ridge_alpha"],
        winsorize_pct=config["beta"]["winsorize_pct"],
        beta_clamp=tuple(config["beta"]["beta_clamp"]),
        default_beta=config["beta"]["default_beta"],
        neutrality_mode=config["universe"].get("neutrality_mode", "beta_neutral"),
    )
    
    backtest_engine = BacktestEngine(
        maker_fee_bps=config["costs"]["maker_fee_bps"],
        taker_fee_bps=config["costs"]["taker_fee_bps"],
        slippage_bps=config["costs"]["slippage_bps"],
        slippage_adv_multiplier=config["costs"]["slippage_adv_multiplier"],
        funding_enabled=config["costs"]["funding_enabled"],
        funding_8h_rate=config["costs"]["funding_8h_rate"],
        vol_target=config["universe"].get("vol_target"),
        regime_position_scaling=config["backtest"].get("regime_position_scaling"),
        risk_management=config["backtest"].get("risk_management"),
    )
    
    def build_alt_basket(asof_date):
        return beta_neutral.build_alt_basket(
            prices, marketcap, volume, asof_date,
            basket_size=config["universe"]["basket_size"],
            min_mcap_usd=config["universe"]["min_mcap_usd"],
            min_volume_usd=config["universe"]["min_volume_usd"],
            per_name_cap=config["universe"]["per_name_cap"],
            exclude_assets=config["universe"].get("exclude_assets", []),
            alt_selection_config=config["universe"].get("alt_selection"),
        )
    
    def estimate_beta(asset_id, asof_date):
        return beta_neutral.estimate_betas(
            prices, asset_id, asof_date,
            tracker_betas=None,
        )
    
    neutrality_mode = config["universe"].get("neutrality_mode", "beta_neutral")
    def solve_neutrality(alt_weights_new, alt_betas_new):
        return beta_neutral.solve_neutrality(
            alt_weights_new, alt_betas_new,
            major_weights={"BTC": 0.0, "ETH": 0.0},
            gross_cap=config["universe"]["gross_cap"],
            neutrality_mode=neutrality_mode,
        )
    
    backtest_results_current = backtest_engine.run_backtest(
        prices, marketcap, volume, funding,
        features, regime_series_current,
        build_alt_basket, estimate_beta, solve_neutrality,
        "2024-01-01", "2025-12-31",
        walk_forward=config["backtest"]["walk_forward"],
        train_window_days=config["backtest"]["train_window_days"],
        test_window_days=config["backtest"]["test_window_days"],
    )
    
    # ===== LEGACY MONITOR =====
    print("\n" + "=" * 80)
    print("LEGACY MONITOR (OwnScripts/regime_backtest)")
    print("=" * 80)
    
    # Compute regime series using legacy logic
    regime_series_legacy = compute_legacy_regime_series(
        prices, marketcap, volume, funding, dates
    )
    
    # Run backtest with legacy regime series
    backtest_results_legacy = backtest_engine.run_backtest(
        prices, marketcap, volume, funding,
        features, regime_series_legacy,  # Use legacy regime series
        build_alt_basket, estimate_beta, solve_neutrality,
        "2024-01-01", "2025-12-31",
        walk_forward=config["backtest"]["walk_forward"],
        train_window_days=config["backtest"]["train_window_days"],
        test_window_days=config["backtest"]["test_window_days"],
    )
    
    # ===== COMPARE RESULTS =====
    print("\n" + "=" * 80)
    print("COMPARISON RESULTS")
    print("=" * 80)
    
    def compute_metrics(bt_results):
        if len(bt_results) == 0:
            return {}
        
        returns = bt_results["r_ls_net"].to_numpy()
        equity = np.cumprod(1.0 + returns)
        
        running_max = np.maximum.accumulate(equity)
        drawdown = (equity - running_max) / running_max
        
        total_return = equity[-1] / equity[0] - 1.0
        n_days = len(returns)
        cagr = (1.0 + total_return) ** (252.0 / n_days) - 1.0
        
        mean_ret = np.mean(returns)
        std_ret = np.std(returns)
        sharpe = (mean_ret / std_ret * np.sqrt(252)) if std_ret > 0 else 0.0
        
        downside = returns[returns < 0]
        downside_std = np.std(downside) if len(downside) > 0 else 0.0
        sortino = (mean_ret / downside_std * np.sqrt(252)) if downside_std > 0 else 0.0
        
        return {
            "total_return": total_return,
            "cagr": cagr,
            "sharpe": sharpe,
            "sortino": sortino,
            "max_drawdown": np.min(drawdown),
            "volatility": std_ret * np.sqrt(252),
            "hit_rate": np.mean(returns > 0),
        }
    
    metrics_current = compute_metrics(backtest_results_current)
    metrics_legacy = compute_metrics(backtest_results_legacy)
    
    print(f"\n{'Metric':<20} {'Current Monitor':<20} {'Legacy Monitor':<20} {'Difference':<20}")
    print("-" * 80)
    
    for key in ["sharpe", "cagr", "max_drawdown", "sortino", "volatility", "hit_rate"]:
        curr = metrics_current.get(key, 0.0)
        leg = metrics_legacy.get(key, 0.0)
        diff = curr - leg
        diff_pct = (diff / abs(leg) * 100) if leg != 0 else 0.0
        
        if key in ["sharpe", "sortino", "hit_rate"]:
            print(f"{key:<20} {curr:<20.4f} {leg:<20.4f} {diff:+.4f} ({diff_pct:+.1f}%)")
        elif key == "max_drawdown":
            print(f"{key:<20} {curr*100:<20.2f}% {leg*100:<20.2f}% {diff*100:+.2f}% ({diff_pct:+.1f}%)")
        else:
            print(f"{key:<20} {curr*100:<20.2f}% {leg*100:<20.2f}% {diff*100:+.2f}% ({diff_pct:+.1f}%)")
    
    # Determine winner
    if metrics_current.get("sharpe", 0.0) > metrics_legacy.get("sharpe", 0.0):
        print("\n>>> WINNER: Current Monitor (majors_alts_monitor) <<<")
    else:
        print("\n>>> WINNER: Legacy Monitor (OwnScripts/regime_backtest) <<<")
    
    # Save results
    backtest_results_current.write_csv("reports/majors_alts/bt_current_monitor.csv")
    backtest_results_legacy.write_csv("reports/majors_alts/bt_legacy_monitor.csv")
    
    print("\nResults saved to:")
    print("  - reports/majors_alts/bt_current_monitor.csv")
    print("  - reports/majors_alts/bt_legacy_monitor.csv")


if __name__ == "__main__":
    run_comparison_backtest()
