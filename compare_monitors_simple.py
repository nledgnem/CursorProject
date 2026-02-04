"""Simple comparison of the two regime monitors using data lake."""

import polars as pl
import numpy as np
from pathlib import Path
from datetime import date
from typing import Dict, List, Optional
import yaml
import logging
import statistics

# Import current monitor components
from majors_alts_monitor.data_io import ReadOnlyDataLoader
from majors_alts_monitor.features import FeatureLibrary
from majors_alts_monitor.regime import RegimeModel
from majors_alts_monitor.beta_neutral import DualBetaNeutralLS
from majors_alts_monitor.backtest import BacktestEngine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def compute_legacy_regime_for_date(
    prices: pl.DataFrame,
    marketcap: pl.DataFrame,
    volume: pl.DataFrame,
    funding: Optional[pl.DataFrame],
    current_date: date,
    f_alt_series: List[float],
    f_btc_series: List[float],
    btc_oi_by_date: Dict[date, Dict[str, float]],
) -> Dict:
    """Compute legacy regime for a single date using data lake."""
    # Get prices for this date
    prices_date = prices.filter(
        pl.col("date") == pl.date(current_date.year, current_date.month, current_date.day)
    )
    
    # Compute returns
    prices_sorted = prices.sort(["asset_id", "date"])
    prices_with_ret = prices_sorted.with_columns([
        (pl.col("close") / pl.col("close").shift(1).over("asset_id") - 1.0).alias("ret_1d"),
        (pl.col("close") / pl.col("close").shift(7).over("asset_id") - 1.0).alias("ret_7d"),
    ])
    
    prices_date_ret = prices_with_ret.filter(
        pl.col("date") == pl.date(current_date.year, current_date.month, current_date.day)
    )
    
    # Build prices dict (legacy format)
    prices_dict = {}
    for row in prices_date_ret.iter_rows(named=True):
        asset_id = row["asset_id"]
        ret_1d = row.get("ret_1d")
        ret_7d = row.get("ret_7d")
        price = row.get("close")
        
        if ret_1d is None or ret_7d is None or price is None:
            continue
        
        prices_dict[asset_id] = {
            "price_usd": float(price),
            "return_1d": float(ret_1d) * 100.0,  # Convert to percentage
            "return_7d": float(ret_7d) * 100.0,
        }
    
    if "BTC" not in prices_dict:
        return {
            "regime_score": 50.0,
            "bucket": "YELLOW",
            "regime": "BALANCED",
        }
    
    # Get ALT symbols (all except BTC, ETH)
    alt_symbols = [a for a in prices_dict.keys() if a not in ["BTC", "ETH"]]
    
    # Compute legacy regime components
    btc_1d = prices_dict.get("BTC", {}).get("return_1d", 0.0)
    btc_7d = prices_dict.get("BTC", {}).get("return_7d", 0.0)
    
    # Alt basket returns
    alt7 = [prices_dict.get(s, {}).get("return_7d", 0.0) for s in alt_symbols if s in prices_dict]
    alt1 = [prices_dict.get(s, {}).get("return_1d", 0.0) for s in alt_symbols if s in prices_dict]
    alt7 = [x for x in alt7 if x is not None]
    alt1 = [x for x in alt1 if x is not None]
    alt7_avg = statistics.fmean(alt7) if alt7 else 0.0
    
    # Trend: BTC vs alt basket, vol-adjusted
    spread7_pct = btc_7d - alt7_avg
    vol_proxy = abs(btc_7d) + 1e-3
    trend_raw = spread7_pct / vol_proxy
    trend_clamped = clamp(trend_raw / 3.0, -1.0, 1.0)
    
    # Approximate "3d" returns
    def approx_3d(ret1, ret7):
        return 0.5 * (ret7 * 3.0 / 7.0 + ret1 / 3.0)
    
    btc_3d = approx_3d(btc_1d, btc_7d)
    
    # Breadth: % of alts outperforming BTC on 3d horizon
    alt3 = []
    for s in alt_symbols:
        if s not in prices_dict:
            continue
        r1 = prices_dict[s].get("return_1d", 0.0)
        r7 = prices_dict[s].get("return_7d", 0.0)
        alt3.append(approx_3d(r1, r7))
    
    if alt3:
        num_outperf = len([x for x in alt3 if x > btc_3d])
        breadth_3d = num_outperf / len(alt3)
    else:
        breadth_3d = 0.0
    breadth_risk = breadth_3d
    
    # OI branch (simplified - use marketcap as proxy)
    btc_oi = btc_oi_by_date.get(current_date, {"oi_usd_all": 0.0, "oi_change_3d_pct": 0.0})
    oi_change = btc_oi.get("oi_change_3d_pct", 0.0)
    
    if oi_change > 0:
        base_oi_risk = clamp(oi_change / 50.0, 0.0, 1.0)
        oi_quality = 1.0 if btc_3d > 0 else 0.5
    else:
        base_oi_risk = 0.0
        oi_quality = 0.0
    oi_risk = base_oi_risk * oi_quality
    
    # Funding risk (from heating calculation)
    if len(f_alt_series) >= 20 and len(f_btc_series) >= 20:
        # Compute heating: short-term (10d) vs long-term (20d) spread
        s_series = [fa - fb for fa, fb in zip(f_alt_series, f_btc_series)]
        s_short = statistics.fmean(s_series[-10:])
        s_long = statistics.fmean(s_series[-20:])
        heating = s_short - s_long
        
        # Map heating to funding_risk [0, 1]
        h_low = 0.0
        h_high = 0.0005
        if heating <= h_low:
            funding_risk = 0.0
        elif heating >= h_high:
            funding_risk = 1.0
        else:
            funding_risk = (heating - h_low) / (h_high - h_low)
        funding_risk = clamp(funding_risk, 0.0, 1.0)
    else:
        funding_risk = 0.5  # Default if not enough history
    
    # Get funding for today
    f_alt_today = f_alt_series[-1] if f_alt_series else 0.0
    f_btc_today = f_btc_series[-1] if f_btc_series else 0.0
    
    # Decomposition
    W_FUNDING = 0.25
    trend_component = trend_clamped
    funding_penalty = W_FUNDING * funding_risk
    oi_penalty = 0.15 * oi_risk
    breadth_penalty = 0.10 * breadth_risk
    total_penalty = funding_penalty + oi_penalty + breadth_penalty
    
    combined_raw = trend_component - total_penalty
    combined_clamped = clamp(combined_raw, -1.0, 1.0)
    regime_score_raw = (combined_clamped + 1.0) / 2.0 * 100.0
    
    # High-vol gate
    high_vol = abs(btc_7d) > 15.0
    regime_score = regime_score_raw
    if high_vol and regime_score > 60.0:
        regime_score = 60.0
    
    # Bucket classification
    if regime_score >= 70:
        bucket = "GREEN"
        regime = "RISK_ON_MAJORS"
    elif regime_score >= 55:
        bucket = "YELLOWGREEN"
        regime = "RISK_ON_MAJORS"  # Weak
    elif regime_score >= 45:
        bucket = "YELLOW"
        regime = "BALANCED"
    elif regime_score >= 30:
        bucket = "ORANGE"
        regime = "RISK_ON_ALTS"  # Weak
    else:
        bucket = "RED"
        regime = "RISK_ON_ALTS"
    
    return {
        "regime_score": regime_score,
        "bucket": bucket,
        "regime": regime,
        "score": regime_score,  # For compatibility
    }


def compute_legacy_regime_series(
    prices: pl.DataFrame,
    marketcap: pl.DataFrame,
    volume: pl.DataFrame,
    funding: Optional[pl.DataFrame],
    dates: List[date],
) -> pl.DataFrame:
    """Compute regime series using legacy logic."""
    regimes = []
    
    # Precompute funding by date
    f_alt_by_date: Dict[date, float] = {}
    f_btc_by_date: Dict[date, float] = {}
    
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
            alt_symbols = [a for a in prices["asset_id"].unique().to_list() if a not in ["BTC", "ETH"]]
            alt_funding = funding_date.filter(pl.col("asset_id").is_in(alt_symbols))
            alt_vals = []
            if len(alt_funding) > 0 and "funding_rate" in alt_funding.columns:
                alt_vals = alt_funding["funding_rate"].to_list()
            
            f_alt = float(np.mean(alt_vals)) if alt_vals else 0.0
            
            f_alt_by_date[d] = f_alt
            f_btc_by_date[d] = f_btc
    
    # Precompute OI (simplified - use marketcap as proxy)
    btc_oi_by_date: Dict[date, Dict[str, float]] = {}
    for idx, d in enumerate(dates):
        mcap_date = marketcap.filter(
            pl.col("date") == pl.date(d.year, d.month, d.day)
        )
        btc_mcap = mcap_date.filter(pl.col("asset_id") == "BTC")
        oi_usd = float(btc_mcap["marketcap"][0]) if len(btc_mcap) > 0 else 0.0
        
        # Compute 3d change
        oi_change_3d = 0.0
        if idx >= 3:
            prev_d = dates[idx - 3]
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
    
    # Build funding series incrementally
    f_alt_series = []
    f_btc_series = []
    
    for d in dates:
        f_alt_today = f_alt_by_date.get(d, 0.0)
        f_btc_today = f_btc_by_date.get(d, 0.0)
        f_alt_series.append(f_alt_today)
        f_btc_series.append(f_btc_today)
        
        # Compute regime
        regime = compute_legacy_regime_for_date(
            prices, marketcap, volume, funding, d,
            f_alt_series, f_btc_series, btc_oi_by_date,
        )
        
        regimes.append({
            "date": d,
            "regime": regime["regime"],
            "score": regime["regime_score"],
        })
    
    return pl.DataFrame(regimes)


def run_comparison():
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
    data_loader = ReadOnlyDataLoader(
        data_lake_dir=Path(config["data"]["data_lake_dir"]),
        duckdb_path=Path(config["data"]["duckdb_path"]) if config["data"].get("duckdb_path") else None,
        universe_snapshots_path=Path(config["data"]["universe_snapshots_path"]) if config["data"].get("universe_snapshots_path") else None,
    )
    
    datasets = data_loader.load_dataset(start=date(2024, 1, 1), end=date(2025, 12, 31))
    
    prices = datasets.get("price")
    marketcap = datasets.get("marketcap")
    volume = datasets.get("volume")
    funding = datasets.get("funding")
    
    # Get date range
    dates = sorted(prices["date"].unique().to_list())
    dates = [d for d in dates if d >= date(2024, 1, 1) and d <= date(2025, 12, 31)]
    
    print(f"\nLoaded {len(dates)} trading days")
    
    # ===== CURRENT MONITOR =====
    print("\n" + "=" * 80)
    print("CURRENT MONITOR (majors_alts_monitor)")
    print("=" * 80)
    
    # Compute features and regime
    feature_lib = FeatureLibrary(
        burn_in_days=config["features"]["burn_in_days"],
        lookback_days=config["features"]["lookback_days"],
    )
    
    features = feature_lib.compute_features(
        prices, marketcap, volume, funding,
        majors=config["universe"]["majors"],
        exclude_assets=config["universe"].get("exclude_assets", []),
    )
    
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
        date(2024, 1, 1), date(2025, 12, 31),
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
        date(2024, 1, 1), date(2025, 12, 31),
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
    Path("reports/majors_alts").mkdir(parents=True, exist_ok=True)
    backtest_results_current.write_csv("reports/majors_alts/bt_current_monitor.csv")
    backtest_results_legacy.write_csv("reports/majors_alts/bt_legacy_monitor.csv")
    
    print("\nResults saved to:")
    print("  - reports/majors_alts/bt_current_monitor.csv")
    print("  - reports/majors_alts/bt_legacy_monitor.csv")


if __name__ == "__main__":
    run_comparison()
