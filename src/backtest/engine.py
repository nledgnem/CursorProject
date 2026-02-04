"""Backtest engine for computing strategy returns."""

import pandas as pd
import numpy as np
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Any, Optional
import yaml


def compute_turnover(old_weights: pd.Series, new_weights: pd.Series) -> float:
    """Compute portfolio turnover (sum of absolute weight changes)."""
    # Align indices
    all_symbols = set(old_weights.index) | set(new_weights.index)
    old_aligned = pd.Series(0.0, index=list(all_symbols))
    new_aligned = pd.Series(0.0, index=list(all_symbols))
    old_aligned[old_weights.index] = old_weights
    new_aligned[new_weights.index] = new_weights
    
    turnover = (new_aligned - old_aligned).abs().sum() / 2.0  # Divide by 2 because we're counting both buys and sells
    return turnover


def apply_gap_fill(prices_df: pd.DataFrame, gap_fill_mode: str) -> pd.DataFrame:
    """
    Apply gap filling to price series based on gap_fill_mode.
    
    Args:
        prices_df: DataFrame with date index and symbol columns
        gap_fill_mode: "none" or "1d" (fill only single-day gaps)
    
    Returns:
        DataFrame with gaps filled (if mode allows)
    """
    if gap_fill_mode == "none":
        return prices_df
    
    if gap_fill_mode == "1d":
        # Fill only single-day gaps (max 1 consecutive missing day)
        filled_df = prices_df.copy()
        
        for col in filled_df.columns:
            series = filled_df[col]
            is_na = series.isna()
            
            if not is_na.any():
                continue  # No gaps
            
            # Find single-day gaps
            # A gap is a single-day gap if:
            # - It's NA
            # - Previous day is not NA
            # - Next day is not NA (or gap is at the end, but we only fill if next day exists)
            
            for i in range(1, len(series) - 1):  # Skip first and last
                if pd.isna(series.iloc[i]):
                    # Check if previous and next are both non-NA
                    if not pd.isna(series.iloc[i-1]) and not pd.isna(series.iloc[i+1]):
                        # Single-day gap: forward fill
                        filled_df.iloc[i, filled_df.columns.get_loc(col)] = series.iloc[i-1]
        
        return filled_df
    
    else:
        raise ValueError(f"Unknown gap_fill_mode: {gap_fill_mode}")


def check_data_quality(
    prices_df: pd.DataFrame,
    symbol: str,
    lookback_start: date,
    lookback_end: date,
    min_history_days: Optional[int],
    max_missing_frac: Optional[float],
    max_consecutive_missing_days: Optional[int],
) -> tuple:
    """
    Check if a symbol meets data quality thresholds.
    
    Returns:
        (is_valid, reason) - is_valid=True if passes all checks, reason explains failure
    """
    if symbol not in prices_df.columns:
        return False, "symbol_not_in_data"
    
    # Filter to lookback window
    window_df = prices_df[(prices_df.index >= lookback_start) & (prices_df.index <= lookback_end)]
    if len(window_df) == 0:
        return False, "no_data_in_window"
    
    series = window_df[symbol]
    
    # Check min_history_days
    if min_history_days is not None:
        non_na_count = series.notna().sum()
        if non_na_count < min_history_days:
            return False, f"insufficient_history_days_{non_na_count}_{min_history_days}"
    
    # Check max_missing_frac
    if max_missing_frac is not None:
        missing_frac = series.isna().sum() / len(series)
        if missing_frac > max_missing_frac:
            return False, f"excessive_missing_frac_{missing_frac:.2%}_{max_missing_frac:.2%}"
    
    # Check max_consecutive_missing_days
    if max_consecutive_missing_days is not None:
        is_na = series.isna()
        max_consecutive = 0
        current_consecutive = 0
        for val in is_na:
            if val:
                current_consecutive += 1
                max_consecutive = max(max_consecutive, current_consecutive)
            else:
                current_consecutive = 0
        
        if max_consecutive > max_consecutive_missing_days:
            return False, f"excessive_consecutive_missing_{max_consecutive}_{max_consecutive_missing_days}"
    
    return True, "passed"


def run_backtest(
    config_path: Path,
    prices_path: Path,
    snapshots_path: Path,
    output_dir: Path,
) -> Dict[str, Any]:
    """
    Run backtest.
    
    Outputs:
        - output_dir/backtest_results.csv
        - output_dir/rebalance_turnover.csv
        - output_dir/report.md
    """
    """
    Run backtest.
    
    Outputs:
        - output_dir/backtest_results.csv
        - output_dir/report.md
    """
    print(f"Loading config from {config_path}")
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    print(f"Loading data...")
    # Load snapshots
    snapshots_df = pd.read_parquet(snapshots_path)
    print(f"  Loaded {len(snapshots_df)} snapshot rows from {snapshots_path}")
    
    # Check if prices_path points to fact table or wide format
    prices_path_str = str(prices_path)
    use_data_lake = 'fact_price' in prices_path_str or 'data_lake' in prices_path_str
    
    if use_data_lake:
        # Load from fact table and convert to wide format
        from src.utils.data_loader import load_prices_wide
        data_lake_dir = prices_path.parent
        prices_df = load_prices_wide(data_lake_dir)
        print(f"  Loaded prices from data lake format")
    else:
        # Load from wide format file (legacy)
        prices_df = pd.read_parquet(prices_path)
    # Convert index to date if datetime
    if isinstance(prices_df.index, pd.DatetimeIndex):
        prices_df.index = prices_df.index.date
    
    # Get config
    start_date = date.fromisoformat(config["start_date"])
    end_date = date.fromisoformat(config["end_date"])
    base_asset = config["base_asset"]
    fee_bps = config["cost_model"]["fee_bps"]
    slippage_bps = config["cost_model"]["slippage_bps"]
    
    # Get backtest data quality settings (with defaults)
    backtest_config = config.get("backtest", {})
    gap_fill_mode = backtest_config.get("gap_fill_mode", "none")  # "none" or "1d"
    min_history_days = backtest_config.get("min_history_days", None)
    max_missing_frac = backtest_config.get("max_missing_frac", None)
    max_consecutive_missing_days = backtest_config.get("max_consecutive_missing_days", None)
    basket_coverage_threshold = backtest_config.get("basket_coverage_threshold", 0.90)  # Default 90%
    lookback_window_days = backtest_config.get("lookback_window_days", 30)  # Default 30 days
    # Missing price policy: how to handle missing prices in basket return calculation
    # "nan": return NaN if coverage < threshold (conservative, default for research)
    # "renormalize": renormalize weights across valid-price symbols (can bias returns upward)
    # "conservative_zero": missing symbols contribute 0 return, no renormalization
    missing_price_policy = backtest_config.get("missing_price_policy", "nan")
    
    print(f"\nBacktest data quality settings:")
    print(f"  - Gap fill mode: {gap_fill_mode}")
    print(f"  - Min history days: {min_history_days}")
    print(f"  - Max missing fraction: {max_missing_frac}")
    print(f"  - Max consecutive missing days: {max_consecutive_missing_days}")
    print(f"  - Basket coverage threshold: {basket_coverage_threshold:.1%}")
    print(f"  - Missing price policy: {missing_price_policy}")
    print(f"  - Lookback window: {lookback_window_days} days")
    
    # Filter prices to date range
    prices_df = prices_df[(prices_df.index >= start_date) & (prices_df.index <= end_date)]
    prices_df = prices_df.sort_index()
    
    # Apply gap filling (if enabled)
    if gap_fill_mode != "none":
        print(f"\nApplying gap filling (mode: {gap_fill_mode})...")
        prices_df = apply_gap_fill(prices_df, gap_fill_mode)
        print(f"  Gap filling complete")
    
    # Get rebalance dates from snapshots
    rebalance_dates = sorted(snapshots_df["rebalance_date"].unique()) if len(snapshots_df) > 0 else []
    
    # Check if we have any snapshots
    if len(rebalance_dates) == 0:
        print(f"\n[WARN] No rebalance dates found in snapshots - creating empty results")
        # Create empty results DataFrame with correct schema
        results_df = pd.DataFrame(columns=["date", "r_btc", "r_basket", "r_ls", "cost", "r_ls_net", "equity_curve"])
        results_df["equity_curve"] = 1.0  # Start at 1.0
        
        # Save empty results
        output_dir.mkdir(parents=True, exist_ok=True)
        results_path = output_dir / "backtest_results.csv"
        results_df.to_csv(results_path, index=False)
        
        # Create empty turnover DataFrame
        turnover_df = pd.DataFrame(columns=["rebalance_date", "turnover", "entered_count", "exited_count", "num_constituents"])
        turnover_path = output_dir / "rebalance_turnover.csv"
        turnover_df.to_csv(turnover_path, index=False)
        
        # Generate minimal report
        generate_report(results_df, turnover_df, config, prices_df, output_dir / "report.md", None)
        
        print(f"\n[WARN] Backtest completed with no snapshots - empty results saved")
        return {
            "row_count": 0,
            "date_range": {
                "start_date": str(start_date),
                "end_date": str(end_date),
            },
            "num_trading_days": 0,
            "num_rebalance_dates": 0,
            "backtest_assumptions": {
                "gap_fill_mode": gap_fill_mode,
                "min_history_days": min_history_days,
                "max_missing_frac": max_missing_frac,
                "max_consecutive_missing_days": max_consecutive_missing_days,
                "basket_coverage_threshold": basket_coverage_threshold,
            "missing_price_policy": missing_price_policy,
                "lookback_window_days": lookback_window_days,
            },
        }
    
    # Initialize results
    results = []
    rebalance_turnover = []  # Track turnover per rebalance
    current_weights: Dict[str, float] = {}
    prev_weights: Dict[str, float] = {}
    current_rebal_date = None
    
    # Track per-asset contributions for concentration analysis (cumulative across backtest)
    asset_contributions: Dict[str, float] = {}  # symbol -> cumulative contribution
    
    print(f"\nRunning backtest from {start_date} to {end_date}...")
    print(f"Rebalance dates: {len(rebalance_dates)}")
    
    for i, trade_date in enumerate(prices_df.index):
        # Check if this is a rebalance date
        if trade_date in rebalance_dates:
            # Load new weights
            snapshot = snapshots_df[snapshots_df["rebalance_date"] == trade_date]
            new_weights = dict(zip(snapshot["symbol"], snapshot["weight"]))
            
            # Calculate turnover and entered/exited counts before updating weights
            # Use current_weights (from previous rebalance) for comparison
            if current_weights:
                turnover = compute_turnover(
                    pd.Series(current_weights),
                    pd.Series(new_weights)
                )
                
                # Compute entered/exited counts using current_weights (previous basket)
                prev_symbols = set(current_weights.keys())
                curr_symbols = set(new_weights.keys())
                entered_count = len(curr_symbols - prev_symbols)
                exited_count = len(prev_symbols - curr_symbols)
            else:
                turnover = 1.0  # First rebalance: full turnover
                entered_count = len(new_weights)
                exited_count = 0
            
            # Clamp floating-point dust to zero
            if abs(turnover) < 1e-12:
                turnover = 0.0
            
            # Store rebalance turnover info
            rebalance_turnover.append({
                "rebalance_date": trade_date,
                "turnover": turnover,
                "entered_count": entered_count,
                "exited_count": exited_count,
                "num_constituents": len(new_weights),
            })
            
            # Update weights for next rebalance comparison and cost calculation
            # IMPORTANT: Update prev_weights BEFORE current_weights to preserve state for cost calculation
            prev_weights = current_weights.copy()  # Save previous basket for cost calculation
            current_weights = new_weights
            current_rebal_date = trade_date
            
            print(f"  Rebalance on {trade_date}: {len(current_weights)} assets, turnover={turnover:.2%}, entered={entered_count}, exited={exited_count}")
        
        # Get prices for this date
        if trade_date not in prices_df.index:
            continue
        
        prices_today = prices_df.loc[trade_date]
        
        # Compute BTC return
        if base_asset not in prices_today.index or pd.isna(prices_today[base_asset]):
            r_btc = np.nan
        else:
            if i == 0:
                r_btc = 0.0  # First day: no return
            else:
                prev_date = prices_df.index[i - 1]
                prices_prev = prices_df.loc[prev_date]
                if base_asset in prices_prev.index and not pd.isna(prices_prev[base_asset]):
                    r_btc = (prices_today[base_asset] / prices_prev[base_asset]) - 1.0
                else:
                    r_btc = np.nan
        
        # Compute basket return
        # Missing price policy determines how to handle missing prices:
        # - "nan": return NaN if coverage < threshold (conservative, default)
        # - "renormalize": renormalize weights across valid-price symbols (can bias returns)
        # - "conservative_zero": missing symbols contribute 0 return, no renormalization
        if not current_weights:
            r_basket = np.nan
        else:
            basket_return = 0.0
            valid_weights_dict = {}  # symbol -> weight (for symbols with valid prices)
            coverage_threshold = basket_coverage_threshold
            total_weight = sum(current_weights.values())
            
            # Apply data quality thresholds if configured
            # Filter out symbols that don't meet quality thresholds
            valid_symbols_for_quality = set(current_weights.keys())
            
            if min_history_days is not None or max_missing_frac is not None or max_consecutive_missing_days is not None:
                # Check data quality for each symbol in current basket
                lookback_start = max(prices_df.index[0], trade_date - timedelta(days=lookback_window_days))
                
                for symbol in list(current_weights.keys()):
                    is_valid, reason = check_data_quality(
                        prices_df,
                        symbol,
                        lookback_start,
                        trade_date,
                        min_history_days,
                        max_missing_frac,
                        max_consecutive_missing_days,
                    )
                    if not is_valid:
                        # Symbol fails quality check - remove from valid set
                        valid_symbols_for_quality.discard(symbol)
            
            # Collect valid symbols (have prices today and yesterday, pass quality checks)
            for symbol, weight in current_weights.items():
                # Skip if symbol failed data quality check
                if symbol not in valid_symbols_for_quality:
                    continue
                
                if symbol not in prices_today.index or pd.isna(prices_today[symbol]):
                    # Missing price today: skip
                    continue
                
                # Check if we can calculate return (need previous price)
                if i == 0:
                    # First day: return is 0, but we still include it for coverage
                    valid_weights_dict[symbol] = weight
                else:
                    prev_date = prices_df.index[i - 1]
                    prices_prev = prices_df.loc[prev_date]
                    if symbol in prices_prev.index and pd.notna(prices_prev[symbol]):
                        # Has both today and yesterday prices: valid
                        valid_weights_dict[symbol] = weight
                    else:
                        # Missing previous price: skip
                        continue
            
            # Calculate coverage (original weight coverage)
            valid_weights_sum = sum(valid_weights_dict.values())
            coverage = valid_weights_sum / total_weight if total_weight > 0 else 0.0
            
            # Check coverage threshold: if too low, mark as NaN (unless policy allows it)
            if coverage < coverage_threshold:
                if missing_price_policy == "nan":
                    r_basket = np.nan
                elif missing_price_policy == "renormalize":
                    # Allow renormalization even if coverage < threshold (not recommended)
                    if len(valid_weights_dict) == 0:
                        r_basket = np.nan
                    else:
                        renormalized_weights = {sym: w / valid_weights_sum for sym, w in valid_weights_dict.items()}
                        for symbol, renormalized_weight in renormalized_weights.items():
                            if i == 0:
                                symbol_return = 0.0
                            else:
                                prev_date = prices_df.index[i - 1]
                                prices_prev = prices_df.loc[prev_date]
                                symbol_return = (prices_today[symbol] / prices_prev[symbol]) - 1.0
                            basket_return += renormalized_weight * symbol_return
                            original_weight = current_weights.get(symbol, 0.0)
                            contribution = original_weight * symbol_return
                            if symbol not in asset_contributions:
                                asset_contributions[symbol] = 0.0
                            asset_contributions[symbol] += contribution
                        r_basket = basket_return
                else:  # "conservative_zero"
                    r_basket = np.nan  # Still NaN if coverage too low
            elif len(valid_weights_dict) == 0:
                # No valid symbols at all
                r_basket = np.nan
            else:
                # Coverage >= threshold: apply missing price policy
                if missing_price_policy == "renormalize":
                    # Renormalize weights to sum to 1.0 for valid symbols
                    renormalized_weights = {sym: w / valid_weights_sum for sym, w in valid_weights_dict.items()}
                    
                    # Calculate basket return using renormalized weights
                    for symbol, renormalized_weight in renormalized_weights.items():
                        if i == 0:
                            symbol_return = 0.0
                        else:
                            prev_date = prices_df.index[i - 1]
                            prices_prev = prices_df.loc[prev_date]
                            symbol_return = (prices_today[symbol] / prices_prev[symbol]) - 1.0
                        
                        basket_return += renormalized_weight * symbol_return
                        
                        # Track per-asset contributions using original weight (for concentration analysis)
                        original_weight = current_weights.get(symbol, 0.0)
                        contribution = original_weight * symbol_return
                        if symbol not in asset_contributions:
                            asset_contributions[symbol] = 0.0
                        asset_contributions[symbol] += contribution
                    
                    r_basket = basket_return
                else:  # "nan" or "conservative_zero"
                    # Conservative: missing symbols contribute 0 return, divide by total_weight (1.0)
                    # This ensures we're not artificially inflating returns by re-scaling
                    for symbol, weight in valid_weights_dict.items():
                        if i == 0:
                            symbol_return = 0.0
                        else:
                            prev_date = prices_df.index[i - 1]
                            prices_prev = prices_df.loc[prev_date]
                            symbol_return = (prices_today[symbol] / prices_prev[symbol]) - 1.0
                        
                        basket_return += weight * symbol_return
                        
                        # Track per-asset contributions
                        contribution = weight * symbol_return
                        if symbol not in asset_contributions:
                            asset_contributions[symbol] = 0.0
                        asset_contributions[symbol] += contribution
                    
                    r_basket = basket_return / total_weight
        
        # Long-short return
        if pd.isna(r_btc) or pd.isna(r_basket):
            r_ls = np.nan
        else:
            r_ls = r_btc - r_basket
        
        # Costs (only on rebalance dates)
        if trade_date == current_rebal_date and current_weights:
            if not prev_weights:
                # First rebalance: full cost
                turnover = 1.0
            else:
                # Subsequent rebalances: cost based on turnover
                turnover = compute_turnover(
                    pd.Series(prev_weights),
                    pd.Series(current_weights)
                )
            cost = (fee_bps + slippage_bps) / 10000.0 * turnover
        else:
            cost = 0.0
        
        # Net return
        if pd.isna(r_ls):
            r_ls_net = np.nan
        else:
            r_ls_net = r_ls - cost
        
        # Store results
        results.append({
            "date": trade_date,
            "r_btc": r_btc,
            "r_basket": r_basket,
            "r_ls": r_ls,
            "cost": cost,
            "r_ls_net": r_ls_net,
        })
    
    # Create results DataFrame
    results_df = pd.DataFrame(results)
    
    # Compute equity curve
    results_df["equity_curve"] = (1.0 + results_df["r_ls_net"].fillna(0.0)).cumprod()
    
    # Save results
    output_dir.mkdir(parents=True, exist_ok=True)
    results_path = output_dir / "backtest_results.csv"
    results_df.to_csv(results_path, index=False)
    
    # Save rebalance turnover data
    turnover_df = pd.DataFrame(rebalance_turnover)
    turnover_path = output_dir / "rebalance_turnover.csv"
    turnover_df.to_csv(turnover_path, index=False)
    
    # Compute basket concentration (last rebalance window)
    # Only compute if we have valid results
    if len(rebalance_dates) > 0 and len(results_df) > 0 and len(current_weights) > 0:
        last_rebal_date = rebalance_dates[-1]
        last_weights = {k: v for k, v in current_weights.items()}
        concentration_report = compute_concentration_report(
            last_weights, last_rebal_date, asset_contributions if asset_contributions else None
        )
    else:
        concentration_report = None
    
    print(f"\n[SUCCESS] Backtest complete!")
    print(f"  Results saved to {results_path}")
    print(f"  Turnover data saved to {turnover_path}")
    
    # Generate report (pass prices_df for coverage stats, concentration report)
    generate_report(results_df, turnover_df, config, prices_df, output_dir / "report.md", concentration_report)
    
    # Return metadata for run_metadata.json
    return {
        "row_count": len(results_df),
        "date_range": {
            "start_date": str(start_date),
            "end_date": str(end_date),
        },
        "num_trading_days": len(results_df),
        "num_rebalance_dates": len(rebalance_dates),
        "backtest_assumptions": {
            "gap_fill_mode": gap_fill_mode,
            "min_history_days": min_history_days,
            "max_missing_frac": max_missing_frac,
            "max_consecutive_missing_days": max_consecutive_missing_days,
            "basket_coverage_threshold": basket_coverage_threshold,
            "missing_price_policy": missing_price_policy,
            "lookback_window_days": lookback_window_days,
        },
    }


def compute_concentration_report(
    weights: Dict[str, float], 
    rebalance_date: date,
    asset_contributions: Dict[str, float] = None
) -> Dict[str, Any]:
    """Compute concentration metrics for the last rebalance window."""
    if not weights:
        return None
    
    # Sort by weight descending
    sorted_weights = sorted(weights.items(), key=lambda x: x[1], reverse=True)
    
    # Top 5 contributors by weight
    top_5_weights = sorted_weights[:5]
    
    # Top 5 contributors by PnL (if contributions available)
    top_5_pnl = None
    if asset_contributions:
        sorted_pnl = sorted(asset_contributions.items(), key=lambda x: abs(x[1]), reverse=True)
        top_5_pnl = sorted_pnl[:5]
        # Include both absolute contribution and percentage of total
        total_pnl = sum(abs(c) for c in asset_contributions.values())
        if total_pnl > 0:
            top_5_pnl = [
                (symbol, contrib, abs(contrib) / total_pnl * 100.0) 
                for symbol, contrib in sorted_pnl[:5]
            ]
        else:
            top_5_pnl = [(symbol, contrib, 0.0) for symbol, contrib in sorted_pnl[:5]]
    
    # Concentration metrics
    top_5_weight = sum(w for _, w in top_5_weights)
    top_10_weight = sum(w for _, w in sorted_weights[:10])
    herfindahl_index = sum(w ** 2 for w in weights.values())
    
    return {
        "rebalance_date": rebalance_date,
        "top_5_contributors_by_weight": top_5_weights,
        "top_5_contributors_by_pnl": top_5_pnl,
        "top_5_weight": top_5_weight,
        "top_10_weight": top_10_weight,
        "herfindahl_index": herfindahl_index,
        "num_constituents": len(weights),
    }


def generate_report(results_df: pd.DataFrame, turnover_df: pd.DataFrame, config: Dict, prices_df: pd.DataFrame, output_path: Path, concentration_report: Dict[str, Any] = None) -> None:
    """Generate summary report."""
    # Calculate metrics
    returns = results_df["r_ls_net"].dropna()
    
    if len(returns) == 0:
        print("[ERROR] No valid returns to report")
        return
    
    total_return = (1.0 + returns).prod() - 1.0
    annualized_return = (1.0 + total_return) ** (252 / len(returns)) - 1.0
    
    # Sharpe ratio (assuming 252 trading days)
    if returns.std() > 0:
        sharpe = np.sqrt(252) * returns.mean() / returns.std()
    else:
        sharpe = 0.0
    
    # Max drawdown
    equity = results_df["equity_curve"]
    running_max = equity.cummax()
    drawdown = (equity / running_max) - 1.0
    max_dd = drawdown.min()
    
    # Turnover (approximate from costs)
    total_costs = results_df["cost"].sum()
    avg_turnover = total_costs / ((config["cost_model"]["fee_bps"] + config["cost_model"]["slippage_bps"]) / 10000.0) if (config["cost_model"]["fee_bps"] + config["cost_model"]["slippage_bps"]) > 0 else 0.0
    
    # Calculate turnover stats from turnover_df
    avg_turnover_per_rebal = turnover_df["turnover"].mean() if len(turnover_df) > 0 else 0.0
    avg_entered = turnover_df["entered_count"].mean() if len(turnover_df) > 0 else 0.0
    avg_exited = turnover_df["exited_count"].mean() if len(turnover_df) > 0 else 0.0
    
    # Calculate coverage stats (missing data rate in prices)
    total_cells = prices_df.shape[0] * prices_df.shape[1] if len(prices_df) > 0 else 0
    missing_cells = prices_df.isna().sum().sum() if len(prices_df) > 0 else 0
    coverage_pct = (1.0 - (missing_cells / total_cells)) * 100.0 if total_cells > 0 else 0.0
    symbols_with_missing = (prices_df.isna().sum(axis=0) > 0).sum() if len(prices_df) > 0 else 0
    
    # Write report
    with open(output_path, "w") as f:
        f.write("# Backtest Report\n\n")
        f.write(f"**Strategy**: {config.get('strategy_name', 'N/A')}\n\n")
        f.write(f"**Period**: {config['start_date']} to {config['end_date']}\n\n")
        f.write(f"**Rebalancing**: {config['rebalance_frequency']}\n\n")
        f.write("## Performance Metrics\n\n")
        f.write(f"- **Total Return**: {total_return:.2%}\n")
        f.write(f"- **Annualized Return**: {annualized_return:.2%}\n")
        f.write(f"- **Annualized Sharpe Ratio**: {sharpe:.2f}\n")
        f.write(f"- **Maximum Drawdown**: {max_dd:.2%}\n")
        f.write(f"- **Average Turnover**: {avg_turnover:.2%}\n")
        f.write(f"- **Average Turnover Per Rebalance**: {avg_turnover_per_rebal:.2%}\n")
        f.write(f"- **Average Entered Per Rebalance**: {avg_entered:.1f} coins\n")
        f.write(f"- **Average Exited Per Rebalance**: {avg_exited:.1f} coins\n")
        f.write(f"- **Total Trading Days**: {len(returns)}\n")
        f.write(f"- **Win Rate**: {(returns > 0).sum() / len(returns):.2%}\n\n")
        
        f.write("## Turnover Details\n\n")
        if len(turnover_df) > 0:
            f.write(f"- **Max Turnover**: {turnover_df['turnover'].max():.2%}\n")
            f.write(f"- **Min Turnover**: {turnover_df['turnover'].min():.2%}\n")
            f.write(f"- **Median Turnover**: {turnover_df['turnover'].median():.2%}\n")
        f.write("\n")
        
        f.write("## Basket Concentration Analysis\n\n")
        
        if concentration_report:
            f.write(f"**Rebalance Date**: {concentration_report['rebalance_date']}\n\n")
            f.write(f"**Number of Constituents**: {concentration_report['num_constituents']}\n\n")
            
            # Top 5 by PnL (most important for spotting outliers)
            if concentration_report.get('top_5_contributors_by_pnl'):
                f.write("### Top 5 PnL Contributors (cumulative contribution to returns)\n\n")
                f.write("| Symbol | Contribution | % of Total |\n")
                f.write("|--------|--------------|------------|\n")
                for item in concentration_report['top_5_contributors_by_pnl']:
                    if len(item) == 3:
                        symbol, contrib, pct = item
                        f.write(f"| **{symbol}** | {contrib:.4%} | {pct:.1f}% |\n")
                    else:
                        symbol, contrib = item
                        f.write(f"| **{symbol}** | {contrib:.4%} | - |\n")
                f.write("\n")
            
            # Top 5 by weight
            f.write("### Top 5 Contributors (by portfolio weight)\n\n")
            for symbol, weight in concentration_report['top_5_contributors_by_weight']:
                f.write(f"- **{symbol}**: {weight:.2%}\n")
            f.write("\n")
            
            # Concentration metrics
            f.write("### Concentration Metrics\n\n")
            f.write(f"- **Top 5 Weight**: {concentration_report['top_5_weight']:.2%}\n")
            f.write(f"- **Top 10 Weight**: {concentration_report['top_10_weight']:.2%}\n")
            f.write(f"- **Herfindahl Index**: {concentration_report['herfindahl_index']:.4f}\n")
            f.write(f"  - *Interpretation: Higher values indicate higher concentration (max = 1.0 for single asset)*\n\n")
        else:
            f.write("*Note: No rebalance data available for concentration analysis.*\n\n")
        
        f.write("## Cost Sensitivity\n\n")
        f.write(f"- **Fee Rate**: {config['cost_model']['fee_bps']} bps\n")
        f.write(f"- **Slippage Rate**: {config['cost_model']['slippage_bps']} bps\n")
        f.write(f"- **Total Cost Impact**: {total_costs:.2%} of initial capital\n")
        f.write(f"- **Average Cost Per Rebalance**: {total_costs / len(turnover_df) if len(turnover_df) > 0 else 0:.4%}\n\n")
        
        f.write("## Data Coverage\n\n")
        f.write(f"- **Overall Coverage**: {coverage_pct:.2f}% (non-NA data points)\n")
        f.write(f"- **Total Price Data Points**: {total_cells:,}\n")
        f.write(f"- **Missing Data Points**: {missing_cells:,}\n")
        f.write(f"- **Symbols with Missing Data**: {symbols_with_missing} out of {prices_df.shape[1] if len(prices_df) > 0 else 0}\n\n")
    
    print(f"  Report saved to {output_path}")

