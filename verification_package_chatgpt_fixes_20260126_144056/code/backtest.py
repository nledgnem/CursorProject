"""Walk-forward backtest engine."""

import polars as pl
import numpy as np
from typing import Dict, List, Optional
from datetime import date, timedelta
import logging

logger = logging.getLogger(__name__)


class BacktestEngine:
    """Vectorized walk-forward backtest engine."""
    
    def __init__(
        self,
        maker_fee_bps: float = 2.0,
        taker_fee_bps: float = 5.0,
        slippage_bps: float = 5.0,
        slippage_adv_multiplier: float = 0.1,
        funding_enabled: bool = True,
        funding_8h_rate: bool = True,
        vol_target: Optional[float] = None,
        regime_position_scaling: Optional[Dict] = None,
        risk_management: Optional[Dict] = None,
        rebalance_frequency_days: Optional[int] = None,  # Fixed schedule rebalancing (None = dynamic)
    ):
        """
        Initialize backtest engine.
        
        Args:
            maker_fee_bps: Maker fee in basis points
            taker_fee_bps: Taker fee in basis points
            slippage_bps: Base slippage in basis points
            slippage_adv_multiplier: Slippage scales with ADV
            funding_enabled: Apply funding carry
            funding_8h_rate: Funding rates are 8-hourly (3x per day)
            vol_target: Optional target volatility (annualized)
        """
        self.maker_fee_bps = maker_fee_bps / 10000.0
        self.taker_fee_bps = taker_fee_bps / 10000.0
        self.slippage_bps = slippage_bps / 10000.0
        self.slippage_adv_multiplier = slippage_adv_multiplier
        self.funding_enabled = funding_enabled
        self.funding_8h_rate = funding_8h_rate
        self.vol_target = vol_target
        self.regime_position_scaling = regime_position_scaling or {}
        self.regime_scaling_enabled = self.regime_position_scaling.get("enabled", False)
        
        # Risk management
        self.risk_management = risk_management or {}
        self.stop_loss_config = self.risk_management.get("stop_loss", {})
        self.stop_loss_enabled = self.stop_loss_config.get("enabled", False)
        self.stop_loss_threshold = self.stop_loss_config.get("daily_loss_threshold", -0.05)
        
        self.vol_targeting_config = self.risk_management.get("volatility_targeting", {})
        self.vol_targeting_enabled = self.vol_targeting_config.get("enabled", False)
        self.vol_target_annual = self.vol_targeting_config.get("target_volatility", 0.20)
        self.vol_lookback = self.vol_targeting_config.get("lookback_days", 20)
        self.vol_min_scale = self.vol_targeting_config.get("min_scale", 0.1)
        self.vol_max_scale = self.vol_targeting_config.get("max_scale", 1.0)
        
        self.trailing_stop_config = self.risk_management.get("trailing_stop", {})
        self.trailing_stop_enabled = self.trailing_stop_config.get("enabled", False)
        self.trailing_stop_threshold = self.trailing_stop_config.get("drawdown_threshold", -0.15)
        self.trailing_stop_lookback = self.trailing_stop_config.get("lookback_days", 20)
        
        self.take_profit_config = self.risk_management.get("take_profit", {})
        self.take_profit_enabled = self.take_profit_config.get("enabled", False)
        self.take_profit_threshold = self.take_profit_config.get("profit_threshold", 0.10)
        self.take_profit_time_days = self.take_profit_config.get("time_based_exit_days", 30)
        self.take_profit_partial = self.take_profit_config.get("partial_profit_taking", False)
        
        # Fixed schedule rebalancing (for Pure MSM mode)
        self.rebalance_frequency_days = rebalance_frequency_days
        self.last_rebalance_date = None
    
    def run_backtest(
        self,
        prices: pl.DataFrame,
        marketcap: pl.DataFrame,
        volume: pl.DataFrame,
        funding: Optional[pl.DataFrame],
        features: pl.DataFrame,
        regime_series: pl.DataFrame,
        alt_basket_builder,
        beta_estimator,
        neutrality_solver,
        start_date: date,
        end_date: date,
        walk_forward: bool = True,
        lookback_window_days: int = 252,
        test_window_days: int = 63,
    ) -> pl.DataFrame:
        """
        Run walk-forward backtest.
        
        Args:
            prices: (asset_id, date, close)
            marketcap: (asset_id, date, marketcap)
            volume: (asset_id, date, volume)
            funding: Optional (asset_id, date, funding_rate)
            features: Feature DataFrame
            regime_series: (date, regime, score)
            alt_basket_builder: Function to build ALT basket
            beta_estimator: Function to estimate betas
            start_date: Backtest start date
            end_date: Backtest end date
            walk_forward: Enable walk-forward validation
            lookback_window_days: Lookback window length (burn-in period for features, not training)
            test_window_days: Test window length
        
        Returns:
            DataFrame with daily PnL and positions
        """
        # Get date range
        dates = (
            prices
            .filter(
                (pl.col("date") >= pl.date(start_date.year, start_date.month, start_date.day)) &
                (pl.col("date") <= pl.date(end_date.year, end_date.month, end_date.day))
            )
            .select("date")
            .unique()
            .sort("date")
        )
        
        if walk_forward:
            # Walk-forward windows
            n_windows = (len(dates) - lookback_window_days) // test_window_days
            logger.info(f"Walk-forward backtest: {n_windows} windows")
            
            results = []
            
            for window_idx in range(n_windows):
                train_start_idx = window_idx * test_window_days
                train_end_idx = train_start_idx + lookback_window_days
                test_end_idx = train_end_idx + test_window_days
                
                if test_end_idx > len(dates):
                    break
                
                test_dates = dates.slice(train_end_idx, test_end_idx - train_end_idx)
                
                logger.info(f"Window {window_idx + 1}/{n_windows}: {test_dates['date'].min()} to {test_dates['date'].max()}")
                
                # Run backtest for this window
                window_results = self._run_window(
                    prices, marketcap, volume, funding,
                    features, regime_series,
                    alt_basket_builder, beta_estimator, neutrality_solver,
                    test_dates,
                )
                
                results.append(window_results)
            
            # Combine results
            if results:
                return pl.concat(results)
            else:
                return pl.DataFrame()
        else:
            # Single backtest
            return self._run_window(
                prices, marketcap, volume, funding,
                features, regime_series,
                alt_basket_builder, beta_estimator, neutrality_solver,
                dates,
            )
    
    def _run_window(
        self,
        prices: pl.DataFrame,
        marketcap: pl.DataFrame,
        volume: pl.DataFrame,
        funding: Optional[pl.DataFrame],
        features: pl.DataFrame,
        regime_series: pl.DataFrame,
        alt_basket_builder,
        beta_estimator,
        neutrality_solver,
        dates: pl.DataFrame,
    ) -> pl.DataFrame:
        """Run backtest for a single window."""
        results = []
        
        # Current positions
        alt_weights = {}
        major_weights = {"BTC": 0.0, "ETH": 0.0}
        
        # Risk management state
        equity_curve = []  # Track equity for trailing stop (will be initialized on first result)
        prev_date = None
        recent_returns = []  # Track recent returns for stop-loss
        initial_equity = 1.0
        
        # Take-profit state: track position entry date and entry equity
        position_entry_date = None
        position_entry_equity = 1.0
        
        # Dynamic rebalancing state
        prev_regime = None
        prev_score = None
        
        for row in dates.iter_rows(named=True):
            current_date = row["date"]
            
            # Get regime for this date
            regime_row = regime_series.filter(pl.col("date") == current_date)
            if len(regime_row) == 0:
                continue
            
            regime = regime_row["regime"][0]
            score = regime_row["score"][0]
            
            # Rebalancing logic: fixed schedule (MSM) or dynamic (strategy)
            needs_rebalance = False
            if self.rebalance_frequency_days is not None:
                # Fixed schedule rebalancing (Pure MSM mode)
                if prev_date is None:
                    needs_rebalance = True  # First day
                elif self.last_rebalance_date is None:
                    needs_rebalance = True  # First rebalance
                else:
                    days_since_rebalance = (current_date - self.last_rebalance_date).days
                    if days_since_rebalance >= self.rebalance_frequency_days:
                        needs_rebalance = True
            else:
                # Dynamic rebalancing: only rebalance if needed (strategy mode)
                if prev_regime is None:
                    needs_rebalance = True  # First day
                elif regime != prev_regime:
                    needs_rebalance = True  # Regime changed
                elif prev_score is not None and abs(score - prev_score) > 0.3:
                    needs_rebalance = True  # Score moved significantly (> 0.3)
                else:
                    # Check position drift (only if we have positions)
                    if len(alt_weights) > 0 or sum(abs(w) for w in major_weights.values()) > 0.01:
                        # Compute expected positions (would be computed if we rebalanced)
                        # For now, skip drift check to avoid complexity
                        # In practice, you'd compute expected positions and compare
                        needs_rebalance = False  # Don't rebalance if no significant change
            
            # Check if we should trade (regime gating)
            # With 5 regimes: trade in STRONG_RISK_ON_MAJORS and WEAK_RISK_ON_MAJORS
            #   Exit when moving to BALANCED or worse (earlier exit signal)
            # With 3 regimes: only trade in RISK_ON_MAJORS
            # Exit positions when not in trading regime to avoid holding during bad regimes
            should_trade = (
                regime == "STRONG_RISK_ON_MAJORS" or 
                regime == "WEAK_RISK_ON_MAJORS" or 
                regime == "RISK_ON_MAJORS"
            )
            
            # Only rebalance if needed (dynamic rebalancing)
            if should_trade and not needs_rebalance:
                # Keep existing positions, skip rebalancing
                should_trade = False  # Don't recompute positions
            
            # Check stop-loss BEFORE computing new positions
            # CRITICAL FIX: Close positions immediately when stop-loss triggers
            stop_loss_triggered = False
            if self.stop_loss_enabled and prev_date is not None and len(recent_returns) > 0:
                # Volatility-adjusted stop-loss: scale threshold by realized volatility
                if len(recent_returns) >= 5:
                    recent_vol = np.std(recent_returns[-5:]) * np.sqrt(252)  # Annualized vol
                    vol_adjusted_threshold = self.stop_loss_threshold * (recent_vol / 0.20)  # Scale by vol / 20% target
                    vol_adjusted_threshold = max(self.stop_loss_threshold * 0.5, min(vol_adjusted_threshold, self.stop_loss_threshold * 2.0))  # Clamp between 0.5x and 2x
                    # Cap at -7.5% max to prevent excessive losses
                    vol_adjusted_threshold = max(vol_adjusted_threshold, -0.075)
                else:
                    vol_adjusted_threshold = self.stop_loss_threshold
                
                # Check both single-day and cumulative loss over multiple days
                lookback_days = self.stop_loss_config.get("lookback_days", 1)
                single_day_loss = recent_returns[-1] if len(recent_returns) > 0 else 0.0
                cumulative_loss = sum(recent_returns[-lookback_days:])
                # Also check 3-day cumulative loss (more robust)
                cumulative_loss_3d = sum(recent_returns[-3:]) if len(recent_returns) >= 3 else cumulative_loss
                
                # Trigger if single-day loss exceeds threshold OR cumulative loss exceeds threshold
                if (single_day_loss < vol_adjusted_threshold or 
                    cumulative_loss < vol_adjusted_threshold or
                    cumulative_loss_3d < (vol_adjusted_threshold * 1.5)):  # 3-day cumulative threshold is 1.5x single-day
                    logger.info(f"Stop-loss triggered on {current_date}: single_day={single_day_loss*100:.2f}%, cumulative_{lookback_days}d={cumulative_loss*100:.2f}%, cumulative_3d={cumulative_loss_3d*100:.2f}% (threshold = {vol_adjusted_threshold*100:.2f}%)")
                    should_trade = False  # Exit positions
                    position_entry_date = None  # Reset position tracking
                    stop_loss_triggered = True
                    # CRITICAL FIX: Close old positions immediately BEFORE computing PnL
                    alt_weights = {}  # Close ALT positions
                    major_weights = {"BTC": 0.0, "ETH": 0.0}  # Close major positions
            
            # Check take-profit BEFORE computing new positions
            if self.take_profit_enabled and position_entry_date is not None:
                # Check profit threshold
                if len(equity_curve) > 0:
                    current_equity = equity_curve[-1]
                    position_return = (current_equity - position_entry_equity) / position_entry_equity
                    
                    if position_return >= self.take_profit_threshold:
                        if self.take_profit_partial:
                            # Partial profit-taking: reduce position by 50%
                            logger.info(f"Take-profit (partial) triggered on {current_date}: return = {position_return*100:.2f}%")
                            # We'll handle partial reduction in the position sizing logic
                        else:
                            # Full exit
                            logger.info(f"Take-profit triggered on {current_date}: return = {position_return*100:.2f}%")
                            should_trade = False  # Exit positions
                            position_entry_date = None  # Reset position tracking
                
                # Check time-based exit
                if position_entry_date is not None:
                    days_held = (current_date - position_entry_date).days
                    if days_held >= self.take_profit_time_days:
                        logger.info(f"Time-based exit triggered on {current_date}: held for {days_held} days")
                        should_trade = False  # Exit positions
                        position_entry_date = None  # Reset position tracking
            
            # Check trailing stop (only if we have equity history)
            if self.trailing_stop_enabled and len(equity_curve) >= self.trailing_stop_lookback:
                current_equity = equity_curve[-1] if equity_curve else initial_equity
                lookback_window = equity_curve[-self.trailing_stop_lookback:] if len(equity_curve) >= self.trailing_stop_lookback else equity_curve
                if len(lookback_window) > 0:
                    peak_equity = max(lookback_window)
                    drawdown = (current_equity - peak_equity) / peak_equity
                    if drawdown < self.trailing_stop_threshold:
                        logger.info(f"Trailing stop triggered on {current_date}: drawdown = {drawdown*100:.2f}%")
                        should_trade = False  # Exit positions
            
            if should_trade:
                # Build ALT basket (PIT) - this returns weights summing to 1.0 (100%)
                alt_weights_new_raw = alt_basket_builder(current_date)
                
                # CRITICAL FIX: Scale ALT weights to 50% BEFORE solving for neutrality
                # This ensures total gross exposure is capped at ~100% (50% short + 50% long)
                alt_total_raw = sum(abs(w) for w in alt_weights_new_raw.values())
                if alt_total_raw > 0:
                    alt_scale = 0.5 / alt_total_raw  # Scale to 50%
                    alt_weights_new = {k: v * alt_scale for k, v in alt_weights_new_raw.items()}
                else:
                    alt_weights_new = alt_weights_new_raw
                
                # Estimate betas for ALTs (use original weights for beta estimation)
                alt_betas = {}
                for alt_id in alt_weights_new_raw.keys():  # Use raw weights for beta estimation
                    alt_betas[alt_id] = beta_estimator(alt_id, current_date)
                
                # Size majors to achieve neutrality using solver
                # Solver returns combined dict with both ALT and major weights
                combined_weights_new = neutrality_solver(alt_weights_new, alt_betas)
                
                # Get regime-based position scaling factor
                regime_scale = self._get_regime_scaling_factor(regime, score)
                
                # Apply volatility targeting if enabled
                vol_scale = 1.0
                if self.vol_targeting_enabled and prev_date is not None:
                    vol_scale = self._get_volatility_scaling_factor(
                        results, prices, current_date, prev_date
                    )
                
                # Apply all scaling factors AFTER solver
                total_scale = regime_scale * vol_scale
                if total_scale < 1.0:
                    combined_weights_new = {k: v * total_scale for k, v in combined_weights_new.items()}
                
                # Separate ALT and major weights from combined dict
                # The solver returns weights where ALT are negative (short) and majors are positive (long)
                alt_weights_final = {k: v for k, v in combined_weights_new.items() if k not in ["BTC", "ETH"]}
                major_weights_new = {k: v for k, v in combined_weights_new.items() if k in ["BTC", "ETH"]}
            else:
                # Exit positions when not in RISK_ON_MAJORS regime or risk management triggered
                alt_weights_final = {}
                major_weights_new = {"BTC": 0.0, "ETH": 0.0}
            
            # Compute returns (always compute PnL, even if we didn't trade)
            if prev_date is not None:
                # Log position sizes for debugging (on worst days)
                alt_gross_prev = sum(abs(w) for w in alt_weights.values())
                major_gross_prev = sum(abs(w) for w in major_weights.values())
                total_gross_prev = alt_gross_prev + major_gross_prev
                
                alt_gross = sum(abs(w) for w in alt_weights_final.values())
                major_gross = sum(abs(w) for w in major_weights_new.values())
                total_gross = alt_gross + major_gross
                
                # Compute portfolio return
                # CRITICAL FIX: If stop-loss triggered, compute PnL with zero positions (we closed at prev close)
                if stop_loss_triggered:
                    # Use zero positions for PnL computation (positions were closed at prev_date close)
                    pnl = self._compute_daily_pnl(
                        prices, funding,
                        {}, {"BTC": 0.0, "ETH": 0.0},  # Zero positions (closed at prev close)
                        alt_weights_final, major_weights_new,
                        prev_date, current_date,
                    )
                else:
                    pnl = self._compute_daily_pnl(
                        prices, funding,
                        alt_weights, major_weights,
                        alt_weights_final, major_weights_new,
                        prev_date, current_date,
                    )
                
                # Warn if gross exposure is too high
                if total_gross > 1.5:
                    logger.warning(f"High gross exposure on {current_date}: {total_gross:.2f} (ALT: {alt_gross:.2f}, Major: {major_gross:.2f})")
                
                # Update equity curve and recent returns for risk management
                if len(equity_curve) == 0:
                    current_equity = initial_equity * (1.0 + pnl["r_ls_net"])
                else:
                    current_equity = equity_curve[-1] * (1.0 + pnl["r_ls_net"])
                equity_curve.append(current_equity)
                recent_returns.append(pnl["r_ls_net"])
                # Keep only recent returns for stop-loss
                if len(recent_returns) > self.stop_loss_config.get("lookback_days", 1):
                    recent_returns = recent_returns[-self.stop_loss_config.get("lookback_days", 1):]
                
                # Track position entry for take-profit
                # If we just entered a position (have positions now but didn't before), record entry
                if position_entry_date is None and total_gross > 0.01 and total_gross_prev < 0.01:
                    position_entry_date = current_date
                    position_entry_equity = current_equity
                # If we exited positions, reset tracking
                elif position_entry_date is not None and total_gross < 0.01:
                    position_entry_date = None
                
                # Track position entry for take-profit
                # If we just entered a position (have positions now but didn't before), record entry
                prev_total_gross = sum(abs(w) for w in alt_weights.values()) + sum(abs(w) for w in major_weights.values())
                if position_entry_date is None and total_gross > 0.01 and prev_total_gross < 0.01:
                    position_entry_date = current_date
                    position_entry_equity = current_equity
                # If we exited positions, reset tracking
                elif position_entry_date is not None and total_gross < 0.01:
                    position_entry_date = None
                
                results.append({
                    "date": current_date,
                    "regime": regime,
                    "score": float(score) if score is not None else 0.0,
                    "pnl": float(pnl["pnl"]),
                    "cost": float(pnl["cost"]),
                    "funding": float(pnl["funding"]),
                    "r_ls_gross": float(pnl.get("r_ls_gross", pnl["pnl"])),
                    "r_ls_net": float(pnl["r_ls_net"]),
                    "alt_turnover": float(pnl["alt_turnover"]),
                    "major_turnover": float(pnl["major_turnover"]),
                    "alt_gross": float(alt_gross),
                    "major_gross": float(major_gross),
                    "total_gross": float(total_gross),
                })
            
            # Update positions
            alt_weights = alt_weights_final
            major_weights = major_weights_new
            prev_date = current_date
            prev_regime = regime
            prev_score = score
            
            # Update last rebalance date if we rebalanced
            if needs_rebalance and should_trade:
                self.last_rebalance_date = current_date
        
        return pl.DataFrame(results)
    
    def _size_majors_for_neutrality(
        self,
        alt_weights: Dict[str, float],
        alt_betas: Dict[str, Dict[str, float]],
        current_major_weights: Dict[str, float],
    ) -> Dict[str, float]:
        """Size majors to achieve approximate neutrality.
        
        CRITICAL: This function must ensure total gross exposure is reasonable.
        If ALT weights sum to 1.0 (100% short), major weights should be sized
        to offset beta exposure WITHOUT creating excessive gross exposure.
        """
        total_alt_exposure = sum(abs(w) for w in alt_weights.values())
        
        # Estimate total ALT beta exposure to BTC and ETH
        alt_btc_exp = sum(alt_weights.get(a, 0.0) * alt_betas.get(a, {}).get("BTC", 1.0) 
                         for a in alt_weights.keys())
        alt_eth_exp = sum(alt_weights.get(a, 0.0) * alt_betas.get(a, {}).get("ETH", 1.0) 
                         for a in alt_weights.keys())
        
        # Size majors to offset beta exposure, but cap total gross exposure
        # If alts have 100% short exposure with avg beta of 1.0 to BTC,
        # we need ~50% long BTC to neutralize (assuming equal BTC/ETH split)
        # But we should cap total gross at reasonable level (e.g., 150%)
        
        # Simple approach: size majors to roughly offset, but don't exceed alt exposure
        btc_weight = -alt_btc_exp * 0.5  # Offset 50% of BTC beta exposure
        eth_weight = -alt_eth_exp * 0.5  # Offset 50% of ETH beta exposure
        
        # Cap individual major weights to prevent extreme positions
        max_major_weight = 0.5  # Cap at 50% per major
        btc_weight = max(-max_major_weight, min(max_major_weight, btc_weight))
        eth_weight = max(-max_major_weight, min(max_major_weight, eth_weight))
        
        return {"BTC": btc_weight, "ETH": eth_weight}
    
    def _get_regime_scaling_factor(self, regime: str, score: Optional[float]) -> float:
        """
        Get position scaling factor based on regime confidence.
        
        Args:
            regime: Current regime name
            score: Current composite score (for score-based scaling)
        
        Returns:
            Scaling factor (0.0 to 1.0)
        """
        if not self.regime_scaling_enabled:
            return 1.0  # No scaling
        
        scaling_config = self.regime_position_scaling
        
        # Check if continuous scaling is enabled (default: True for better granularity)
        use_continuous_scaling = scaling_config.get("use_score_magnitude", True)
        
        # For 5 regimes: use regime-specific scaling or continuous
        if regime == "STRONG_RISK_ON_MAJORS":
            if use_continuous_scaling and score is not None:
                # Continuous scaling based on score magnitude
                threshold_strong_high = 1.5  # Default (could be passed as param)
                score_magnitude = abs(score) / max(threshold_strong_high, 0.1)
                score_magnitude = min(1.0, max(0.6, score_magnitude))  # Between 0.6 and 1.0
                return score_magnitude
            return scaling_config.get("strong_risk_on_majors", 1.0)
        elif regime == "WEAK_RISK_ON_MAJORS":
            if use_continuous_scaling and score is not None:
                # Continuous scaling: scale between 0.4 and 0.8 based on score
                threshold_high = 0.5  # Default threshold
                threshold_strong_high = 1.5
                # Interpolate between thresholds
                if abs(score) <= threshold_high:
                    scale = 0.4
                elif abs(score) >= threshold_strong_high:
                    scale = 0.8
                else:
                    # Linear interpolation
                    scale = 0.4 + (abs(score) - threshold_high) / (threshold_strong_high - threshold_high) * 0.4
                return scale
            return scaling_config.get("weak_risk_on_majors", 0.6)
        elif regime == "RISK_ON_MAJORS":
            # For 3-regime mode, use continuous scaling
            if use_continuous_scaling and score is not None:
                # Scale by score magnitude relative to threshold
                threshold_high = 0.5  # Default threshold (could be passed as param)
                score_magnitude = abs(score) / max(threshold_high, 0.1)  # Normalize
                score_magnitude = min(1.0, max(0.5, score_magnitude))  # Between 0.5 and 1.0
                return score_magnitude
            else:
                return scaling_config.get("risk_on_majors", 1.0)
        else:
            # Should not trade in other regimes, but return 0.0 as safety
            return 0.0
    
    def _get_volatility_scaling_factor(
        self,
        results: List[Dict],
        prices: pl.DataFrame,
        current_date: date,
        prev_date: date,
    ) -> float:
        """
        Get position scaling factor based on realized volatility.
        
        Args:
            results: List of previous results (for computing portfolio volatility)
            prices: Price DataFrame
            current_date: Current date
            prev_date: Previous date
        
        Returns:
            Scaling factor (min_scale to max_scale)
        """
        if not self.vol_targeting_enabled or len(results) < self.vol_lookback:
            return 1.0
        
        # Compute realized volatility from recent returns
        recent_returns = [r["r_ls_net"] for r in results[-self.vol_lookback:]]
        if len(recent_returns) < 2:
            return 1.0
        
        # Annualized volatility
        mean_ret = np.mean(recent_returns)
        std_ret = np.std(recent_returns)
        annualized_vol = std_ret * np.sqrt(252)
        
        # Scale inversely with volatility
        if annualized_vol > 0:
            vol_scale = self.vol_target_annual / annualized_vol
            vol_scale = max(self.vol_min_scale, min(self.vol_max_scale, vol_scale))
        else:
            vol_scale = 1.0
        
        return vol_scale
    
    def _compute_daily_pnl(
        self,
        prices: pl.DataFrame,
        funding: Optional[pl.DataFrame],
        alt_weights_old: Dict[str, float],
        major_weights_old: Dict[str, float],
        alt_weights_new: Dict[str, float],
        major_weights_new: Dict[str, float],
        prev_date: date,
        current_date: date,
    ) -> Dict:
        """Compute daily PnL including costs and funding."""
        # Get prices
        prices_prev = prices.filter(pl.col("date") == pl.date(prev_date.year, prev_date.month, prev_date.day))
        prices_curr = prices.filter(pl.col("date") == pl.date(current_date.year, current_date.month, current_date.day))
        
        # Compute returns
        pnl = 0.0
        cost = 0.0
        funding_cost = 0.0
        
        # ALT returns
        for alt_id, weight in alt_weights_old.items():
            price_prev = prices_prev.filter(pl.col("asset_id") == alt_id)["close"]
            price_curr = prices_curr.filter(pl.col("asset_id") == alt_id)["close"]
            
            if len(price_prev) > 0 and len(price_curr) > 0:
                ret = (price_curr[0] / price_prev[0]) - 1.0
                pnl += -weight * ret  # Short position
        
        # Major returns
        for major_id, weight in major_weights_old.items():
            price_prev = prices_prev.filter(pl.col("asset_id") == major_id)["close"]
            price_curr = prices_curr.filter(pl.col("asset_id") == major_id)["close"]
            
            if len(price_prev) > 0 and len(price_curr) > 0:
                ret = (price_curr[0] / price_prev[0]) - 1.0
                pnl += weight * ret  # Long position
        
        # Compute turnover and costs
        alt_turnover = sum(abs(alt_weights_new.get(a, 0.0) - alt_weights_old.get(a, 0.0)) 
                          for a in set(list(alt_weights_old.keys()) + list(alt_weights_new.keys())))
        major_turnover = sum(abs(major_weights_new.get(a, 0.0) - major_weights_old.get(a, 0.0)) 
                            for a in set(list(major_weights_old.keys()) + list(major_weights_new.keys())))
        
        total_turnover = alt_turnover + major_turnover
        
        # Costs (simplified: use taker fee)
        cost = total_turnover * self.taker_fee_bps
        
        # Funding (if enabled) - position-weighted per asset
        if self.funding_enabled and funding is not None:
            funding_prev = funding.filter(pl.col("date") == pl.date(prev_date.year, prev_date.month, prev_date.day))
            if len(funding_prev) > 0:
                # Position-weighted funding: sum(w_i * funding_i) for all positions
                # Short positions: receive funding (positive), long positions: pay funding (negative)
                funding_cost = 0.0
                
                # Position-weighted funding calculation
                # Convention: positive funding_rate means longs pay shorts
                # - Short positions: receive funding (positive contribution to PnL)
                # - Long positions: pay funding (negative contribution to PnL)
                
                # ALT funding (short positions receive funding)
                for alt_id, weight in alt_weights_old.items():
                    alt_funding = funding_prev.filter(pl.col("asset_id") == alt_id)["funding_rate"]
                    if len(alt_funding) > 0:
                        daily_funding = alt_funding[0]
                        if self.funding_8h_rate:
                            daily_funding = daily_funding * 3.0  # 3x per day
                        # Short position: receive funding (positive), weight is negative
                        # So: abs(weight) * daily_funding is positive (we receive)
                        funding_cost -= abs(weight) * daily_funding  # Negative cost = positive PnL
                
                # Major funding (long positions pay funding)
                for major_id, weight in major_weights_old.items():
                    major_funding = funding_prev.filter(pl.col("asset_id") == major_id)["funding_rate"]
                    if len(major_funding) > 0:
                        daily_funding = major_funding[0]
                        if self.funding_8h_rate:
                            daily_funding = daily_funding * 3.0  # 3x per day
                        # Long position: pay funding (negative), weight is positive
                        # So: abs(weight) * daily_funding is positive (we pay)
                        funding_cost += abs(weight) * daily_funding  # Positive cost = negative PnL
        
        # Gross return (before costs and funding)
        r_ls_gross = pnl
        
        # Net return (after costs and funding)
        r_ls_net = pnl - cost - funding_cost
        
        return {
            "pnl": pnl,
            "cost": cost,
            "funding": funding_cost,
            "r_ls_gross": r_ls_gross,
            "r_ls_net": r_ls_net,
            "alt_turnover": alt_turnover,
            "major_turnover": major_turnover,
        }
