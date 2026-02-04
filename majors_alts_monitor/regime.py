"""Regime modeling (composite score + unsupervised)."""

import polars as pl
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import date
from itertools import product
import logging

logger = logging.getLogger(__name__)


class RegimeModel:
    """Regime model with composite score or unsupervised methods."""
    
    def __init__(
        self,
        mode: str = "composite",
        default_weights: Optional[Dict[str, float]] = None,
        threshold_low: float = -0.5,
        threshold_high: float = 0.5,
        threshold_strong_low: float = -1.5,
        threshold_strong_high: float = 1.5,
        hysteresis_low: float = -0.3,
        hysteresis_high: float = 0.3,
        n_regimes: int = 3,  # 3 or 5 regimes
    ):
        """
        Initialize regime model.
        
        Args:
            mode: "composite" or "unsupervised"
            default_weights: Default feature weights for composite mode
            threshold_low: Lower threshold for regime classification
            threshold_high: Upper threshold for regime classification
            threshold_strong_low: Strong lower threshold (for 5-regime mode)
            threshold_strong_high: Strong upper threshold (for 5-regime mode)
            hysteresis_low: Hysteresis band around lower threshold
            hysteresis_high: Hysteresis band around upper threshold
            n_regimes: Number of regimes (3 or 5)
        """
        self.mode = mode
        self.default_weights = default_weights or {}
        self.threshold_low = threshold_low
        self.threshold_high = threshold_high
        self.threshold_strong_low = threshold_strong_low
        self.threshold_strong_high = threshold_strong_high
        self.hysteresis_low = hysteresis_low
        self.hysteresis_high = hysteresis_high
        self.n_regimes = n_regimes
        
        # State for hysteresis
        self.current_regime = None
    
    def compute_composite_score(
        self,
        features: pl.DataFrame,
        weights: Optional[Dict[str, float]] = None,
        prices: Optional[pl.DataFrame] = None,  # Added for high-vol gate
    ) -> pl.DataFrame:
        """
        Compute composite regime score.
        
        Args:
            features: Feature DataFrame with date and feature columns
            weights: Feature weights (defaults to self.default_weights)
            prices: Optional price DataFrame for high-vol gate (needs BTC for 7d return)
        
        Returns:
            DataFrame with date, score, and regime columns
        """
        if weights is None:
            weights = self.default_weights
        
        # Get z-scored features
        z_cols = [c for c in features.columns if c.startswith("z_")]
        
        # Feature name mapping (config name -> z-scored column to use)
        # Use the primary feature from each group
        feat_mapping = {
            "alt_breadth": "z_alt_breadth_pct_up",  # Use % up as primary
            "btc_dominance": "z_btc_dominance",  # Use main dominance metric
            "funding_skew": "z_funding_skew",
            "funding_heating": "z_funding_heating",  # New: funding heating
            "liquidity": "z_liquidity_7d_median",  # Use 7d median as primary
            "volatility_spread": "z_volatility_spread",
            "momentum": "z_momentum_spread_7d",  # Use 7d spread as primary
            "oi_risk": "z_oi_risk",  # New: OI risk
        }
        
        # Compute weighted sum
        score = pl.lit(0.0)
        for feat_name, weight in weights.items():
            # Try direct mapping first
            z_col = feat_mapping.get(feat_name)
            
            # If not in mapping, try to find by substring
            if not z_col:
                for z in z_cols:
                    if feat_name.lower() in z.lower():
                        z_col = z
                        break
            
            if z_col and z_col in features.columns:
                # Treat missing feature values as 0 contribution (neutral),
                # instead of nulling the entire composite score.
                score = score + pl.col(z_col).fill_null(0.0) * weight
            else:
                logger.warning(f"Feature {feat_name} not found (tried {z_col}, available: {[c for c in z_cols if feat_name.lower() in c.lower()][:3]})")
        
        # Compute score on full features DataFrame, then select
        result = features.with_columns([
            score.alias("score"),
        ]).select(["date", "score"])
        
        # Fill NaN scores with 0 (neutral)
        result = result.with_columns([
            pl.col("score").fill_null(0.0).alias("score")
        ])
        
        # Apply high-vol gate if prices provided (from legacy monitor)
        # When BTC 7d return > 15%, cap regime score to prevent overconfidence
        if prices is not None:
            # Compute BTC 7d return
            btc_prices = prices.filter(pl.col("asset_id") == "BTC").sort("date")
            btc_7d_ret = (
                btc_prices
                .with_columns([
                    (pl.col("close") / pl.col("close").shift(7) - 1.0).alias("btc_7d_ret")
                ])
                .select(["date", "btc_7d_ret"])
            )
            
            # Join with result and apply high-vol gate
            result = result.join(btc_7d_ret, on="date", how="left")
            # Cap score at threshold_high (neutral-positive) when BTC vol > 15%
            # This prevents false signals during extreme moves
            result = result.with_columns([
                pl.when(
                    (pl.col("btc_7d_ret").abs() > 0.15) & (pl.col("score") > self.threshold_high)
                )
                .then(pl.lit(float(self.threshold_high)))  # Cap at threshold_high (neutral-positive)
                .otherwise(pl.col("score"))
                .alias("score")
            ])
            result = result.select(["date", "score"])
        
        # Classify regimes with hysteresis
        result = self._classify_regimes(result)
        
        return result
    
    def _classify_regimes(self, scores: pl.DataFrame) -> pl.DataFrame:
        """Classify regimes from scores using hysteresis."""
        if self.n_regimes == 5:
            return self._classify_regimes_5(scores)
        else:
            return self._classify_regimes_3(scores)
    
    def _classify_regimes_3(self, scores: pl.DataFrame) -> pl.DataFrame:
        """Classify 3 regimes: RISK_ON_ALTS, BALANCED, RISK_ON_MAJORS with persistence."""
        regimes = []
        current_regime = None
        regime_start_idx = None  # Track when current regime started
        min_regime_duration = 3  # Minimum days before allowing switch (persistence)
        
        for idx, row in enumerate(scores.iter_rows(named=True)):
            score = row["score"]
            
            # Handle None/NaN scores
            if score is None or (isinstance(score, float) and (score != score)):  # NaN check
                score = 0.0
            
            if current_regime is None:
                # First observation
                if score < self.threshold_low:
                    current_regime = "RISK_ON_ALTS"
                elif score > self.threshold_high:
                    current_regime = "RISK_ON_MAJORS"
                else:
                    current_regime = "BALANCED"
                regime_start_idx = idx
            else:
                # Check regime age (persistence)
                regime_age = idx - regime_start_idx if regime_start_idx is not None else 0
                requires_stronger_signal = regime_age < min_regime_duration
                
                # Apply hysteresis with persistence
                if current_regime == "RISK_ON_ALTS":
                    # Only switch if score > threshold_low + hysteresis_high
                    # If regime is young, require even stronger signal
                    hysteresis_adjusted = self.hysteresis_high
                    if requires_stronger_signal:
                        hysteresis_adjusted = self.hysteresis_high * 1.5  # 50% stronger signal required
                    
                    if score > self.threshold_low + hysteresis_adjusted:
                        if score > self.threshold_high:
                            current_regime = "RISK_ON_MAJORS"
                            regime_start_idx = idx
                        else:
                            current_regime = "BALANCED"
                            regime_start_idx = idx
                elif current_regime == "RISK_ON_MAJORS":
                    # Only switch if score < threshold_high - hysteresis_low
                    # If regime is young, require even stronger signal
                    hysteresis_adjusted = self.hysteresis_low
                    if requires_stronger_signal:
                        hysteresis_adjusted = self.hysteresis_low * 1.5  # 50% stronger signal required
                    
                    if score < self.threshold_high + hysteresis_adjusted:
                        if score < self.threshold_low:
                            current_regime = "RISK_ON_ALTS"
                            regime_start_idx = idx
                        else:
                            current_regime = "BALANCED"
                            regime_start_idx = idx
                else:  # BALANCED
                    # BALANCED is more flexible, allow switches more easily
                    if score < self.threshold_low:
                        current_regime = "RISK_ON_ALTS"
                        regime_start_idx = idx
                    elif score > self.threshold_high:
                        current_regime = "RISK_ON_MAJORS"
                        regime_start_idx = idx
            
            regimes.append(current_regime)
        
        return scores.with_columns([
            pl.Series("regime", regimes),
        ])
    
    def _classify_regimes_5(self, scores: pl.DataFrame) -> pl.DataFrame:
        """Classify 5 regimes with persistence: STRONG_RISK_ON_ALTS, WEAK_RISK_ON_ALTS, BALANCED, WEAK_RISK_ON_MAJORS, STRONG_RISK_ON_MAJORS."""
        regimes = []
        current_regime = None
        regime_start_idx = None  # Track when current regime started
        min_regime_duration = 3  # Minimum days before allowing switch (persistence)
        
        for idx, row in enumerate(scores.iter_rows(named=True)):
            score = row["score"]
            
            # Handle None/NaN scores
            if score is None or (isinstance(score, float) and (score != score)):  # NaN check
                score = 0.0
            
            if current_regime is None:
                # First observation - classify based on score
                if score < self.threshold_strong_low:
                    current_regime = "STRONG_RISK_ON_ALTS"
                elif score < self.threshold_low:
                    current_regime = "WEAK_RISK_ON_ALTS"
                elif score > self.threshold_strong_high:
                    current_regime = "STRONG_RISK_ON_MAJORS"
                elif score > self.threshold_high:
                    current_regime = "WEAK_RISK_ON_MAJORS"
                else:
                    current_regime = "BALANCED"
                regime_start_idx = idx
            else:
                # Check regime age (persistence)
                regime_age = idx - regime_start_idx if regime_start_idx is not None else 0
                requires_stronger_signal = regime_age < min_regime_duration
                
                # Apply hysteresis with 5 regimes and persistence
                # Transition rules: allow gradual transitions but prevent churn
                if current_regime == "STRONG_RISK_ON_ALTS":
                    hysteresis_adjusted = self.hysteresis_high * (1.5 if requires_stronger_signal else 1.0)
                    if score > self.threshold_strong_low + hysteresis_adjusted:
                        if score > self.threshold_low:
                            if score > self.threshold_strong_high:
                                current_regime = "STRONG_RISK_ON_MAJORS"
                                regime_start_idx = idx
                            elif score > self.threshold_high:
                                current_regime = "WEAK_RISK_ON_MAJORS"
                                regime_start_idx = idx
                            else:
                                current_regime = "BALANCED"
                                regime_start_idx = idx
                        else:
                            current_regime = "WEAK_RISK_ON_ALTS"
                            regime_start_idx = idx
                
                elif current_regime == "WEAK_RISK_ON_ALTS":
                    hysteresis_adjusted = self.hysteresis_high * (1.5 if requires_stronger_signal else 1.0)
                    if score > self.threshold_low + hysteresis_adjusted:
                        if score > self.threshold_strong_high:
                            current_regime = "STRONG_RISK_ON_MAJORS"
                            regime_start_idx = idx
                        elif score > self.threshold_high:
                            current_regime = "WEAK_RISK_ON_MAJORS"
                            regime_start_idx = idx
                        else:
                            current_regime = "BALANCED"
                            regime_start_idx = idx
                    elif score < self.threshold_strong_low:
                        current_regime = "STRONG_RISK_ON_ALTS"
                        regime_start_idx = idx
                
                elif current_regime == "BALANCED":
                    # BALANCED is more flexible, allow switches more easily
                    if score < self.threshold_strong_low:
                        current_regime = "STRONG_RISK_ON_ALTS"
                        regime_start_idx = idx
                    elif score < self.threshold_low:
                        current_regime = "WEAK_RISK_ON_ALTS"
                        regime_start_idx = idx
                    elif score > self.threshold_strong_high:
                        current_regime = "STRONG_RISK_ON_MAJORS"
                        regime_start_idx = idx
                    elif score > self.threshold_high:
                        current_regime = "WEAK_RISK_ON_MAJORS"
                        regime_start_idx = idx
                
                elif current_regime == "WEAK_RISK_ON_MAJORS":
                    hysteresis_adjusted = self.hysteresis_low * (1.5 if requires_stronger_signal else 1.0)
                    if score < self.threshold_high + hysteresis_adjusted:
                        if score < self.threshold_strong_low:
                            current_regime = "STRONG_RISK_ON_ALTS"
                            regime_start_idx = idx
                        elif score < self.threshold_low:
                            current_regime = "WEAK_RISK_ON_ALTS"
                            regime_start_idx = idx
                        else:
                            current_regime = "BALANCED"
                            regime_start_idx = idx
                    elif score > self.threshold_strong_high:
                        current_regime = "STRONG_RISK_ON_MAJORS"
                        regime_start_idx = idx
                
                elif current_regime == "STRONG_RISK_ON_MAJORS":
                    hysteresis_adjusted = self.hysteresis_low * (1.5 if requires_stronger_signal else 1.0)
                    if score < self.threshold_strong_high + hysteresis_adjusted:
                        if score < self.threshold_strong_low:
                            current_regime = "STRONG_RISK_ON_ALTS"
                            regime_start_idx = idx
                        elif score < self.threshold_low:
                            current_regime = "WEAK_RISK_ON_ALTS"
                            regime_start_idx = idx
                        elif score < self.threshold_high:
                            current_regime = "BALANCED"
                            regime_start_idx = idx
                        else:
                            current_regime = "WEAK_RISK_ON_MAJORS"
                            regime_start_idx = idx
            
            regimes.append(current_regime)
        
        return scores.with_columns([
            pl.Series("regime", regimes),
        ])
    
    def walk_forward_grid_search(
        self,
        features: pl.DataFrame,
        ls_returns: pl.DataFrame,
        lookback_window_days: int = 252,
        test_window_days: int = 63,
        eval_horizon_days: int = 20,
        weight_ranges: Optional[Dict[str, Tuple[float, float]]] = None,
        threshold_range_low: Tuple[float, float] = (-2.0, 0.0),
        threshold_range_high: Tuple[float, float] = (0.0, 2.0),
        n_samples: int = 50,
    ) -> Dict:
        """
        Walk-forward grid search to optimize weights and thresholds.
        
        Args:
            features: Feature DataFrame
            ls_returns: Long-short returns DataFrame (date, r_ls_net)
            lookback_window_days: Lookback window length (burn-in period, not training)
            test_window_days: Test window length
            eval_horizon_days: Forward return horizon for evaluation
            weight_ranges: Feature weight ranges {feat_name: (min, max)}
            threshold_range_low: Lower threshold range
            threshold_range_high: Upper threshold range
            n_samples: Number of grid search samples
        
        Returns:
            Dict with best parameters and results
        """
        # Align features and returns
        aligned = features.join(ls_returns, on="date", how="inner").sort("date")
        
        if len(aligned) < lookback_window_days + test_window_days:
            logger.warning("Insufficient data for walk-forward")
            return {"best_params": None, "best_sharpe": None}
        
        # Generate grid
        if weight_ranges is None:
            weight_ranges = {}
        
        # Sample weights and thresholds
        best_sharpe = -np.inf
        best_params = None
        
        dates = aligned["date"].to_list()
        n_windows = (len(dates) - lookback_window_days) // test_window_days
        
        logger.info(f"Walk-forward grid search: {n_windows} windows, {n_samples} samples per window")
        
        for sample_idx in range(n_samples):
            # Sample weights
            weights = {}
            for feat_name, (min_w, max_w) in weight_ranges.items():
                weights[feat_name] = np.random.uniform(min_w, max_w)
            
            # Normalize weights
            total = sum(abs(w) for w in weights.values())
            if total > 0:
                weights = {k: v / total for k, v in weights.items()}
            
            # Sample thresholds
            threshold_low = np.random.uniform(*threshold_range_low)
            threshold_high = np.random.uniform(*threshold_range_high)
            
            # Walk-forward evaluation
            sharpe_sum = 0.0
            n_valid_windows = 0
            
            for window_idx in range(n_windows):
                train_start = window_idx * test_window_days
                train_end = train_start + lookback_window_days
                test_end = train_end + test_window_days
                
                if test_end > len(dates):
                    break
                
                # Train on training window
                train_data = aligned.slice(train_start, train_end - train_start)
                
                # Compute composite score
                model = RegimeModel(
                    mode="composite",
                    default_weights=weights,
                    threshold_low=threshold_low,
                    threshold_high=threshold_high,
                )
                
                train_scores = model.compute_composite_score(train_data)
                
                # Test on test window
                test_data = aligned.slice(train_end, test_end - train_end)
                test_scores = model.compute_composite_score(test_data)
                
                # Compute forward returns for evaluation
                # (This is simplified - in practice, you'd compute actual forward returns)
                test_returns = test_data.select(["date", "r_ls_net"]).to_pandas()
                test_returns = test_returns.set_index("date")
                
                # Compute regime-filtered returns
                regime_returns = []
                for regime in ["RISK_ON_MAJORS", "BALANCED", "RISK_ON_ALTS"]:
                    regime_mask = test_scores.filter(pl.col("regime") == regime)["date"].to_list()
                    if len(regime_mask) > 0:
                        regime_ret = test_returns.loc[regime_mask, "r_ls_net"].mean()
                        regime_returns.append(regime_ret)
                
                if len(regime_returns) > 0:
                    mean_ret = np.mean(regime_returns)
                    std_ret = np.std(regime_returns)
                    if std_ret > 0:
                        sharpe = mean_ret / std_ret * np.sqrt(252)  # Annualized
                        sharpe_sum += sharpe
                        n_valid_windows += 1
            
            if n_valid_windows > 0:
                avg_sharpe = sharpe_sum / n_valid_windows
                if avg_sharpe > best_sharpe:
                    best_sharpe = avg_sharpe
                    best_params = {
                        "weights": weights,
                        "threshold_low": threshold_low,
                        "threshold_high": threshold_high,
                    }
                    logger.info(f"New best Sharpe: {best_sharpe:.4f} (sample {sample_idx})")
        
        logger.info(f"Grid search complete: best Sharpe = {best_sharpe:.4f}")
        
        return {
            "best_params": best_params,
            "best_sharpe": best_sharpe,
        }
