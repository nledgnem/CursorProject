"""Dual-beta neutral LS construction."""

import polars as pl
import numpy as np
from typing import List, Dict, Optional
from datetime import date
from scipy.optimize import minimize
from sklearn.linear_model import Ridge
import logging

logger = logging.getLogger(__name__)


class DualBetaNeutralLS:
    """Construct dual-beta neutral long-short portfolio."""
    
    def __init__(
        self,
        majors: List[str] = ["BTC", "ETH"],
        lookback_days: int = 60,
        ridge_alpha: float = 0.1,
        winsorize_pct: float = 0.05,
        beta_clamp: tuple = (0.0, 3.0),
        default_beta: float = 1.0,
    ):
        """
        Initialize dual-beta neutral LS constructor.
        
        Args:
            majors: List of major asset IDs (long leg)
            lookback_days: Rolling window for beta estimation
            ridge_alpha: Ridge regression regularization
            winsorize_pct: Winsorize extreme returns
            beta_clamp: Clamp betas to this range
            default_beta: Default beta if estimation fails
        """
        self.majors = majors
        self.lookback_days = lookback_days
        self.ridge_alpha = ridge_alpha
        self.winsorize_pct = winsorize_pct
        self.beta_clamp = beta_clamp
        self.default_beta = default_beta
    
    def estimate_betas(
        self,
        prices: pl.DataFrame,
        asset_id: str,
        asof_date: date,
        tracker_betas: Optional[Dict[str, Dict[str, float]]] = None,
    ) -> Dict[str, float]:
        """
        Estimate betas to BTC and ETH using rolling ridge regression.
        
        Args:
            prices: (asset_id, date, close)
            asset_id: Asset to estimate betas for
            asof_date: Date to estimate as-of (PIT)
            tracker_betas: Optional pre-computed betas from tracker
        
        Returns:
            Dict with keys: 'BTC', 'ETH' and beta values
        """
        # Check tracker betas first
        if tracker_betas and asset_id in tracker_betas:
            betas = tracker_betas[asset_id]
            if "BTC" in betas and "ETH" in betas:
                logger.debug(f"Using tracker betas for {asset_id}: {betas}")
                return {"BTC": betas["BTC"], "ETH": betas["ETH"]}
        
        # Get price data up to asof_date
        prices_filtered = prices.filter(
            pl.col("date") <= pl.date(asof_date.year, asof_date.month, asof_date.day)
        )
        
        # Get asset prices
        asset_prices = prices_filtered.filter(pl.col("asset_id") == asset_id)
        if len(asset_prices) < self.lookback_days:
            logger.warning(f"Insufficient data for {asset_id}, using default beta")
            return {"BTC": self.default_beta, "ETH": self.default_beta}
        
        # Get major prices
        btc_prices = prices_filtered.filter(pl.col("asset_id") == "BTC")
        eth_prices = prices_filtered.filter(pl.col("asset_id") == "ETH")
        
        # Join on date
        joined = (
            asset_prices.select(["date", "close"]).rename({"close": "asset_close"})
            .join(btc_prices.select(["date", "close"]).rename({"close": "btc_close"}), on="date", how="inner")
            .join(eth_prices.select(["date", "close"]).rename({"close": "eth_close"}), on="date", how="inner")
            .sort("date")
            .tail(self.lookback_days)
        )
        
        if len(joined) < 30:
            logger.warning(f"Insufficient overlapping data for {asset_id}, using default beta")
            return {"BTC": self.default_beta, "ETH": self.default_beta}
        
        # Convert to numpy
        asset_close = joined["asset_close"].to_numpy()
        btc_close = joined["btc_close"].to_numpy()
        eth_close = joined["eth_close"].to_numpy()
        
        # Compute log returns
        asset_ret = np.diff(np.log(asset_close))
        btc_ret = np.diff(np.log(btc_close))
        eth_ret = np.diff(np.log(eth_close))
        
        # Winsorize
        def winsorize(x, pct):
            lower = np.percentile(x, pct * 100)
            upper = np.percentile(x, (1 - pct) * 100)
            return np.clip(x, lower, upper)
        
        asset_ret = winsorize(asset_ret, self.winsorize_pct)
        btc_ret = winsorize(btc_ret, self.winsorize_pct)
        eth_ret = winsorize(eth_ret, self.winsorize_pct)
        
        # Ridge regression
        X = np.column_stack([btc_ret, eth_ret])
        y = asset_ret
        
        try:
            model = Ridge(alpha=self.ridge_alpha)
            model.fit(X, y)
            betas = {
                "BTC": float(np.clip(model.coef_[0], self.beta_clamp[0], self.beta_clamp[1])),
                "ETH": float(np.clip(model.coef_[1], self.beta_clamp[0], self.beta_clamp[1])),
            }
            logger.debug(f"Estimated betas for {asset_id}: {betas}")
            return betas
        except Exception as e:
            logger.warning(f"Beta estimation failed for {asset_id}: {e}, using default")
            return {"BTC": self.default_beta, "ETH": self.default_beta}
    
    def solve_neutrality(
        self,
        alt_weights: Dict[str, float],
        alt_betas: Dict[str, Dict[str, float]],
        major_weights: Dict[str, float],
        gross_cap: float = 1.0,
        neutrality_mode: str = "dollar_neutral",
    ) -> Dict[str, float]:
        """
        Solve for dual-beta neutral weights.
        
        Minimizes exposure to BTC and ETH factors subject to constraints.
        
        Args:
            alt_weights: Initial ALT weights {asset_id: weight}
            alt_betas: ALT betas {asset_id: {BTC: beta, ETH: beta}}
            major_weights: Initial major weights {asset_id: weight}
            gross_cap: Maximum gross exposure
            neutrality_mode: "dollar_neutral" (enforce dollar-neutrality first) or 
                           "beta_neutral" (enforce beta-neutrality first, allow non-zero net)
        
        Returns:
            Adjusted weights {asset_id: weight}
        """
        # Collect all assets (union of ALT and major)
        all_assets = sorted(list(set(list(alt_weights.keys()) + list(major_weights.keys()))))
        
        # BETA-NEUTRAL SIZING APPROACH:
        # Mode 1 (dollar_neutral): Enforce dollar-neutrality first, then minimize beta exposure
        # Mode 2 (beta_neutral): Enforce beta-neutrality first, allow non-zero net exposure
        
        if neutrality_mode == "beta_neutral":
            return self._solve_beta_neutral_first(alt_weights, alt_betas, major_weights, gross_cap)
        else:
            return self._solve_dollar_neutral_first(alt_weights, alt_betas, major_weights, gross_cap)
    
    def _solve_dollar_neutral_first(
        self,
        alt_weights: Dict[str, float],
        alt_betas: Dict[str, Dict[str, float]],
        major_weights: Dict[str, float],
        gross_cap: float,
    ) -> Dict[str, float]:
        """
        Solve for neutrality: dollar-neutrality first, then minimize beta exposure.
        
        This is the default mode that ensures net exposure = 0.
        """
        
        alt_total = sum(abs(w) for w in alt_weights.values())
        if alt_total == 0:
            return {}
        
        # Step 1: Scale ALT weights to 50% (maintains relative weights)
        alt_scale = 0.5 / alt_total
        scaled_alt_weights = {k: -abs(v) * alt_scale for k, v in alt_weights.items()}  # Negative for short
        
        # Step 2: Calculate ALT beta exposure to BTC and ETH
        # ALT weights are negative (short), so beta exposure is also negative
        alt_btc_beta_exp = sum(scaled_alt_weights.get(a, 0.0) * alt_betas.get(a, {}).get("BTC", 1.0) 
                               for a in alt_weights.keys())
        alt_eth_beta_exp = sum(scaled_alt_weights.get(a, 0.0) * alt_betas.get(a, {}).get("ETH", 1.0) 
                               for a in alt_weights.keys())
        
        # Step 3: Size majors to achieve dollar-neutrality AND beta-neutrality
        # ALT weights sum to -0.5 (50% short), so we need majors to sum to +0.5 (50% long)
        # We want to minimize: (alt_btc_exp + btc_weight * 1.0)^2 + (alt_eth_exp + eth_weight * 1.0)^2
        # Subject to: btc_weight + eth_weight = 0.5 (dollar-neutrality)
        #            btc_weight >= 0, eth_weight >= 0 (long positions)
        
        # Analytical solution for beta-neutrality:
        # Let btc_weight = x, eth_weight = 0.5 - x
        # Objective: (alt_btc_exp + x)^2 + (alt_eth_exp + 0.5 - x)^2
        # Expand: alt_btc_exp^2 + 2*alt_btc_exp*x + x^2 + alt_eth_exp^2 + 2*alt_eth_exp*(0.5-x) + (0.5-x)^2
        # Derivative w.r.t. x: 2*alt_btc_exp + 2*x - 2*alt_eth_exp - 2*(0.5-x) = 0
        #            = alt_btc_exp + x - alt_eth_exp - 0.5 + x = 0
        #            = 2*x = 0.5 + alt_eth_exp - alt_btc_exp
        #            = x = 0.25 + 0.5*(alt_eth_exp - alt_btc_exp)
        
        # Note: alt_btc_exp and alt_eth_exp are negative (because ALT weights are negative)
        # So we're offsetting negative exposure with positive major weights
        optimal_btc_weight = 0.25 + 0.5 * (alt_eth_beta_exp - alt_btc_beta_exp)
        optimal_btc_weight = max(0.0, min(0.5, optimal_btc_weight))  # Clamp to [0, 0.5]
        optimal_eth_weight = 0.5 - optimal_btc_weight  # Ensure sum = 0.5 (dollar-neutrality)
        
        # Verify: major weights should sum to 0.5
        major_total = optimal_btc_weight + optimal_eth_weight
        if abs(major_total - 0.5) > 0.001:
            logger.warning(f"Major weights don't sum to 0.5: {major_total:.4f}, normalizing")
            scale = 0.5 / major_total
            optimal_btc_weight *= scale
            optimal_eth_weight *= scale
        
        # Verify beta neutrality
        final_btc_exp = alt_btc_beta_exp + optimal_btc_weight * 1.0 + optimal_eth_weight * 0.0
        final_eth_exp = alt_eth_beta_exp + optimal_btc_weight * 0.0 + optimal_eth_weight * 1.0
        
        majors = {
            "BTC": optimal_btc_weight,
            "ETH": optimal_eth_weight,
        }
        
        # DEBUG: Verify major weights before combining
        major_sum = optimal_btc_weight + optimal_eth_weight
        major_gross_calc = abs(optimal_btc_weight) + abs(optimal_eth_weight)
        if abs(major_sum - 0.5) > 0.001 or abs(major_gross_calc - 0.5) > 0.001:
            logger.warning(f"Major weights issue: sum={major_sum:.4f}, gross={major_gross_calc:.4f}, "
                          f"BTC={optimal_btc_weight:.4f}, ETH={optimal_eth_weight:.4f}")
        
        # Combine ALT and major weights
        adjusted = {**scaled_alt_weights, **majors}
        
        # Verify dollar-neutrality
        net_exposure = sum(adjusted.values())
        if abs(net_exposure) > 0.01:
            logger.warning(f"Dollar-neutrality violation: net exposure = {net_exposure:.4f}")
        
        # Verify gross exposure
        gross_exposure = sum(abs(w) for w in adjusted.values())
        alt_gross = sum(abs(w) for w in scaled_alt_weights.values())
        major_gross = sum(abs(w) for w in majors.values())
        if abs(gross_exposure - 1.0) > 0.01:
            logger.warning(f"Gross exposure = {gross_exposure:.4f} (expected 1.0), "
                          f"ALT={alt_gross:.4f}, Major={major_gross:.4f}")
        
        logger.debug(f"Beta-neutral sizing: BTC={optimal_btc_weight:.3f}, ETH={optimal_eth_weight:.3f}, "
                    f"Major_gross={major_gross:.3f}, BTC_exp={final_btc_exp:.4f}, ETH_exp={final_eth_exp:.4f}")
        
        return adjusted
    
    def _solve_beta_neutral_first(
        self,
        alt_weights: Dict[str, float],
        alt_betas: Dict[str, Dict[str, float]],
        major_weights: Dict[str, float],
        gross_cap: float,
    ) -> Dict[str, float]:
        """
        Solve for neutrality: beta-neutrality first, allow non-zero net exposure.
        
        This mode prioritizes minimizing BTC and ETH factor exposure over dollar-neutrality.
        Net exposure may be non-zero if needed to achieve perfect beta-neutrality.
        """
        alt_total = sum(abs(w) for w in alt_weights.values())
        if alt_total == 0:
            return {}
        
        # Step 1: Scale ALT weights to target gross (e.g., 50% for 100% total gross)
        # But we'll let the solver determine optimal scaling
        alt_scale = 0.5 / alt_total if alt_total > 0 else 1.0
        scaled_alt_weights = {k: -abs(v) * alt_scale for k, v in alt_weights.items()}  # Negative for short
        
        # Step 2: Calculate ALT beta exposure
        alt_btc_beta_exp = sum(scaled_alt_weights.get(a, 0.0) * alt_betas.get(a, {}).get("BTC", 1.0) 
                               for a in alt_weights.keys())
        alt_eth_beta_exp = sum(scaled_alt_weights.get(a, 0.0) * alt_betas.get(a, {}).get("ETH", 1.0) 
                               for a in alt_weights.keys())
        
        # Step 3: Solve for majors that achieve perfect beta-neutrality
        # We want: alt_btc_exp + btc_weight * 1.0 + eth_weight * 0.0 = 0
        #         alt_eth_exp + btc_weight * 0.0 + eth_weight * 1.0 = 0
        # This gives: btc_weight = -alt_btc_exp, eth_weight = -alt_eth_exp
        
        optimal_btc_weight = -alt_btc_beta_exp  # Direct offset
        optimal_eth_weight = -alt_eth_beta_exp  # Direct offset
        
        # Cap individual positions to prevent extreme sizing
        max_major_weight = 0.75  # Allow up to 75% per major (more flexible than dollar-neutral mode)
        optimal_btc_weight = max(-max_major_weight, min(max_major_weight, optimal_btc_weight))
        optimal_eth_weight = max(-max_major_weight, min(max_major_weight, optimal_eth_weight))
        
        # If capping was needed, we won't achieve perfect beta-neutrality, but we'll be close
        majors = {
            "BTC": optimal_btc_weight,
            "ETH": optimal_eth_weight,
        }
        
        # Verify beta neutrality
        final_btc_exp = alt_btc_beta_exp + optimal_btc_weight * 1.0 + optimal_eth_weight * 0.0
        final_eth_exp = alt_eth_beta_exp + optimal_btc_weight * 0.0 + optimal_eth_weight * 1.0
        
        # Calculate net exposure (may be non-zero)
        net_exposure = sum(scaled_alt_weights.values()) + optimal_btc_weight + optimal_eth_weight
        
        # Combine ALT and major weights
        adjusted = {**scaled_alt_weights, **majors}
        
        # Verify gross exposure
        gross_exposure = sum(abs(w) for w in adjusted.values())
        if gross_exposure > gross_cap * 1.2:  # Allow some tolerance
            logger.warning(f"High gross exposure in beta-neutral mode: {gross_exposure:.4f}, scaling down")
            scale_factor = gross_cap / gross_exposure
            adjusted = {k: v * scale_factor for k, v in adjusted.items()}
            # Recalculate after scaling
            final_btc_exp *= scale_factor
            final_eth_exp *= scale_factor
            net_exposure *= scale_factor
        
        logger.debug(f"Beta-neutral-first sizing: BTC={optimal_btc_weight:.3f}, ETH={optimal_eth_weight:.3f}, "
                    f"BTC_exp={final_btc_exp:.4f}, ETH_exp={final_eth_exp:.4f}, "
                    f"net_exp={net_exposure:.4f}, gross={gross_exposure:.4f}")
        
        return adjusted
    
    def build_alt_basket(
        self,
        prices: pl.DataFrame,
        marketcap: pl.DataFrame,
        volume: pl.DataFrame,
        asof_date: date,
        basket_size: int = 20,
        min_mcap_usd: float = 50_000_000,
        min_volume_usd: float = 1_000_000,
        per_name_cap: float = 0.10,
        exclude_assets: Optional[List[str]] = None,
        alt_selection_config: Optional[Dict] = None,
    ) -> Dict[str, float]:
        """
        Build ALT basket (PIT-safe).
        
        Args:
            prices: (asset_id, date, close)
            marketcap: (asset_id, date, marketcap)
            volume: (asset_id, date, volume)
            asof_date: Date to build basket as-of
            basket_size: Top N liquid alts
            min_mcap_usd: Minimum market cap
            min_volume_usd: Minimum volume (7d median)
            per_name_cap: Maximum weight per name
            exclude_assets: Assets to exclude
        
        Returns:
            Dict {asset_id: weight}
        """
        if exclude_assets is None:
            exclude_assets = []
        
        # Filter to asof_date
        prices_filtered = prices.filter(
            pl.col("date") <= pl.date(asof_date.year, asof_date.month, asof_date.day)
        )
        marketcap_filtered = marketcap.filter(
            pl.col("date") <= pl.date(asof_date.year, asof_date.month, asof_date.day)
        )
        volume_filtered = volume.filter(
            pl.col("date") <= pl.date(asof_date.year, asof_date.month, asof_date.day)
        )
        
        # Get latest marketcap and volume
        latest_mcap = (
            marketcap_filtered
            .sort("date", descending=True)
            .group_by("asset_id")
            .first()
        )
        
        # 7d median volume
        volume_7d = (
            volume_filtered
            .sort("date", descending=True)
            .group_by("asset_id")
            .head(7)
            .group_by("asset_id")
            .agg(pl.col("volume").median().alias("volume_7d_median"))
        )
        
        # Filter by basic criteria
        candidates = (
            latest_mcap
            .join(volume_7d, on="asset_id", how="inner")
            .filter(
                (pl.col("marketcap") >= min_mcap_usd) &
                (pl.col("volume_7d_median") >= min_volume_usd) &
                (~pl.col("asset_id").is_in(exclude_assets + ["BTC", "ETH"]))
            )
        )
        
        if len(candidates) == 0:
            logger.warning(f"No ALT candidates found for {asof_date}")
            return {}
        
        # Enhanced ALT selection: apply volatility, correlation, and momentum filters
        if alt_selection_config and alt_selection_config.get("enabled", False):
            candidates = self._apply_enhanced_filters(
                candidates, prices_filtered, asof_date, alt_selection_config
            )
        
        # Sort by volume and take top N
        candidates = candidates.sort("volume_7d_median", descending=True).head(basket_size)
        
        if len(candidates) == 0:
            logger.warning(f"No ALT candidates found after filtering for {asof_date}")
            return {}
        
        # Weight by inverse volatility if enabled, otherwise equal weight
        if alt_selection_config and alt_selection_config.get("weight_by_inverse_vol", False):
            weights = self._weight_by_inverse_volatility(
                candidates, prices_filtered, asof_date,
                alt_selection_config.get("volatility_lookback_days", 20),
                per_name_cap
            )
        else:
            # Equal weight, capped
            n = len(candidates)
            base_weight = 1.0 / n
            weights = {}
            
            for row in candidates.iter_rows(named=True):
                asset_id = row["asset_id"]
                weight = min(base_weight, per_name_cap)
                weights[asset_id] = weight
            
            # Renormalize if needed
            total = sum(weights.values())
            if total > 0:
                weights = {k: v / total for k, v in weights.items()}
        
        logger.info(f"Built ALT basket for {asof_date}: {len(weights)} assets")
        
        return weights
    
    def build_msm_basket(
        self,
        prices: pl.DataFrame,
        marketcap: pl.DataFrame,
        volume: pl.DataFrame,
        asof_date: date,
        n: int = 20,
        min_mcap_usd: float = 50_000_000,
        min_volume_usd: float = 1_000,  # Light liquidity sanity check (very permissive)
        exclude_assets: Optional[List[str]] = None,
        weighting: str = "equal",  # "equal" or "mcap"
    ) -> Dict[str, float]:
        """
        Build pure MSM basket: market cap-based selection, no enhanced filters.
        
        This is selection-independent - tests market state for shorting alts using
        a fixed, non-alpha basket definition.
        
        Args:
            prices: (asset_id, date, close)
            marketcap: (asset_id, date, marketcap)
            volume: (asset_id, date, volume)
            asof_date: Date to build basket as-of
            n: Top N alts by market cap
            min_mcap_usd: Minimum market cap (basic filter)
            min_volume_usd: Minimum volume (light liquidity sanity check, very permissive)
            exclude_assets: Assets to exclude (stables, exchange tokens, wrapped)
            weighting: "equal" or "mcap" (market cap weighted)
        
        Returns:
            Dict {asset_id: weight}
        """
        if exclude_assets is None:
            exclude_assets = []
        
        # Filter to asof_date (PIT-safe)
        prices_filtered = prices.filter(
            pl.col("date") <= pl.date(asof_date.year, asof_date.month, asof_date.day)
        )
        marketcap_filtered = marketcap.filter(
            pl.col("date") <= pl.date(asof_date.year, asof_date.month, asof_date.day)
        )
        volume_filtered = volume.filter(
            pl.col("date") <= pl.date(asof_date.year, asof_date.month, asof_date.day)
        )
        
        # Get latest marketcap
        latest_mcap = (
            marketcap_filtered
            .sort("date", descending=True)
            .group_by("asset_id")
            .first()
        )
        
        # Get latest volume (for liquidity sanity check)
        latest_volume = (
            volume_filtered
            .sort("date", descending=True)
            .group_by("asset_id")
            .first()
        )
        
        # Join and filter by basic criteria (mcap + light liquidity check)
        candidates = (
            latest_mcap
            .join(latest_volume, on="asset_id", how="inner")
            .filter(
                (pl.col("marketcap") >= min_mcap_usd) &
                (pl.col("volume") >= min_volume_usd) &  # Light liquidity sanity check
                (~pl.col("asset_id").is_in(exclude_assets + ["BTC", "ETH"]))
            )
        )
        
        if len(candidates) == 0:
            logger.warning(f"No MSM ALT candidates found for {asof_date}")
            return {}
        
        # Sort by market cap (descending) and take top N
        candidates = candidates.sort("marketcap", descending=True).head(n)
        
        if len(candidates) == 0:
            logger.warning(f"No MSM ALT candidates found after filtering for {asof_date}")
            return {}
        
        # Weighting: equal or market cap weighted
        if weighting == "mcap":
            # Market cap weighted
            total_mcap = candidates["marketcap"].sum()
            weights = {
                row["asset_id"]: row["marketcap"] / total_mcap
                for row in candidates.iter_rows(named=True)
            }
        else:
            # Equal weight (default)
            n_assets = len(candidates)
            base_weight = 1.0 / n_assets
            weights = {
                row["asset_id"]: base_weight
                for row in candidates.iter_rows(named=True)
            }
        
        logger.info(f"Built MSM basket for {asof_date}: {len(weights)} assets (top {n} by mcap, {weighting} weighted)")
        
        return weights
    
    def _apply_enhanced_filters(
        self,
        candidates: pl.DataFrame,
        prices: pl.DataFrame,
        asof_date: date,
        config: Dict,
    ) -> pl.DataFrame:
        """Apply volatility, correlation, and momentum filters to ALT candidates."""
        filtered_assets = []
        
        # Get BTC and ETH prices for correlation calculation
        btc_prices = prices.filter(pl.col("asset_id") == "BTC").sort("date", descending=True)
        eth_prices = prices.filter(pl.col("asset_id") == "ETH").sort("date", descending=True)
        
        if len(btc_prices) == 0 or len(eth_prices) == 0:
            logger.warning("Missing BTC/ETH prices for enhanced filtering, skipping filters")
            return candidates
        
        for row in candidates.iter_rows(named=True):
            asset_id = row["asset_id"]
            asset_prices = prices.filter(pl.col("asset_id") == asset_id).sort("date", descending=True)
            
            if len(asset_prices) < max(
                config.get("volatility_lookback_days", 20),
                config.get("correlation_lookback_days", 60),
                config.get("momentum_lookback_days", 7),
            ):
                continue  # Skip if insufficient data
            
            # Volatility filter
            if config.get("max_volatility") is not None:
                vol_lookback = config.get("volatility_lookback_days", 20)
                asset_recent = asset_prices.head(vol_lookback)
                if len(asset_recent) >= 2:
                    returns = asset_recent["close"].pct_change().drop_nulls()
                    if len(returns) > 0:
                        vol_std = returns.std()
                        if vol_std is not None:
                            annualized_vol = vol_std * np.sqrt(252)
                            if annualized_vol > config.get("max_volatility", 1.0):
                                continue  # Exclude high volatility assets
            
            # Correlation filter
            if config.get("min_correlation") is not None:
                corr_lookback = config.get("correlation_lookback_days", 60)
                asset_recent = asset_prices.head(corr_lookback)
                btc_recent = btc_prices.head(corr_lookback)
                eth_recent = eth_prices.head(corr_lookback)
                
                # Join on date
                joined = (
                    asset_recent.select(["date", "close"]).rename({"close": "asset_close"})
                    .join(btc_recent.select(["date", "close"]).rename({"close": "btc_close"}), on="date", how="inner")
                    .join(eth_recent.select(["date", "close"]).rename({"close": "eth_close"}), on="date", how="inner")
                )
                
                if len(joined) >= 30:  # Need minimum data for correlation
                    # Compute returns
                    joined = joined.sort("date").with_columns([
                        (pl.col("asset_close").pct_change()).alias("asset_ret"),
                        (pl.col("btc_close").pct_change()).alias("btc_ret"),
                        (pl.col("eth_close").pct_change()).alias("eth_ret"),
                    ]).drop_nulls()
                    
                    if len(joined) > 10:
                        # Compute correlation to BTC and ETH (use max)
                        corr_btc = joined["asset_ret"].corr(joined["btc_ret"])
                        corr_eth = joined["asset_ret"].corr(joined["eth_ret"])
                        max_corr = max(abs(corr_btc) if corr_btc is not None else 0.0,
                                     abs(corr_eth) if corr_eth is not None else 0.0)
                        
                        if max_corr < config.get("min_correlation", 0.3):
                            continue  # Exclude low correlation assets
            
            # Momentum filter
            if config.get("max_momentum") is not None or config.get("min_momentum") is not None:
                mom_lookback = config.get("momentum_lookback_days", 7)
                asset_recent = asset_prices.head(mom_lookback + 1)
                if len(asset_recent) >= 2:
                    first_price = asset_recent.tail(1)["close"][0]
                    last_price = asset_recent.head(1)["close"][0]
                    momentum = (last_price / first_price) - 1.0
                    
                    if config.get("max_momentum") is not None and momentum > config.get("max_momentum", 0.5):
                        continue  # Exclude extreme positive momentum (avoid catching falling knives)
                    if config.get("min_momentum") is not None and momentum < config.get("min_momentum", -0.5):
                        continue  # Exclude extreme negative momentum (avoid shorting at bottom)
            
            filtered_assets.append(asset_id)
        
        # Filter candidates to only include assets that passed all filters
        return candidates.filter(pl.col("asset_id").is_in(filtered_assets))
    
    def _weight_by_inverse_volatility(
        self,
        candidates: pl.DataFrame,
        prices: pl.DataFrame,
        asof_date: date,
        vol_lookback: int,
        per_name_cap: float,
    ) -> Dict[str, float]:
        """Weight ALTs by inverse volatility (less volatile = higher weight)."""
        weights = {}
        inv_vols = {}
        
        for row in candidates.iter_rows(named=True):
            asset_id = row["asset_id"]
            asset_prices = prices.filter(pl.col("asset_id") == asset_id).sort("date", descending=True).head(vol_lookback)
            
            if len(asset_prices) >= 2:
                returns = asset_prices["close"].pct_change().drop_nulls()
                if len(returns) > 0:
                    vol = returns.std()
                    if vol is not None and vol > 0:
                        inv_vols[asset_id] = 1.0 / vol
                    else:
                        inv_vols[asset_id] = 1.0  # Default if zero volatility
                else:
                    inv_vols[asset_id] = 1.0
            else:
                inv_vols[asset_id] = 1.0
        
        # Normalize inverse volatilities to weights
        total_inv_vol = sum(inv_vols.values())
        if total_inv_vol > 0:
            for asset_id, inv_vol in inv_vols.items():
                weight = (inv_vol / total_inv_vol)
                weight = min(weight, per_name_cap)  # Cap per name
                weights[asset_id] = weight
        
        # Renormalize after capping
        total = sum(weights.values())
        if total > 0:
            weights = {k: v / total for k, v in weights.items()}
        
        return weights
