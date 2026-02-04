"""PIT-safe feature computation library."""

import polars as pl
import numpy as np
from typing import Dict, Optional
from datetime import date
import logging

logger = logging.getLogger(__name__)


class FeatureLibrary:
    """Compute rolling, PIT-safe features with expanding burn-in."""
    
    def __init__(
        self,
        burn_in_days: int = 60,
        lookback_days: int = 252,
    ):
        """
        Initialize feature library.
        
        Args:
            burn_in_days: Minimum observations before features are valid
            lookback_days: Maximum lookback for rolling windows
        """
        self.burn_in_days = burn_in_days
        self.lookback_days = lookback_days
    
    def compute_features(
        self,
        prices: pl.DataFrame,
        marketcap: pl.DataFrame,
        volume: pl.DataFrame,
        funding: Optional[pl.DataFrame] = None,
        open_interest: Optional[pl.DataFrame] = None,
        majors: list = ["BTC", "ETH"],
        exclude_assets: Optional[list] = None,
    ) -> pl.DataFrame:
        """
        Compute all features.
        
        Args:
            prices: (asset_id, date, close)
            marketcap: (asset_id, date, marketcap)
            volume: (asset_id, date, volume)
            funding: Optional (asset_id, date, funding_rate)
            open_interest: Optional (asset_id, date, open_interest_usd)
            majors: List of major asset IDs
            exclude_assets: Assets to exclude from ALT set
        
        Returns:
            DataFrame with date index and feature columns (raw and z-scored)
        """
        if exclude_assets is None:
            exclude_assets = []
        
        # Get ALT assets (all except majors and excluded)
        all_assets = set(prices["asset_id"].unique().to_list())
        alt_assets = [a for a in all_assets if a not in majors and a not in exclude_assets]
        
        logger.info(f"Computing features: {len(majors)} majors, {len(alt_assets)} alts")
        
        # Compute individual feature groups
        features_list = []
        
        # 1. ALT breadth & dispersion
        alt_breadth = self._compute_alt_breadth(prices, alt_assets)
        features_list.append(alt_breadth)
        
        # 2. BTC dominance shift
        btc_dominance = self._compute_btc_dominance(marketcap, majors, alt_assets)
        features_list.append(btc_dominance)
        
        # 3. Funding skew and heating (if available)
        if funding is not None:
            funding_skew = self._compute_funding_skew(funding, majors, alt_assets)
            features_list.append(funding_skew)
            # Add funding heating (short-term vs long-term spread)
            funding_heating = self._compute_funding_heating(funding, majors, alt_assets)
            features_list.append(funding_heating)
        
        # 3b. OI risk (use real OI data if available, otherwise marketcap proxy)
        oi_risk = self._compute_oi_risk(marketcap, prices, majors, open_interest=open_interest)
        features_list.append(oi_risk)
        
        # 4. Liquidity/flow proxies
        liquidity = self._compute_liquidity(volume, alt_assets)
        features_list.append(liquidity)
        
        # 5. Volatility spread
        vol_spread = self._compute_volatility_spread(prices, marketcap, majors, alt_assets)
        features_list.append(vol_spread)
        
        # 6. Cross-sectional momentum
        momentum = self._compute_momentum(prices, majors, alt_assets)
        features_list.append(momentum)
        
        # Combine all features
        features = features_list[0]
        for f in features_list[1:]:
            features = features.join(f, on="date", how="outer", coalesce=True)
        
        # Sort by date
        features = features.sort("date")
        
        # Add z-scored versions
        feature_cols = [c for c in features.columns if c != "date"]
        for col in feature_cols:
            if col.startswith("raw_"):
                z_col = col.replace("raw_", "z_")
                # Rolling z-score (60-day window)
                features = features.with_columns([
                    ((pl.col(col) - pl.col(col).rolling_mean(window_size=60)) / 
                     pl.col(col).rolling_std(window_size=60)).alias(z_col)
                ])
        
        # Mark burn-in period
        features = features.with_columns([
            (pl.int_range(pl.len()) >= self.burn_in_days).alias("valid")
        ])
        
        logger.info(f"Computed features: {len(features)} dates, {len(feature_cols)} features")
        
        return features
    
    def _compute_alt_breadth(self, prices: pl.DataFrame, alt_assets: list) -> pl.DataFrame:
        """Compute ALT breadth & dispersion features."""
        # Filter to ALT assets
        alt_prices = prices.filter(pl.col("asset_id").is_in(alt_assets))
        
        # Compute daily returns
        alt_returns = (
            alt_prices
            .sort(["asset_id", "date"])
            .with_columns([
                (pl.col("close") / pl.col("close").shift(1).over("asset_id") - 1.0).alias("ret_1d")
            ])
        )
        
        # Aggregate by date
        daily_stats = (
            alt_returns
            .group_by("date")
            .agg([
                ((pl.col("ret_1d") > 0).sum().cast(pl.Float64) / pl.count() * 100.0).alias("raw_alt_breadth_pct_up"),
                pl.col("ret_1d").median().alias("raw_alt_breadth_median_ret"),
                (pl.col("ret_1d").quantile(0.75) - pl.col("ret_1d").quantile(0.25)).alias("raw_alt_breadth_iqr"),
            ])
        )
        
        # Add 5d and 20d breadth slopes
        daily_stats = daily_stats.with_columns([
            (pl.col("raw_alt_breadth_pct_up") - 
             pl.col("raw_alt_breadth_pct_up").shift(5)).alias("raw_alt_breadth_slope_5d"),
            (pl.col("raw_alt_breadth_pct_up") - 
             pl.col("raw_alt_breadth_pct_up").shift(20)).alias("raw_alt_breadth_slope_20d"),
        ])
        
        return daily_stats
    
    def _compute_btc_dominance(
        self,
        marketcap: pl.DataFrame,
        majors: list,
        alt_assets: list,
    ) -> pl.DataFrame:
        """Compute BTC dominance shift features."""
        # Get BTC marketcap
        btc_mcap = marketcap.filter(pl.col("asset_id") == "BTC")
        
        # Get ALT marketcap (sum)
        alt_mcap = (
            marketcap
            .filter(pl.col("asset_id").is_in(alt_assets))
            .group_by("date")
            .agg(pl.col("marketcap").sum().alias("alt_mcap"))
        )
        
        # Join and compute dominance
        dominance = (
            btc_mcap
            .join(alt_mcap, on="date", how="outer")
            .with_columns([
                (pl.col("marketcap") / (pl.col("marketcap") + pl.col("alt_mcap"))).alias("raw_btc_dominance")
            ])
            .select(["date", "raw_btc_dominance"])
        )
        
        # Add deltas
        dominance = dominance.with_columns([
            (pl.col("raw_btc_dominance") - pl.col("raw_btc_dominance").shift(1)).alias("raw_btc_dominance_d1d"),
            (pl.col("raw_btc_dominance") - pl.col("raw_btc_dominance").shift(5)).alias("raw_btc_dominance_d5d"),
        ])
        
        # Rolling z-score
        dominance = dominance.with_columns([
            ((pl.col("raw_btc_dominance") - pl.col("raw_btc_dominance").rolling_mean(window_size=60)) /
             pl.col("raw_btc_dominance").rolling_std(window_size=60)).alias("raw_btc_dominance_zscore")
        ])
        
        return dominance
    
    def _compute_funding_skew(
        self,
        funding: pl.DataFrame,
        majors: list,
        alt_assets: list,
    ) -> pl.DataFrame:
        """Compute funding skew features."""
        # Get major funding (BTC/ETH)
        major_funding = (
            funding
            .filter(pl.col("asset_id").is_in(majors))
            .group_by("date")
            .agg(pl.col("funding_rate").mean().alias("major_funding"))
        )
        
        # Get ALT funding
        alt_funding = (
            funding
            .filter(pl.col("asset_id").is_in(alt_assets))
            .group_by("date")
            .agg(pl.col("funding_rate").median().alias("alt_funding"))
        )
        
        # Join and compute skew
        skew = (
            major_funding
            .join(alt_funding, on="date", how="outer")
            .with_columns([
                (pl.col("alt_funding") - pl.col("major_funding")).alias("raw_funding_skew")
            ])
            .select(["date", "raw_funding_skew"])
        )
        
        # Add 3d z-score
        skew = skew.with_columns([
            ((pl.col("raw_funding_skew") - pl.col("raw_funding_skew").rolling_mean(window_size=3)) /
             pl.col("raw_funding_skew").rolling_std(window_size=3)).alias("raw_funding_skew_zscore_3d")
        ])
        
        # Flag missing days
        skew = skew.with_columns([
            pl.col("raw_funding_skew").is_not_null().alias("has_funding")
        ])
        
        return skew
    
    def _compute_funding_heating(
        self,
        funding: pl.DataFrame,
        majors: list,
        alt_assets: list,
        h_short: int = 10,
        h_long: int = 20,
    ) -> pl.DataFrame:
        """
        Compute funding heating feature (legacy monitor concept).
        
        Heating = short-term (10d) vs long-term (20d) funding spread.
        Captures whether funding is "heating up" (short-term widening vs long-term).
        """
        # Get major funding (BTC/ETH average)
        major_funding = (
            funding
            .filter(pl.col("asset_id").is_in(majors))
            .group_by("date")
            .agg(pl.col("funding_rate").mean().alias("major_funding"))
        )
        
        # Get ALT funding (median)
        alt_funding = (
            funding
            .filter(pl.col("asset_id").is_in(alt_assets))
            .group_by("date")
            .agg(pl.col("funding_rate").median().alias("alt_funding"))
        )
        
        # Join and compute spread
        spread = (
            major_funding
            .join(alt_funding, on="date", how="outer")
            .sort("date")
            .with_columns([
                (pl.col("alt_funding") - pl.col("major_funding")).alias("funding_spread")
            ])
        )
        
        # Compute heating: short-term (10d) vs long-term (20d) spread mean
        spread = spread.with_columns([
            pl.col("funding_spread").rolling_mean(window_size=h_short).alias("spread_short"),
            pl.col("funding_spread").rolling_mean(window_size=h_long).alias("spread_long"),
        ])
        
        # Heating = short - long (positive = heating up)
        spread = spread.with_columns([
            (pl.col("spread_short") - pl.col("spread_long")).alias("raw_funding_heating")
        ])
        
        return spread.select(["date", "raw_funding_heating"])
    
    def _compute_oi_risk(
        self,
        marketcap: pl.DataFrame,
        prices: pl.DataFrame,
        majors: list,
        open_interest: Optional[pl.DataFrame] = None,
    ) -> pl.DataFrame:
        """
        Compute OI risk feature.
        
        If open_interest data is available, use real OI data.
        Otherwise, use marketcap as proxy for OI.
        
        OI risk = BTC OI 3d change, gated by BTC 3d return quality.
        """
        # Use real OI data if available, otherwise fall back to marketcap proxy
        if open_interest is not None and not open_interest.is_empty():
            # Get BTC OI
            btc_oi = open_interest.filter(pl.col("asset_id") == "BTC").sort("date")
            
            if len(btc_oi) > 0:
                # Compute 3d change in OI
                btc_oi = btc_oi.with_columns([
                    (pl.col("open_interest_usd") / pl.col("open_interest_usd").shift(3) - 1.0).alias("oi_change_3d_pct")
                ])
                logger.info("Using real OI data for OI risk feature")
            else:
                # Fall back to marketcap proxy
                logger.warning("BTC OI data not found, using marketcap as proxy")
                btc_oi = marketcap.filter(pl.col("asset_id") == "BTC").sort("date")
                btc_oi = btc_oi.with_columns([
                    (pl.col("marketcap") / pl.col("marketcap").shift(3) - 1.0).alias("oi_change_3d_pct")
                ])
        else:
            # Fall back to marketcap proxy
            logger.warning("Open interest data not available, using marketcap as proxy for OI risk")
            btc_oi = marketcap.filter(pl.col("asset_id") == "BTC").sort("date")
            btc_oi = btc_oi.with_columns([
                (pl.col("marketcap") / pl.col("marketcap").shift(3) - 1.0).alias("oi_change_3d_pct")
            ])
        
        # Get BTC returns for quality gate
        btc_prices = prices.filter(pl.col("asset_id") == "BTC").sort("date")
        btc_returns = btc_prices.with_columns([
            (pl.col("close") / pl.col("close").shift(1) - 1.0).alias("ret_1d"),
            (pl.col("close") / pl.col("close").shift(7) - 1.0).alias("ret_7d"),
        ])
        
        # Approximate 3d return
        btc_returns = btc_returns.with_columns([
            (0.5 * (pl.col("ret_7d") * 3.0 / 7.0 + pl.col("ret_1d") / 3.0)).alias("btc_ret_3d")
        ])
        
        # Join
        oi_data = (
            btc_oi.select(["date", "oi_change_3d_pct"])
            .join(btc_returns.select(["date", "btc_ret_3d"]), on="date", how="outer")
            .sort("date")
        )
        
        # Compute OI risk
        # If OI change > 0, base_oi_risk = clamp(oi_change / 50.0, 0.0, 1.0)
        # oi_quality = 1.0 if btc_3d > 0 else 0.5
        # oi_risk = base_oi_risk * oi_quality
        oi_data = oi_data.with_columns([
            pl.when(pl.col("oi_change_3d_pct") > 0)
            .then(pl.min_horizontal([pl.col("oi_change_3d_pct") / 50.0, pl.lit(1.0)]))
            .otherwise(0.0)
            .alias("base_oi_risk"),
            pl.when(pl.col("btc_ret_3d") > 0)
            .then(1.0)
            .otherwise(0.5)
            .alias("oi_quality"),
        ])
        
        oi_data = oi_data.with_columns([
            (pl.col("base_oi_risk") * pl.col("oi_quality")).alias("raw_oi_risk")
        ])
        
        return oi_data.select(["date", "raw_oi_risk"])
    
    def _compute_liquidity(self, volume: pl.DataFrame, alt_assets: list) -> pl.DataFrame:
        """Compute liquidity/flow proxy features."""
        # Filter to ALT assets
        alt_volume = volume.filter(pl.col("asset_id").is_in(alt_assets))
        
        # Aggregate by date
        daily_volume = (
            alt_volume
            .group_by("date")
            .agg([
                pl.col("volume").sum().alias("total_alt_volume"),
            ])
        )
        
        # 7d rolling median
        daily_volume = daily_volume.with_columns([
            pl.col("total_alt_volume").rolling_median(window_size=7).alias("raw_liquidity_7d_median"),
        ])
        
        # Z-score of delta
        daily_volume = daily_volume.with_columns([
            (pl.col("total_alt_volume") - pl.col("raw_liquidity_7d_median")).alias("volume_delta"),
        ])
        
        daily_volume = daily_volume.with_columns([
            ((pl.col("volume_delta") - pl.col("volume_delta").rolling_mean(window_size=30)) /
             pl.col("volume_delta").rolling_std(window_size=30)).alias("raw_liquidity_z_delta"),
        ])
        
        # Fraction of alts at 30d volume highs
        alt_volume_30d_high = (
            alt_volume
            .sort(["asset_id", "date"])
            .with_columns([
                (pl.col("volume") == pl.col("volume").rolling_max(window_size=30).over("asset_id")).alias("is_30d_high")
            ])
            .group_by("date")
            .agg([
                (pl.col("is_30d_high").sum().cast(pl.Float64) / pl.count() * 100.0).alias("raw_liquidity_pct_at_high")
            ])
        )
        
        # Join
        liquidity = daily_volume.join(alt_volume_30d_high, on="date", how="outer")
        
        return liquidity.select([
            "date",
            "raw_liquidity_7d_median",
            "raw_liquidity_z_delta",
            "raw_liquidity_pct_at_high",
        ])
    
    def _compute_volatility_spread(
        self,
        prices: pl.DataFrame,
        marketcap: pl.DataFrame,
        majors: list,
        alt_assets: list,
    ) -> pl.DataFrame:
        """Compute volatility spread features."""
        # Compute returns
        prices_sorted = prices.sort(["asset_id", "date"])
        returns = prices_sorted.with_columns([
            (pl.col("close") / pl.col("close").shift(1).over("asset_id") - 1.0).alias("ret")
        ])
        
        # BTC volatility (7d realized)
        btc_ret = returns.filter(pl.col("asset_id") == "BTC")
        btc_vol = (
            btc_ret
            .with_columns([
                pl.col("ret").rolling_std(window_size=7).alias("btc_vol_7d")
            ])
            .select(["date", "btc_vol_7d"])
        )
        
        # ALT index volatility (cap-weighted)
        # Get marketcap for weighting
        alt_mcap = marketcap.filter(pl.col("asset_id").is_in(alt_assets))
        alt_ret = returns.filter(pl.col("asset_id").is_in(alt_assets))
        
        # Join returns with marketcap
        alt_ret_mcap = alt_ret.join(alt_mcap, on=["asset_id", "date"], how="inner")
        
        # Compute cap-weighted ALT index return
        alt_ret_mcap = alt_ret_mcap.with_columns([
            (pl.col("ret") * pl.col("marketcap")).alias("ret_mcap"),
        ])
        
        daily_alt_ret = (
            alt_ret_mcap
            .group_by("date")
            .agg([
                pl.col("ret_mcap").sum().alias("ret_mcap_sum"),
                pl.col("marketcap").sum().alias("mcap_sum"),
            ])
            .with_columns([
                (pl.col("ret_mcap_sum") / pl.col("mcap_sum")).alias("alt_index_ret")
            ])
            .select(["date", "alt_index_ret"])
        )
        
        # ALT index volatility
        alt_vol = daily_alt_ret.with_columns([
            pl.col("alt_index_ret").rolling_std(window_size=7).alias("alt_vol_7d")
        ])
        
        # Volatility spread
        vol_spread = (
            btc_vol
            .join(alt_vol, on="date", how="outer")
            .with_columns([
                (pl.col("alt_vol_7d") - pl.col("btc_vol_7d")).alias("raw_volatility_spread")
            ])
            .select(["date", "raw_volatility_spread"])
        )
        
        return vol_spread
    
    def _compute_momentum(
        self,
        prices: pl.DataFrame,
        majors: list,
        alt_assets: list,
    ) -> pl.DataFrame:
        """Compute cross-sectional momentum features."""
        # Compute returns
        prices_sorted = prices.sort(["asset_id", "date"])
        returns = prices_sorted.with_columns([
            (pl.col("close") / pl.col("close").shift(1).over("asset_id") - 1.0).alias("ret")
        ])
        
        # ALT returns
        alt_ret = returns.filter(pl.col("asset_id").is_in(alt_assets))
        
        # Median ALT returns (3d, 7d)
        alt_momentum = (
            alt_ret
            .with_columns([
                pl.col("ret").rolling_mean(window_size=3).over("asset_id").alias("ret_3d"),
                pl.col("ret").rolling_mean(window_size=7).over("asset_id").alias("ret_7d"),
            ])
            .group_by("date")
            .agg([
                pl.col("ret_3d").median().alias("raw_momentum_alt_3d"),
                pl.col("ret_7d").median().alias("raw_momentum_alt_7d"),
            ])
        )
        
        # Major returns (BTC/ETH average)
        major_ret = returns.filter(pl.col("asset_id").is_in(majors))
        major_momentum = (
            major_ret
            .with_columns([
                pl.col("ret").rolling_mean(window_size=3).over("asset_id").alias("ret_3d"),
                pl.col("ret").rolling_mean(window_size=7).over("asset_id").alias("ret_7d"),
            ])
            .group_by("date")
            .agg([
                pl.col("ret_3d").mean().alias("major_ret_3d"),
                pl.col("ret_7d").mean().alias("major_ret_7d"),
            ])
        )
        
        # Momentum spread
        momentum = (
            alt_momentum
            .join(major_momentum, on="date", how="outer")
            .with_columns([
                (pl.col("raw_momentum_alt_3d") - pl.col("major_ret_3d")).alias("raw_momentum_spread_3d"),
                (pl.col("raw_momentum_alt_7d") - pl.col("major_ret_7d")).alias("raw_momentum_spread_7d"),
            ])
            .select([
                "date",
                "raw_momentum_alt_3d",
                "raw_momentum_alt_7d",
                "raw_momentum_spread_3d",
                "raw_momentum_spread_7d",
            ])
        )
        
        return momentum
