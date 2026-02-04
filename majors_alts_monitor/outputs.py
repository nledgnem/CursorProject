"""Output generation (CSV, parquet, HTML dashboard)."""

import polars as pl
import numpy as np
from pathlib import Path
from datetime import date
import json
import logging
from typing import Optional, Dict, Any

try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False
    logger.warning("Plotly not available, skipping HTML dashboard")

from .experiment_manager import ExperimentManager

logger = logging.getLogger(__name__)


class OutputGenerator:
    """Generate outputs (CSV, parquet, HTML dashboard)."""
    
    def __init__(
        self,
        reports_dir: Path,
        artifacts_dir: Path,
        experiment_manager: Optional[ExperimentManager] = None,
    ):
        """
        Initialize output generator.
        
        Args:
            reports_dir: Directory for reports (CSV, HTML)
            artifacts_dir: Directory for artifacts (model params, logs)
            experiment_manager: Optional experiment manager for run manifests and catalog
        """
        self.reports_dir = Path(reports_dir)
        self.artifacts_dir = Path(artifacts_dir)
        self.experiment_manager = experiment_manager
        
        # Create directories
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
    
    def generate_outputs(
        self,
        regime_series: pl.DataFrame,
        features: pl.DataFrame,
        backtest_results: pl.DataFrame,
        start_date: date,
        end_date: date,
    ):
        """Generate all outputs."""
        # 1. Regime timeline
        self._write_regime_timeline(regime_series)
        
        # 2. Features
        self._write_features(features)
        
        # 3. Backtest results
        self._write_backtest_results(backtest_results)
        
        # 4. KPIs
        kpis = self._compute_kpis(backtest_results)
        self._write_kpis(kpis)
        
        # 5. By-regime metrics
        regime_metrics = self._compute_regime_metrics(backtest_results, regime_series)
        self._write_regime_metrics(regime_metrics)
        
        # 6. HTML dashboard
        if PLOTLY_AVAILABLE:
            self._generate_dashboard(regime_series, features, backtest_results)
        
        logger.info(f"Outputs written to {self.reports_dir}")
    
    def _write_regime_timeline(self, regime_series: pl.DataFrame):
        """Write regime timeline CSV."""
        path = self.reports_dir / "regime_timeline.csv"
        regime_series.write_csv(path)
        logger.info(f"Wrote regime timeline: {path}")
    
    def _write_features(self, features: pl.DataFrame):
        """Write features parquet."""
        path = self.reports_dir / "features_wide.parquet"
        features.write_parquet(path)
        logger.info(f"Wrote features: {path}")
    
    def _write_backtest_results(self, backtest_results: pl.DataFrame):
        """Write backtest results."""
        if len(backtest_results) == 0:
            logger.warning("Backtest results are empty, writing empty files")
            # Write empty DataFrames with expected schema
            empty_equity = pl.DataFrame({
                "date": [],
                "equity_curve": [],
            })
            empty_pnl = pl.DataFrame({
                "date": [],
                "r_ls_net": [],
            })
            empty_equity.write_csv(self.reports_dir / "bt_equity_curve.csv")
            empty_pnl.write_csv(self.reports_dir / "bt_daily_pnl.csv")
            return
        
        # Equity curve - start at 1.0, then compound returns
        if "r_ls_net" in backtest_results.columns:
            # Start with 1.0, then multiply by (1 + return) for each day
            equity = backtest_results.with_columns([
                (pl.lit(1.0) * (1.0 + pl.col("r_ls_net")).cum_prod()).alias("equity_curve")
            ])
        else:
            equity = backtest_results
        
        path = self.reports_dir / "bt_equity_curve.csv"
        equity.write_csv(path)
        
        # Daily PnL
        path = self.reports_dir / "bt_daily_pnl.csv"
        backtest_results.write_csv(path)
        
        logger.info(f"Wrote backtest results: {path}")
    
    def _compute_kpis(self, backtest_results: pl.DataFrame) -> dict:
        """Compute KPIs."""
        if len(backtest_results) == 0:
            return {}
        
        returns = backtest_results["r_ls_net"].to_numpy()
        returns = returns[~np.isnan(returns)]
        
        if len(returns) == 0:
            return {}
        
        # Equity curve
        equity = np.cumprod(1.0 + returns)
        
        # CAGR
        n_days = len(returns)
        total_return = equity[-1] / equity[0] - 1.0
        cagr = (1.0 + total_return) ** (252.0 / n_days) - 1.0
        
        # Sharpe
        mean_ret = np.mean(returns)
        std_ret = np.std(returns)
        sharpe = (mean_ret / std_ret * np.sqrt(252)) if std_ret > 0 else 0.0
        
        # Sortino (downside deviation)
        downside_returns = returns[returns < 0]
        downside_std = np.std(downside_returns) if len(downside_returns) > 0 else 0.0
        sortino = (mean_ret / downside_std * np.sqrt(252)) if downside_std > 0 else 0.0
        
        # Max drawdown
        running_max = np.maximum.accumulate(equity)
        drawdown = (equity - running_max) / running_max
        max_dd = np.min(drawdown)
        
        # Calmar
        calmar = cagr / abs(max_dd) if max_dd != 0 else 0.0
        
        # Hit rate
        hit_rate = np.mean(returns > 0)
        
        # Turnover
        turnover = backtest_results["alt_turnover"].mean() if "alt_turnover" in backtest_results.columns else 0.0
        
        # Funding
        avg_funding = backtest_results["funding"].mean() if "funding" in backtest_results.columns else 0.0
        
        return {
            "cagr": cagr,
            "sharpe": sharpe,
            "sortino": sortino,
            "max_drawdown": max_dd,
            "calmar": calmar,
            "hit_rate": hit_rate,
            "avg_turnover": turnover,
            "avg_funding_daily": avg_funding,
        }
    
    def compute_stability_metrics(self, regime_series: pl.DataFrame) -> Dict[str, Any]:
        """Compute stability metrics from regime series."""
        if self.experiment_manager:
            return self.experiment_manager.compute_stability_metrics(regime_series)
        else:
            # Fallback implementation
            if len(regime_series) == 0:
                return {
                    "switches_per_year": 0.0,
                    "avg_regime_duration_days": 0.0,
                    "regime_distribution": {},
                }
            
            regime_col = regime_series["regime"]
            switches = (regime_col != regime_col.shift(1)).sum() - 1
            switches = max(0, switches)
            
            dates = regime_series["date"].sort()
            if len(dates) > 1:
                total_days = (dates.max() - dates.min()).days + 1
                years = total_days / 365.25
                switches_per_year = switches / years if years > 0 else 0.0
            else:
                switches_per_year = 0.0
                total_days = 1
            
            regime_durations = []
            current_regime = None
            current_start_idx = None
            
            for idx, row in enumerate(regime_series.iter_rows(named=True)):
                regime = row["regime"]
                if regime != current_regime:
                    if current_regime is not None and current_start_idx is not None:
                        duration = idx - current_start_idx
                        regime_durations.append(duration)
                    current_regime = regime
                    current_start_idx = idx
            
            if current_start_idx is not None:
                duration = len(regime_series) - current_start_idx
                regime_durations.append(duration)
            
            avg_regime_duration = sum(regime_durations) / len(regime_durations) if regime_durations else 0.0
            
            regime_counts = regime_series["regime"].value_counts()
            regime_distribution = {
                regime: count / len(regime_series) * 100.0
                for regime, count in zip(regime_counts["regime"], regime_counts["count"])
            }
            
            return {
                "switches_per_year": switches_per_year,
                "avg_regime_duration_days": avg_regime_duration,
                "regime_distribution": regime_distribution,
                "total_switches": int(switches),
                "total_days": int(total_days),
            }
    
    def _write_kpis(self, kpis: dict):
        """Write KPIs JSON."""
        path = self.reports_dir / "kpis.json"
        with open(path, "w") as f:
            json.dump(kpis, f, indent=2)
        logger.info(f"Wrote KPIs: {path}")
    
    def _compute_regime_metrics(
        self,
        backtest_results: pl.DataFrame,
        regime_series: pl.DataFrame,
    ) -> pl.DataFrame:
        """Compute by-regime metrics."""
        if len(backtest_results) == 0:
            logger.warning("Backtest results are empty, returning empty regime metrics")
            return pl.DataFrame({
                "regime": [],
                "n_days": [],
                "mean_return": [],
            })
        
        # Join
        if "date" not in backtest_results.columns:
            logger.warning("Backtest results missing 'date' column")
            return pl.DataFrame()
        
        joined = backtest_results.join(regime_series, on="date", how="inner")
        
        # Group by regime
        regime_stats = (
            joined
            .group_by("regime")
            .agg([
                pl.count().alias("n_days"),
                pl.col("r_ls_net").mean().alias("mean_return"),
                pl.col("r_ls_net").median().alias("median_return"),
                pl.col("r_ls_net").std().alias("std_return"),
                (pl.col("r_ls_net").mean() / pl.col("r_ls_net").std()).alias("sharpe_like"),
            ])
        )
        
        return regime_stats
    
    def _write_regime_metrics(self, regime_metrics: pl.DataFrame):
        """Write regime metrics CSV."""
        path = self.reports_dir / "regime_metrics.csv"
        regime_metrics.write_csv(path)
        logger.info(f"Wrote regime metrics: {path}")
    
    def _generate_dashboard(
        self,
        regime_series: pl.DataFrame,
        features: pl.DataFrame,
        backtest_results: pl.DataFrame,
    ):
        """Generate Plotly HTML dashboard."""
        if len(backtest_results) == 0:
            logger.warning("Skipping dashboard generation: backtest results are empty")
            return
        
        fig = make_subplots(
            rows=4, cols=1,
            subplot_titles=("Regime & Score", "Equity Curve", "Drawdown", "Daily Returns"),
            vertical_spacing=0.1,
        )
        
        # Convert to pandas for plotting
        regime_df = regime_series.to_pandas()
        bt_df = backtest_results.to_pandas()
        
        if "r_ls_net" not in bt_df.columns:
            logger.warning("Skipping dashboard: missing r_ls_net column")
            return
        
        # 1. Regime & Score
        regime_df = regime_df.set_index("date")
        fig.add_trace(
            go.Scatter(x=regime_df.index, y=regime_df["score"], name="Score", line=dict(color="blue")),
            row=1, col=1,
        )
        
        # 2. Equity curve
        equity = (1.0 + bt_df["r_ls_net"]).cumprod()
        bt_df = bt_df.set_index("date")
        fig.add_trace(
            go.Scatter(x=bt_df.index, y=equity, name="Equity", line=dict(color="green")),
            row=2, col=1,
        )
        
        # 3. Drawdown
        running_max = equity.expanding().max()
        drawdown = (equity - running_max) / running_max
        fig.add_trace(
            go.Scatter(x=bt_df.index, y=drawdown, name="Drawdown", fill="tozeroy", line=dict(color="red")),
            row=3, col=1,
        )
        
        # 4. Daily returns
        fig.add_trace(
            go.Bar(x=bt_df.index, y=bt_df["r_ls_net"], name="Daily Return"),
            row=4, col=1,
        )
        
        fig.update_layout(height=1200, title_text="Majors vs Alts Regime Monitor Dashboard")
        
        path = self.reports_dir / "dashboard.html"
        fig.write_html(path)
        logger.info(f"Wrote dashboard: {path}")
