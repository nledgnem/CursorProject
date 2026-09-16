"""Transaction-cost, funding and cost-sensitivity analysis.

Defaults mirror what the repo already uses or has measured live:
  * 5 bp fee + 5 bp slippage per side for majors (configs/golden.yaml);
  * ~28 bp effective taker fees and ~60 bp round trip for alt perps (Apathy live, BACKTEST.md);
  * square-root impact k * sigma * sqrt(participation) for size-dependent cost.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from qlib import stats


@dataclass
class CostModel:
    fee_bps: float = 5.0            # per side
    slippage_bps: float = 5.0       # per side, fixed
    half_spread_bps: float = 0.0    # per side
    impact_k: float = 0.0           # square-root impact coefficient (x daily vol)

    def side_bps(self, participation: float = 0.0, daily_vol: float = 0.0) -> float:
        impact = self.impact_k * daily_vol * np.sqrt(max(participation, 0.0)) * 1e4
        return self.fee_bps + self.slippage_bps + self.half_spread_bps + impact

    def round_trip(self, participation: float = 0.0, daily_vol: float = 0.0) -> float:
        """Round-trip cost as a decimal return."""
        return 2 * self.side_bps(participation, daily_vol) / 1e4


MAJORS = CostModel(fee_bps=5, slippage_bps=5)
ALT_PERPS = CostModel(fee_bps=5, slippage_bps=10, half_spread_bps=5, impact_k=0.1)
ALT_PERPS_STRESSED = CostModel(fee_bps=5, slippage_bps=25, half_spread_bps=15, impact_k=0.2)


def funding_pnl(position_sign, funding_per_interval) -> float:
    """P&L from funding for a perp position: longs pay positive funding."""
    f = np.asarray(funding_per_interval, dtype=float)
    return float(-np.sign(position_sign) * np.nansum(f))


def cost_sensitivity(gross_trade_returns, grid_bps=(0, 10, 20, 40, 60, 100, 150),
                     trades_per_year: float | None = None) -> pd.DataFrame:
    """Net mean / win rate / Sharpe of per-trade returns across round-trip cost levels.

    Also returns the break-even round-trip cost in bp (mean gross return).
    """
    g = pd.Series(gross_trade_returns).dropna()
    rows = []
    for c in grid_bps:
        net = g - c / 1e4
        r = {"round_trip_bps": c, "n": len(net), "net_mean": net.mean(), "net_median": net.median(),
             "win_rate": (net > 0).mean()}
        if trades_per_year:
            r["ann_sharpe_approx"] = net.mean() / net.std() * np.sqrt(trades_per_year) if net.std() > 0 else np.nan
        rows.append(r)
    out = pd.DataFrame(rows)
    out.attrs["breakeven_bps"] = float(g.mean() * 1e4)
    return out


def turnover_cost_series(weights: pd.Series, side_bps: float) -> pd.Series:
    """Daily cost drag from changes in a weight series (decimal)."""
    return weights.diff().abs().fillna(weights.abs()) * side_bps / 1e4


def net_of_costs_summary(gross: pd.Series, weights: pd.Series, grid_bps=(0, 5, 10, 20, 40)) -> pd.DataFrame:
    rows = []
    for c in grid_bps:
        net = gross - turnover_cost_series(weights, c)
        rows.append({"side_bps": c, **stats.perf_summary(net)})
    return pd.DataFrame(rows)
