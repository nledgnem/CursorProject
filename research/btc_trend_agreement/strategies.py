"""
Exposure mappings, causal backtest engine, overlays and performance metrics.

EXECUTION / LOOK-AHEAD CONVENTION
---------------------------------
    TrendScore(t)    computed from closes up to 23:59:59 UTC on date t
    target w(t)      = map[TrendScore(t)] * DVOLmult(DVOL(t))     (known at t)
    executed on      close of date t + (SIGNAL_LAG - 1)
    earns            the return of date t + SIGNAL_LAG,
                     i.e. close(t+LAG-1) -> close(t+LAG)

With SIGNAL_LAG = 2 (the default) the signal from Monday's close is traded at
Tuesday's close and first earns Wednesday's return. That is one full day more
conservative than the theoretical minimum; SIGNAL_LAG = 1 (trade at the same
close you observed) is reported as a sensitivity.

The drawdown brake is applied inside the day loop using ONLY the strategy
equity curve through the previous day, so it is causal by construction and
cannot peek at the drawdown it is about to experience.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from config import (BASE_COST_BPS, DD_FLOOR, DD_FULL, DD_MIN_MULT,
                    DVOL_THRESHOLD_DEFAULT, SIGNAL_LAG, TRADING_DAYS_PER_YEAR)


# --------------------------------------------------------------------------
# Exposure construction
# --------------------------------------------------------------------------
def trend_exposure(score: pd.Series, mapping: dict[int, float]) -> pd.Series:
    return score.map(mapping).astype(float).rename("trend_exposure")


def dvol_multiplier(dvol: pd.Series, threshold: float = DVOL_THRESHOLD_DEFAULT) -> pd.Series:
    """One-way volatility brake: min(1, threshold / DVOL).

    NOTE: this is OUR INTERPRETATION of Coinbase's verbal description, not a
    verified reproduction of their formula. DVOL <= threshold leaves exposure
    untouched; above it, exposure scales down inversely with implied vol. It
    can only ever reduce exposure -- never lever up.
    """
    return np.minimum(1.0, threshold / dvol.astype(float)).rename("dvol_mult")


def drawdown_multiplier_from_dd(dd: float) -> float:
    """Piecewise-linear equity-curve brake (dd is a POSITIVE fraction)."""
    if dd <= DD_FULL:
        return 1.0
    if dd >= DD_FLOOR:
        return DD_MIN_MULT
    frac = (dd - DD_FULL) / (DD_FLOOR - DD_FULL)
    return 1.0 - frac * (1.0 - DD_MIN_MULT)


# --------------------------------------------------------------------------
# Backtest
# --------------------------------------------------------------------------
def run_backtest(
    asset_ret: pd.Series,
    target_weight: pd.Series,
    cost_bps: float = BASE_COST_BPS,
    lag: int = SIGNAL_LAG,
    use_drawdown_brake: bool = False,
    cash_rate_annual: float = 0.0,
) -> pd.DataFrame:
    """Daily causal backtest of a long-only BTC exposure schedule.

    asset_ret     : simple daily returns, indexed by UTC date
    target_weight : desired exposure KNOWN AT date t (pre-lag, pre-brake)
    Returns a frame with executed weight, strategy return and equity.
    """
    idx = asset_ret.index
    tw = target_weight.reindex(idx)
    lagged = tw.shift(lag)

    r = asset_ret.to_numpy(dtype=float)
    w_pre = lagged.to_numpy(dtype=float)
    n = len(idx)

    daily_cash = (1.0 + cash_rate_annual) ** (1.0 / TRADING_DAYS_PER_YEAR) - 1.0
    c = cost_bps / 10_000.0

    w_exec = np.zeros(n)
    ret = np.zeros(n)
    equity = np.ones(n)
    dd_mult = np.ones(n)

    peak = 1.0
    eq = 1.0
    w_prev = 0.0

    for i in range(n):
        wi = w_pre[i]
        if not np.isfinite(wi):
            wi = 0.0
        if use_drawdown_brake:
            dd = 0.0 if peak <= 0 else max(0.0, 1.0 - eq / peak)
            m = drawdown_multiplier_from_dd(dd)
        else:
            m = 1.0
        dd_mult[i] = m
        wi = wi * m
        w_exec[i] = wi

        ri = r[i] if np.isfinite(r[i]) else 0.0
        gross = wi * ri + (1.0 - wi) * daily_cash
        turn_cost = abs(wi - w_prev) * c
        net = gross - turn_cost
        ret[i] = net

        eq = eq * (1.0 + net)
        equity[i] = eq
        peak = max(peak, eq)
        w_prev = wi

    return pd.DataFrame(
        {"asset_ret": r, "target_weight": tw.values, "dd_mult": dd_mult,
         "weight": w_exec, "strategy_ret": ret, "equity": equity},
        index=idx,
    )


# --------------------------------------------------------------------------
# Metrics
# --------------------------------------------------------------------------
def _max_drawdown(equity: pd.Series) -> float:
    return float((equity / equity.cummax() - 1.0).min())


def performance_metrics(bt: pd.DataFrame, bench_ret: pd.Series | None = None,
                        label: str = "") -> dict:
    r = bt["strategy_ret"].astype(float)
    eq = bt["equity"].astype(float)
    n = len(r)
    if n == 0:
        return {}
    years = n / TRADING_DAYS_PER_YEAR
    total = float(eq.iloc[-1] - 1.0)
    cagr = float(eq.iloc[-1] ** (1.0 / years) - 1.0) if years > 0 else np.nan
    vol = float(r.std(ddof=1) * np.sqrt(TRADING_DAYS_PER_YEAR))
    downside = r[r < 0]
    dvolat = float(downside.std(ddof=1) * np.sqrt(TRADING_DAYS_PER_YEAR)) if len(downside) > 1 else np.nan
    mdd = _max_drawdown(eq)
    sharpe = float(cagr / vol) if vol and np.isfinite(vol) and vol > 0 else np.nan
    sortino = float(cagr / dvolat) if dvolat and np.isfinite(dvolat) and dvolat > 0 else np.nan
    calmar = float(cagr / abs(mdd)) if mdd < 0 else np.nan

    by_year = (1 + r).groupby(r.index.year).prod() - 1
    w = bt["weight"].astype(float)
    dw = w.diff().abs().fillna(w.abs())

    monthly = (1 + r).groupby([r.index.year, r.index.month]).prod() - 1
    roll5 = (1 + r).rolling(5).apply(np.prod, raw=True) - 1
    roll20 = (1 + r).rolling(20).apply(np.prod, raw=True) - 1

    out = {
        "strategy": label,
        "start": str(r.index[0].date()),
        "end": str(r.index[-1].date()),
        "years": round(years, 2),
        "cumulative_return": total,
        "CAGR": cagr,
        "ann_vol": vol,
        "downside_vol": dvolat,
        "Sharpe": sharpe,
        "Sortino": sortino,
        "max_drawdown": mdd,
        "Calmar": calmar,
        "worst_year": float(by_year.min()),
        "worst_year_label": int(by_year.idxmin()),
        "best_year": float(by_year.max()),
        "best_year_label": int(by_year.idxmax()),
        "worst_month": float(monthly.min()),
        "worst_5d": float(roll5.min()),
        "worst_20d": float(roll20.min()),
        "pct_time_invested": float((w > 0.01).mean()),
        "avg_exposure": float(w.mean()),
        "turnover_ann": float(dw.sum() / years),
        "n_position_changes": int((dw > 0.01).sum()),
    }

    if bench_ret is not None:
        b = bench_ret.reindex(r.index).astype(float).fillna(0.0)
        up = b > 0
        dn = b < 0
        out["upside_capture"] = float(r[up].mean() / b[up].mean()) if up.sum() and b[up].mean() != 0 else np.nan
        out["downside_capture"] = float(r[dn].mean() / b[dn].mean()) if dn.sum() and b[dn].mean() != 0 else np.nan
    return out


def buy_and_hold(asset_ret: pd.Series) -> pd.DataFrame:
    idx = asset_ret.index
    r = asset_ret.fillna(0.0)
    return pd.DataFrame(
        {"asset_ret": r, "target_weight": 1.0, "dd_mult": 1.0, "weight": 1.0,
         "strategy_ret": r, "equity": (1 + r).cumprod()},
        index=idx,
    )
