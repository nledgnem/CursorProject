"""Performance metrics calculation."""

import pandas as pd
import numpy as np


def calculate_sharpe(returns: pd.Series, periods_per_year: int = 252) -> float:
    """Calculate annualized Sharpe ratio."""
    if len(returns) == 0 or returns.std() == 0:
        return 0.0
    return np.sqrt(periods_per_year) * returns.mean() / returns.std()


def calculate_max_drawdown(equity: pd.Series) -> float:
    """Calculate maximum drawdown."""
    running_max = equity.cummax()
    drawdown = (equity / running_max) - 1.0
    return drawdown.min()


def calculate_calmar(annual_return: float, max_dd: float) -> float:
    """Calculate Calmar ratio (annual return / abs(max drawdown))."""
    if max_dd == 0:
        return 0.0
    return annual_return / abs(max_dd)

