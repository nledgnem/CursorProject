"""
CHZ World Cup Event Study Analysis
Comprehensive event study and correlation analysis for Chiliz (CHZ) around major football events.
"""

import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')

# Try to import CCXT, fallback to CoinGecko
try:
    import ccxt
    HAS_CCXT = True
except ImportError:
    HAS_CCXT = False
    print("[WARN] CCXT not installed. Will use CoinGecko API instead.")

# Try to import CoinGecko provider from codebase
HAS_COINGECKO_PROVIDER = False
try:
    import sys
    parent_dir = Path(__file__).parent.parent
    if (parent_dir / "src" / "providers" / "coingecko.py").exists():
        sys.path.insert(0, str(parent_dir))
        from src.providers.coingecko import fetch_price_history, COINGECKO_BASE
        HAS_COINGECKO_PROVIDER = True
except ImportError:
    pass

# Fallback: direct CoinGecko API calls
HAS_COINGECKO = True  # We can always try direct API


# Event definitions - All events
EVENTS = {
    "FIFA_WC_2018": {
        "name": "FIFA World Cup 2018 (Russia)",
        "start": date(2018, 6, 14),
        "end": date(2018, 7, 15),
    },
    "FIFA_WC_2022": {
        "name": "FIFA World Cup 2022 (Qatar)",
        "start": date(2022, 11, 20),
        "end": date(2022, 12, 18),
    },
    "EURO_2020": {
        "name": "UEFA Euro 2020 (played 2021)",
        "start": date(2021, 6, 11),
        "end": date(2021, 7, 11),
    },
    "EURO_2024": {
        "name": "UEFA Euro 2024",
        "start": date(2024, 6, 14),
        "end": date(2024, 7, 14),
    },
    "COPA_2024": {
        "name": "Copa América 2024",
        "start": date(2024, 6, 20),
        "end": date(2024, 7, 14),
    },
    "FIFA_WC_2026": {
        "name": "FIFA World Cup 2026 (USA/Canada/Mexico)",
        "start": date(2026, 6, 8),
        "end": date(2026, 7, 8),
    },
}

# Window definitions (days relative to event start)
WINDOWS = {
    "pre_120_90": (-120, -90),
    "pre_90_60": (-90, -60),
    "pre_60_30": (-60, -30),
    "pre_30_14": (-30, -14),
    "pre_14_0": (-14, 0),
    "event_0_7": (0, 7),
    "event_0_14": (0, 14),
    "event_0_30": (0, 30),
    "post_14_30": (14, 30),
    "post_30_60": (30, 60),
    "post_60_90": (60, 90),
}


def fetch_ccxt_data(symbol: str, start_date: date, end_date: date, exchange_name: str = "binance") -> pd.DataFrame:
    """Fetch OHLCV data using CCXT."""
    if not HAS_CCXT:
        raise ImportError("CCXT not available")
    
    exchange_class = getattr(ccxt, exchange_name)
    exchange = exchange_class({
        'enableRateLimit': True,
    })
    
    # Convert dates to timestamps
    since = int(datetime.combine(start_date, datetime.min.time()).timestamp() * 1000)
    until = int(datetime.combine(end_date, datetime.max.time()).timestamp() * 1000)
    
    # Fetch in chunks if needed (CCXT limit is typically 1000-5000)
    all_ohlcv = []
    current_since = since
    chunk_days = 365  # Fetch 1 year at a time
    
    while current_since < until:
        try:
            ohlcv = exchange.fetch_ohlcv(symbol, '1d', since=current_since, limit=1000)
            if not ohlcv:
                break
            all_ohlcv.extend(ohlcv)
            # Move to next chunk (use last timestamp + 1 day)
            last_ts = ohlcv[-1][0]
            current_since = last_ts + (24 * 60 * 60 * 1000)  # Add 1 day in ms
            if current_since >= until:
                break
        except Exception as e:
            print(f"    Warning: CCXT fetch error: {e}. Using available data.")
            break
    
    if not all_ohlcv:
        raise ValueError(f"No data fetched for {symbol}")
    
    df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df = df.drop_duplicates(subset=['timestamp']).sort_values('timestamp')
    df['date'] = pd.to_datetime(df['timestamp'], unit='ms').dt.date
    df = df[['date', 'open', 'high', 'low', 'close', 'volume']]
    df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
    df = df.sort_values('date').reset_index(drop=True)
    
    return df


def fetch_coingecko_data(symbol: str, coingecko_id: str, start_date: date, end_date: date) -> pd.DataFrame:
    """Fetch price data using CoinGecko."""
    if HAS_COINGECKO_PROVIDER:
        try:
            prices, mcaps, volumes = fetch_price_history(coingecko_id, start_date, end_date)
            
            # Convert to DataFrame
            dates = sorted(prices.keys())
            df = pd.DataFrame({
                'date': dates,
                'close': [prices[d] for d in dates],
                'volume': [volumes.get(d, np.nan) for d in dates],
            })
            df['open'] = df['close'].shift(1).fillna(df['close'])
            df['high'] = df['close']
            df['low'] = df['close']
            return df
        except Exception as e:
            print(f"  CoinGecko provider failed: {e}. Trying direct API...")
    
    # Direct API call fallback
    import requests
    import time
    
    url = f"https://api.coingecko.com/api/v3/coins/{coingecko_id}/market_chart/range"
    start_ts = int(datetime.combine(start_date, datetime.min.time()).timestamp())
    end_ts = int(datetime.combine(end_date, datetime.max.time()).timestamp())
    
    params = {
        "vs_currency": "usd",
        "from": start_ts,
        "to": end_ts,
    }
    
    resp = requests.get(url, params=params, timeout=30)
    if resp.status_code != 200:
        raise Exception(f"CoinGecko API error: {resp.status_code}")
    
    data = resp.json()
    prices_data = data.get("prices", [])
    
    dates = []
    closes = []
    for ts_ms, price in prices_data:
        d = datetime.fromtimestamp(ts_ms / 1000.0).date()
        if start_date <= d <= end_date:
            dates.append(d)
            closes.append(float(price))
    
    df = pd.DataFrame({'date': dates, 'close': closes})
    df = df.sort_values('date').reset_index(drop=True)
    df['open'] = df['close'].shift(1).fillna(df['close'])
    df['high'] = df['close']
    df['low'] = df['close']
    df['volume'] = np.nan
    
    return df


def fetch_price_data(symbol: str, start_date: date, end_date: date, 
                     coingecko_id: Optional[str] = None) -> pd.DataFrame:
    """
    Fetch price data, trying CCXT first, then CoinGecko.
    
    Args:
        symbol: Trading symbol (e.g., "CHZ/USDT")
        start_date: Start date
        end_date: End date
        coingecko_id: CoinGecko ID (e.g., "chiliz")
    """
    # Try CCXT first
    if HAS_CCXT:
        try:
            print(f"  Fetching {symbol} from Binance via CCXT...")
            return fetch_ccxt_data(symbol, start_date, end_date)
        except Exception as e:
            print(f"  CCXT failed: {e}. Trying CoinGecko...")
    
    # Fallback to CoinGecko
    if coingecko_id:
        print(f"  Fetching {coingecko_id} from CoinGecko...")
        return fetch_coingecko_data(symbol, coingecko_id, start_date, end_date)
    else:
        raise ValueError(f"Cannot fetch data for {symbol}: need coingecko_id if CCXT unavailable")


def compute_returns(df: pd.DataFrame) -> pd.DataFrame:
    """Compute daily returns and cumulative returns."""
    df = df.copy()
    df['return'] = df['close'].pct_change()
    df['cum_return'] = (1 + df['return']).cumprod() - 1
    return df


def compute_window_metrics(df: pd.DataFrame, window_start: int, window_end: int, 
                          event_start_date: date) -> Dict:
    """
    Compute metrics for a specific window relative to event start.
    
    Args:
        df: DataFrame with date and return columns
        window_start: Days before/after event start (negative = before)
        window_end: Days before/after event start
        event_start_date: Event start date
    
    Returns:
        Dictionary of metrics
    """
    start_date = event_start_date + timedelta(days=window_start)
    end_date = event_start_date + timedelta(days=window_end)
    
    window_df = df[(df['date'] >= start_date) & (df['date'] <= end_date)].copy()
    
    if len(window_df) == 0:
        return {
            'return': np.nan,
            'cum_return': np.nan,
            'max_drawdown': np.nan,
            'volatility': np.nan,
            'sharpe': np.nan,
            'peak_return': np.nan,
            'n_days': 0,
        }
    
    window_df['cum_return'] = (1 + window_df['return']).cumprod() - 1
    total_return = window_df['cum_return'].iloc[-1] if len(window_df) > 0 else 0.0
    
    # Max drawdown
    running_max = window_df['cum_return'].expanding().max()
    drawdown = window_df['cum_return'] - running_max
    max_drawdown = drawdown.min()
    
    # Volatility (annualized)
    volatility = window_df['return'].std() * np.sqrt(252)
    
    # Sharpe-like (using daily returns, assuming 0 risk-free rate)
    sharpe = (total_return / (volatility + 1e-8)) if volatility > 0 else np.nan
    
    # Peak return during window
    peak_return = window_df['cum_return'].max()
    
    return {
        'return': total_return,
        'cum_return': total_return,
        'max_drawdown': max_drawdown,
        'volatility': volatility,
        'sharpe': sharpe,
        'peak_return': peak_return,
        'n_days': len(window_df),
    }


def estimate_market_model(chz_df: pd.DataFrame, btc_df: pd.DataFrame, 
                         estimation_start: date, estimation_end: date) -> Tuple[float, float]:
    """
    Estimate market model: r_CHZ = alpha + beta * r_BTC + epsilon
    
    Returns:
        (alpha, beta)
    """
    # Merge on date
    merged = pd.merge(
        chz_df[['date', 'return']].rename(columns={'return': 'chz_return'}),
        btc_df[['date', 'return']].rename(columns={'return': 'btc_return'}),
        on='date',
        how='inner'
    )
    
    # Filter to estimation window
    est_data = merged[
        (merged['date'] >= estimation_start) & 
        (merged['date'] <= estimation_end)
    ].copy()
    
    if len(est_data) < 30:
        return (0.0, 1.0)  # Default if insufficient data
    
    # Remove NaN
    est_data = est_data.dropna()
    
    if len(est_data) < 30:
        return (0.0, 1.0)
    
    # OLS regression
    X = est_data['btc_return'].values
    y = est_data['chz_return'].values
    
    # Simple OLS
    X_with_const = np.column_stack([np.ones(len(X)), X])
    beta_hat = np.linalg.lstsq(X_with_const, y, rcond=None)[0]
    alpha, beta = beta_hat[0], beta_hat[1]
    
    return (alpha, beta)


def compute_abnormal_returns(chz_df: pd.DataFrame, btc_df: pd.DataFrame,
                            event_start: date, estimation_days: int = 120) -> pd.DataFrame:
    """
    Compute abnormal returns using market model.
    
    Args:
        chz_df: CHZ price data
        btc_df: BTC price data
        event_start: Event start date
        estimation_days: Days before event to use for beta estimation
    
    Returns:
        DataFrame with abnormal returns and CAR
    """
    # Estimation window: [-180, -60] days before event
    est_start = event_start - timedelta(days=180)
    est_end = event_start - timedelta(days=60)
    
    alpha, beta = estimate_market_model(chz_df, btc_df, est_start, est_end)
    
    # Merge data
    merged = pd.merge(
        chz_df[['date', 'return']].rename(columns={'return': 'chz_return'}),
        btc_df[['date', 'return']].rename(columns={'return': 'btc_return'}),
        on='date',
        how='inner'
    )
    
    # Compute expected return
    merged['expected_return'] = alpha + beta * merged['btc_return']
    
    # Abnormal return
    merged['abnormal_return'] = merged['chz_return'] - merged['expected_return']
    
    # Cumulative abnormal return
    merged['car'] = merged['abnormal_return'].cumsum()
    
    return merged


def bootstrap_ci(data: np.ndarray, n_boot: int = 10000, ci: float = 0.95) -> Tuple[float, float, float]:
    """
    Bootstrap confidence interval for mean.
    
    Returns:
        (mean, lower_ci, upper_ci)
    """
    if len(data) == 0 or np.all(np.isnan(data)):
        return (np.nan, np.nan, np.nan)
    
    data_clean = data[~np.isnan(data)]
    if len(data_clean) == 0:
        return (np.nan, np.nan, np.nan)
    
    means = []
    for _ in range(n_boot):
        sample = np.random.choice(data_clean, size=len(data_clean), replace=True)
        means.append(np.mean(sample))
    
    means = np.array(means)
    mean_val = np.mean(data_clean)
    lower = np.percentile(means, (1 - ci) / 2 * 100)
    upper = np.percentile(means, (1 + ci) / 2 * 100)
    
    return (mean_val, lower, upper)


def wilcoxon_test(data: np.ndarray) -> Tuple[float, float]:
    """
    Wilcoxon signed-rank test (tests if median is significantly different from 0).
    
    Returns:
        (statistic, p_value)
    """
    from scipy import stats
    
    data_clean = data[~np.isnan(data)]
    if len(data_clean) < 3:
        return (np.nan, np.nan)
    
    try:
        stat, pval = stats.wilcoxon(data_clean)
        return (stat, pval)
    except:
        return (np.nan, np.nan)


def compute_rolling_beta(chz_df: pd.DataFrame, btc_df: pd.DataFrame, window: int = 60) -> pd.DataFrame:
    """Compute rolling beta of CHZ vs BTC."""
    merged = pd.merge(
        chz_df[['date', 'return']].rename(columns={'return': 'chz_return'}),
        btc_df[['date', 'return']].rename(columns={'return': 'btc_return'}),
        on='date',
        how='inner'
    ).sort_values('date')
    
    merged['beta'] = np.nan
    
    for i in range(window, len(merged)):
        window_data = merged.iloc[i-window:i]
        window_data = window_data.dropna()
        
        if len(window_data) < 30:
            continue
        
        X = window_data['btc_return'].values
        y = window_data['chz_return'].values
        
        X_with_const = np.column_stack([np.ones(len(X)), X])
        beta_hat = np.linalg.lstsq(X_with_const, y, rcond=None)[0]
        merged.iloc[i, merged.columns.get_loc('beta')] = beta_hat[1]
    
    return merged[['date', 'beta']]


def compute_regime_splits(btc_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute BTC trend and volatility regimes.
    
    Returns:
        DataFrame with date, trend_regime, vol_regime columns
    """
    btc_df = btc_df.copy()
    btc_df = btc_df.sort_values('date').reset_index(drop=True)
    
    # Trend: 200D MA
    btc_df['ma200'] = btc_df['close'].rolling(200, min_periods=1).mean()
    btc_df['trend_regime'] = (btc_df['close'] > btc_df['ma200']).astype(int)
    
    # Volatility: rolling 30D vol
    btc_df['return'] = btc_df['close'].pct_change()
    btc_df['vol_30d'] = btc_df['return'].rolling(30, min_periods=1).std() * np.sqrt(252)
    vol_median = btc_df['vol_30d'].median()
    btc_df['vol_regime'] = (btc_df['vol_30d'] > vol_median).astype(int)
    
    return btc_df[['date', 'trend_regime', 'vol_regime']]


def main():
    """Main analysis function."""
    print("=" * 80)
    print("CHZ World Cup Event Study Analysis")
    print("=" * 80)
    
    # Create output directory
    output_dir = Path(__file__).parent / "outputs"
    output_dir.mkdir(exist_ok=True)
    
    # Determine date range (need data from before first event to after last event)
    first_event = min(evt['start'] for evt in EVENTS.values())
    last_event = max(evt['end'] for evt in EVENTS.values())
    
    # Add buffer for pre-event windows and post-event windows
    start_date = first_event - timedelta(days=180)
    # For 2026 World Cup, extend to cover post-event windows
    end_date = last_event + timedelta(days=120)
    
    # Fetch data up to today (January 14, 2026)
    from datetime import date as date_class
    today = date(2026, 1, 14)  # Set to today's date
    data_end_date = min(end_date, today)  # Don't fetch beyond today
    
    print(f"\nDate range: {start_date} to {end_date}")
    print(f"Events: {len(EVENTS)}")
    
    # Fetch data
    print("\n" + "=" * 80)
    print("FETCHING DATA")
    print("=" * 80)
    
    print("\nFetching CHZ...")
    chz_df = fetch_price_data("CHZ/USDT", start_date, data_end_date, coingecko_id="chiliz")
    chz_df = compute_returns(chz_df)
    print(f"  CHZ: {len(chz_df)} days, {chz_df['date'].min()} to {chz_df['date'].max()}")
    
    print("\nFetching BTC...")
    btc_df = fetch_price_data("BTC/USDT", start_date, data_end_date, coingecko_id="bitcoin")
    btc_df = compute_returns(btc_df)
    print(f"  BTC: {len(btc_df)} days, {btc_df['date'].min()} to {btc_df['date'].max()}")
    
    print("\nFetching ETH...")
    eth_df = fetch_price_data("ETH/USDT", start_date, data_end_date, coingecko_id="ethereum")
    eth_df = compute_returns(eth_df)
    print(f"  ETH: {len(eth_df)} days, {eth_df['date'].min()} to {eth_df['date'].max()}")
    
    # Save raw data
    chz_df.to_csv(output_dir / "chz_data.csv", index=False)
    btc_df.to_csv(output_dir / "btc_data.csv", index=False)
    eth_df.to_csv(output_dir / "eth_data.csv", index=False)
    
    print("\n" + "=" * 80)
    print("COMPUTING WINDOW METRICS")
    print("=" * 80)
    
    # Compute window metrics for each event
    all_results = []
    
    for event_id, event_info in EVENTS.items():
        print(f"\nProcessing {event_info['name']}...")
        event_start = event_info['start']
        
        for window_id, (w_start, w_end) in WINDOWS.items():
            metrics = compute_window_metrics(chz_df, w_start, w_end, event_start)
            
            # Also compute excess returns vs BTC and ETH
            btc_metrics = compute_window_metrics(btc_df, w_start, w_end, event_start)
            eth_metrics = compute_window_metrics(eth_df, w_start, w_end, event_start)
            
            metrics['excess_vs_btc'] = metrics['return'] - btc_metrics['return']
            metrics['excess_vs_eth'] = metrics['return'] - eth_metrics['return']
            
            all_results.append({
                'event_id': event_id,
                'event_name': event_info['name'],
                'window_id': window_id,
                'window_start': w_start,
                'window_end': w_end,
                **metrics,
            })
    
    results_df = pd.DataFrame(all_results)
    results_df.to_csv(output_dir / "window_metrics.csv", index=False)
    print(f"\nSaved window metrics to {output_dir / 'window_metrics.csv'}")
    
    # Compute abnormal returns (CAR)
    print("\n" + "=" * 80)
    print("COMPUTING ABNORMAL RETURNS (CAR)")
    print("=" * 80)
    
    car_results = []
    for event_id, event_info in EVENTS.items():
        print(f"\nComputing CAR for {event_info['name']}...")
        event_start = event_info['start']
        
        car_df = compute_abnormal_returns(chz_df, btc_df, event_start)
        car_df['event_id'] = event_id
        car_df['event_name'] = event_info['name']
        # Convert date to datetime if needed, then compute days difference
        if isinstance(car_df['date'].iloc[0], date):
            car_df['days_from_event'] = car_df['date'].apply(lambda x: (x - event_start).days)
        else:
            car_df['date'] = pd.to_datetime(car_df['date']).dt.date
            car_df['days_from_event'] = car_df['date'].apply(lambda x: (x - event_start).days)
        
        car_results.append(car_df)
    
    car_all = pd.concat(car_results, ignore_index=True)
    car_all.to_csv(output_dir / "abnormal_returns.csv", index=False)
    print(f"\nSaved abnormal returns to {output_dir / 'abnormal_returns.csv'}")
    
    # Compute rolling beta
    print("\n" + "=" * 80)
    print("COMPUTING ROLLING BETA")
    print("=" * 80)
    
    rolling_beta = compute_rolling_beta(chz_df, btc_df, window=60)
    rolling_beta.to_csv(output_dir / "rolling_beta.csv", index=False)
    print(f"\nSaved rolling beta to {output_dir / 'rolling_beta.csv'}")
    
    # Compute regime splits
    print("\n" + "=" * 80)
    print("COMPUTING REGIME SPLITS")
    print("=" * 80)
    
    regimes = compute_regime_splits(btc_df)
    regimes.to_csv(output_dir / "btc_regimes.csv", index=False)
    print(f"\nSaved regime splits to {output_dir / 'btc_regimes.csv'}")
    
    # Statistical tests
    print("\n" + "=" * 80)
    print("STATISTICAL TESTS")
    print("=" * 80)
    
    # Focus on key windows: pre-event and event windows
    key_windows = ['pre_60_30', 'pre_30_14', 'pre_14_0', 'event_0_7', 'event_0_14', 'event_0_30']
    
    stats_results = []
    for window_id in key_windows:
        window_data = results_df[results_df['window_id'] == window_id]['return'].values
        
        mean_val, lower_ci, upper_ci = bootstrap_ci(window_data)
        try:
            from scipy import stats
            wilcox_stat, wilcox_pval = wilcoxon_test(window_data)
        except:
            wilcox_stat, wilcox_pval = np.nan, np.nan
        
        stats_results.append({
            'window_id': window_id,
            'n_events': len(window_data),
            'mean_return': mean_val,
            'lower_ci_95': lower_ci,
            'upper_ci_95': upper_ci,
            'median_return': np.nanmedian(window_data),
            'hit_rate': np.mean(window_data > 0) if len(window_data) > 0 else np.nan,
            'wilcoxon_stat': wilcox_stat,
            'wilcoxon_pval': wilcox_pval,
        })
    
    stats_df = pd.DataFrame(stats_results)
    stats_df.to_csv(output_dir / "statistical_tests.csv", index=False)
    print(f"\nSaved statistical tests to {output_dir / 'statistical_tests.csv'}")
    
    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)
    print(f"\nAll outputs saved to: {output_dir}")
    print("\nNext steps:")
    print("  1. Run visualization script to generate charts")
    print("  2. Generate research memo")
    print("  3. Create tradeable playbook")


if __name__ == "__main__":
    main()
