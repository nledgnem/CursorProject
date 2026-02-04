"""
Visualization script for CHZ World Cup event study.
Generates all required charts and tables.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import date, timedelta
from pathlib import Path

# Try to import seaborn, make it optional
try:
    import seaborn as sns
    sns.set_style("whitegrid")
    HAS_SEABORN = True
except ImportError:
    HAS_SEABORN = False
    plt.style.use('default')

# Set style
plt.rcParams['figure.figsize'] = (14, 8)
plt.rcParams['font.size'] = 10

# Event definitions (must match main script) - All events
EVENTS = {
    "FIFA_WC_2018": {
        "name": "FIFA World Cup 2018",
        "start": date(2018, 6, 14),
        "end": date(2018, 7, 15),
        "color": "#1f77b4",
    },
    "FIFA_WC_2022": {
        "name": "FIFA World Cup 2022",
        "start": date(2022, 11, 20),
        "end": date(2022, 12, 18),
        "color": "#ff7f0e",
    },
    "EURO_2020": {
        "name": "UEFA Euro 2020 (played 2021)",
        "start": date(2021, 6, 11),
        "end": date(2021, 7, 11),
        "color": "#2ca02c",
    },
    "EURO_2024": {
        "name": "UEFA Euro 2024",
        "start": date(2024, 6, 14),
        "end": date(2024, 7, 14),
        "color": "#d62728",
    },
    "COPA_2024": {
        "name": "Copa América 2024",
        "start": date(2024, 6, 20),
        "end": date(2024, 7, 14),
        "color": "#9467bd",
    },
    "FIFA_WC_2026": {
        "name": "FIFA World Cup 2026",
        "start": date(2026, 6, 8),
        "end": date(2026, 7, 8),
        "color": "#8c564b",
    },
}


def load_data(output_dir: Path):
    """Load all data files."""
    chz_df = pd.read_csv(output_dir / "chz_data.csv")
    chz_df['date'] = pd.to_datetime(chz_df['date']).dt.date
    
    btc_df = pd.read_csv(output_dir / "btc_data.csv")
    btc_df['date'] = pd.to_datetime(btc_df['date']).dt.date
    
    results_df = pd.read_csv(output_dir / "window_metrics.csv")
    
    car_df = pd.read_csv(output_dir / "abnormal_returns.csv")
    car_df['date'] = pd.to_datetime(car_df['date']).dt.date
    
    rolling_beta = pd.read_csv(output_dir / "rolling_beta.csv")
    rolling_beta['date'] = pd.to_datetime(rolling_beta['date']).dt.date
    
    return chz_df, btc_df, results_df, car_df, rolling_beta


def plot_price_chart_with_events(chz_df: pd.DataFrame, btc_df: pd.DataFrame, 
                                 output_dir: Path):
    """Plot price chart with shaded event windows."""
    fig, axes = plt.subplots(2, 1, figsize=(16, 10), sharex=True)
    
    # CHZ chart
    ax1 = axes[0]
    chz_df_sorted = chz_df.sort_values('date')
    ax1.plot(chz_df_sorted['date'], chz_df_sorted['close'], 
             label='CHZ', linewidth=2, color='#2E86AB')
    
    # Shade event windows and add to legend
    legend_elements = [plt.Line2D([0], [0], color='#2E86AB', linewidth=2, label='CHZ')]
    for event_id, event_info in EVENTS.items():
        start = event_info['start']
        end = event_info['end']
        ax1.axvspan(start, end, alpha=0.2, color=event_info['color'])
        legend_elements.append(plt.Rectangle((0, 0), 1, 1, facecolor=event_info['color'], 
                                             alpha=0.2, label=event_info['name']))
    
    ax1.set_ylabel('CHZ Price (USD)', fontsize=12, fontweight='bold')
    ax1.set_title('CHZ Price with Major Football Event Windows', 
                  fontsize=14, fontweight='bold')
    ax1.legend(handles=legend_elements, loc='upper left', fontsize=9)
    ax1.grid(True, alpha=0.3)
    ax1.set_yscale('log')
    
    # BTC chart
    ax2 = axes[1]
    btc_df_sorted = btc_df.sort_values('date')
    ax2.plot(btc_df_sorted['date'], btc_df_sorted['close'], 
             label='BTC', linewidth=2, color='#F24236', alpha=0.7)
    
    # Shade event windows
    for event_id, event_info in EVENTS.items():
        start = event_info['start']
        end = event_info['end']
        ax2.axvspan(start, end, alpha=0.2, color=event_info['color'])
    
    ax2.set_ylabel('BTC Price (USD)', fontsize=12, fontweight='bold')
    ax2.set_xlabel('Date', fontsize=12, fontweight='bold')
    ax2.legend(loc='upper left', fontsize=9)
    ax2.grid(True, alpha=0.3)
    ax2.set_yscale('log')
    
    # Format x-axis - extend to December 2026 to show 2026 World Cup
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    ax2.xaxis.set_major_locator(mdates.YearLocator())
    # Set x-axis limits to show future event - extend to December 2026
    ax2.set_xlim(left=btc_df_sorted['date'].min(), right=date(2026, 12, 31))
    plt.xticks(rotation=45)
    
    plt.tight_layout()
    plt.savefig(output_dir / "price_chart_with_events.png", dpi=300, bbox_inches='tight')
    print(f"  Saved: price_chart_with_events.png")
    plt.close()


def plot_chz_btc_ratio(chz_df: pd.DataFrame, btc_df: pd.DataFrame, 
                       output_dir: Path):
    """Plot CHZ/BTC ratio to show relative performance."""
    # Merge dataframes on date
    merged = pd.merge(
        chz_df[['date', 'close']].rename(columns={'close': 'chz_price'}),
        btc_df[['date', 'close']].rename(columns={'close': 'btc_price'}),
        on='date',
        how='inner'
    ).sort_values('date')
    
    # Calculate ratio
    merged['chz_btc_ratio'] = merged['chz_price'] / merged['btc_price']
    
    fig, ax = plt.subplots(figsize=(16, 8))
    
    # Plot ratio
    ax.plot(merged['date'], merged['chz_btc_ratio'], 
            linewidth=2, color='#2E86AB', label='CHZ/BTC Ratio', zorder=2)
    
    # Add horizontal line at 1.0 for reference (if ratio was normalized)
    # Instead, add a moving average for context
    merged['ratio_ma30'] = merged['chz_btc_ratio'].rolling(30, min_periods=1).mean()
    ax.plot(merged['date'], merged['ratio_ma30'], 
            linewidth=1.5, color='#F24236', linestyle='--', alpha=0.7, 
            label='30-Day Moving Average', zorder=1)
    
    # Shade event windows and add to legend - match price chart style
    legend_elements = [
        plt.Line2D([0], [0], color='#2E86AB', linewidth=2, label='CHZ/BTC Ratio'),
        plt.Line2D([0], [0], color='#F24236', linewidth=1.5, linestyle='--', 
                  alpha=0.7, label='30-Day Moving Average')
    ]
    
    # Plot all events - standardize to match price_chart_with_events style
    for event_id, event_info in EVENTS.items():
        start = event_info['start']
        end = event_info['end']
        
        # Special handling for Copa América 2024 (overlaps with Euro 2024)
        if event_id == "COPA_2024":
            # Use diagonal hatch pattern to make it visible even when overlapping
            ax.axvspan(start, end, alpha=0.2, facecolor=event_info['color'], 
                      hatch='///', zorder=0, edgecolor=event_info['color'], linewidth=1.5)
            # Add text annotation
            if len(merged[merged['date'] <= start]) > 0:
                ratio_at_start = merged[merged['date'] <= start]['chz_btc_ratio'].iloc[-1]
            else:
                ratio_at_start = merged['chz_btc_ratio'].mean() if len(merged) > 0 else 0.1
            ax.annotate('Copa América\n2024', 
                       xy=(start, ratio_at_start), 
                       xytext=(10, 20), textcoords='offset points',
                       fontsize=8, fontweight='bold',
                       bbox=dict(boxstyle='round,pad=0.3', facecolor=event_info['color'], 
                                alpha=0.3, edgecolor=event_info['color'], linewidth=1.5),
                       arrowprops=dict(arrowstyle='->', color=event_info['color'], lw=1.5),
                       zorder=10)
            legend_elements.append(plt.Rectangle((0, 0), 1, 1, facecolor=event_info['color'], 
                                                 alpha=0.2, hatch='///',
                                                 label=event_info['name']))
        else:
            # Standard shading for all other events - match price chart (alpha=0.2)
            ax.axvspan(start, end, alpha=0.2, color=event_info['color'], zorder=0)
            legend_elements.append(plt.Rectangle((0, 0), 1, 1, facecolor=event_info['color'], 
                                                 alpha=0.2, label=event_info['name']))
    
    ax.set_ylabel('CHZ/BTC Ratio', fontsize=12, fontweight='bold')
    ax.set_xlabel('Date', fontsize=12, fontweight='bold')
    ax.set_title('CHZ vs BTC Relative Performance (Ratio)', 
                fontsize=14, fontweight='bold')
    ax.legend(handles=legend_elements, loc='best', fontsize=9, ncol=2)
    ax.grid(True, alpha=0.3)
    
    # Format x-axis - extend to December 2026 to show 2026 World Cup
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    ax.xaxis.set_major_locator(mdates.YearLocator())
    # Set x-axis limits to show future event - extend to December 2026
    ax.set_xlim(left=merged['date'].min(), right=date(2026, 12, 31))
    plt.xticks(rotation=45)
    
    # Remove vertical lines - keep it simple like price chart
    
    plt.tight_layout()
    plt.savefig(output_dir / "chz_btc_ratio_chart.png", dpi=300, bbox_inches='tight')
    print(f"  Saved: chz_btc_ratio_chart.png")
    plt.close()


def plot_window_returns(results_df: pd.DataFrame, output_dir: Path):
    """Plot bar chart of returns by window for each event."""
    # Focus on key windows
    key_windows = ['pre_60_30', 'pre_30_14', 'pre_14_0', 'event_0_7', 'event_0_14', 'event_0_30']
    key_results = results_df[results_df['window_id'].isin(key_windows)].copy()
    
    # Pivot for easier plotting
    pivot_data = key_results.pivot_table(
        index='event_name', 
        columns='window_id', 
        values='return',
        aggfunc='first'
    )
    
    fig, ax = plt.subplots(figsize=(14, 8))
    
    x = np.arange(len(pivot_data.index))
    width = 0.12
    
    window_labels = {
        'pre_60_30': 'Pre [-60,-30]',
        'pre_30_14': 'Pre [-30,-14]',
        'pre_14_0': 'Pre [-14,0]',
        'event_0_7': 'Event [0,+7]',
        'event_0_14': 'Event [0,+14]',
        'event_0_30': 'Event [0,+30]',
    }
    
    colors = plt.cm.viridis(np.linspace(0, 1, len(key_windows)))
    
    for i, window_id in enumerate(key_windows):
        if window_id in pivot_data.columns:
            values = pivot_data[window_id].values
            ax.bar(x + i * width, values, width, label=window_labels[window_id], 
                  color=colors[i], alpha=0.8)
    
    ax.set_xlabel('Event', fontsize=12, fontweight='bold')
    ax.set_ylabel('Return', fontsize=12, fontweight='bold')
    ax.set_title('CHZ Returns by Window Across Events', fontsize=14, fontweight='bold')
    ax.set_xticks(x + width * (len(key_windows) - 1) / 2)
    ax.set_xticklabels(pivot_data.index, rotation=45, ha='right')
    ax.legend(loc='upper left', fontsize=9)
    ax.axhline(y=0, color='black', linestyle='--', linewidth=0.8)
    ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig(output_dir / "window_returns_barchart.png", dpi=300, bbox_inches='tight')
    print(f"  Saved: window_returns_barchart.png")
    plt.close()


def plot_car_by_event(car_df: pd.DataFrame, output_dir: Path):
    """Plot Cumulative Abnormal Returns (CAR) for each event."""
    fig, axes = plt.subplots(len(EVENTS), 1, figsize=(14, 4 * len(EVENTS)), sharex=True)
    
    if len(EVENTS) == 1:
        axes = [axes]
    
    for idx, (event_id, event_info) in enumerate(EVENTS.items()):
        ax = axes[idx]
        
        event_car = car_df[car_df['event_id'] == event_id].sort_values('days_from_event')
        
        ax.plot(event_car['days_from_event'], event_car['car'], 
               linewidth=2, color=event_info['color'], label='CAR')
        ax.axhline(y=0, color='black', linestyle='--', linewidth=0.8)
        ax.axvline(x=0, color='red', linestyle='--', linewidth=1, alpha=0.7, label='Event Start')
        
        # Shade event period
        event_start = event_info['start']
        event_end = event_info['end']
        event_days = (event_end - event_start).days
        ax.axvspan(0, event_days, alpha=0.1, color=event_info['color'])
        
        ax.set_ylabel('CAR', fontsize=11, fontweight='bold')
        ax.set_title(f"{event_info['name']} - Cumulative Abnormal Returns", 
                    fontsize=12, fontweight='bold')
        ax.legend(loc='best', fontsize=9)
        ax.grid(True, alpha=0.3)
    
    axes[-1].set_xlabel('Days from Event Start', fontsize=12, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(output_dir / "car_by_event.png", dpi=300, bbox_inches='tight')
    print(f"  Saved: car_by_event.png")
    plt.close()


def plot_rolling_beta(rolling_beta: pd.DataFrame, chz_df: pd.DataFrame, 
                      output_dir: Path):
    """Plot rolling beta of CHZ vs BTC."""
    fig, axes = plt.subplots(2, 1, figsize=(14, 10), sharex=True)
    
    # Beta chart
    ax1 = axes[0]
    rolling_beta_sorted = rolling_beta.sort_values('date')
    ax1.plot(rolling_beta_sorted['date'], rolling_beta_sorted['beta'], 
            linewidth=2, color='#2E86AB', label='Rolling 60D Beta (CHZ vs BTC)')
    ax1.axhline(y=1.0, color='red', linestyle='--', linewidth=1, alpha=0.7, label='Beta = 1.0')
    
    # Mark event starts
    for event_id, event_info in EVENTS.items():
        ax1.axvline(x=event_info['start'], color=event_info['color'], 
                   linestyle=':', linewidth=1, alpha=0.5)
    
    ax1.set_ylabel('Beta', fontsize=12, fontweight='bold')
    ax1.set_title('Rolling Beta: CHZ vs BTC (60-day window)', 
                 fontsize=14, fontweight='bold')
    ax1.legend(loc='best', fontsize=9)
    ax1.grid(True, alpha=0.3)
    
    # CHZ price for context
    ax2 = axes[1]
    chz_df_sorted = chz_df.sort_values('date')
    ax2.plot(chz_df_sorted['date'], chz_df_sorted['close'], 
            linewidth=2, color='#2E86AB', label='CHZ Price')
    
    # Mark event starts
    for event_id, event_info in EVENTS.items():
        ax2.axvline(x=event_info['start'], color=event_info['color'], 
                   linestyle=':', linewidth=1, alpha=0.5, label=event_info['name'] 
                   if event_id == list(EVENTS.keys())[0] else "")
    
    ax2.set_ylabel('CHZ Price (USD)', fontsize=12, fontweight='bold')
    ax2.set_xlabel('Date', fontsize=12, fontweight='bold')
    ax2.legend(loc='upper left', fontsize=9)
    ax2.grid(True, alpha=0.3)
    ax2.set_yscale('log')
    
    # Format x-axis - extend to December 2026 to show 2026 World Cup
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    ax2.xaxis.set_major_locator(mdates.YearLocator())
    # Set x-axis limits to show future event - extend to December 2026
    if chz_df_sorted['date'].max() < date(2026, 12, 31):
        ax2.set_xlim(left=chz_df_sorted['date'].min(), right=date(2026, 12, 31))
    plt.xticks(rotation=45)
    
    plt.tight_layout()
    plt.savefig(output_dir / "rolling_beta.png", dpi=300, bbox_inches='tight')
    print(f"  Saved: rolling_beta.png")
    plt.close()


def plot_drawdown_curves(chz_df: pd.DataFrame, output_dir: Path):
    """Plot drawdown curves around events."""
    fig, axes = plt.subplots(len(EVENTS), 1, figsize=(14, 4 * len(EVENTS)), sharex=True)
    
    if len(EVENTS) == 1:
        axes = [axes]
    
    for idx, (event_id, event_info) in enumerate(EVENTS.items()):
        ax = axes[idx]
        
        event_start = event_info['start']
        window_start = event_start - timedelta(days=120)
        window_end = event_start + timedelta(days=90)
        
        window_df = chz_df[
            (chz_df['date'] >= window_start) & 
            (chz_df['date'] <= window_end)
        ].copy().sort_values('date')
        
        if len(window_df) == 0:
            continue
        
        # Compute drawdown
        window_df['cum_return'] = (1 + window_df['return']).cumprod() - 1
        running_max = window_df['cum_return'].expanding().max()
        drawdown = window_df['cum_return'] - running_max
        
        # Days from event
        if isinstance(window_df['date'].iloc[0], date):
            window_df['days_from_event'] = window_df['date'].apply(lambda x: (x - event_start).days)
        else:
            window_df['date'] = pd.to_datetime(window_df['date']).dt.date
            window_df['days_from_event'] = window_df['date'].apply(lambda x: (x - event_start).days)
        
        ax.fill_between(window_df['days_from_event'], 0, drawdown, 
                       alpha=0.3, color='red', label='Drawdown')
        ax.plot(window_df['days_from_event'], drawdown, 
               linewidth=2, color='red', label='Drawdown')
        ax.axhline(y=0, color='black', linestyle='--', linewidth=0.8)
        ax.axvline(x=0, color='blue', linestyle='--', linewidth=1, alpha=0.7, label='Event Start')
        
        # Shade event period
        event_days = (event_info['end'] - event_info['start']).days
        ax.axvspan(0, event_days, alpha=0.1, color='blue')
        
        ax.set_ylabel('Drawdown', fontsize=11, fontweight='bold')
        ax.set_title(f"{event_info['name']} - Drawdown Curve", 
                    fontsize=12, fontweight='bold')
        ax.legend(loc='best', fontsize=9)
        ax.grid(True, alpha=0.3)
    
    axes[-1].set_xlabel('Days from Event Start', fontsize=12, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(output_dir / "drawdown_curves.png", dpi=300, bbox_inches='tight')
    print(f"  Saved: drawdown_curves.png")
    plt.close()


def create_summary_table(results_df: pd.DataFrame, output_dir: Path):
    """Create summary table of key metrics."""
    key_windows = ['pre_60_30', 'pre_30_14', 'pre_14_0', 'event_0_7', 'event_0_14', 'event_0_30']
    key_results = results_df[results_df['window_id'].isin(key_windows)].copy()
    
    summary = []
    for window_id in key_windows:
        window_data = key_results[key_results['window_id'] == window_id]
        
        summary.append({
            'Window': window_id,
            'N Events': len(window_data),
            'Mean Return': f"{window_data['return'].mean():.2%}",
            'Median Return': f"{window_data['return'].median():.2%}",
            'Hit Rate': f"{window_data['return'].gt(0).mean():.1%}",
            'Mean Excess vs BTC': f"{window_data['excess_vs_btc'].mean():.2%}",
            'Mean Excess vs ETH': f"{window_data['excess_vs_eth'].mean():.2%}",
            'Mean Max DD': f"{window_data['max_drawdown'].mean():.2%}",
            'Mean Volatility': f"{window_data['volatility'].mean():.1%}",
        })
    
    summary_df = pd.DataFrame(summary)
    summary_df.to_csv(output_dir / "summary_table.csv", index=False)
    print(f"  Saved: summary_table.csv")
    
    # Also create LaTeX table (optional)
    try:
        latex_table = summary_df.to_latex(index=False, float_format="%.2f")
        with open(output_dir / "summary_table.tex", 'w') as f:
            f.write(latex_table)
        print(f"  Saved: summary_table.tex")
    except ImportError:
        print(f"  Skipped: summary_table.tex (jinja2 not available)")


def main():
    """Main visualization function."""
    print("=" * 80)
    print("CHZ World Cup Event Study - Visualization")
    print("=" * 80)
    
    output_dir = Path(__file__).parent / "outputs"
    
    if not (output_dir / "chz_data.csv").exists():
        print("\n[ERROR] Data files not found. Please run chz_event_study.py first.")
        return
    
    print("\nLoading data...")
    chz_df, btc_df, results_df, car_df, rolling_beta = load_data(output_dir)
    
    print("\nGenerating visualizations...")
    plot_price_chart_with_events(chz_df, btc_df, output_dir)
    plot_chz_btc_ratio(chz_df, btc_df, output_dir)
    plot_window_returns(results_df, output_dir)
    plot_car_by_event(car_df, output_dir)
    plot_rolling_beta(rolling_beta, chz_df, output_dir)
    plot_drawdown_curves(chz_df, output_dir)
    
    print("\nCreating summary tables...")
    create_summary_table(results_df, output_dir)
    
    print("\n" + "=" * 80)
    print("VISUALIZATION COMPLETE")
    print("=" * 80)
    print(f"\nAll outputs saved to: {output_dir}")


if __name__ == "__main__":
    main()
