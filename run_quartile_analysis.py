import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

def run_quartile_analysis(csv_path: str):
    # 1. Load data and filter to last 2 years (approx 104 weeks)
    df = pd.read_csv(csv_path, parse_dates=["decision_date"])
    df = df.sort_values("decision_date").dropna(subset=["F_tk", "y"])
    df_2yr = df.tail(104).copy()

    # 2. Divide the weeks into 4 Funding Quartiles
    df_2yr['Funding_Quartile'] = pd.qcut(
        df_2yr['F_tk'], 
        q=4, 
        labels=['Q1 (Lowest)', 'Q2 (Low-Mid)', 'Q3 (Mid-High)', 'Q4 (Highest)']
    )

    # 3. Sum log returns for each quartile and convert to arithmetic
    quartile_log_returns = df_2yr.groupby('Funding_Quartile', observed=True)['y'].sum()
    quartile_arith_returns = (np.exp(quartile_log_returns) - 1.0) * 100

    # 4. Plotting
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Color code: Green if profitable, Red if negative
    colors = ['seagreen' if val > 0 else 'firebrick' for val in quartile_arith_returns]
    
    bars = ax.bar(quartile_arith_returns.index, quartile_arith_returns.values, color=colors, edgecolor='black')
    ax.axhline(0, color='black', linewidth=1.5)
    
    ax.set_ylabel('Cumulative Return Contribution (%)', fontsize=12, fontweight='bold')
    ax.set_xlabel('Average Alt Funding Rate Quartiles', fontsize=12, fontweight='bold')
    ax.set_title('Strategy Return by Funding Rate Quartile (Last 2 Years)', fontsize=14)
    
    # Add data labels on top of bars
    for bar in bars:
        height = bar.get_height()
        label_y = height + (max(quartile_arith_returns.max() * 0.05, 5) if height > 0 else -max(abs(quartile_arith_returns.min()) * 0.05, 5))
        ax.text(bar.get_x() + bar.get_width()/2., label_y,
                f'{height:.1f}%', ha='center', va='bottom' if height > 0 else 'top', fontweight='bold')

    fig.tight_layout()
    
    output_path = Path("reports/funding_quartiles_bar.png")
    output_path.parent.mkdir(exist_ok=True, parents=True)
    plt.savefig(output_path, dpi=300)
    print(f"Quartile chart saved to {output_path}")

if __name__ == "__main__":
    run_quartile_analysis("reports/msm_funding_v0/20260310_103356/msm_timeseries.csv")
