import pandas as pd
import argparse


def generate_tearsheet(csv_path: str):
    # 1. Load data
    df = pd.read_csv(csv_path, parse_dates=["decision_date"])

    # Define the target columns we want to audit
    target_cols = ["decision_date", "F_tk", "y", "ret_btcdom_binance", "ret_btcdom_recon"]

    # Gracefully keep only the columns that actually exist in the CSV
    existing_cols = [c for c in target_cols if c in df.columns]
    df_clean = df[existing_cols].dropna()

    print("\n" + "=" * 50)
    print("DATA SANITY TEARSHEET")
    print("=" * 50)

    print(f"\nTotal Valid Weeks: {len(df_clean)}")
    print(f"Date Range: {df_clean['decision_date'].min().date()} to {df_clean['decision_date'].max().date()}")

    print("\n=== 1. Summary Statistics (Distributions) ===")
    print(df_clean.drop(columns=["decision_date"]).describe().round(4))

    print("\n=== 2. Correlation Matrix (Signal Alignment) ===")
    print(df_clean.corr(numeric_only=True).round(3))

    if "y" in df_clean.columns:
        print("\n=== 3. The 3 Best L/S Weeks (Upside Tail) ===")
        print(df_clean.nlargest(3, "y").to_string(index=False))

        print("\n=== 4. The 3 Worst L/S Weeks (Downside Tail Risk) ===")
        print(df_clean.nsmallest(3, "y").to_string(index=False))

    print("\n" + "=" * 50)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a quick sanity tearsheet from msm_timeseries.csv")
    parser.add_argument(
        "--csv",
        type=str,
        default="reports/msm_funding_v0/20260310_103356/msm_timeseries.csv",
        help="Path to msm_timeseries.csv",
    )
    args = parser.parse_args()
    generate_tearsheet(args.csv)
