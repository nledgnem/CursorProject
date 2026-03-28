"""One-off diagnostic: check fact_price.parquet for date continuity between 2026-01-20 and 2026-03-02."""
import pandas as pd
df = pd.read_parquet("data/curated/data_lake/fact_price.parquet")
# Normalize date column for comparison
df["date"] = pd.to_datetime(df["date"], utc=True).dt.date
start, end = pd.to_datetime("2026-01-20").date(), pd.to_datetime("2026-03-02").date()
in_range = df[df["date"].between(start, end)]
unique_dates = in_range["date"].nunique()
all_dates_in_range = (df["date"] >= start) & (df["date"] <= end)
date_counts = df.loc[all_dates_in_range].groupby("date").size()
print("fact_price.parquet diagnostic (2026-01-20 to 2026-03-02):")
print(f"  Rows in range: {len(in_range)}")
print(f"  Unique dates in range: {unique_dates}")
print(f"  Expected calendar days: {(end - start).days + 1}")
print(f"  Date range in file: {df['date'].min()} to {df['date'].max()}")
if len(date_counts) > 0:
    idx = sorted(date_counts.index)
    print(f"  Sample dates (first 5): {idx[:5]}")
    print(f"  Sample dates (last 5): {idx[-5:]}")
