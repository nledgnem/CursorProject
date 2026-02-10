import pandas as pd

df = pd.read_csv('reports/msm_funding_v0/msm_v0_feb2024_onwards/msm_timeseries.csv')
print(f'Total weeks: {len(df)}')
print(f'label_v0_0 non-NA: {df["label_v0_0"].notna().sum()}')
print(f'label_v0_1 non-NA: {df["label_v0_1"].notna().sum()}')
print(f'\nFirst 5 rows:')
print(df[['decision_date', 'F_tk', 'label_v0_0', 'label_v0_1', 'y']].head(5).to_string())

print(f'\nSummary by label v0_0:')
summary_v0_0 = pd.read_csv('reports/msm_funding_v0/msm_v0_feb2024_onwards/summary_by_label_v0_0.csv')
print(summary_v0_0.to_string())

print(f'\nSummary by label v0_1:')
try:
    summary_v0_1 = pd.read_csv('reports/msm_funding_v0/msm_v0_feb2024_onwards/summary_by_label_v0_1.csv')
    print(summary_v0_1.to_string())
except:
    print('No v0_1 summary (insufficient history for 52-week window)')
