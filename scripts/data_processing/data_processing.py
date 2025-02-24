import os
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

merged_file = os.path.join(base_dir, "data", "merged_data", "merged_dataset_20250206.csv.gz")
processed_dir = os.path.join(base_dir, "data", "processed_data")
os.makedirs(processed_dir, exist_ok=True)
processed_file = os.path.join(processed_dir, "processed_dataset_20250206.csv.gz")

try:
    df = pd.read_csv(merged_file, compression="gzip")
except Exception as e:
    print(f"Error loading the merged dataset: {e}")
    exit(1)


df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

df = df.dropna(subset=['timestamp', 'measurement'])

# Outlier Removal
def remove_outliers(group):
    Q1 = group['measurement'].quantile(0.25)
    Q3 = group['measurement'].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    return group[(group['measurement'] >= lower_bound) & (group['measurement'] <= upper_bound)]

df = df.groupby('measurement_type').apply(remove_outliers).reset_index(drop=True)

# Normalization:
def normalize(group):
    min_val = group['measurement'].min()
    max_val = group['measurement'].max()
    if max_val != min_val:
        group['normalized_measurement'] = (group['measurement'] - min_val) / (max_val - min_val)
    else:
        group['normalized_measurement'] = 0.0
    return group

df = df.groupby('measurement_type').apply(normalize).reset_index(drop=True)

df.to_csv(processed_file, index=False, compression="gzip")
print(f"Processed dataset saved to {processed_file}")
