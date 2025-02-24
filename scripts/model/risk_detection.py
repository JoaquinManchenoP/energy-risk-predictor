import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import sys
from scipy.stats import zscore

base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
merged_dir = os.path.join(base_dir, "data", "merged_data")
merged_files = [f for f in os.listdir(merged_dir) if f.endswith(".csv.gz")]

if not merged_files:
    print("\u26a0\ufe0f No merged dataset found in the merged_data directory. Risk detection cannot proceed.")
    sys.exit(1)

merged_files.sort()
data_path = os.path.join(merged_dir, merged_files[0])

df = pd.read_csv(data_path, compression='gzip')
df['timestamp'] = pd.to_datetime(df['timestamp'])

print("🔍 Available columns in the dataset:", df.columns.tolist())

required_columns = ['load_value', 'generation_forecast', 'energy_price']
missing_columns = [col for col in required_columns if col not in df.columns]

if missing_columns:
    print(f"\u26a0\ufe0f Required columns missing from dataset: {missing_columns}")
    print("\u26a0\ufe0f Available columns:", df.columns.tolist())
    sys.exit(1)

numeric_cols = df.select_dtypes(include=[np.number]).columns 
df_resampled = df.set_index('timestamp')[numeric_cols].resample('5D').mean().reset_index()

df_resampled.reset_index(inplace=True)

plots_dir = os.path.join(base_dir, "data", "plots")
os.makedirs(plots_dir, exist_ok=True)

# Graph 1: Actual Load vs Generation 
plt.figure(figsize=(14, 6))
plt.plot(df_resampled['timestamp'], df_resampled['load_value'], label='Actual Load (5-Day Avg)', color='blue', linewidth=2)
plt.plot(df_resampled['timestamp'], df_resampled['generation_forecast'], label='Generation Forecast (5-Day Avg)', linestyle='dashed', color='orange', linewidth=2)
ax2 = plt.gca().twinx()
ax2.set_ylabel("Energy Price (€)")
ax2.plot(df_resampled['timestamp'], df_resampled['energy_price'], linestyle='--', color='green', linewidth=2, marker='o', alpha=0.7, label='Energy Price (5-Day Avg)')
plt.xlabel("Time", fontsize=14)
plt.ylabel("Energy Load (MW)", fontsize=14)
plt.title("Actual Load, Generation Forecast, and Energy Price (5-Day Avg)", fontsize=16)

lines, labels = plt.gca().get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
plt.legend(lines + lines2, labels + labels2, loc='upper left')

plt.grid(True, linestyle="--", alpha=0.6)
plot1_path = os.path.join(plots_dir, "actual_generation_forecast_price.png")
plt.savefig(plot1_path)
print(f"✅ Graph 1 saved to {plot1_path}")
plt.show()

# Graph 2: Actual Load vs Generation Forecast
plt.figure(figsize=(14, 6))
plt.plot(df_resampled['timestamp'], df_resampled['load_value'], label="Actual Load (5-Day Avg)", color='blue', linewidth=2)
plt.plot(df_resampled['timestamp'], df_resampled['generation_forecast'], linestyle='dashed', color='orange', linewidth=2, label='Generation Forecast (5-Day Avg)')
ax2 = plt.gca().twinx()
ax2.set_ylabel("Energy Price (€)")
ax2.plot(df_resampled['timestamp'], df_resampled['energy_price'], linestyle='--', color='green', linewidth=2, marker='o', alpha=0.5, label='Energy Price (5-Day Avg)')
plt.xlabel("Time", fontsize=14)
plt.ylabel("Energy Load (MW)", fontsize=14)
plt.title("Actual vs Generation Forecast (5-Day Avg)", fontsize=16)

lines, labels = plt.gca().get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
plt.legend(lines + lines2, labels + labels2, loc='upper left')

plt.grid(True, linestyle="--", alpha=0.6)
plot2_path = os.path.join(plots_dir, "actual_vs_generation_forecast.png")
plt.savefig(plot2_path)
print(f"✅ Graph 2 saved to {plot2_path}")
plt.show()

# Risk Calculation
forecast_dev_threshold = df['load_value'].mean() + 2.5 * df['generation_forecast'].std()
df['risk_level'] = abs(df['load_value'] - df['generation_forecast']) + df['energy_price'].pct_change(fill_method=None).abs() * 100
df['risk_flag'] = df['risk_level'] > forecast_dev_threshold

df_resampled['baseline_risk'] = df_resampled['load_value'].rolling(window=5, min_periods=1).mean()
df_resampled.loc[df['risk_flag'], 'baseline_risk'] = df_resampled.loc[df['risk_flag'], 'load_value']

# Graph 3: Risk Visualization with Background
plt.figure(figsize=(14, 6))
plt.plot(df_resampled['timestamp'], df_resampled['load_value'], label='Actual Load (Muted)', color='blue', linestyle='dashed', alpha=0.3, linewidth=2)
plt.plot(df_resampled['timestamp'], df_resampled['generation_forecast'], label='Generation Forecast (Muted)', linestyle='dashed', color='orange', alpha=0.3, linewidth=2)
plt.plot(df_resampled['timestamp'], df_resampled['energy_price'], linestyle='--', color='green', linewidth=2, marker='o', alpha=0.7, label='Energy Price (5-Day Avg)')
plt.plot(df_resampled['timestamp'], df_resampled['baseline_risk'], label='Risk Level', linestyle='-', color='red', linewidth=3, alpha=1.0, marker='o')
plt.xlabel("Time", fontsize=14)
plt.ylabel("Risk Level", fontsize=14)
plt.title("Risk Detection Visualization (5-Day Avg)", fontsize=16)

lines, labels = plt.gca().get_legend_handles_labels()
plt.legend(lines, labels, loc='upper left')

plt.grid(True, linestyle="--", alpha=0.6)
plot3_path = os.path.join(plots_dir, "risk_visualization.png")
plt.savefig(plot3_path)
print(f"✅ Graph 3 saved to {plot3_path}")
plt.show()

risk_stats = pd.DataFrame({
    "Metric": ["Mean Load Value", "Mean Generation Forecast", "Mean Energy Price", "Forecast Deviation Threshold"],
    "Value": [df['load_value'].mean(), df['generation_forecast'].mean(), df['energy_price'].mean(), forecast_dev_threshold]
})

print("\n🔍 Risk Calculation Summary:\n")
print(risk_stats.to_string(index=False))

sys.exit(0)











