import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np

def detect_outliers(series, threshold=3):
    mean_val = series.mean()
    std_dev = series.std()
    z_scores = (series - mean_val) / std_dev
    return abs(z_scores) > threshold

def remove_outliers(df, column):
    if column not in df.columns:
        return df 
    
    outliers = detect_outliers(df[column])
    df_cleaned = df[~outliers]
    return df_cleaned

def main():
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    merged_dir = os.path.join(base_dir, "data", "merged_data")
    
    files = [f for f in os.listdir(merged_dir) if f.startswith("merged_dataset_") and f.endswith(".csv.gz")]
    files.sort()
    if not files:
        print("Error: No merged dataset found.")
        return
    
    merged_file = os.path.join(merged_dir, files[-1])
    df = pd.read_csv(merged_file, compression="gzip")
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    
    pivot = df.pivot_table(index="timestamp", columns="measurement_type", values="measurement", aggfunc="mean")
    pivot = pivot.dropna()
    
    # Resample data to 4-day averages
    pivot_weekly = pivot.resample("5D").mean()
    
    # Remove outliers from "actual_load", "generation_forecast", and "energy_price"
    for col in ["actual_load", "generation_forecast", "energy_price"]:
        if col in pivot_weekly.columns:
            pivot_weekly = remove_outliers(pivot_weekly, col)
    
    # Plot results
    fig, ax1 = plt.subplots(figsize=(12, 5), constrained_layout=True)
    if "actual_load" in pivot_weekly.columns:
        ax1.plot(pivot_weekly.index, pivot_weekly["actual_load"], label="Actual Load (4-Day Avg)", marker="o", linestyle="-")
    if "generation_forecast" in pivot_weekly.columns:
        ax1.plot(pivot_weekly.index, pivot_weekly["generation_forecast"], label="Generation Forecast (4-Day Avg)", marker="x", linestyle="-")
    
    ax1.set_xlabel("Timestamp")
    ax1.set_ylabel("MW")
    ax1.set_title("4-Day Average Generation Forecast, Actual Load, and Energy Price (Filtered)")
    ax1.legend(loc="upper left")
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    plt.xticks(rotation=45)
    ax1.grid(True, linestyle="--", alpha=0.6)
    
    if "energy_price" in pivot_weekly.columns:
        ax2 = ax1.twinx()
        ax2.plot(pivot_weekly.index, pivot_weekly["energy_price"], label="Energy Price (4-Day Avg)", marker="s", linestyle="--", color="red")
        ax2.set_ylabel("Energy Price (€/MWh)")
        ax2.legend(loc="upper right")
    
    # Save 
    plots_dir = os.path.join(base_dir, "data", "plots")
    os.makedirs(plots_dir, exist_ok=True)
    plot_path = os.path.join(plots_dir, "filtered_4day_generation_vs_actual_load_price.png")
    plt.savefig(plot_path)
    print(f"Filtered 4-day average plot with energy price saved to: {plot_path}")
    plt.show()

if __name__ == "__main__":
    main()
