import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def main():
    # Determine the base directory (project root) relative to this file
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    print("Base directory:", base_dir)
    
    # Construct the path to the merged dataset file
    merged_file = os.path.join(base_dir, "data", "merged_data", "merged_dataset_20250206.csv.gz")
    print("Merged dataset path:", merged_file)
    
    # Load the merged dataset from the CSV.gz file
    try:
        df = pd.read_csv(merged_file, compression="gzip")
    except Exception as e:
        print("Error loading merged dataset:", e)
        return

    # Convert the timestamp column to datetime objects
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    
    # -----------------------------
    # Console Output: Basic EDA
    # -----------------------------
    print("\nFirst 5 rows of the merged dataset:")
    print(df.head())
    
    print("\nDataFrame Information:")
    print(df.info())
    
    print("\nSummary Statistics:")
    print(df.describe())
    
    print("\nMissing Values by Column:")
    print(df.isnull().sum())
    
    duplicates = df.duplicated().sum()
    print(f"\nNumber of duplicate rows: {duplicates}")
    
    # -----------------------------
    # Visualization 1: Distribution of Measurement Values
    # -----------------------------
    plt.figure(figsize=(10, 6))
    # **Added color "skyblue", grid, and detailed axis labels with units (note: values may be in MW or €/MWh)**
    sns.histplot(df['measurement'], bins=50, kde=True, color="skyblue")
    plt.title("Distribution of Measurement Values in Merged Dataset")
    plt.xlabel("Measurement Value (MW or €/MWh)")
    plt.ylabel("Frequency")
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.tight_layout()
    dist_plot_path = os.path.join(base_dir, "data", "merged_data", "measurement_distribution.png")
    plt.savefig(dist_plot_path)
    print(f"Measurement distribution plot saved to: {dist_plot_path}")
    plt.show()
    
    # -----------------------------
    # Visualization 2: Counts of Each Measurement Type
    # -----------------------------
    plt.figure(figsize=(8, 4))
    # **Using a 'viridis' color palette and annotating each bar with count values**
    ax = sns.countplot(x='measurement_type', data=df, palette="viridis")
    plt.title("Records per Energy Measurement Type")
    plt.xlabel("Energy Measurement Type")
    plt.ylabel("Count")
    plt.grid(axis='y', linestyle="--", alpha=0.6)
    for p in ax.patches:
        height = p.get_height()
        ax.annotate(f'{height}', (p.get_x() + p.get_width() / 2., height),
                    ha='center', va='bottom', fontsize=10, color='black')
    plt.tight_layout()
    count_plot_path = os.path.join(base_dir, "data", "merged_data", "measurement_type_counts.png")
    plt.savefig(count_plot_path)
    print(f"Measurement type count plot saved to: {count_plot_path}")
    plt.show()
    
    # -----------------------------
    # Visualization 3: Time Series Plots by Measurement Type
    # -----------------------------
    measurement_types = df['measurement_type'].unique()
    for m_type in measurement_types:
        plt.figure(figsize=(12, 4))
        subset = df[df['measurement_type'] == m_type]
        # **Choose a specific color based on measurement type**
        if m_type == "actual_load":
            plot_color = "blue"
            ylabel_text = "Energy Load (MW)"
        elif m_type == "generation_forecast":
            plot_color = "green"
            ylabel_text = "Generation Forecast (MW)"
        elif m_type == "energy_price":
            plot_color = "red"
            ylabel_text = "Energy Price (€/MWh)"
        else:
            plot_color = "gray"
            ylabel_text = "Measurement"
        
        plt.plot(subset['timestamp'], subset['measurement'], marker='o', linestyle='-', color=plot_color, label=m_type)
        plt.title(f"Time Series for {m_type.replace('_', ' ').title()}")
        plt.xlabel("Timestamp")
        plt.ylabel(ylabel_text)
        plt.xticks(rotation=45)
        plt.grid(True, linestyle="--", alpha=0.6)
        plt.legend()
        plt.tight_layout()
        timeseries_plot_path = os.path.join(base_dir, "data", "merged_data", f"time_series_{m_type}.png")
        plt.savefig(timeseries_plot_path)
        print(f"Time series plot for {m_type} saved to: {timeseries_plot_path}")
        plt.show()

if __name__ == "__main__":
    main()
