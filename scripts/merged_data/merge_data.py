import os
import glob
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# -----------------------------
# Define base directory
# -----------------------------
# Assumes this script is located in root/scripts/ (or one level below root)
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
print("Base directory:", base_dir)

# -----------------------------
# Define directory paths for each dataset
# -----------------------------
load_dir = os.path.join(base_dir, "data", "load")
generation_dir = os.path.join(base_dir, "data", "generation")
price_dir = os.path.join(base_dir, "data", "price")
print("Load directory:", load_dir)
print("Generation directory:", generation_dir)
print("Price directory:", price_dir)

# -----------------------------
# Use glob to find CSV files in each directory (files with extension .csv.gz)
# -----------------------------
load_files = glob.glob(os.path.join(load_dir, "*.csv.gz"))
generation_files = glob.glob(os.path.join(generation_dir, "*.csv.gz"))
price_files = glob.glob(os.path.join(price_dir, "*.csv.gz"))

if not load_files:
    print("No load CSV file found in", load_dir)
    exit(1)
else:
    load_file = load_files[0]
    print("Found load file:", load_file)

if not generation_files:
    print("No generation CSV file found in", generation_dir)
    exit(1)
else:
    generation_file = generation_files[0]
    print("Found generation file:", generation_file)

if not price_files:
    print("No price CSV file found in", price_dir)
    exit(1)
else:
    price_file = price_files[0]
    print("Found price file:", price_file)

# -----------------------------
# Load the datasets
# -----------------------------
try:
    df_load = pd.read_csv(load_file)
    df_generation = pd.read_csv(generation_file)
    df_price = pd.read_csv(price_file)
    print("Datasets loaded successfully.")
except Exception as e:
    print(f"Error loading one or more files: {e}")
    exit(1)

# -----------------------------
# Convert the timestamp columns to datetime objects
# -----------------------------
df_load['timestamp'] = pd.to_datetime(df_load['timestamp'], errors='coerce')
df_generation['timestamp'] = pd.to_datetime(df_generation['timestamp'], errors='coerce')
df_price['timestamp'] = pd.to_datetime(df_price['timestamp'], errors='coerce')

# -----------------------------
# Standardize and reformat each dataset
# -----------------------------
# Actual Total Load: rename "load_value" to "measurement" and add constant labels.
df_load = df_load.rename(columns={"load_value": "measurement"})
df_load["measurement_type"] = "actual_load"
df_load["measurement_unit"] = "MW"
df_load = df_load[["timestamp", "country", "day_of_week", "measurement_type", "measurement", "measurement_unit"]]

# Generation Forecast Day-Ahead: rename "generation_forecast" to "measurement" and add labels.
df_generation = df_generation.rename(columns={"generation_forecast": "measurement"})
df_generation["measurement_type"] = "generation_forecast"
df_generation["measurement_unit"] = "MW"
df_generation = df_generation[["timestamp", "country", "day_of_week", "measurement_type", "measurement", "measurement_unit"]]

# Energy Price: rename "energy_price" to "measurement" and add labels.
df_price = df_price.rename(columns={"energy_price": "measurement"})
df_price["measurement_type"] = "energy_price"
df_price["measurement_unit"] = "€/MWh"
df_price = df_price[["timestamp", "country", "day_of_week", "measurement_type", "measurement", "measurement_unit"]]

# -----------------------------
# Merge the datasets by stacking them vertically
# -----------------------------
merged_df = pd.concat([df_load, df_generation, df_price], axis=0, ignore_index=True)

# -----------------------------
# Sort the merged DataFrame by timestamp and country for clarity
# -----------------------------
merged_df.sort_values(by=["timestamp", "country"], inplace=True)

# -----------------------------
# Feature Engineering: Extract additional time-based features
# -----------------------------
merged_df["hour"] = merged_df["timestamp"].dt.hour
merged_df["day"] = merged_df["timestamp"].dt.day
merged_df["month"] = merged_df["timestamp"].dt.month
merged_df["year"] = merged_df["timestamp"].dt.year

# -----------------------------
# (Optional) Reorder columns if desired
# -----------------------------
# Here we choose the order: timestamp, country, day_of_week, measurement_type, measurement, measurement_unit, hour, day, month, year
merged_df = merged_df[["timestamp", "country", "day_of_week", "measurement_type", "measurement", "measurement_unit", "hour", "day", "month", "year"]]

# -----------------------------
# Save the merged dataset
# -----------------------------
merged_dir = os.path.join(base_dir, "data", "merged_data")
os.makedirs(merged_dir, exist_ok=True)
merged_output_path = os.path.join(merged_dir, "merged_dataset_20250206.csv.gz")

# Remove the existing file if it exists so that the new one is written fresh
if os.path.exists(merged_output_path):
    os.remove(merged_output_path)
    print(f"Existing file at {merged_output_path} removed.")

merged_df.to_csv(merged_output_path, index=False, compression="gzip")
print(f"Merged dataset saved to {merged_output_path}")

