import os
import sys
import pandas as pd
from datetime import datetime

def get_single_file(directory):
    try:
        files = [f for f in os.listdir(directory) if f.endswith(".csv.gz")]
        if files:
            files.sort() 
            return os.path.join(directory, files[0])
        else:
            return None
    except Exception as e:
        print(f"Error listing files in {directory}: {e}")
        return None

def main():
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    print("Base directory:", base_dir)
    
    # Directories for each dataset
    load_dir = os.path.join(base_dir, "data", "load")
    generation_dir = os.path.join(base_dir, "data", "generation")
    price_dir = os.path.join(base_dir, "data", "price")
    
    print("Load directory:", load_dir)
    print("Generation directory:", generation_dir)
    print("Price directory:", price_dir)
    
    load_file = get_single_file(load_dir)
    generation_file = get_single_file(generation_dir)
    price_file = get_single_file(price_dir)
    
    if load_file:
        print("Found load file:", load_file)
    else:
        print(f"Error: No load CSV file found in {load_dir}.")
        sys.exit(1)
        
    if generation_file:
        print("Found generation file:", generation_file)
    else:
        print(f"Error: No generation CSV file found in {generation_dir}.")
        sys.exit(1)
    
    if price_file:
        print("Found price file:", price_file)
        try:
            df_price = pd.read_csv(price_file, compression="gzip")
            print(f"Loaded price data with {df_price.shape[0]} rows.")
        except Exception as e:
            print(f"Error loading price file: {e}")
            df_price = pd.DataFrame(columns=[
                "timestamp", "country", "day_of_week", "measurement_type",
                "measurement", "measurement_unit", "hour", "day", "month", "year"
            ])
    else:
        print(f"Warning: No price CSV file found in {price_dir}. Price data will be skipped.")
        df_price = pd.DataFrame(columns=[
            "timestamp", "country", "day_of_week", "measurement_type",
            "measurement", "measurement_unit", "hour", "day", "month", "year"
        ])

    try:
        print("Loading load data from:", load_file)
        df_load = pd.read_csv(load_file, compression="gzip")
        print(f"Loaded load data with {df_load.shape[0]} rows.")
    except Exception as e:
        print(f"Error loading load file: {e}")
        sys.exit(1)
    
    try:
        print("Loading generation data from:", generation_file)
        df_generation = pd.read_csv(generation_file, compression="gzip")
        print(f"Loaded generation data with {df_generation.shape[0]} rows.")
    except Exception as e:
        print(f"Error loading generation file: {e}")
        sys.exit(1)
    
    if 'data_type' in df_load.columns:
        df_load.rename(columns={'data_type': 'measurement_type'}, inplace=True)
    else:
        df_load['measurement_type'] = 'actual_load'
        
    if 'data_type' in df_generation.columns:
        df_generation.rename(columns={'data_type': 'measurement_type'}, inplace=True)
    else:
        df_generation['measurement_type'] = 'generation_forecast'
        
    if 'data_type' in df_price.columns:
        df_price.rename(columns={'data_type': 'measurement_type'}, inplace=True)
    else:
        df_price['measurement_type'] = 'energy_price'
    
    if 'load_value' in df_load.columns:
        df_load['measurement'] = df_load['load_value']
    else:
        print("Warning: 'load_value' column missing in load data.")
    
    if 'generation_forecast' in df_generation.columns:
        df_generation['measurement'] = df_generation['generation_forecast']
    else:
        print("Warning: 'generation_forecast' column missing in generation data.")
    
    if 'energy_price' in df_price.columns:
        df_price['measurement'] = df_price['energy_price']
    else:
        print("Warning: 'energy_price' column missing in price data.")
    
    merged_df = pd.concat([df_load, df_generation, df_price], axis=0, ignore_index=True)
    
    merged_df['timestamp'] = pd.to_datetime(merged_df['timestamp'])
    merged_df['hour'] = merged_df['timestamp'].dt.hour
    merged_df['day'] = merged_df['timestamp'].dt.day
    merged_df['month'] = merged_df['timestamp'].dt.month
    merged_df['year'] = merged_df['timestamp'].dt.year
    
    merged_df.sort_values(by=["timestamp", "country"], inplace=True)

    merged_dir = os.path.join(base_dir, "data", "merged_data")
    os.makedirs(merged_dir, exist_ok=True)
    merged_output_path = os.path.join(merged_dir, f"merged_dataset_{datetime.now().strftime('%Y%m%d')}.csv.gz")
    

    if os.path.exists(merged_output_path):
        os.remove(merged_output_path)
        print(f"Existing file at {merged_output_path} removed.")
    
    merged_df.to_csv(merged_output_path, index=False, compression="gzip")
    print(f"Merged dataset saved to {merged_output_path}")
    
if __name__ == "__main__":
    main()
