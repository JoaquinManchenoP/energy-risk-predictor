import os
import pandas as pd
from datetime import datetime
from sklearn.model_selection import train_test_split
from dotenv import load_dotenv

def get_single_file(directory):
    try:
        files = [f for f in os.listdir(directory) if f.endswith(".csv.gz")]
        if files:
            files.sort()  # sort alphabetically
            return os.path.join(directory, files[0])
        else:
            return None
    except Exception as e:
        print(f"Error listing files in {directory}: {e}")
        return None

base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

merged_dir = os.path.join(base_dir, "data", "merged_data")
merged_file = get_single_file(merged_dir)

if merged_file is None:
    print(f"Error: No merged dataset file found in {merged_dir}")
    exit(1)
else:
    print(f"Using merged dataset file: {merged_file}")

try:
    merged_df = pd.read_csv(merged_file, compression="gzip")
except Exception as e:
    print(f"Error loading the merged dataset: {e}")
    exit(1)

merged_df['timestamp'] = pd.to_datetime(merged_df['timestamp'], errors='coerce')

if merged_df['measurement_type'].isnull().any():
    print("Warning: Dropping rows with missing 'measurement_type' values for stratification.")
    merged_df = merged_df.dropna(subset=['measurement_type'])

# Split test set
train_val_df, test_df = train_test_split(
    merged_df,
    test_size=0.20,
    random_state=42,
    stratify=merged_df['measurement_type']
)

# Further split the remaining 80% into training and validation sets
train_df, val_df = train_test_split(
    train_val_df,
    test_size=0.25,  
    random_state=42,
    stratify=train_val_df['measurement_type']
)


print("Shape of training set:", train_df.shape)
print("Shape of validation set:", val_df.shape)
print("Shape of test set:", test_df.shape)

splits_dir = os.path.join(base_dir, "data", "data_splitting")
os.makedirs(splits_dir, exist_ok=True)

train_output_path = os.path.join(splits_dir, "train_dataset.csv.gz")
val_output_path = os.path.join(splits_dir, "validation_dataset.csv.gz")
test_output_path = os.path.join(splits_dir, "test_dataset.csv.gz")

train_df.to_csv(train_output_path, index=False, compression="gzip")
val_df.to_csv(val_output_path, index=False, compression="gzip")
test_df.to_csv(test_output_path, index=False, compression="gzip")

print(f"Training set saved to {train_output_path}")
print(f"Validation set saved to {val_output_path}")
print(f"Test set saved to {test_output_path}")
