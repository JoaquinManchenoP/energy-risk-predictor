import os
import pandas as pd
import numpy as np
from sklearn.metrics import mean_squared_error
from xgboost import XGBRegressor
from sklearn.preprocessing import OneHotEncoder
import joblib
import matplotlib.pyplot as plt

base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
splits_dir = os.path.join(base_dir, "data", "data_splitting")
processed_data_dir = os.path.join(base_dir, "data", "processed_data")

train_file = os.path.join(splits_dir, "train_dataset.csv.gz")
val_file = os.path.join(splits_dir, "validation_dataset.csv.gz")

df_train = pd.read_csv(train_file, compression="gzip")
df_val = pd.read_csv(val_file, compression="gzip")

df_train.columns = df_train.columns.str.lower()
df_val.columns = df_val.columns.str.lower()

df_train = df_train[df_train['measurement_type'] == 'generation_forecast']
df_val = df_val[df_val['measurement_type'] == 'generation_forecast']

if df_train.empty or df_val.empty:
    print("Error: One or both filtered datasets for 'generation_forecast' are empty. Check the input data.")
    exit(1)

def add_time_features(df):
    if not np.issubdtype(df['timestamp'].dtype, np.datetime64):
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    for col, func in zip(['hour', 'day', 'month', 'year'],
                           [lambda x: x.dt.hour, lambda x: x.dt.day, lambda x: x.dt.month, lambda x: x.dt.year]):
        if col not in df.columns:
            df[col] = func(df['timestamp'])
    return df

df_train = add_time_features(df_train)
df_val = add_time_features(df_val)

features = ['hour', 'day_of_week', 'day', 'month', 'year', 'country']
target = 'measurement'

# Combine training and validation to ensure consistent encoding for 'country'
combined = pd.concat([df_train[features], df_val[features]], axis=0)

if combined.empty:
    print("Error: Combined features for OneHotEncoder are empty. Check filtering conditions on 'measurement_type'.")
    exit(1)

encoder = OneHotEncoder(sparse_output=False, handle_unknown='ignore')
encoder.fit(combined[['country']])

def prepare_features(df):
    X_numeric = df[['hour', 'day_of_week', 'day', 'month', 'year']].copy()
    X_cat = encoder.transform(df[['country']])
    X_cat_df = pd.DataFrame(X_cat, columns=encoder.get_feature_names_out(['country']), index=df.index)
    X = pd.concat([X_numeric, X_cat_df], axis=1)
    return X

X_train = prepare_features(df_train)
y_train = df_train[target].values

X_val = prepare_features(df_val)
y_val = df_val[target].values

model = XGBRegressor(
    objective='reg:squarederror',
    n_estimators=100,
    learning_rate=0.1,
    random_state=42
)

model.fit(
    X_train,
    y_train,
    eval_set=[(X_val, y_val)],
    verbose=True
)

y_pred = model.predict(X_val)
rmse = np.sqrt(mean_squared_error(y_val, y_pred))
print(f"Validation RMSE: {rmse:.2f}")

predictions_path = os.path.join(processed_data_dir, "model_predictions.csv")
pd.DataFrame({
    'timestamp': df_val['timestamp'],
    'actual_load': y_val,
    'forecasted_load': y_pred
}).to_csv(predictions_path, index=False)
print(f"Model predictions saved to {predictions_path}")

model_dir = os.path.join(base_dir, "models")
os.makedirs(model_dir, exist_ok=True)
model_path = os.path.join(model_dir, "xgboost_generation_forecast_model.joblib")
joblib.dump(model, model_path)
print(f"Model saved to {model_path}")