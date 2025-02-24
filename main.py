import os
import sys
import subprocess

def run_script(script_path):
    print(f"\nRunning script: {script_path}")
    # Set the working directory for the script to its own directory.
    script_dir = os.path.dirname(script_path)
    print(f"Setting working directory to: {script_dir}")
    result = subprocess.run([sys.executable, script_path], cwd=script_dir)
    if result.returncode != 0:
        print(f"Script {script_path} failed with return code {result.returncode}.")
        sys.exit(result.returncode)
    else:
        print(f"Script {script_path} executed successfully.\n")

if __name__ == "__main__":
    # Define the base directory (this file is located in the root of the project)
    base_dir = os.path.abspath(os.path.dirname(__file__))
    print(f"Base directory is: {base_dir}\n")
    
    # List the scripts to run in the desired order.
    # The order is as follows:
    # 1. Load the Actual Total Load data.
    # 2. Load the Generation Forecast Day-Ahead data.
    # 3. Load the Energy Price data.
    # 4. Merge the individual datasets into a single merged dataset.
    # 5. Split the merged dataset into training, validation, and test sets.
    # 6. Train the model using XGBoost.
    # 7. Run the Exploratory Data Analysis (EDA) on the merged data.
    # 8. Run Risk Detection after training has successfully completed.
    scripts_to_run = [
        os.path.join(base_dir, "scripts", "load", "actual_total_load.py"),
        os.path.join(base_dir, "scripts", "generation", "generation_forecast_day_ahead.py"),
        os.path.join(base_dir, "scripts", "price", "energy_prices.py"),
        os.path.join(base_dir, "scripts", "merged_data", "merge_data.py"),
        os.path.join(base_dir, "scripts", "data_splitting", "train_test_split.py"),
        os.path.join(base_dir, "scripts", "model", "xgboost_model.py"),  # Model training script
        os.path.join(base_dir, "scripts", "eda", "eda_analysis.py"),  # EDA Analysis
    ]
    
    # Run each script sequentially.
    for script in scripts_to_run:
        run_script(script)
    
    print("All scripts executed successfully. Running risk detection...")
    
    # Run risk detection script only if all previous scripts succeeded
    risk_detection_script = os.path.join(base_dir, "scripts", "model", "risk_detection.py")
    run_script(risk_detection_script)
    
    print("✅ All processes, including risk detection, completed successfully.")
