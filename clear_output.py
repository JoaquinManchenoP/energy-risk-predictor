import os
import shutil
print("hello")

def clear_directory(directory):
    print("clear history function is called")
    """Delete all files and subdirectories in the given directory."""
    if os.path.exists(directory):
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                    print(f"Deleted file: {file_path}")
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
                    print(f"Deleted directory and its contents: {file_path}")
            except Exception as e:
                print(f"Failed to delete {file_path}. Reason: {e}")
    else:
        print(f"Directory does not exist: {directory}")

def main():
    print("main function is called")
    # Define the base directory (assumes this script is in the project root)
    base_dir = os.path.abspath(os.path.dirname(__file__))

    # List of output directories to clear
    output_dirs = [
        os.path.join(base_dir, "models"),
        os.path.join(base_dir, "data", "data_splitting"),
        os.path.join(base_dir, "data", "generation"),
        os.path.join(base_dir, "data", "load"),
        os.path.join(base_dir, "data", "merged_data"),
        os.path.join(base_dir, "data", "plots"),
        os.path.join(base_dir, "data", "price")
    ]
    
    print("Starting to clear output directories...\n")
    for directory in output_dirs:
        print(f"Clearing directory: {directory}")
        clear_directory(directory)
    print("\nAll specified output files and folders")

main()