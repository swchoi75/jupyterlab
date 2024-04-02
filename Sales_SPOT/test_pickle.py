import os
import pickle
import datetime
import pandas as pd
from pathlib import Path


# Path
input_path = Path(
    r"C:\Users\uid98421\Vitesco Technologies\Controlling VT Korea - Documents\110. Sales controlling\FY2024\080. SPOT"
)
output_path = Path(__file__).parent / "output"


# Filename
# input_file = input_path / "SPOT 202401 ICH.xlsm"
# input_file = input_path / "SPOT 202402 ICH.xlsm"
input_file = input_path / "SPOT 202403 ICH.xlsm"
output_file = output_path / "SPOT_2024.pkl"

# Read data
df = pd.read_excel(input_file, sheet_name="Monthly SPOT Overview", skiprows=9)


# Add metadata
metadata = {
    "file_path": str(input_file),
    "read_time": datetime.datetime.now().isoformat(),
}

data_with_metadata = {"metadata": metadata, "data": df.to_json()}


# Functions
def append_to_pickle(filename, new_data, new_metadata):
    """Appends new data and metadata to a pickle file (dictionary structure).

    Args:
        filename (str): Path to the pickle file.
        new_data (dict): The data to be appended (as a dictionary).
        new_metadata (dict): The metadata associated with the new data.
    """

    # Check if file exists
    if not os.path.exists(filename):
        # Create an empty dictionary if file doesn't exist
        existing_data = {}
    else:
        # Load existing data if file exists
        try:
            with open(filename, "rb") as f:
                existing_data = pickle.load(f)
        except Exception as e:
            print(f"Error loading existing pickle file: {e}")
            return

    # Update existing data
    existing_data[f"dataset_{len(existing_data)}"] = {
        "data": new_data,
        "metadata": new_metadata,
    }

    # Save modified data
    try:
        with open(filename, "wb") as f:
            pickle.dump(existing_data, f)
    except Exception as e:
        print(f"Error appending data to pickle file: {e}")


append_to_pickle(output_file, df, metadata)


# Read pickled data
filename = output_file

try:
    with open(filename, "rb") as f:
        data = pickle.load(f)
except Exception as e:
    print(f"Error loading pickle file: {e}")
    data = None  # Set data to None if loading fails

# Inspect the data
if data:  # Check if data was loaded successfully
    for key, value in data.items():
        print(f"Dataset Name: {key}")
        print(f"Data: {value['data']}")
        print(f"Metadata: {value['metadata']}")
        print("-" * 20)
