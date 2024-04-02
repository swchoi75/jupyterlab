import os
import json
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
output_file = output_path / "SPOT_2024.json"

# Read data
df = pd.read_excel(input_file, sheet_name="Monthly SPOT Overview", skiprows=9)


# Add metadata
metadata = {
    "file_path": str(input_file),
    "read_time": datetime.datetime.now().isoformat(),
}

data_with_metadata = {"metadata": metadata, "data": df.to_json()}


# Functions
def append_to_json(filename, new_metadata, new_data):
    """Appends new data and metadata to a JSON file.

    Args:
        filename (str): Path to the JSON file.
        new_metadata (dict): The metadata associated with the new data.
        new_data (pandas.DataFrame): The data to be appended.
    """

    # Check if file exists
    if not os.path.exists(filename):
        # Create a new dictionary if file doesn't exist
        existing_data = {}
    else:
        # Load existing data if file exists
        with open(filename, "r") as f:
            existing_data = json.load(f)

    # Convert new DataFrame to JSON string
    new_data_json = new_data.to_json(orient="records")

    # Update existing dictionary (modify for your structure)
    new_dataset_name = "new_dataset"  # Replace with unique identifier/name
    existing_data[new_dataset_name] = {"data": new_data_json, "metadata": new_metadata}

    # Save data with indentation
    with open(filename, "w") as f:
        json.dump(existing_data, f, indent=4)


append_to_json(output_file, metadata, df)
