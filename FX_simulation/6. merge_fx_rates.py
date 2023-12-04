import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
try:
    path = Path(__file__).parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent


data_path = path / "data" / "FX Rates"
output_file = path / "data" / "fx_rates_vt.csv"


# Input data: List of multiple files
csv_files = [
    file for file in data_path.iterdir() if file.is_file() and file.suffix == ".csv"
]


# Functions
def read_multiple_files(list_of_files):
    dataframes = [pd.read_csv(file) for file in list_of_files]
    # Merge the list of DataFrames into a single DataFrame
    df = pd.concat(dataframes)
    return df


def fill_missing_values(df):
    # Fill in missing values in Dec with Nov data in 2023
    df["Dec"] = df["Dec"].where(df["Dec"] > 0, df["Nov"])
    return df


# Process data
df = read_multiple_files(csv_files)
df = fill_missing_values(df)  # Fill in missing values in Dec with Nov data in 2023


# Write data
df.to_csv(output_file, index=False)
print("A file is created")
