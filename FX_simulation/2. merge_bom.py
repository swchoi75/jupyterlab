import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
try:
    path = Path(__file__).parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent


data_path = path / "data" / "BOM"
output_file = path / "data" / "BOM.parquet"


# Input data: List of multiple files
parquet_files = [
    file for file in data_path.iterdir() if file.is_file() and file.suffix == ".parquet"
]


# Function
def read_multiple_files(list_of_files):
    dataframes = [pd.read_parquet(file) for file in list_of_files]

    # Merge the list of DataFrames into a single DataFrame
    df = pd.concat(dataframes)

    return df


# Process data
df = read_multiple_files(parquet_files)


# Write data
df.to_parquet(output_file)
print("A file is created")
