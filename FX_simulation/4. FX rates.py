import pandas as pd
import numpy as np
from pathlib import Path
from janitor import clean_names


# Path
try:
    path = Path(__file__).parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent


# Filenames
year = "2023"
input_file = path / "data" / "zf_rate.xlsx"
output_file = path / "data" / f"FX_{year}.csv"


# Read data
df = pd.read_excel(
    input_file, sheet_name=f"FY{year}", skiprows=7, nrows=7, usecols="A:N"
)


# New list of column names
new_column_names = [
    "cur",
    "py_Dec",
    "ytd_Jan",
    "ytd_Feb",
    "ytd_Mar",
    "ytd_Apr",
    "ytd_May",
    "ytd_Jun",
    "ytd_Jul",
    "ytd_Aug",
    "ytd_Sep",
    "ytd_Oct",
    "ytd_Nov",
    "ytd_Dec",
]

# Replace existing column names with the new list
df.columns = new_column_names


# Extract alphabet
df["cur"] = df["cur"].str.extract(r"([A-Z]{3})")


# Identify the row index
row_index = 6  # 100 JPY

# Select numerical columns
numerical_columns = df.select_dtypes(include=[np.number])

# Apply division
for col in numerical_columns:
    df.loc[row_index, col] /= 100


# Write data
df.to_csv(output_file, index=False)
print("A file is created.")
