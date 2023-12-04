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
year = "2023"  # from 2019 to 2023 ytd
fx_type = "spot"
input_file = path / "data" / "FX Rates" / "zf_rate.xlsx"
output_file = path / "data" / "FX Rates" / f"FX {fx_type}_{year}.csv"


# Read data
df = pd.read_excel(
    input_file, sheet_name=f"FY{year}", skiprows=18, nrows=7, usecols="A:N"
)


# Replace existing column names with the new list
df.columns = [
    "cur",
    "py_Dec",
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
]


# Add new columns of fx_type and year
df["fx_type"] = fx_type
df["year"] = year
# reorder columns
df = df[
    ["fx_type", "year"] + [col for col in df.columns if col not in ["fx_type", "year"]]
]


# Extract Currencies in Alphabet letters
df["cur"] = df["cur"].str.extract(r"([A-Z]{3})")


# Divide by 100 for 100 JPY
# Identify the row index
row_index = 6  # 100 JPY
# Select numerical columns
numerical_columns = df.select_dtypes(include=[np.number])
# Apply division
for col in numerical_columns:
    df.loc[row_index, col] /= 100


# Select major currencies
df = df[df["cur"].isin(["USD", "EUR", "JPY"])]


# Write data
df.to_csv(output_file, index=False)
print("A file is created.")
