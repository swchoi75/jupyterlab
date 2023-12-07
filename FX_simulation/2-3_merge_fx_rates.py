import pandas as pd
from pathlib import Path


# Path
try:
    path = Path(__file__).parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent


data_path = path / "data" / "FX Rates"
output_file = path / "data" / "fx_rates_VT_actual.csv"


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


def pivot_longer(df):
    df = df.drop(columns=["py_Dec"])
    df = df.melt(
        id_vars=["fx_type", "cur", "year"], var_name="month", value_name="fx_act"
    )
    return df


def sort_table(df):
    df = df.sort_values(by=["fx_type", "cur", "year"], ascending=[False, False, True])
    return df


def month_to_number(df):
    replacement_dict = {
        "Jan": "01",
        "Feb": "02",
        "Mar": "03",
        "Apr": "04",
        "May": "05",
        "Jun": "06",
        "Jul": "07",
        "Aug": "08",
        "Sep": "09",
        "Oct": "10",
        "Nov": "11",
        "Dec": "12",
    }
    df["month"] = df["month"].replace(replacement_dict)
    return df


# Process data
df = read_multiple_files(csv_files)
df = (
    df.pipe(fill_missing_values)
    .pipe(pivot_longer)
    .pipe(sort_table)
    .pipe(month_to_number)
)


# Write data
df.to_csv(output_file, index=False)
print("A file is created")
