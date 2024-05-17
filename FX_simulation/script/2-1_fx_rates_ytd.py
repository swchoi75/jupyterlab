import pandas as pd
import numpy as np
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Functions
def read_data(filename, year):
    df = pd.read_excel(
        filename, sheet_name=f"FY{year}", skiprows=7, nrows=7, usecols="A:N"
    )
    return df


def new_col_names(df):
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
    return df


def add_col_fx_type(df, fx_type):
    # Add new columns of fx_type
    df["fx_type"] = fx_type
    return df


def add_col_year(df, year):
    # Add new columns of year
    df["year"] = year
    return df


def reorder_cols(df):
    df = df[
        ["fx_type", "year"]
        + [col for col in df.columns if col not in ["fx_type", "year"]]
    ]
    return df


def extract_currencies(df):
    # Extract Currencies in Alphabet letters
    df["cur"] = df["cur"].str.extract(r"([A-Z]{3})")
    return df


def process_JPY(df):
    """Divide by 100 for 100 JPY"""

    # Identify the row index
    row_index = 6  # 100 JPY

    # Select numerical columns
    numerical_columns = df.select_dtypes(include=[np.number])

    # Apply division by 100 to numerical columns
    for col in numerical_columns:
        df.loc[row_index, col] /= 100

    return df


def select_major_curr(df):
    # Select major currencies
    df = df[df["cur"].isin(["USD", "EUR", "JPY"])]
    return df


def main():

    # Variables
    year = "2024"  # from 2019 to 2024
    fx_type = "ytd"  # ytd or spot

    # Filenames
    input_file = path / "data" / "FX Rates" / "zf_rate.xlsx"
    output_file = path / "data" / "FX Rates" / f"FX {fx_type}_{year}.csv"

    # Read data
    df = read_data(input_file, year)

    # Process data
    df = (
        df.pipe(new_col_names)
        .pipe(add_col_fx_type, fx_type)
        .pipe(add_col_year, year)
        .pipe(extract_currencies)
        .pipe(process_JPY)
        .pipe(select_major_curr)
    )

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created.")


if __name__ == "__main__":
    main()
