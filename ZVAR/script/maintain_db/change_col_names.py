import pandas as pd
from janitor import clean_names
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Functions
def clean_trailing_underscore(column_name):
    """Functions to clean column names"""
    return column_name.rstrip("_")


def clean_column_names(df):
    """Apply the cleaning function to all column names"""
    df.columns = df.columns.map(clean_trailing_underscore)
    return df


def rename_columns(df):
    df = df.rename(columns={"per": "period"})
    return df


# Main
def main():

    # Variable
    year = "2023"

    # Filenames
    input_file = path / "db" / f"ZVAR_{year}.parquet"
    output_file = path / "db" / f"ZVAR_{year}_new.parquet"

    # Read data
    df = pd.read_parquet(input_file)

    # Process data
    # df = df.pipe(clean_column_names)
    df = df.pipe(rename_columns)

    # Write data
    df.to_parquet(output_file)
    print("A file is created")


if __name__ == "__main__":
    main()
