import pandas as pd
from janitor import clean_names
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Functions
def clean_column_names(df):
    """Apply the cleaning function to all column names"""
    # clean trailing underscore
    df.columns = df.columns.map(lambda x: x.rstrip("_"))
    return df


# Main
def main():

    # Variable
    year = "2023"

    # Filenames
    input_file = path / "db" / f"ZMPV_{year}.parquet"
    output_file = path / "db" / f"ZMPV_{year}_new.parquet"

    # Read data
    df = pd.read_parquet(input_file)

    # Process data
    df = df.pipe(clean_column_names)

    # Write data
    df.to_parquet(output_file)
    print("A file is created")


if __name__ == "__main__":
    main()
