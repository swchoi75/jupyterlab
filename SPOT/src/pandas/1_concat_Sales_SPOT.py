import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent.parent


# Functions
def read_multiple_files(list_of_files):
    dataframes = [
        pd.read_excel(file, sheet_name="Monthly SPOT Overview", skiprows=9)
        for file in list_of_files
    ]

    # Add a new column with filename to each DataFrame
    for i, df in enumerate(dataframes):
        df["source"] = list_of_files[i].stem

    # Merge the list of DataFrames into a single DataFrame
    df = pd.concat(dataframes)

    # reorder columns
    df = df[["source"] + [col for col in df.columns if col not in ["source"]]]
    return df


def clean_trailing_underscore(column_name):
    """Functions to clean column names"""
    return column_name.rstrip("_")


def clean_column_names(df):
    """Apply the cleaning function to all column names"""
    df = df.clean_names()
    df.columns = df.columns.map(clean_trailing_underscore)
    return df


def rename_columns_in_2022(df):
    """This is used only for SPOT 2022 data"""
    df = df.rename(
        columns={
            "st": "volume_p_a_2031_st",
            "st_1": "volume_p_a_2032_st",
            "st_2": "volume_p_a_overall_result_st",
        }
    )
    return df


def extract_numbers(df):
    # Extract 6 digit numbers
    df["source"] = df["source"].str.extract(r"(\d{6})")
    return df


def main():
    # Variable
    year = 2024

    # Path
    data_path = path / "data" / f"{year}"

    # Filenames
    xls_files = [
        file
        for file in data_path.iterdir()
        if file.is_file() and file.suffix in [".xlsm", ".xlsx"]
    ]
    output_file = path / "output" / f"SPOT_combined_{year}.csv"

    # Read data
    df = read_multiple_files(xls_files)

    # Process data
    if year == 2022:
        df = (
            df.pipe(clean_column_names)
            .pipe(rename_columns_in_2022)
            .pipe(extract_numbers)
        )
    else:
        df = df.pipe(clean_column_names).pipe(extract_numbers)

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
