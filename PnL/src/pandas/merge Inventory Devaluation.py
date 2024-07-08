import pandas as pd
import re
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent.parent


# Functions
def read_multiple_files(list_of_files):
    dataframes = [
        pd.read_excel(
            file,
            sheet_name="Total devaluation",
            skiprows=5,
            usecols="B:BD",
        )  # .assign(source=re.search(r"[0-9.-]+", file).group())
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


def remove_na(df):
    df = df.dropna(subset=["plant"])
    return df


def year_month(df):
    df["source"] = df["source"].str.extract(r"([0-9.-]+)")
    return df


def filter_data(df):
    # Over +/- 10 Mio KRW
    df = df[(df["monthly_impact"] > 10**7) | (df["monthly_impact"] < -(10**7))]
    return df


def main():

    # Path
    data_path = path / "data" / "Inventory devaluation"

    # Filenames
    output_file = path / "output" / "Inventory devaluation.csv"

    # Input data: List of multiple text files
    xls_files = [
        file
        for file in data_path.iterdir()
        if file.is_file() and file.suffix == ".xlsx"
    ]

    # Read data
    df = read_multiple_files(xls_files)

    # Process data
    df = df.pipe(clean_names).pipe(remove_na).pipe(year_month).pipe(filter_data)

    # Output data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
