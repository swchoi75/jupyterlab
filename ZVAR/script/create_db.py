import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent


# Functions
def concat_data_files(list_of_files):
    """Define a function to read multiple data files and concatenate them into a single DataFrame"""

    # Use list comprehension to read .dat files into a list of DataFrames
    dataframes = [
        pd.read_csv(
            file,
            sep="\t",
            skiprows=8,
            encoding="UTF-16LE",
            skipinitialspace=True,
            thousands=",",
            engine="python",
            dtype={
                "Order": str,
                "Cost Ctr": str,
                "Blk-Ind": str,
                "Dv": str,
                "No-Post": str,
                "ValCl": str,
                "Prod.Proc": str,
            },
        )
        for file in list_of_files
    ]

    # Merge the list of DataFrames into a single DataFrame
    df = pd.concat(dataframes)

    return df


def remove_col_row(df):
    # Remove first two columns and sub-total rows
    df = df.drop(columns=["Unnamed: 0", "Unnamed: 1", "Unnamed: 4"])
    df = df.dropna(subset=["Order"])
    # Remove repeating header rows
    df = df[df.ne(df.columns).any(axis="columns")]
    return df


def clean_trailing_underscore(column_name):
    """Functions to clean column names"""
    return column_name.rstrip("_")


def clean_column_names(df):
    """Apply the cleaning function to all column names"""
    df = df.clean_names()
    df.columns = df.columns.map(clean_trailing_underscore)
    return df


def main():

    # Variable
    from variable_year import year

    # path
    data_path = path / "data"

    # Filnames
    output_file = path / "db" / f"ZVAR_{year}.csv"

    # List of multiple files
    txt_files = [
        file for file in data_path.iterdir() if file.is_file() and file.suffix == ".txt"
    ]

    # Read data
    df = concat_data_files(txt_files)

    # Process data
    df = df.pipe(remove_col_row).pipe(clean_column_names)

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
