import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent.parent


# Functions
def concat_data_files(list_of_files):
    """Define a function to read multiple data files and concatenate them into a single DataFrame"""

    # Use list comprehension to read .dat files into a list of DataFrames
    dataframes = [
        pd.read_csv(
            file,
            delimiter="\t",
            skiprows=7,
            encoding="UTF-16LE",
            skipinitialspace=True,
            thousands=",",
            # engine='python'  # Error occurs
            dtype={
                "M/Y (from-": str,
                "SAP Plant": str,
                "Outlet": str,
                "Vendor": str,
                "Trading Pr": str,
                "Accounts f": str,
                "Document d": str,
            },
        )
        for file in list_of_files
    ]

    # Use list comprehension to rename columns of dataframes
    dataframes = [
        df.rename(
            columns={
                "Inco Terms Descr": "Inco Terms",
                "GR Quantit": "GR Quantity",
                "Sum STD Pr": "Sum STD Price",
                "STD Custom": "STD Customs",
            }
        )
        for df in dataframes
    ]

    # Merge the list of DataFrames into a single DataFrame
    df = pd.concat(dataframes)

    return df


def remove_col_row(df):
    """Remove first two columns and sub-total rows"""
    df = df.drop(columns=["Unnamed: 0", "Unnamed: 1"])
    df = df.dropna(subset="Profit Cen")
    return df


def rename_cols(df):
    new_columns = {
        "Net PPV...23": "Net PPV",
        "Net PM_PPV...24": "Net PPV ratio",
        "STD Other...39": "STD Other",
        "STD Other...40": "STD Other 2",
    }
    return df.rename(columns=new_columns)


def clean_column_names(df):
    """Apply the cleaning function to all column names"""
    df = df.clean_names()
    # clean trailing underscore
    df.columns = df.columns.map(lambda x: x.rstrip("_"))
    return df


def main():

    # Variable
    from variable_year import year

    # path
    data_path = path / "data"

    # Filnames
    output_file = path / "db" / f"ZMPV_{year}.parquet"

    # List of multiple files
    txt_files = [
        file for file in data_path.iterdir() if file.is_file() and file.suffix == ".txt"
    ]

    # Read data
    df = concat_data_files(txt_files)

    # Process data
    df = df.pipe(remove_col_row).pipe(rename_cols).pipe(clean_column_names)

    # Write data
    df.to_parquet(output_file)
    print("Files are created")


if __name__ == "__main__":
    main()
