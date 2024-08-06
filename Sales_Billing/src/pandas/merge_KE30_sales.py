import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent.parent


# Functions
def read_multiple_files(list_of_files):
    dataframes = [
        pd.read_csv(
            file,
            skiprows=7,
            sep="\t",
            encoding="UTF-16LE",
            skipinitialspace=True,
            thousands=",",
            engine="python",
            dtype={
                # text
                "AAG": str,
                "Customer": str,
                "Tr.Prt": str,
                "Dv": str,
                "Dir Cust.": str,
                "Fin.Cust.": str,
                # number
                "Revenues": int,
                "Stock val.": int,
                "Quantity": int,
            },
        ).rename(
            # To avoid the same column names after clean_names
            columns={
                "Record Type": "Record Type 2",
            }
        )
        for file in list_of_files
    ]

    # Merge the list of DataFrames into a single DataFrame
    df = pd.concat(dataframes)

    return df


def remove_columns(df):
    cols_to_remove = ["unnamed_0", "unnamed_3"]
    df = df[[col for col in df.columns if col not in cols_to_remove]]
    return df


def remove_missing_values(df):
    df = df.dropna(subset=["record_type"])
    return df


def extract_profit_center(df):
    df["profit_center"] = df["profit_center"].str.replace("5668/", "")
    return df


def main():
    # Variables
    year = "2024"

    # Path
    # data_path = path / "data" / "KE30" / "Archive" / f"{year}"
    data_path = path / "data" / "KE30"  # for current year

    # Filenames
    input_files = [
        file
        for file in data_path.iterdir()
        if file.is_file() and (file.suffix.lower() == ".xls")
    ]
    output_file = path / "db" / f"KE30_{year}.csv"

    # Read data
    df = read_multiple_files(input_files).clean_names(
        strip_underscores=True, case_type="snake"
    )

    # Process data
    df = df.pipe(remove_columns).pipe(remove_missing_values).pipe(extract_profit_center)

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
