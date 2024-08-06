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
            skiprows=5,
            sep="\t",
            encoding="UTF-16LE",
            skipinitialspace=True,
            thousands=",",
            engine="python",
            dtype={
                # text
                "Period": str,
                "Year/month": str,
                "DocumentNo": str,
                "Reference": str,
                "Customer": str,
                "PK": str,
                # number
                "Quantity": float,
                "Amount in doc. curr.": float,
                "Amount in local cur.": float,
            },
        ).rename(
            # Correct the occasional column name in the data source
            columns={
                "Amount in DC": "Amount in doc. curr.",
                "Amt in loc.cur.": "Amount in local cur.",
            }
        )
        for file in list_of_files
    ]

    # Merge the list of DataFrames into a single DataFrame
    df = pd.concat(dataframes)

    return df


def remove_columns(df):
    cols_to_remove = ["unnamed_0", "unnamed_1", "unnamed_5"]
    df = df[[col for col in df.columns if col not in cols_to_remove]]
    return df


def remove_missing_values(df):
    df = df.dropna(subset=["g_l"])
    return df


def conver_data_type(df):
    # change from float to integer
    columns_to_change = ["quantity", "amount_in_doc_curr", "amount_in_local_cur"]
    df[columns_to_change] = df[columns_to_change].astype(int)
    return df


def main():
    # Variables
    year = "2024"

    # Path
    # data_path = path / "data" / "GL" / "Archive" / f"{year}"
    data_path = path / "data" / "GL"  # for current year

    # Filenames
    input_files = [
        file
        for file in data_path.iterdir()
        if file.is_file() and (file.suffix.lower() == ".xls")
    ]
    output_file = path / "db" / f"GL_{year}.csv"

    # Read data
    df = read_multiple_files(input_files).clean_names(
        strip_underscores=True, case_type="snake"
    )

    # Process data
    df = df.pipe(remove_columns).pipe(remove_missing_values).pipe(conver_data_type)

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
