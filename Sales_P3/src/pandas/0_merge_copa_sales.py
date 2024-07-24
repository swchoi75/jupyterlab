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
            delimiter="\t",
            skiprows=10,
            encoding="UTF-16LE",
            skipinitialspace=True,
            thousands=",",
            engine="python",
            dtype={
                "Period": str,
                "CoCd": str,
                "Doc. no.": str,
                "Ref.doc.no": str,
                "AC DocumentNo": str,
                "Delivery": str,
                # "Item": int,
                "Plnt": str,
                "Tr.Prt": str,
                "ConsUnit": str,
                "FIRE Plant": str,
                "FIREOutlet": str,
            },
        ).rename(
            # Rename columns that are changing between data source
            columns={"Stock val.": "Stock Value"}
        )
        for file in list_of_files
    ]

    # Merge the list of DataFrames into a single DataFrame
    df = pd.concat(dataframes)

    return df


def remove_first_two_columns(df):
    return df.iloc[:, 2:]


def remove_sub_total_rows(df):
    return df.loc[df["RecordType"].notna()]


def clean_column_names(df):
    df = df.clean_names()
    df.columns = df.columns.str.strip("_")
    return df


def main():
    # Variable
    year = 2024

    # Filenames
    data_path = path / "data" / "Actual"
    output_file = path / "db" / f"COPA_Sales_{year}.parquet"

    # Input data: List of multiple text files
    txt_files = [
        file for file in data_path.iterdir() if file.is_file() and file.suffix == ".TXT"
    ]

    # Process data
    df = read_multiple_files(txt_files)
    df = (
        df.pipe(remove_first_two_columns)
        .pipe(remove_sub_total_rows)
        .pipe(clean_column_names)
    )

    # Write to Parquet file
    df.to_parquet(output_file, index=False)
    print("A parquet file is created.")


if __name__ == "__main__":
    main()
