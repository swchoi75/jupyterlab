import pandas as pd
import re
from pathlib import Path


# Path
path = Path(__file__).parent.parent.parent


# Functions
def read_multiple_files(list_of_files):
    dataframes = [
        pd.read_csv(
            file,
            sep="\t",
        )
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


def select_columns(df):
    numeric_cols = [
        "01",
        "02",
        "03",
        "04",
        "05",
        "06",
        "07",
        "08",
        "09",
        "10",
        "11",
        "12",
    ]

    cols_to_keep = ["source", "OneGL B/S + P/L"] + numeric_cols
    df = df[cols_to_keep]
    return df


def process_textual_columns(df):
    # Create regular expression patterns (capture group)
    profit_center_regex = r"([0-9\-]{8,9})"
    fs_item_regex = r"([0-9]+|^K[0-9]+|^P[0-9]+)"

    # Extract text using regex
    df = df.assign(
        source=df["source"].str.extract(profit_center_regex),
        Key=df["OneGL B/S + P/L"].str.extract(fs_item_regex),
    )
    return df


def process_numeric_columns(df):
    numeric_cols = [
        "01",
        "02",
        "03",
        "04",
        "05",
        "06",
        "07",
        "08",
        "09",
        "10",
        "11",
        "12",
    ]

    df[numeric_cols] = df[numeric_cols].astype(float).fillna(0)
    return df


def lookup_table(filename):
    lookup_df = pd.read_csv(filename, dtype="str")
    lookup_df["Key"] = lookup_df["Key"].apply(
        lambda x: re.search(r"[0-9]+|^K[0-9]+|^P[0-9]+", x).group(0)
    )
    return lookup_df


def join_with_lookup(df, filename):
    lookup_df = lookup_table(filename)
    df = pd.merge(df, lookup_df, on="Key", how="left")
    df = df.dropna(subset="A")
    return df


def change_sign_logic(df):
    numeric_cols = [
        "01",
        "02",
        "03",
        "04",
        "05",
        "06",
        "07",
        "08",
        "09",
        "10",
        "11",
        "12",
    ]

    df[numeric_cols] = df[numeric_cols].apply(lambda x: x / -1000)
    return df


def reorder_columns(df):
    numeric_cols = [
        "01",
        "02",
        "03",
        "04",
        "05",
        "06",
        "07",
        "08",
        "09",
        "10",
        "11",
        "12",
    ]

    cols = ["source", "A", "B", "C", "D"] + numeric_cols + ["Key", "OneGL B/S + P/L"]
    df = df.loc[:, cols]
    return df


def change_column_names(df):
    return df.rename(
        columns={
            "source": "PrCr",
            # "OneGL B/S + P/L": "Item",
            "01": "Jan",
            "02": "Feb",
            "03": "Mar",
            "04": "Apr",
            "05": "May",
            "06": "Jun",
            "07": "Jul",
            "08": "Aug",
            "09": "Sep",
            "10": "Oct",
            "11": "Nov",
            "12": "Dec",
        }
    )


def main():

    # Path
    data_path = path / "data" / "SAP YGL0"

    # Filenames
    meta_file = path / "meta" / "Lookup_table.csv"
    output_file = path / "output" / "SAP YGL0 P&L.csv"

    # Input data: List of multiple text files
    txt_files = [
        file for file in data_path.iterdir() if file.is_file() and file.suffix == ".dat"
    ]

    # Read data
    df = read_multiple_files(txt_files)

    # Process data
    df = (
        df.pipe(select_columns)
        .pipe(process_textual_columns)
        .pipe(process_numeric_columns)
        .pipe(join_with_lookup, meta_file)
        .pipe(change_sign_logic)
        .pipe(reorder_columns)
        .pipe(change_column_names)
    )

    # Output data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
