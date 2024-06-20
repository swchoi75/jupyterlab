import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent.parent


# Functions
def read_txt_file(filename):
    # Read a tab-delimited file
    df = pd.read_csv(
        filename,
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
            "No-Post": str,
        },
    )
    return df


def remove_col_row(df):
    # Remove first two columns and sub-total rows
    df = df.drop(columns=["Unnamed: 0", "Unnamed: 1", "Unnamed: 4"])
    df = df.dropna(subset=["Order"])
    # Remove repeating header rows
    df = df[df.ne(df.columns).any(axis="columns")]
    return df


def main():

    # Filenames
    input_file = path / "data" / "ZVAR_2024_01.txt"
    db_file = path / "db" / "ZVAR_2024.csv"

    # Read data
    df = read_txt_file(input_file)

    # Process data
    df = df.pipe(remove_col_row).pipe(clean_names)

    # Write data
    df.to_csv(db_file, mode="a", header=False, index=False)
    print("A file is updated")


if __name__ == "__main__":
    main()
