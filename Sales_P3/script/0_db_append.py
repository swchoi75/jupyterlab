import pandas as pd
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Functions
def read_txt_file(filename):
    # Read a tab-delimited file
    df = pd.read_csv(
        filename,
        delimiter="\t",
        skiprows=10,
        encoding="UTF-16LE",
        skipinitialspace=True,
        thousands=",",
        engine="python",
        dtype=str,
    )
    return df


def remove_first_two_columns(df):
    return df.iloc[:, 2:]


def remove_sub_total_rows(df):
    return df.loc[df["RecordType"].notna()]


def main():
    # Filenames
    input_file = "data/Actual/COPA_2024_03.TXT"  # Update monthly
    output_file = path / "db" / "COPA_2024.csv"

    # Read data
    df = read_txt_file(input_file)

    # Process data
    df = df.pipe(remove_first_two_columns).pipe(remove_sub_total_rows)

    # Write data
    df.to_csv(output_file, mode="a", header=False, index=False)
    print("A file is updated")


if __name__ == "__main__":
    main()
