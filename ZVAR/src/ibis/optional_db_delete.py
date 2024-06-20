import pandas as pd
from pathlib import Path


# Path
path = Path(__file__).parent.parent.parent


# Functions
def read_data(filename):
    # Read a data file in CSV format
    df = pd.read_csv(
        filename,
        dtype={
            "order": str,
            "cost_ctr": str,
            "blk_ind": str,
            "no_post": str,
        },
    )
    return df


def delete_last_month(df):
    # last_month = df["Per"].iloc[-1]  # Find the last row value
    last_month = df["per"].max()  # Find the biggest value in "per" (= Period)

    # Filter out last month
    df = df[(df["per"] != last_month)]

    return df


def main():

    # filenames
    db_file = path / "db" / "ZVAR_2024.csv"

    # Read data
    df = read_data(db_file)

    # Process data
    df = df.pipe(delete_last_month)

    # Write data
    df.to_csv(db_file, index=False)
    print("A file is updated")


if __name__ == "__main__":
    main()
