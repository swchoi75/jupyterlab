import pandas as pd
from pathlib import Path


# Path
path = Path(__file__).parent.parent.parent


# Functions
def read_data(filename):
    df = df = pd.read_csv(filename, dtype={"costctr": str})
    return df


def delete_last_month(path):

    # last_month = df["Period"].iloc[-1] # Find the last row
    last_month = df["period"].max()  # Find the biggest value

    # Filter out last month
    df = df[df["period"] != last_month]

    return df


def main():

    # Filenames
    db_file_cf = path / "db" / "CF_2023.csv"
    db_file_pl = path / "db" / "PL_2023.csv"

    # Read data
    df_cf = read_data(db_file_cf)
    df_pl = read_data(db_file_pl)

    # Process data
    df_cf = df_cf.pipe(delete_last_month)
    df_pl = df_pl.pipe(delete_last_month)

    # Write data
    df_cf.to_csv(db_file_cf, index=False)
    df_pl.to_csv(db_file_pl, index=False)
    print("Files are updated")


if __name__ == "__main__":
    main()
