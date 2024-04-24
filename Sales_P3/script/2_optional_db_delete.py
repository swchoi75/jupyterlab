import pandas as pd
from pathlib import Path

# Path
path = Path(__file__).parent.parent


# Functions
def db_delete_last_month(df):

    # last_month = df["Period"].iloc[-1] # Find the last row
    last_month = df["Period"].max()  # Find the biggest value

    # Filter out last month
    df = df[df["Period"] != last_month]

    return df


def main():

    # Filename
    db_file = path / "db" / "COPA_2023.csv"

    # Read data
    df = pd.read_csv(db_file)

    # Process data
    df = db_delete_last_month(df)

    # Write a data file in CSV format
    df.to_csv(db_file, index=False)
    print("A file is updated")


if __name__ == "__main__":
    main()
