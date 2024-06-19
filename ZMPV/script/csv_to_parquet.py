import pandas as pd
from janitor import clean_names
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Functions
def read_csv_data(path):
    df = pd.read_csv(
        path,
        dtype={
            "m_y_from_": str,
            "sap_plant": str,
            "trading_pr": str,
            "document_d": str,
        },
    )  # .clean_names()
    return df


def main():

    # Variable
    year = "2023"

    # Filenames
    input_file = path / "db" / f"ZMPV_{year}.csv"
    output_file = path / "db" / f"ZMPV_{year}.parquet"

    # Read data
    df = read_csv_data(input_file)

    # Process data

    # Write data
    df.to_parquet(output_file)
    print("A file is created")


if __name__ == "__main__":
    main()
