import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent


# Functions
def read_data(filename):
    df = pd.read_csv(filename, dtype={"Cctr": str}).clean_names()
    df["period"] = pd.to_datetime(df["period"])
    return df


def filter_by_year(df, year):
    df = df[df["period"].dt.year == year]
    return df


def main():

    # Filenames
    input_file = path / "data" / "0004_TABLE_OUTPUT_Cctr report common.csv"
    output_file = path / "output" / "cost_center_report.csv"

    # Read data
    df = read_data(input_file)

    # Process data
    df = df.pipe(filter_by_year, 2024)

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
