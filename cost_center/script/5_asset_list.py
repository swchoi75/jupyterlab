import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent


# Functions
def read_data(filename):
    df = pd.read_csv(
        filename,
        dtype={
            "Cctr": str,
            "Asset class": str,
            "Asset number": str,
            "Sub number": str,
            "P/O": str,
        },
    ).clean_names()
    # df = df.rename(
    #     columns={"month": "period", "data_type": "version", "profit_ctr": "pctr"}
    # )
    df["acquisition_date"] = pd.to_datetime(df["acquisition_date"])
    df["depreciation_start_date"] = pd.to_datetime(df["depreciation_start_date"])
    df["period"] = pd.to_datetime(df["period"])
    return df


def filter_by_period(df, year, month):
    df = df[(df["period"].dt.year == year) & (df["period"].dt.month == month)]
    return df


def main():

    # Variables
    from common_variable import year, month

    # Filenames
    input_file = path / "data" / "0004_TABLE_OUTPUT_Cctr asset ledger.csv"
    output_file = path / "output" / "5_asset_list.csv"

    # Read data
    df = read_data(input_file)

    # Process data
    df = df.pipe(filter_by_period, year, month)

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
