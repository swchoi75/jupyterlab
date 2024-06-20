import pandas as pd
from janitor import clean_names
from pathlib import Path


# Path
path = Path(__file__).parent.parent.parent


# Functions
def read_csv_data(path):
    df = pd.read_csv(
        path,
        dtype={
            # String
            "ce_act": str,
            "order": str,
            "cost_ctr": str,
            "dv": str,
            "no_post": str,
            "valcl": str,
            "cocd": str,
            "plnt": str,
            "coar": str,
            "prod_proc": str,
            # Int64
            "per": "int64",
            "year": "int64",
            "itm": "int64",
            "cat": "int64",
            "le": "int64",
            "vsn": "int64",
        },
    )  # .clean_names()
    return df


def remove_zero_in_string(df):
    """Remove ".0" at the end of strings"""
    df["valcl"] = df["valcl"].str.replace(r"\.0$", "", regex=True)
    df["prod_proc"] = df["prod_proc"].str.rstrip(".0")
    return df


def rename_columns(df):
    df = df.rename(columns={"per": "period"})
    return df


def main():

    # Variable
    from variable_year import year

    # Filenames
    input_file = path / "db" / f"ZVAR_{year}.csv"
    output_file = path / "db" / f"ZVAR_{year}.parquet"

    # Read data
    df = read_csv_data(input_file)

    # Process data
    # df = df.pipe(remove_zero_in_string)
    df = df.pipe(rename_columns)

    # Write data
    df.to_parquet(output_file)
    print("A file is created")


if __name__ == "__main__":
    main()
