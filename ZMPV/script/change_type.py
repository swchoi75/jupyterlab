import pandas as pd
from janitor import clean_names
from pathlib import Path


# Path
path = Path(__file__).parent.parent


def main():

    # Variables
    year = "2022"

    # Filenames
    # input_file = path / "db" / "ZMPV.parquet"
    input_file = path / "db" / f"ZMPV_{year}.parquet"
    output_file = path / "db" / f"ZMPV_{year}_new.parquet"

    # Read data
    df = pd.read_parquet(input_file)
    df.info()

    # Process data
    # 2018
    df["psegment"] = df["psegment"].astype("str")

    # 2021
    df["gross_ppv"] = df["gross_ppv"].astype("int64")
    df["net_pm_ppv"] = df["net_pm_ppv"].astype("int64")
    df["net_pm_ppv_1"] = df["net_pm_ppv_1"].astype("float64")

    # 2023
    df["outlet"] = df["outlet"].astype("str")

    # Write data
    df.to_parquet(output_file)
    print("A file is created")


if __name__ == "__main__":
    main()
