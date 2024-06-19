import pandas as pd
from janitor import clean_names
from pathlib import Path


def main():

    # Variables
    year = "2022"

    # Path
    path = Path(__file__).parent.parent

    # Filenames
    input_file = path / "db" / "ZVAR.parquet"
    # input_file = path / "db" / f"ZVAR_{year}.parquet"
    # output_file = path / "db" / f"ZVAR_{year}_new.parquet"

    # Read data
    df = pd.read_parquet(input_file)
    df.info()

    # Process data
    # 2022
    # df["avpr"] = df["avpr"].fillna(0).astype("int64")

    # Write data
    # df.to_parquet(output_file)
    # print("A file is created")


if __name__ == "__main__":
    main()
