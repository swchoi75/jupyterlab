import pandas as pd
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Functions
def merge_dataframes(list_of_files):
    dataframes = [pd.read_parquet(file) for file in list_of_files]
    df = pd.concat(dataframes)
    return df


def main():

    # Path
    data_path = path / "db"

    # Filenames
    output_file = path / "db" / "ZVAR.parquet"

    # Input data: List of multiple text files
    parquet_files = [
        file
        for file in data_path.iterdir()
        if file.is_file() and file.suffix == ".parquet"
    ]

    # Process data
    df = merge_dataframes(parquet_files)

    # Write data
    df.to_parquet(output_file)
    print("A file is created")


if __name__ == "__main__":
    main()
