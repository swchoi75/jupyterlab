import pandas as pd
from pathlib import Path


# Path
try:
    path = Path(__file__).parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent


# Input data: List of multiple text files
parquet_files = [
    file for file in path.iterdir() if file.is_file() and file.suffix == ".parquet"
]


def merge_parquet_files(list_of_files):
    dataframes = [pd.read_parquet(file) for file in list_of_files]
    df = pd.concat(dataframes)
    return df


# Output data
df = merge_parquet_files(parquet_files)
df.info()
df.to_parquet(path / "COPA_Sales.parquet", index=False)
