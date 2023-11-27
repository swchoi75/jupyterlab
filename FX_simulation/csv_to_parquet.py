import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
try:
    path = Path(__file__).parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent


# Filenames
input_file = path / "data" / "BOM price.txt"
output_file = path / "data" / "BOM_2023.parquet"


# Read txt file
df = pd.read_csv(
    input_file,
    delimiter="\t",
    skiprows=3,
    encoding="UTF-16LE",
    skipinitialspace=True,
    thousands=",",
    engine="python",
    dtype={
        "DV": str,
        # "Quota": int,
        "Amount(Doc)": float,
        "Total Amount(Org. CUR)": float,
        "Total Amount(Locl)": float,
        "Exchange Rate": float,
        "Vendor ID": str,
        "Planned price 1": float,
        "Planned price 2": float,
    },
)


# Remove first two columns
df = df.iloc[:, 2:]

# Clean column names
df = df.clean_names()

# Write data
df.to_parquet(output_file)
print("A parquet file is created")
