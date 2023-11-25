import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
try:
    path = Path(__file__).parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent


input_file = path / "data" / "BOM price.txt"
output_file = path / "data" / "BOM_2023.parquet"

df = pd.read_csv(
    input_file,
    sep="\t",
    skiprows=5,
    usecols=(range(2, 29)),
    header=None,
    thousands=",",
    encoding="utf-16le",
)


# New list of column names
new_column_names = [
    "Product",
    "DV",
    "Component No",
    "Material Type",
    "Description",
    "Quota",
    "Quantity",
    "Qty Unit",
    "Price",
    "CUR",
    "Unit",
    "Amount(Doc)",
    "Freight",
    "Custom",
    "Total Amount(Org. CUR)",
    "Total Amount(Locl)",
    "Exchange Rate",
    "Type None",
    "Vendor ID",
    "Vendor Name",
    "Planned price 1",
    "Planned date 1",
    "Planned price 2",
    "Planned date 2",
    "Planned price 3",
    "Planned date 3",
    "IcoT",
]

# Replace existing column names with the new list
df.columns = new_column_names

df = df.astype(
    {
        "DV": str,
        # "Quota": int,
        "Amount(Doc)": float,
        "Total Amount(Org. CUR)": float,
        "Total Amount(Locl)": float,
        "Exchange Rate": float,
        "Vendor ID": str,
        "Planned price 1": float,
        "Planned price 2": float,
    }
)
df["DV"] = df["DV"].str.rstrip(".0")
df["DV"] = df["DV"].str.zfill(2)

df = df.clean_names()

df.info()

df.to_parquet(output_file)
