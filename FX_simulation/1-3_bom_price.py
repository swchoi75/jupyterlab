import pandas as pd
from pathlib import Path


# Path
try:
    path = Path(__file__).parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent


# Filenames
input_file = path / "data" / "BOM.parquet"
output_file = path / "data" / "BOM_price.csv"


# Read data
df = pd.read_parquet(input_file)


# Filter data
df = df[df["cur"] != "CNY"]


# Aggregate data
df = (
    df.groupby(["year", "product", "cur"], dropna=False)  # "type",
    .agg({"amount_doc_": "sum", "total_amount_org_cur_": "sum"})
    .reset_index()
)


# Write data
df.to_csv(output_file, index=False)
print("A file is created.")
