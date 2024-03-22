import pandas as pd
import numpy as np
from pathlib import Path


# Path
try:
    path = Path(__file__).parent.parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent.parent


# Filenames
input_file = path / "data" / "BOM.parquet"
output_file = path / "data" / "BOM_price.csv"


# Read data
df = pd.read_parquet(input_file)


# Filter data
df = df[df["cur"] != "CNY"]


# Add ICO column
df["outs_ic"] = np.where(
    (df["component_no"] == "AAA2043950000") & (df["cur"] == "USD"), "IC", "FR"
)


# Aggregate data
df = (
    df.groupby(["year", "product", "cur", "outs_ic"], dropna=False)
    .agg({"total_amount_org_cur_": "sum"})
    .reset_index()
)


# Write data
df.to_csv(output_file, index=False)
print("A file is created.")
