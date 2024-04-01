import pandas as pd
from janitor import clean_names
from pathlib import Path


# Path
try:
    path = Path(__file__).parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent

    
filename = path / "COPA_Sales_2021.parquet"
output = path / "COPA_Sales_2021_new.parquet"


# Read data
df = pd.read_parquet(filename)
df["period"] = df["period"].str.replace("2022.01$", "2022.010", regex=True)

df["period"].unique()


# Write data
df.to_parquet(output)
