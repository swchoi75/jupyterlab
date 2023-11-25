import pandas as pd
from pathlib import Path

# Path
try:
    path = Path(__file__).parent
except NameError:
    import inspect
    
    path = Path(inspect.getfile(lambda: None)).resolve().parent


input_file = path / "db" / "ZSales 2013-2020.parquet"

df = pd.read_parquet(input_file)

df = df[["fy", "sales_hw"]]

df = df.groupby("fy").sum()

df
