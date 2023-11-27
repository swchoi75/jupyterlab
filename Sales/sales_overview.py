import pandas as pd
from pathlib import Path

# Path
try:
    path = Path(__file__).parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent


input_file_1 = path / "db" / "ZSales 2013-2020.parquet"
input_file_2 = path / "db" / "COPA_Sales 2021-2022.parquet"

df_1 = pd.read_parquet(input_file_1)
df_2 = pd.read_parquet(input_file_2)

df_1 = df_1[["fy", "sales_hw"]]
df_2 = df_2[["period", "totsaleslc"]]

df_1 = df_1.groupby("fy").sum()
df_2 = df_2.groupby("period").sum()

df_1

df_2
