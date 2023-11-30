import pandas as pd
import numpy as np
from pathlib import Path


# Path
try:
    path = Path(__file__).parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent


# Filenames
input_file_1 = path / "db" / "ZSales 2013-2020.parquet"
input_file_2 = path / "db" / "COPA_Sales 2021-2022.parquet"
input_file_3 = path / "db" / "COPA_Sales_2023.parquet"

out_parquet_file = path / "output" / "Sales 2019-2023 YTD.parquet"
out_csv_file = path / "output" / "Sales 2019-2023 YTD.csv"


# Read data
df_1 = pd.read_parquet(input_file_1)
df_2 = pd.read_parquet(input_file_2)
df_3 = pd.read_parquet(input_file_3)


# Rename columns from Zsales to COPA Sales
df_1 = df_1.rename(
    columns={
        "gl_acct": "cost_elem_",
        "material": "product",
        "material_description": "matnr_descr_",
        "billto_name": "sold_to_name_1",
        "bill_qty": "quantity",
        "sales_hw": "totsaleslc",
    }
)


# Filter ZSales 2019-2020
df_1["fy"] = (df_1["fy"].astype(int) - 1).astype(str)
df_1 = df_1[df_1["fy"].isin(["2019", "2020"])]


# Add new columns for ZSales
df_1["recordtype"] = np.where(df_1["cost_elem_"] == "M08001", "B", "F")


# COPA Sales 2021-2022
df_2[["fy", "month"]] = df_2["period"].str.split(".", expand=True)
df_2["fy"] = (df_2["fy"].astype(int) - 1).astype(str)


# COPA Sales 2023
df_3[["fy", "month"]] = df_3["period"].str.split(".", expand=True)
df_3["fy"] = (df_3["fy"].astype(int) - 1).astype(str)


# Function to generate sales overview
def process_dataframe(df):
    df = (
        df.groupby(
            ["fy", "profit_ctr", "recordtype", "cost_elem_"],
            dropna=False,  # To avoid deleting rows with missing values
        )
        .agg({"quantity": "sum", "totsaleslc": "sum"})
        .reset_index()
    )
    return df


# Wrangle dataframes
df_1 = process_dataframe(df_1)
df_2 = process_dataframe(df_2)
df_3 = process_dataframe(df_3)

df = pd.concat([df_1, df_2, df_3])


# Write data
# df.to_parquet(out_parquet_file, index=False)
df.to_csv(out_csv_file, index=False)
print("A file is created.")