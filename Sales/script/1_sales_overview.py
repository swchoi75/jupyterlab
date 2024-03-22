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
input_file_0 = path / "db" / "ZSales 2013-2020.parquet"
input_file_1 = path / "db" / "COPA_Sales_2021.parquet"
input_file_2 = path / "db" / "COPA_Sales_2022.parquet"
input_file_3 = path / "db" / "COPA_Sales_2023.parquet"
input_file_4 = path / "db" / "COPA_Sales_2024.parquet"

output_file = path / "output" / "Sales 2019-2024 YTD.csv"


# Read data
df_0 = pd.read_parquet(input_file_0)
df_1 = pd.read_parquet(input_file_1)
df_2 = pd.read_parquet(input_file_2)
df_3 = pd.read_parquet(input_file_3)
df_4 = pd.read_parquet(input_file_4)


# Rename columns from Zsales to COPA Sales
df_0 = df_0.rename(
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
df_0["fy"] = (df_0["fy"].astype(int) - 1).astype(str)
df_0 = df_0[df_0["fy"].isin(["2019", "2020"])]


# Add new columns for ZSales
df_0["recordtype"] = np.where(df_0["cost_elem_"] == "M08001", "B", "F")


# COPA Sales 2021-2024
df = pd.concat([df_1, df_2, df_3, df_4])


df[["fy", "month"]] = df["period"].str.split(".", expand=True)
df["fy"] = (df["fy"].astype(int) - 1).astype(str)


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
df_0 = process_dataframe(df_0)  # ZSales
df = process_dataframe(df)  # COPA Sales


df = pd.concat([df_0, df])


# Write data
df.to_csv(output_file, index=False)
print("A file is created.")
