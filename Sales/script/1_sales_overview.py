import pandas as pd
import numpy as np
from pathlib import Path


# Path
try:
    path = Path(__file__).parent.parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent.parent


# Functions
def rename_columns_to_COPA(df):
    """Rename columns from Zsales to COPA Sales"""
    df = df.rename(
        columns={
            "gl_acct": "cost_elem_",
            "material": "product",
            "material_description": "matnr_descr_",
            "billto_name": "sold_to_name_1",
            "bill_qty": "quantity",
            "sales_hw": "totsaleslc",
        }
    )
    return df


def filter_by_year(df, list_of_years=["2019", "2020"]):
    """Filter ZSales in 2019-2020"""
    df["fy"] = (df["fy"].astype(int) - 1).astype(str)
    df = df[df["fy"].isin(list_of_years)]
    return df


def add_recordtype(df):
    """Add new columns for ZSales"""
    df["recordtype"] = np.where(df["cost_elem_"] == "M08001", "B", "F")
    return df


def process_fy_month(df):
    df[["fy", "month"]] = df["period"].str.split(".", expand=True)
    df["fy"] = (df["fy"].astype(int) - 1).astype(str)
    return df


def sales_overview(df):
    """Generate Sales Overview"""
    df = (
        df.groupby(
            ["fy", "profit_ctr", "recordtype", "cost_elem_"],
            dropna=False,  # To avoid deleting rows with missing values
        )
        .agg({"quantity": "sum", "totsaleslc": "sum"})
        .reset_index()
    )
    return df


def main():

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

    # Process data
    zsales = (
        df_0.pipe(rename_columns_to_COPA)
        .pipe(filter_by_year, ["2019", "2020"])
        .pipe(add_recordtype)
    )
    copa = pd.concat([df_1, df_2, df_3, df_4]).pipe(process_fy_month)

    # Sales overview
    zsales = sales_overview(zsales)  # ZSales
    copa = sales_overview(copa)  # COPA Sales
    df = pd.concat([zsales, copa])

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created.")


if __name__ == "__main__":
    main()
