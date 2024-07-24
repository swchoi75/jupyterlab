import pandas as pd
import numpy as np
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent.parent


# Functions
def bud_price(df, col_name):
    """Define a custom function to calculate the budget price"""
    col = col_name
    df = (
        df.loc[:, ["version", "month", "profit_ctr", col, "qty", "sales_lc"]]
        .query('version == "Budget"')
        .groupby(["version", "profit_ctr", col])
        .agg(qty=("qty", "sum"), sales_lc=("sales_lc", "sum"))
        .assign(bud_price=lambda x: np.round(x.sales_lc / x.qty, 0))
        .reset_index()
        .dropna(subset=[col])
        .loc[:, ["profit_ctr", col, "bud_price"]]
    )
    df["mapping key"] = df[col]
    return df


# 3 different groups
def select_div_e(df):
    df = df.query('division == "E"')
    return df


def select_div_p(df):
    df = df.query('outlet_name.isin(["PL EAC", "PL HYD", "PL MES", "PL DAC E"])')
    return df


def select_pl_cm(df):
    df = df.query('outlet_name.isin(["PL CM CCN", "PL CM CVS", "PL CM PSS"])')
    return df


def sales_ytd(df):
    df_act = df.query("version == 'Actual'")
    last_month = df_act["month"].max()
    df = df[df["month"] <= last_month]
    return df


# mapping between between actual and budget
def spv_mapping(df, bud_price, col_name):
    df = df.merge(bud_price, on=["profit_ctr", col_name], how="left")
    return df


def main():

    # Filenames
    input_file = path / "output" / "5_sales_with_master_data.csv"
    output_1 = path / "output" / "bud_price_div_e.csv"
    output_2 = path / "output" / "bud_price_div_p.csv"
    output_3 = path / "output" / "bud_price_pl_cm.csv"
    output_file = path / "output" / "6_ytd_sales_spv_mapping.csv"

    # Read data
    df = pd.read_csv(input_file)

    # Process data
    ## create budget price tables for 3 different mapping methods
    df_1 = df.pipe(select_div_e).pipe(bud_price, "product_hierarchy")
    df_2 = df.pipe(select_div_p).pipe(bud_price, "product")
    df_3 = df.pipe(select_pl_cm).pipe(bud_price, "cm_cluster")

    ## derive YTD sales
    df_ytd = sales_ytd(df)

    ## Sales P3: SPV mapping for 3 different scenarios
    df_ytd_act = df_ytd[df_ytd["version"] == "Actual"]
    df_ytd_bud = df_ytd[df_ytd["version"] == "Budget"]

    map_div_e = spv_mapping(select_div_e(df_ytd_act), df_1, "product_hierarchy")
    map_div_p = spv_mapping(select_div_p(df_ytd_act), df_2, "product")
    map_pl_cm = spv_mapping(select_pl_cm(df_ytd_act), df_3, "cm_cluster")

    df_result = pd.concat([map_div_e, map_div_p, map_pl_cm, df_ytd_bud])

    # Write data
    df_1.to_csv(output_1, index=False)
    df_2.to_csv(output_2, index=False)
    df_3.to_csv(output_3, index=False)
    df_result.to_csv(output_file, index=False)
    print("Files are created")


if __name__ == "__main__":
    main()
