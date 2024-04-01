import numpy as np


# Define a custom function to calculate the budget price
def bud_price(df, col_name):
    col = col_name
    df = (
        df.loc[:, ["Version", "Month", "Profit Ctr", col, "Qty", "Sales_LC"]]
        .query('Version == "Budget"')
        .groupby(["Version", "Profit Ctr", col])
        .agg(Qty=("Qty", "sum"), Sales_LC=("Sales_LC", "sum"))
        .assign(bud_price=lambda x: np.round(x.Sales_LC / x.Qty, 0))
        .reset_index()
        .dropna(subset=[col])
        .loc[:, ["Profit Ctr", col, "bud_price"]]
    )
    df["mapping key"] = df[col]
    return df


# 3 different groups
def df_div_e(df):
    return df.query('Division == "E"')


def df_div_p(df):
    return df.query('`Outlet name`.isin(["PL EAC", "PL HYD", "PL MES"])')


def df_pl_cm(df):
    return df.query('`Outlet name`.isin(["PL CM CCN", "PL CM CVS", "PL CM PSS"])')


# Create budget price tables for 3 different groups
def bud_price_div_e(df):
    df = df_div_e(df)
    df = bud_price(df, "Customer Material")
    return df


def bud_price_div_p(df):
    df = df_div_p(df)
    df = bud_price(df, "Product")
    return df


def bud_price_pl_cm(df):
    df = df_pl_cm(df)
    df = bud_price(df, "CM Cluster")
    return df


# mapping between between actual and budget
def spv_mapping(df, bud_price, col_name):
    return df.merge(bud_price, on=["Profit Ctr", col_name], how="left")
