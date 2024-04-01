import pandas as pd
from masterdata import (
    master_cc,
    master_gl,
    master_poc,
    master_cm_cluster,
    customer_material,
    material_master,
    master_ph,
)


def process_copa_sales(df):
    # Join main data with meta data
    df = join_customer_center(df)
    df = join_gl_accounts(df)
    df = join_with_poc(df)
    df = join_cm_cluster(df)
    df = join_customer_material(df)
    df = join_material_master(df)
    df = join_product_hierarchy(df)
    return df


def join_customer_center(df):
    cc = master_cc().loc[:, ["Sold-to party", "Customer Center"]]
    df = pd.merge(df, cc, on="Sold-to party", how="left")
    return df


def join_gl_accounts(df):
    gl = master_gl().loc[:, ["G/L account", "G/L account name"]]
    df = pd.merge(
        df, gl, left_on="Cost Elem.", right_on="G/L account", how="left"
    ).drop(columns="G/L account")
    return df


def join_with_poc(df):
    poc = master_poc().drop("CU", axis=1)
    df = pd.merge(
        df, poc, left_on="Profit Ctr", right_on="Profit Center", how="left"
    ).drop(columns="Profit Center")
    return df


def join_cm_cluster(df):
    cm_cluster = master_cm_cluster().loc[:, ["Material", "CM Cluster"]]
    df = pd.merge(
        df, cm_cluster, left_on="Product", right_on="Material", how="left"
    ).drop(columns="Material")
    return df


def join_customer_material(df):
    cust_mat = customer_material()
    df = pd.merge(df, cust_mat, on="Product", how="left")
    return df


def join_material_master(df):
    mat = material_master()
    df = pd.merge(df, mat, on=["Profit Ctr", "Product"], how="left")
    return df


def join_product_hierarchy(df):
    ph = master_ph()
    df = pd.merge(df, ph, on=["Profit Ctr", "Product Hierarchy"], how="left")
    return df


def sales_ytd(df):
    df_act = df.query("Version == 'Actual'")
    last_month = df_act["Month"].max()
    df = df[df["Month"] <= last_month]
    return df
