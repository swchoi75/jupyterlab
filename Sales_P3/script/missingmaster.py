import pandas as pd
from masterdata import master_cc, master_gl, material_master, master_ph


def missing_customer_center(df):
    # load master customer center data
    cc = master_cc()[["Sold-to party"]]

    # filter dataframe columns and remove rows with missing Sold-to party
    df = (
        df[["Sold-to party", "Sold-to Name 1"]]
        .drop_duplicates()
        .dropna(subset=["Sold-to party"])
    )

    # merge dataframes on Sold-to party, keep only rows missing from master data
    return (
        pd.merge(df, cc, on="Sold-to party", how="outer", indicator=True)
        .query('_merge == "left_only"')  # Left anti-join
        .drop(columns=["_merge"])
    )


def missing_gl_accounts(df):
    # load master G/L accounts data
    gl = master_gl()[["G/L account"]]

    # filter dataframe columns and remove rows with missing Cost Elem.
    df = df[["Cost Elem."]].drop_duplicates().dropna(subset=["Cost Elem."])

    # merge dataframes on Cost Elem., keep only rows missing from master data
    return (
        pd.merge(
            df,
            gl,
            left_on="Cost Elem.",
            right_on="G/L account",
            how="outer",
            indicator=True,
        )
        .drop(columns="G/L account")
        .query('_merge == "left_only"')  # Left anti-join
        .drop(columns=["_merge"])
    )


def missing_material_master(df):
    # load master material data
    mat = material_master()[["Profit Ctr", "Product"]]

    # filter dataframe columns and remove rows with missing Product
    df = df[["Profit Ctr", "Product"]].drop_duplicates().dropna(subset=["Product"])

    # merge dataframes on Profit Ctr and Product, keep only rows missing from master data
    return (
        pd.merge(df, mat, on=["Profit Ctr", "Product"], how="outer", indicator=True)
        .query('_merge == "left_only"')  # Left anti-join
        .drop(columns=["_merge"])
    )


def missing_customer_material(df):
    # Filter relevant rows
    df = df[
        (df["Division"] == "E")
        & (df["Customer Material"].isna())
        & (df["Material type"].isin(["FERT", "HALB", "HAWA"]))
    ]

    # Select relevant columns
    df = df.loc[
        :, ["Version", "Month", "Profit Ctr", "Product", "Material Description"]
    ]

    return df


def missing_product_hierarchy(df):
    ph = master_ph()[["Profit Ctr", "Product Hierarchy"]]
    cm_profitctr = ["50803-045", "50803-044", "50803-046"]

    df = df[~df["Profit Ctr"].isin(cm_profitctr)]
    df = df.loc[:, ["Profit Ctr", "Product Hierarchy"]].drop_duplicates()
    df = df.dropna(subset=["Product Hierarchy"])

    return (
        pd.merge(
            df, ph, on=["Profit Ctr", "Product Hierarchy"], how="left", indicator=True
        )
        .query('_merge == "left_only"')  # Left anti-join
        .drop(columns=["_merge"])
    )
