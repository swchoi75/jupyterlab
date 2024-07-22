import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent


# Functions
def rename_columns(df):
    df = df.rename(
        columns={
            "Profit Center": "Profit Ctr",
            "Product Hierarchy": "Product Hierarchy",
            "PH_3 simple": "PH_3 simple",
            "PRD/MER": "PRD/MER",
        }
    )
    return df


def select_columns(df, list_of_cols):
    df = df[list_of_cols]
    return df


def join_customer_center(df, df_meta):
    df_meta = df_meta.clean_names()
    df = pd.merge(df, df_meta, on="sold_to_party", how="left")
    return df


def join_gl_accounts(df, df_meta):
    df_meta = df_meta.clean_names()
    df = pd.merge(
        df, df_meta, left_on="cost_elem", right_on="g_l_account", how="left"
    ).drop(columns="g_l_account")
    return df


def join_with_poc(df, df_meta):
    df_meta = df_meta.clean_names()
    df = pd.merge(
        df, df_meta, left_on="profit_ctr", right_on="profit_center", how="left"
    ).drop(columns="profit_center")
    return df


def join_cm_cluster(df, df_meta):
    df_meta = df_meta.clean_names()
    df = pd.merge(df, df_meta, left_on="product", right_on="material", how="left").drop(
        columns="material"
    )
    return df


def join_customer_material(df, df_meta):
    df_meta = df_meta.clean_names()
    df = pd.merge(df, df_meta, on="product", how="left")
    return df


def join_material_master(df, df_meta):
    df_meta = df_meta.clean_names()
    df = pd.merge(df, df_meta, on=["profit_ctr", "product"], how="left")
    return df


def join_product_hierarchy(df, df_meta):
    df_meta = df_meta.clean_names()
    df = pd.merge(df, df_meta, on=["profit_ctr", "product_hierarchy"], how="left")
    return df


def sales_ytd(df):
    df_act = df.query("version == 'Actual'")
    last_month = df_act["month"].max()
    df = df[df["month"] <= last_month]
    return df


def main():

    # Filenames
    input_file = path / "output" / "4_actual_and_budget_sales.csv"
    meta_1 = path / "meta" / "CC_2024.csv"
    meta_2 = path / "meta" / "GL.csv"
    meta_3 = path / "meta" / "POC.csv"
    meta_4 = path / "meta" / "YPC1 costing_Icheon.xlsx"
    meta_5 = path / "meta" / "Customer Material.xlsx"
    meta_6 = path / "meta" / "material_master.csv"
    meta_7 = path / "meta" / "PH info.csv"
    output_file = path / "output" / "5_sales_with_meta_data.csv"

    # Read data
    df = pd.read_csv(input_file, dtype=str)
    df_cc = pd.read_csv(meta_1, dtype=str)
    df_gl = pd.read_csv(meta_2, dtype=str)
    df_poc = pd.read_csv(meta_3, dtype=str)
    df_cm_cluster = pd.read_excel(meta_4, sheet_name="YPC1")
    df_cust_mat = pd.read_excel(meta_5, sheet_name="Sheet1", dtype=str)
    df_mat = pd.read_csv(meta_6, dtype=str)
    df_ph = pd.read_csv(meta_7, dtype=str)

    # Process data
    df_cc = df_cc.pipe(select_columns, ["Sold-to party", "Customer Center"])

    df_gl = df_gl.pipe(select_columns, ["G/L account", "G/L account name"])

    df_poc = df_poc.drop("CU", axis=1)

    df_cm_cluster = df_cm_cluster.pipe(select_columns, ["Material", "CM Cluster"])

    df_cust_mat = df_cust_mat.pipe(
        select_columns, ["Product", "Customer Material"]
    ).dropna(subset=["Customer Material"])

    df_ph = df_ph.pipe(rename_columns).pipe(
        select_columns, ["Profit Ctr", "Product Hierarchy", "PH_3 simple", "PRD/MER"]
    )

    df = (
        df.pipe(join_customer_center, df_cc)
        .pipe(join_gl_accounts, df_gl)
        .pipe(join_with_poc, df_poc)
        .pipe(join_cm_cluster, df_cm_cluster)
        .pipe(join_customer_material, df_cust_mat)
        .pipe(join_material_master, df_mat)
        .pipe(join_product_hierarchy, df_ph)
    )

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
