import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent


# Functions
def missing_customer_center(df):
    # select columns
    df = df[["sold_to_party", "sold_to_name_1", "customer_center"]]
    # filter missing values
    df = df[df["customer_center"].isna()]
    # Drop duplicates
    df = df.drop_duplicates()
    return df


def missing_gl_accounts(df):
    # select columns
    df = df[["cost_elem", "account_class", "g_l_account_name"]]
    # filter missing values
    df = df[df["g_l_account_name"].isna()]
    # Drop duplicates
    df = df.drop_duplicates()
    return df


def missing_poc(df):
    # select columns
    df = df[["profit_ctr", "outlet"]]
    # filter missing values
    df = df[df["outlet"].isna()]
    # Drop duplicates
    df = df.drop_duplicates()
    return df


def missing_cm_cluster(df):
    # select columns
    df = df[["profit_ctr", "product", "cm_cluster"]]
    # filter missing values
    df = df[df["cm_cluster"].isna()]
    # Drop duplicates
    df = df.drop_duplicates()
    return df


def missing_customer_material(df):
    # select columns
    df = df[["profit_ctr", "product", "customer_material"]]
    # filter missing values
    df = df[df["customer_material"].isna()]
    # Drop duplicates
    df = df.drop_duplicates()
    return df


def missing_material_master(df):
    # select columns
    df = df[["version", "year", "month", "profit_ctr", "product", "material_type"]]
    # filter missing values
    df = df[df["material_type"].isna()]
    # Drop duplicates
    df = df.drop_duplicates()
    return df


def missing_product_hierarchy(df):
    # select columns
    df = df[["profit_ctr", "product_hierarchy", "ph_3_simple"]]
    # filter missing values
    df = df[df["ph_3_simple"].isna()]
    # Drop duplicates
    df = df.drop_duplicates()
    return df


def main():

    # Filenames
    input_file = path / "output" / "5_sales_with_meta_data.csv"

    output_1 = path / "meta" / "test_CC_2024_missing.csv"
    output_2 = path / "meta" / "test_GL_missing.csv"
    output_3 = path / "meta" / "test_POC_missing.csv"
    output_4 = path / "meta" / "test_CM_cluster_missing.csv"
    output_5 = path / "meta" / "test_customer_material_missing.csv"
    output_6 = path / "meta" / "test_material_master_missing.csv"
    output_7 = path / "meta" / "test_PH_info_missing.csv"

    # Read data
    df = pd.read_csv(input_file, dtype=str)

    # Process data
    df_cc = df.pipe(missing_customer_center)
    df_gl = df.pipe(missing_gl_accounts)
    df_poc = df.pipe(missing_poc)
    df_cm_cluster = df.pipe(missing_cm_cluster)
    df_cust_mat = df.pipe(missing_customer_material)
    df_mat = df.pipe(missing_material_master)
    df_ph = df.pipe(missing_product_hierarchy)

    # Write data
    df_cc.to_csv(output_1, index=False)
    df_gl.to_csv(output_2, index=False)
    df_poc.to_csv(output_3, index=False)
    df_cm_cluster.to_csv(output_4, index=False)
    df_cust_mat.to_csv(output_5, index=False)
    df_mat.to_csv(output_6, index=False)
    df_ph.to_csv(output_7, index=False)

    print("Files are created")


if __name__ == "__main__":
    main()
