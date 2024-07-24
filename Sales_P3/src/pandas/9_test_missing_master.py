import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent.parent


# Functions
def missing_customer_center(df):
    # select columns
    df = df[["sold_to_party", "sold_to_name_1", "customer_center"]]
    # filter out missing values
    df = df.dropna(subset="sold_to_name_1")
    # filter missing values
    df = df[df["customer_center"].isna()]
    # Drop duplicates
    df = df.drop_duplicates()
    return df


def missing_gl_accounts(df):
    # select columns
    df = df[["cost_elem", "account_class", "g_l_account_name"]]
    # filter out missing values
    df = df.dropna(subset="cost_elem")
    # filter missing values
    df = df[df["g_l_account_name"].isna()]
    # Drop duplicates
    df = df.drop_duplicates()
    return df


def missing_poc(df):
    # select columns
    df = df[["profit_ctr", "outlet"]]
    # filter out missing values
    df = df.dropna(subset="profit_ctr")
    # filter missing values
    df = df[df["outlet"].isna()]
    # Drop duplicates
    df = df.drop_duplicates()
    return df


def missing_cm_cluster(df):
    # select columns
    df = df[
        ["version", "month", "profit_ctr", "product", "material_type", "cm_cluster"]
    ]
    # filter out missing values
    df = df.dropna(subset="product")
    # filter missing values
    df = df[df["cm_cluster"].isna()]
    # Drop duplicates
    df = df.drop_duplicates()

    # filter contract manufacturing product
    df = df[df["profit_ctr"].isin(["50803-044", "50803-045", "50803-046"])]
    df = df[df["material_type"].isin(["FERT"])]

    return df


def missing_customer_material(df):
    # select columns
    df = df[["version", "division", "material_type", "product", "customer_material"]]
    # filter out missing values
    df = df.dropna(subset="product")
    # filter missing values
    df = df[df["customer_material"].isna()]
    # Drop duplicatesm
    df = df.drop_duplicates()

    # filter Division E finished / semi-finished
    df = df[df["division"] == "E"]
    df = df[df["material_type"].isin(["FERT", "HALB"])]

    return df


def missing_material_master(df):
    # select columns
    df = df[["version", "year", "profit_ctr", "product", "material_type"]]
    # filter out missing values
    df = df.dropna(subset="product")
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

    # filter out contract manufacturing
    df = df[~df["profit_ctr"].isin(["50803-044", "50803-045", "50803-046"])]

    return df


def main():

    # Filenames
    input_file = path / "output" / "5_sales_with_master_data.csv"

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
    df_1 = missing_customer_center(df)
    df_2 = missing_gl_accounts(df)
    df_3 = missing_poc(df)
    df_4 = missing_cm_cluster(df)
    df_5 = missing_customer_material(df)
    df_6 = missing_material_master(df)
    df_7 = missing_product_hierarchy(df)

    # Write data
    df_1.to_csv(output_1, index=False)
    df_2.to_csv(output_2, index=False)
    df_3.to_csv(output_3, index=False)
    df_4.to_csv(output_4, index=False)
    df_5.to_csv(output_5, index=False)
    df_6.to_csv(output_6, index=False)
    df_7.to_csv(output_7, index=False)

    print("Files are created")


if __name__ == "__main__":
    main()
