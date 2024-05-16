import pandas as pd
from janitor import clean_names
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Functions
def missing_customer_master(df, df_meta):
    # select columns
    df = df[["fy", "material_type", "sold_to_name_1"]]
    # filter out missing values
    df = df.dropna(subset="sold_to_name_1")
    # join two dataframes
    df = df.merge(df_meta, on="sold_to_name_1", how="left")
    # filter missing values
    df = df[df["customer_group"].isna()]
    # Drop duplicates
    df = df.drop_duplicates()
    return df


def missing_material_master(df, df_meta):
    # select columns
    df = df[["fy", "material_type", "product"]]
    # filter out missing values
    df = df.dropna(subset="product")
    # join two dataframes
    df = df.merge(df_meta, left_on="product", right_on="material", how="left")
    # filter missing values
    df = df[df["profit_center"].isna()]
    # Drop duplicates
    df = df.drop_duplicates()
    return df


def missing_product_group(df, df_meta):
    # select columns
    df = df[["fy", "material_type", "product_group"]]
    # filter out missing values
    df = df.dropna(subset="product_group")
    # join two dataframes
    df = df.merge(df_meta, on="product_group", how="left")
    # filter missing values
    df = df[df["productline"].isna()]
    # Drop duplicates
    df = df.drop_duplicates()
    return df


def missing_product_hierarchy(df, df_meta):
    # select columns
    df = df[["fy", "recordtype", "material_type", "product_hierarchy"]]
    # filter out missing values
    df = df.dropna(subset="product_hierarchy")
    # join two dataframes
    df = df.merge(df_meta, on="product_hierarchy", how="left")
    # filter missing values
    df = df[df["PH_description"].isna()]
    # Drop duplicates
    df = df.drop_duplicates()
    # filter out recordtype B and ROH
    df = df[(df["recordtype"] != "B") & (df["material_type"] != "ROH")]

    return df


def main():

    # Filenames
    input_file = path / "output" / "Sales Div E_with meta.csv"

    meta_1 = path / "meta" / "customer_group.xlsx"
    meta_2 = path / "meta" / "material_master_Div E.xlsx"
    meta_3 = path / "meta" / "product_group_Div E.xlsx"
    meta_4 = path / "meta" / "product_hierarchy_Div E.xlsx"

    output_1 = path / "meta" / "test_customer_master_missing.csv"
    output_2 = path / "meta" / "test_material_master_missing.csv"
    output_3 = path / "meta" / "test_product_group_missing.csv"
    output_4 = path / "meta" / "test_product_hierarchy_missing.csv"

    # Read data
    df = pd.read_csv(input_file)
    df_cg = pd.read_excel(meta_1)
    df_mm = pd.read_excel(meta_2).clean_names()[["material", "profit_center"]]
    df_pg = pd.read_excel(meta_3)
    df_ph = pd.read_excel(meta_4)[["product_hierarchy", "PH_description"]]

    # Process data
    df_1 = missing_customer_master(df, df_cg)
    df_2 = missing_material_master(df, df_mm)
    df_3 = missing_product_group(df, df_pg)
    df_4 = missing_product_hierarchy(df, df_ph)

    # Write data
    df_1.to_csv(output_1, index=False)
    df_2.to_csv(output_2, index=False)
    df_3.to_csv(output_3, index=False)
    df_4.to_csv(output_4, index=False)
    print("Files are created.")


if __name__ == "__main__":
    main()
