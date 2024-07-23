import pandas as pd
from janitor import clean_names
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Functions
def missing_customer_master(df):
    # select columns
    df = df[["fy", "material_type", "sold_to_name_1", "customer_group"]]
    # filter out missing values
    df = df.dropna(subset="sold_to_name_1")
    # filter missing values
    df = df[df["customer_group"].isna()]
    # Drop duplicates
    df = df.drop_duplicates()
    return df


def missing_material_master(df):
    # select columns
    df = df[["fy", "profit_ctr", "product", "material_type"]]
    # filter out missing values
    df = df.dropna(subset="product")
    # filter missing values
    df = df[df["material_type"].isna()]
    # Drop duplicates
    df = df.drop_duplicates()
    return df


def missing_product_group(df):
    # select columns
    df = df[["fy", "material_type", "product_group", "productline"]]
    # filter out missing values
    df = df.dropna(subset="product_group")
    # filter missing values
    df = df[df["productline"].isna()]
    # Drop duplicates
    df = df.drop_duplicates()
    return df


def missing_product_hierarchy(df):
    # select columns
    df = df[
        ["fy", "recordtype", "material_type", "product_hierarchy", "PH_description"]
    ]
    # filter out missing values
    df = df.dropna(subset="product_hierarchy")
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

    output_1 = path / "meta" / "test_customer_master_missing.csv"
    output_2 = path / "meta" / "test_material_master_missing.csv"
    output_3 = path / "meta" / "test_product_group_missing.csv"
    output_4 = path / "meta" / "test_product_hierarchy_missing.csv"

    # Read data
    df = pd.read_csv(input_file)

    # Process data
    df_1 = missing_customer_master(df)
    df_2 = missing_material_master(df)
    df_3 = missing_product_group(df)
    df_4 = missing_product_hierarchy(df)

    # Write data
    df_1.to_csv(output_1, index=False)
    df_2.to_csv(output_2, index=False)
    df_3.to_csv(output_3, index=False)
    df_4.to_csv(output_4, index=False)
    print("Files are created.")


if __name__ == "__main__":
    main()
