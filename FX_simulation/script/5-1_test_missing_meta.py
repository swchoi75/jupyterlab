import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent


# Functions
def missing_material_master(df, df_meta):
    # select columns
    df = df[["fy", "representative_pn"]]
    # filter out missing values
    df = df.dropna(subset="representative_pn")
    # join two dataframes
    df = df.merge(df_meta, left_on="representative_pn", right_on="material", how="left")
    # filter missing values
    df = df[df["profit_center"].isna()]
    # Drop duplicates
    df = df.drop_duplicates()
    return df


def filter_reprseentative_pn(df):
    df = df.dropna(subset="representative_pn")
    return df


def filter_missing_fx_scenario(df):
    df = df[df["fx_scenario"].isna()]
    return df


def filter_product_to_HMG(df):
    df = df[
        (df["recordtype"] == "F")
        & (df["material_type"].isin(["FERT", "HALB"]))
        # filter HMG and affiliates
        & (df["customer_group"].isin(["HMG", "Hyundai Transys", "Kefico", "MOBIS"]))
    ]
    return df


def missing_product_hierarchy(df):
    list_of_cols = [
        "fy",
        "productline",
        "product_group",
        "product_hierarchy",
        "PH_description",
        "representative_pn",
        "HMG_PN",
    ]
    df = df[list_of_cols]
    df = df.sort_values(by=list_of_cols).drop_duplicates()
    return df


def main():

    # Filenames
    input_file = path / "output" / "sales with bom costs.csv"
    meta_file = path / "meta" / "Representative PN_Material_Master.xlsx"
    output_1 = path / "meta" / "test_material_master_missing.csv"
    output_2 = path / "meta" / "test_product_hierarchy_missing.csv"

    # Read data
    df = pd.read_csv(input_file)
    df_meta = pd.read_excel(meta_file).clean_names()[["material", "profit_center"]]

    # Process data
    df_mm = missing_material_master(df, df_meta)
    df_ph = (
        df.pipe(filter_reprseentative_pn)
        .pipe(filter_missing_fx_scenario)
        .pipe(filter_product_to_HMG)
        .pipe(missing_product_hierarchy)
    )

    # Write data
    df_mm.to_csv(output_1, index=False)
    df_ph.to_csv(output_2, index=False)
    print("files are created")


if __name__ == "__main__":
    main()
