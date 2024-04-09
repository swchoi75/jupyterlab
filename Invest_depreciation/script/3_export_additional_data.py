import pandas as pd
from pathlib import Path
from janitor import clean_names

# from script.common_function import add_responsibilities
from common_function import add_responsibilities, add_useful_life_year

# Variables
from common_variable import spending_total_col

# Path
path = Path(__file__).parent.parent


# Functions
def read_data(input_gpa, input_sap, input_ppap):
    """Read GPA spending, SAP assets, PPAP info"""
    df_gpa = pd.read_csv(input_gpa)
    df_sap = pd.read_csv(input_sap, dtype={"asset_class": str})
    df_ppap = (
        pd.read_csv(input_ppap)[["sub", "PPAP"]]
        .rename(columns={"PPAP": "info_PPAP"})
        .drop_duplicates()
    )
    return df_gpa, df_sap, df_ppap


def add_col_to_gpa(df):
    """Add columns to GPA master"""
    df["source"] = "GPA"
    df["acquisition"] = df[spending_total_col]
    df["responsibilities"] = df.apply(add_responsibilities, axis="columns")
    df["useful_life_year"] = df.apply(add_useful_life_year, axis="columns")
    return df


def summarize_gpa(df):
    selected_columns = [
        "source",
        "responsibilities",
        # GPA
        "outlet_sender",
        "status",
        "master",
        "master_description",
        "sub",
        "sub_description",
        "category_of_investment",
        "category_description",
        # Meta
        "fs_item_sub",
        "fs_item_description",
        "gl_account",
        "gl_account_description",
        "basic_or_project",
        "cost_center",
        "fix_var",
        "useful_life_year",
        # "acquisition",
    ]

    # aggregate data
    df = (
        df[selected_columns + ["acquisition"]]
        .groupby(selected_columns, dropna=False)
        .agg({"acquisition": "sum"})
        .reset_index()
    )
    return df


def auc_data(df, df_ppap):
    """Asset Under Construction"""
    # Filter data
    df_filtered = df[df["asset_class"].isin(["991", "997", "998"])]
    df = df_filtered.reset_index()

    # Add columns
    df.loc[:, "source"] = "SAP_AUC"
    df["responsibilities"] = df.apply(add_responsibilities, axis="columns")

    # Fill missing values with 0
    df["useful_life_year"] = df["useful_life_year"].fillna(0)

    # Import PPAP
    df = df.merge(df_ppap, how="left", on="sub")
    return df


def summarize_auc(df):
    # select columns
    selected_columns = [
        "source",
        "responsibilities",
        # GPA
        "outlet_sender",
        "master",
        "master_description",
        "sub",
        "sub_description",
        # SAP
        "profit_center",
        "asset_class",
        "asset_class_name",
        "cost_center",
        "asset_no",
        "sub_no",
        "asset_description",
        "acquisition_date",
        # Meta
        "fs_item_sub",
        "fs_item_description",
        "gl_account",
        "gl_account_description",
        "fix_var",
        # Key information
        "info_PPAP",
        "useful_life_year",
        # "acquisition",
    ]

    # aggregate data
    df = (
        df[selected_columns + ["acquisition"]]
        .groupby(selected_columns, dropna=False)
        .agg({"acquisition": "sum"})
        .reset_index()
    )
    return df


def main():

    # Filenames
    input_gpa = path / "output" / "1_fc_monthly_spending.csv"
    input_sap = path / "output" / "2_fc_acquisition_existing_assets.csv"
    input_ppap = path / "output" / "2_fc_acquisition_future_assets.csv"

    output_gpa = path / "output" / "fc_GPA_master.csv"
    output_auc = path / "output" / "fc_SAP_AUC.csv"

    # Process data
    df_gpa, df_sap, df_ppap = read_data(input_gpa, input_sap, input_ppap)
    df_gpa = df_gpa.pipe(add_col_to_gpa).pipe(summarize_gpa)
    df_auc = auc_data(df_sap, df_ppap).pipe(summarize_auc)

    # Write data
    df_gpa.to_csv(output_gpa, index=False)
    df_auc.to_csv(output_auc, index=False)
    print("Files are created")


if __name__ == "__main__":
    main()
