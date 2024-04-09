import pandas as pd
from pathlib import Path
from janitor import clean_names

# from script.common_function import add_responsibilities
from common_function import add_responsibilities

# Variables
from common_variable import spending_total_col


# Path
path = Path(__file__).parent.parent


# Filenames
input_gpa = path / "output" / "1_fc_monthly_spending.csv"
input_sap = path / "output" / "2_fc_acquisition_existing_assets.csv"
input_ppap = path / "output" / "2_fc_acquisition_future_assets.csv"

output_gpa = path / "output" / "fc_GPA_master.csv"
output_auc = path / "output" / "fc_SAP_AUC.csv"


# Read data
# def read_data(filename):
#     df = pd.read_csv(filename, dtype={"asset_class": str})
#     return df


df_gpa = pd.read_csv(input_gpa)
df_sap = pd.read_csv(input_sap, dtype={"asset_class": str})
df_ppap = (
    pd.read_csv(input_ppap)[["sub", "PPAP"]]
    .rename(columns={"PPAP": "info_PPAP"})
    .drop_duplicates()
)


# Functions
def add_useful_life_year(row):
    if row["fix_var"] == "fix":
        return 8
    elif row["fix_var"] == "var":
        return 4


# # GPA master # #

# Add columns
df_gpa["source"] = "GPA"
df_gpa["acquisition"] = df_gpa[spending_total_col]
df_gpa["responsibilities"] = df_gpa.apply(add_responsibilities, axis="columns")
df_gpa["useful_life_year"] = df_gpa.apply(add_useful_life_year, axis="columns")


# select columns
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
df_gpa = (
    df_gpa[selected_columns + ["acquisition"]]
    .groupby(selected_columns, dropna=False)
    .agg({"acquisition": "sum"})
    .reset_index()
)


# # Asset Under Construction # #

# filter rows
df_filtered = df_sap[df_sap["asset_class"].isin(["991", "997", "998"])]
df_auc = df_filtered.reset_index()

# Add columns
df_auc.loc[:, "source"] = "SAP_AUC"
df_auc["responsibilities"] = df_auc.apply(add_responsibilities, axis="columns")

# Fill missing values with 0
df_auc["useful_life_year"] = df_auc["useful_life_year"].fillna(0)

# Import PPAP
df_auc = df_auc.merge(df_ppap, how="left", on="sub")

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
df_auc = (
    df_auc[selected_columns + ["acquisition"]]
    .groupby(selected_columns, dropna=False)
    .agg({"acquisition": "sum"})
    .reset_index()
)


# Write data
df_gpa.to_csv(output_gpa, index=False)
df_auc.to_csv(output_auc, index=False)
print("Files are created")
