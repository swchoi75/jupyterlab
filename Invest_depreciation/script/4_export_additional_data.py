import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
try:
    path = Path(__file__).parent.parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent.parent


# Filenames
input_file = path / "fc_output" / "fc_depreciation_combined.csv"
output_master = path / "fc_output" / "fc_GPA_master.csv"
output_auc = path / "fc_output" / "fc_SAP_AUC.csv"


# Read data
df = pd.read_csv(
    input_file,
    dtype={
        # GPA data
        "investment_type": str,
        "financial_statement_item": str,
        "useful_life_year": int,
        # SAP data
        "asset_class": str,
        "cost_center": str,
        "asset_no": str,
        "sub_no": str,
    },
)


# # GPA master # #

# filter rows
df_master = df[df["source"] == "GPA"]

# select columns
selected_columns = [
    "source",
    "responsibilities",
    "outlet_sender",
    "status",
    "master",
    "master_description",
    "sub",
    "sub_description",
    "category_of_investment",
    "category_description",
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
df_master = (
    df_master[selected_columns + ["acquisition"]]
    .groupby(selected_columns, dropna=False)
    .agg({"acquisition": "sum"})
    .reset_index()
)


# # Asset Under Construction # #

# filter rows
df_auc = df[df["asset_class"].isin(["991", "997", "998"])]

# select columns
selected_columns = [
    "source",
    "responsibilities",
    "profit_center",
    "asset_class",
    "asset_class_name",
    "cost_center",
    "asset_no",
    "sub_no",
    "asset_description",
    "acquisition_date",
    "start_of_depr",
    "fs_item_sub",
    "fs_item_description",
    "gl_account",
    "gl_account_description",
    "fix_var",
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
df_master.to_csv(output_master, index=False)
df_auc.to_csv(output_auc, index=False)
print("Files are created")
