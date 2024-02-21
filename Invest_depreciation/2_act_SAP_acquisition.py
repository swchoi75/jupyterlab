import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
try:
    path = Path(__file__).parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent


# Filenames
input_file = path / "fc_data" / "2023-11_Asset History Leger_20231130.xlsx"
meta_file = path / "meta" / "0012_TABLE_MASTER_SAP-Fire mapping table.xlsx"
meta_cc = path / "meta" / "0000_TABLE_MASTER_Cost center.xlsx"
meta_pc = path / "meta" / "2023_profit_center_master_KE5X.xlsx"
output_file = path / "fc_output" / "fc_acquisition_existing_assets.csv"


# Read data
df = pd.read_excel(
    input_file,
    sheet_name="Asset ledger 1130",
    header=3,
    usecols="C:U",
    dtype={
        "Asset Clas": str,
        "Cost Cente": str,
        "Asset no": str,
        "Sub No": str,
    },
    parse_dates=["Acquisitio", "ODep.Start"],
)


# Functions to clean column names
def clean_preceding_underscore(column_name):
    return column_name.lstrip("_")


# Apply the cleaning function to all column names
df = df.clean_names()
df.columns = df.columns.map(clean_preceding_underscore)


# Rename columns
df = df.rename(
    columns={
        "asset_clas": "asset_class",
        "cost_cente": "cost_center",
        "description": "asset_description",
        "acquisitio": "acquisition_date",
        "con": "useful_life_year",
        "con_p": "useful_life_month",
        "acqusition": "acquisition",
        "odep_start": "start_of_depr",
    }
)


# Drop columns
df = df.drop(
    columns=["kor", "sie", "total", "book_value", "vendor_name", "p_o", "vendor"]
)


# Filter out missing or zero value
df = df.dropna(subset="asset_class")


# Read meta data
df_meta = pd.read_excel(
    meta_file,
    sheet_name="Sheet1",
    dtype={
        "Asset class": str,
        "FIRE account": str,
    },
).clean_names()

# Rename columns
df_meta = df_meta.rename(
    columns={
        "sap_description": "asset_class_name",
        "fire_account": "financial_statement_item",
        "race_description": "fs_item_description",
    }
)

# select columns
selected_columns = [
    "asset_class",
    "asset_class_name",
    "financial_statement_item",
    "fs_item_description",
    "zv2_account",
    "gl_account",
    "fs_item_sub",
    "fix_var",
    "mv_type",
]
df_meta = df_meta.select(columns=selected_columns)


# Read cost center master data
cc_master = pd.read_excel(
    meta_cc,
    sheet_name="General master",
    dtype={
        "Cctr": str,
    },
).clean_names()

# Rename columns
cc_master = cc_master.rename(
    columns={
        "cctr": "cost_center",
        "validity": "cc_validity",
        "pctr": "profit_center",
    }
)

# select columns
selected_columns = [
    "cost_center",
    "cc_validity",
    "profit_center",
]
cc_master = cc_master.select(columns=selected_columns)


## Read profit center master data
pc_master = pd.read_excel(
    meta_pc,
    sheet_name="Sheet1",
).clean_names()


# select & rename columns
pc_master = pc_master[["profit_ctr", "rec_prctr", "percentage"]].rename(
    columns={"profit_ctr": "profit_center"}
)
# # central function allocation ratio
pc_master = pc_master[pc_master["profit_center"] == "50899-999"]


# Add meta data
df = df.merge(df_meta, how="left", on="asset_class")
df = df.merge(cc_master, how="left", on="cost_center")
df = df.merge(pc_master, how="left", on="profit_center")


# fill missing percentage with 100
df["percentage"].fillna(100, inplace=True)


# Multiply percentage to value columns
df["percentage"] = df["percentage"] * 0.01
value_columns = ["acquisition", "previous", "current"]
df[value_columns] = df[value_columns].mul(df["percentage"], axis=0)


# # Asset Under Construction # #

# filter rows
auc = df[df["asset_class"].isin(["991", "997", "998"])]

# select columns
selected_columns = [
    "asset_class",
    "cost_center",
    "asset_no",
    "sub_no",
    "description",
    "acquisition_date",
    "useful_life_year",
    "acquisition",
    "profit_center",
]
# auc = auc.select(columns=selected_columns)


# Write data
df.to_csv(output_file, index=False)
print("A file is created")
