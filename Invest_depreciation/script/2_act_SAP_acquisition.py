import pandas as pd
from pathlib import Path
from janitor import clean_names


# Variables
from common_variable import asset_filename


# Path
try:
    path = Path(__file__).parent.parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent.parent


# Filenames
input_file = path / "data" / asset_filename
meta_file = path / "meta" / "0012_TABLE_MASTER_SAP-Fire mapping table.xlsx"
meta_cc = path / "meta" / "0000_TABLE_MASTER_Cost center.xlsx"
meta_poc = path / "meta" / "POC_for_GPA.xlsx"
meta_prj = path / "meta" / "project_for_assets.csv"
meta_gpa = path / "data" / "920 GPA Register of Subs.xlsx"
output_file = path / "output" / "2_fc_acquisition_existing_assets.csv"


# Read data
df = pd.read_excel(
    input_file,
    header=3,
    dtype={
        "Asset Clas": str,
        "Cost Cente": str,
        "Asset no": str,
        "Sub No": str,
    },
    parse_dates=["Acquisitio", "ODep.Start"],
)
df = df.drop(df.columns[:2], axis="columns")  # Drop first two columns, which are empty


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
df_meta = pd.read_excel(meta_file, sheet_name="Sheet1", dtype=str).clean_names()

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
    "fs_item_sub",
    "zv2_account",
    "gl_account",
    "gl_account_description",
    "fix_var",
    "mv_type",
]
df_meta = df_meta.select(columns=selected_columns)


# Read cost center master data
def read_cc_master(filename):
    # read data
    df = pd.read_excel(filename, sheet_name="General master", dtype=str).clean_names()
    # Rename columns
    df = df.rename(
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
    df = df.select(columns=selected_columns)

    return df


cc_master = read_cc_master(meta_cc)


# Read POC (Plant Outlet Combination) master data
def read_poc_master(filename):
    # read data
    df = pd.read_excel(filename, dtype=str)
    # string manipulation
    df["plant_name"] = df["plant_name"].str.replace("ICH ", "")
    # rename columns
    df = df.rename(
        columns={
            "plant_name": "location_sender",
            "outlet_name": "outlet_sender",
        }
    )
    # select columns
    df = df[["location_sender", "outlet_sender", "profit_center"]]
    # drop duplicates
    df = df.drop_duplicates(subset="profit_center")

    return df


poc_master = read_poc_master(meta_poc)


# Read project info for assets
def read_prj_master(filename):
    # read data
    df = pd.read_csv(filename, dtype=str)
    # select columns
    df = df[["sub", "wbs_element", "asset_no", "sub_no"]]

    return df


prj_master = read_prj_master(meta_prj)


# Read GPA master to link to asset list
def read_gpa_master(filename):
    # read data
    df = pd.read_excel(filename, sheet_name="Sheet1", dtype=str).clean_names()
    # rename columns
    df = df.rename(
        columns={
            "unnamed_4": "master_description",
            "unnamed_9": "sub_description",
        }
    )
    # select columns
    df = df[["master", "master_description", "sub", "sub_description"]]

    return df


gpa_master = read_gpa_master(meta_gpa)


# Add meta data
df = (
    df.merge(df_meta, how="left", on="asset_class")
    .merge(cc_master, how="left", on="cost_center")
    .merge(poc_master, how="left", on="profit_center")
    .merge(prj_master, how="left", on=["asset_no", "sub_no"])
    .merge(gpa_master, how="left", on="sub")
)


# Write data
df.to_csv(output_file, index=False)
print("A file is created")
