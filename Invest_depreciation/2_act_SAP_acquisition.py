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
meta_poc = path / "meta" / "POC_for_GPA.xlsx"
output_file = path / "fc_output" / "fc_acquisition_existing_assets.csv"


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
cc_master = pd.read_excel(meta_cc, sheet_name="General master", dtype=str).clean_names()

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


# Read POC (Plant Outlet Combination) master data
poc_master = pd.read_excel(meta_poc, dtype=str)
poc_master["plant_name"] = poc_master["plant_name"].str.replace("ICH ", "")
poc_master = poc_master.rename(
    columns={
        "plant_name": "location_sender",
        "outlet_name": "outlet_sender",
    }
)


# Add meta data
df = (
    df.merge(df_meta, how="left", on="asset_class")
    .merge(cc_master, how="left", on="cost_center")
    .merge(poc_master, how="left", on="profit_center")
    # .merge(pc_master, how="left", on="profit_center")
)


# Write data
df.to_csv(output_file, index=False)
print("A file is created")
