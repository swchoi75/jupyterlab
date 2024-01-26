import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
try:
    path = Path(__file__).parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent


# Variables
period_start = "2023-01-31"
period_end = "2024-01-01"


# Filenames
input_file = path / "fc_data" / "2023-11_Asset History Leger_20231130.xlsx"
meta_file = path / "meta" / "0012_TABLE_MASTER_SAP-Fire mapping table.xlsx"
output_file = path / "fc_output" / "fc_depreciation_existing_assets.csv"


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
        "acquisitio": "acquisition_date",
        "con": "useful_life",
        "acqusition": "acquisition",
        "odep_start": "start_of_depr",
    }
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
]
df_meta = df_meta.select(columns=selected_columns)


# Business Logic: Monthly deprecation #
df_month_ends = pd.DataFrame(
    {"month_ends": pd.date_range(period_start, period_end, freq="M")}
)


def calc_depr_end(row):
    years = row["useful_life"]
    row = row["acquisition_date"] + pd.DateOffset(years=years)
    return row


def filter_depr_periods(df):
    depr_periods = (df["depr_start"] < df["month_ends"]) & (
        df["month_ends"] < df["depr_end"]
    )
    df["monthly_depr"] = df["monthly_depr"].where(depr_periods, 0)
    return df


df = df.assign(
    monthly_depr=lambda row: row["acquisition"] / row["useful_life"] / 12,
    depr_start=lambda row: row["acquisition_date"],
)
df["useful_life"].fillna(0, inplace=True)  # fill missing values with 0
df["depr_end"] = df.apply(calc_depr_end, axis="columns")


df = df_month_ends.join(df, how="cross")
df = filter_depr_periods(df)


# Join two dataframes
df = df.merge(df_meta, how="left", on="asset_class")


# Write data
df.to_csv(output_file, index=False)
print("A file is created")
