import re
import pandas as pd
import numpy as np
from pathlib import Path
from janitor import clean_names


# Variables
from common_variable import period_start, period_end


# Path
path = Path(__file__).parent.parent


# Filenames
input_file = path / "output" / "2_fc_acquisition_existing_assets.csv"
master_file = path / "data" / "fc_AUC_list.xlsx"
output_file = path / "output" / "4_fc_depreciation_asset_under_construction.csv"


# Read data
df = pd.read_csv(
    input_file,
    dtype={
        "asset_class": str,
        "cost_center": str,
        "asset_no": str,
        "sub_no": str,
    },
    parse_dates=["acquisition_date", "start_of_depr"],
)
# Filter Asset Under Construction
df = df[df["asset_class"].isin(["991", "997", "998"])]

df_meta = pd.read_excel(
    master_file,
    sheet_name="Manual input",
    skiprows=3,
    dtype={
        "asset_class": str,
        "cost_center": str,
        "asset_no": str,
        "sub_no": str,
        # "input_useful_life_year": int,
    },
    parse_dates=["PPAP"],
)
df_meta = df_meta[["asset_no", "PPAP", "input_useful_life_year"]]


# Join two dataframes
df = df.merge(df_meta, how="left", on="asset_no")


# Asset Under Construction: overwrite existing values if Manual input value is available
df["useful_life_year"] = np.where(
    pd.isna(df["input_useful_life_year"]),
    df["useful_life_year"],
    df["input_useful_life_year"],
)
df["start_of_depr"] = np.where(pd.isna(df["PPAP"]), df["start_of_depr"], df["PPAP"])


# # Business Logic: Monthly deprecation # #


# Dataframe for month end dates
df_month_ends = pd.DataFrame(
    {"month_ends": pd.date_range(period_start, period_end, freq="ME")}
)


# Functions
def calc_monthly_depr(row, period_start, period_end):
    if (
        # To avoid Division by zero error (e.g. Asset under construction)
        row["useful_life_year"]
        == row["useful_life_month"]
        == 0
    ):
        return 0

    # Main calculation logic
    monthly_depr = row["acquisition"] / (
        row["useful_life_year"] * 12 + row["useful_life_month"]
    )
    return monthly_depr


def calc_depr_end(row):
    years = row["useful_life_year"]
    months = row["useful_life_month"]
    row = (
        row["start_of_depr"] + pd.DateOffset(years=years) + pd.DateOffset(months=months)
    )
    return row


def filter_depr_periods(df, period_start):
    depr_periods = (df["depr_start"] < df["month_ends"]) & (
        df["month_ends"] < df["depr_end"]
    )
    # If it is still depreciated even after depr_end date as usage is restarted
    exceptions = (df["depr_end"] < pd.to_datetime(period_start)) & (
        abs(df["current"]) > 0
    )

    df["monthly_depr"] = df["monthly_depr"].where(depr_periods | exceptions, 0)
    return df


# Fill missing values with 0
df["useful_life_year"] = df["useful_life_year"].fillna(0)
df["useful_life_month"] = df["useful_life_month"].fillna(0)


# Create new columns
df["depr_start"] = df["start_of_depr"]
df["depr_end"] = df.apply(calc_depr_end, axis="columns")
df["monthly_depr"] = df.apply(
    calc_monthly_depr, axis="columns", period_start=period_start, period_end=period_end
)


# Cross Join & apply function "filter_depr_periods"
df = df_month_ends.join(df, how="cross")
df = filter_depr_periods(df, period_start)


# Pivot_wider
df["month_ends"] = df["month_ends"].astype(str)
df = pd.pivot(
    df,
    index=[col for col in df.columns if col not in ["month_ends", "monthly_depr"]],
    columns="month_ends",
    values="monthly_depr",
).reset_index()


# New column: Depreciation in current year
pattern = re.compile(r"\d{4}-\d{2}-\d{2}")
value_columns = df.columns[df.columns.str.contains(pattern)].tolist()
df["depr_current"] = df[value_columns].sum(axis="columns")


# Reclassify "Asset Under Construction" to other FS Items
conditions = [
    df["useful_life_year"] > 20,
    (df["useful_life_year"] > 4) & (df["useful_life_year"] <= 20),
    (df["useful_life_year"] > 0) & (df["useful_life_year"] <= 4),
]


fs_item_values = ["122617100", "122622000", "122637000"]
fs_item_description_values = [
    "Buildings and land rights",
    "Technical equipment & machinery",
    "Molds / containers / tooling",
]
gl_account_values = ["K413", "K413", "K432"]
fix_var_values = ["fix", "fix", "var"]


df["financial_statement_item"] = np.select(
    conditions, fs_item_values, default="122632000"
)
df["fs_item_description"] = np.select(
    conditions,
    fs_item_description_values,
    default="Assets under construction and advances to suppliers",
)
df["gl_account"] = np.select(
    conditions,
    gl_account_values,
    default="K413",
)
df["fix_var"] = np.select(
    conditions,
    fix_var_values,
    default="fix",
)


# Write data
df.to_csv(output_file, index=False)
print("A file is created")