import re
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
meta_2 = path / "meta" / "0000_TABLE_MASTER_Cost center.xlsx"
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
    meta_2,
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
        "pctr": "profit_ctr",
    }
)

# select columns
selected_columns = [
    "cost_center",
    "cc_validity",
    "profit_ctr",
]
cc_master = cc_master.select(columns=selected_columns)


# Join dataframes
df = df.merge(df_meta, how="left", on="asset_class")
df = df.merge(cc_master, how="left", on="cost_center")


# # Business Logic: Monthly deprecation # #


# Dataframe for month end dates
df_month_ends = pd.DataFrame(
    {"month_ends": pd.date_range(period_start, period_end, freq="M")}
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
    alterative_monthly_depr = row["current"] * -1 / 12

    # If the depreciation starts after period_start
    if pd.to_datetime(period_start) < row["depr_start"]:
        date1 = pd.to_datetime(period_start)
        date2 = row["depr_start"]
        months_difference = (date2.year - date1.year) * 12 + date2.month - date1.month
        return row["current"] * -1 / (12 - months_difference)

    # If the depreciation ends between period_start and period_end
    elif pd.to_datetime(period_start) < row["depr_end"] and row[
        "depr_end"
    ] < pd.to_datetime(period_end):
        date1 = row["depr_end"]
        date2 = pd.to_datetime(period_end)
        months_difference = (date2.year - date1.year) * 12 + date2.month - date1.month
        return row["current"] * -1 / (12 - months_difference)

    # If it is still depreciated even after depr_end date because usage is restarted
    elif row["depr_end"] < pd.to_datetime(period_start) and abs(row["current"]) > 0:
        return alterative_monthly_depr

    # Yearly Depreciation is not equal to the expected value
    # due to additional investment, ICO asset transfer, etc
    elif abs(row["current"] + (monthly_depr * 12)) > 1:
        return alterative_monthly_depr

    else:
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
df["useful_life_year"].fillna(0, inplace=True)
df["useful_life_month"].fillna(0, inplace=True)


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


# Write data
df.to_csv(output_file, index=False)
print("A file is created")
