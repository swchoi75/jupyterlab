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
]
df_meta = df_meta.select(columns=selected_columns)


# Join two dataframes
df = df.merge(df_meta, how="left", on="asset_class")


# # Business Logic: Monthly deprecation # #

# Dataframe for month end dates
df_month_ends = pd.DataFrame(
    {"month_ends": pd.date_range(period_start, period_end, freq="M")}
)


# Functions
def calc_monthly_depr(row):
    if (
        # To avoid Division by zero error (e.g. Asset under construction)
        row["useful_life_year"] == row["useful_life_month"] == 0
        # Depreciation is finished already    
        or row["acquisition"] + row["previous"] == 0
    ):
        return 0
    
    elif (
        # Yearly Depreciation is bigger than expected due to additional investment
        abs(row["current"])
        > abs(
            row["acquisition"]
            / ((row["useful_life_year"] + row["useful_life_month"] / 12))
        )
        # ICO asset transfer
        or "(ICO)" in row["description"]
    ):
        return row["current"] * -1 / 12
    
    # If it is still depreciated even after depr_end date as usage is restarted
    elif row["depr_end"] < pd.to_datetime(period_start) and abs(row["current"]) > 0:
        return row["current"] * -1 / 12
    
    # Main calculation logic
    else:
        return row["acquisition"] / (
            row["useful_life_year"] * 12 + row["useful_life_month"]
        )


def calc_depr_end(row):
    years = row["useful_life_year"]
    months = row["useful_life_month"]
    row = (
        row["start_of_depr"] + pd.DateOffset(years=years) + pd.DateOffset(months=months)
    )
    return row


def filter_depr_periods(row):
    if (
        # If month_ends are between depr_start and depr_end
        (row["depr_start"] < row["month_ends"] < row["depr_end"])
        or
        # If it is still depreciated even after depr_end date as usage is restarted
        (row["depr_end"] < pd.to_datetime(period_start) and abs(row["current"]) > 0)
    ):
        return row["monthly_depr"]
    else:
        return 0


# Fill missing values
df["useful_life_year"].fillna(0, inplace=True)  # fill missing values with 0
df["useful_life_month"].fillna(0, inplace=True)  # fill missing values with 0


# Create new columns
df["depr_start"] = df["start_of_depr"]
df["depr_end"] = df.apply(calc_depr_end, axis="columns")
df["monthly_depr"] = df.apply(calc_monthly_depr, axis="columns")


# Cross Join & Filter deprciation periods
df = df_month_ends.join(df, how="cross")
df["monthly_depr"] = df.apply(filter_depr_periods, axis="columns")


# Pivot_wider
df["month_ends"] = df["month_ends"].astype(str)
df = pd.pivot(
    df,
    index=[col for col in df.columns if col not in ["month_ends", "monthly_depr"]],
    columns="month_ends",
    values="monthly_depr",
).reset_index()


# Write data
df.to_csv(output_file, index=False)
print("A file is created")
