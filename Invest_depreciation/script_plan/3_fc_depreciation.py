import re
import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
try:
    path = Path(__file__).parent.parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent.parent


# Variables
period_start = "2023-01-31"
period_end = "2024-01-01"


# Filenames
input_file = path / "fc_output" / "fc_acquisition_future_assets.csv"
output_file = path / "fc_output" / "fc_depreciation_future_assets.csv"


# Read data
df = pd.read_csv(
    input_file,
    dtype={
        "investment_type": str,
        "financial_statement_item": str,
    },
    parse_dates=["acquisition_date", "PPAP", "start_of_depr"],
)


# Forecast assumptions
df["acquisition"] = df["spend_amt"]


# # Business Logic: Monthly deprecation # #


# Dataframe for month end dates
df_month_ends = pd.DataFrame(
    {"month_ends": pd.date_range(period_start, period_end, freq="ME")}
)


# Functions
def calc_monthly_depr(row):
    monthly_depr = row["acquisition"] / (row["useful_life_year"] * 12)
    return monthly_depr


def calc_depr_end(row):
    years = row["useful_life_year"]
    row = row["start_of_depr"] + pd.DateOffset(years=years)
    return row


def filter_depr_periods(df):
    depr_periods = (df["depr_start"] < df["month_ends"]) & (
        df["month_ends"] < df["depr_end"]
    )
    df["monthly_depr"] = df["monthly_depr"].where(depr_periods, 0)
    return df


# Create new columns
df["depr_start"] = df["start_of_depr"]
df["depr_end"] = df.apply(calc_depr_end, axis="columns")
df["monthly_depr"] = df.apply(calc_monthly_depr, axis="columns")


# Cross Join & apply function "filter_depr_periods"
df = df_month_ends.join(df, how="cross")
df = filter_depr_periods(df)


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
