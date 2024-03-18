import pandas as pd
import numpy as np
from pathlib import Path
from janitor import clean_names


# Path
try:
    path = Path(__file__).parent.parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent.parent


# Variables
spending_total_col = "spend_plan_2024"
current_year = "2024"
current_year_end = pd.to_datetime(current_year + "-12-31")
actual_month_end = "2023-07-31"


# Filenames
input_file = path / "plan_output" / "plan_monthly_spending.csv"
master_file = path / "plan_data" / "plan_GPA_master.xlsx"
output_file = path / "plan_output" / "plan_acquisition_future_assets.csv"


# Read data
df = pd.read_csv(
    input_file,
    dtype={
        "investment_type": str,
        "financial_statement_item": str,
    },
)
df = df.drop(columns=spending_total_col)

df_meta = pd.read_excel(
    master_file,
    sheet_name="Manual input",
    skiprows=3,
    dtype={
        "input_cost_center": str,
        # "input_useful_life_year": int,
    },
    parse_dates=["PPAP"],
)
df_meta = df_meta[
    ["sub", "input_gl_account", "input_cost_center", "PPAP", "input_useful_life_year"]
]


# Join two dataframes
df = df.merge(df_meta, how="left", on="sub")


# # Business Logic: Get the last spending months
# Melt the dataframe
value_columns = df.columns[df.columns.str.contains("spend")].tolist()
key_columns = [col for col in df.columns if col not in value_columns]

df = df.melt(
    id_vars=key_columns,
    value_vars=value_columns,
    var_name="spend_month",
    value_name="spend_amt",
)

df = df.where(df["spend_amt"] != 0, np.nan)  # turn 0 into n/a values
df = df.dropna(subset="spend_amt")  # remove rows with n/a values


# New column: "Acquisition date" based on last spending months
def str_to_month_ends(series):
    # Convert year_month to datetime with day set to 1st
    series = pd.to_datetime(series, format="%m_%Y")
    # Add one month and subtract one day to get the month end
    series = series + pd.DateOffset(months=1, days=-1)
    return series


s = df["spend_month"].str.replace("spend_plan", "")
s = str_to_month_ends(s)
df["acquisition_date"] = s


# New column: "Start of Depreciation"
df["start_of_depr"] = np.where(pd.isna(df["PPAP"]), df["acquisition_date"], df["PPAP"])


# New column: "asset category" based on the column "start_of_depr"
df["asset_category"] = np.where(
    df["start_of_depr"] < pd.to_datetime(actual_month_end), "past fc", "future fc"
)


# New column "useful_life_year" with default values
def add_useful_life_year(row):
    if row["fix_var"] == "fix":
        return 8
    elif row["fix_var"] == "var":
        return 4


df["useful_life_year"] = df.apply(add_useful_life_year, axis="columns")


# Overwrite existing values if Manual input value is available
df["gl_account"] = np.where(
    pd.isna(df["input_gl_account"]), df["gl_account"], df["input_gl_account"]
)
df["cost_center"] = np.where(
    pd.isna(df["input_cost_center"]), df["cost_center"], df["input_cost_center"]
)
df["useful_life_year"] = np.where(
    pd.isna(df["input_useful_life_year"]),
    df["useful_life_year"],
    df["input_useful_life_year"],
)


# Business Logic: Asset Under Construction if PPAP is in the future year #
def reclassfy_fs_item(df):
    ppap_future_year = df["PPAP"] > current_year_end

    df["financial_statement_item"] = df["financial_statement_item"].where(
        ~ppap_future_year, "122632000"
    )

    df["fs_item_description"] = df["fs_item_description"].where(
        ~ppap_future_year, "Assets under construction and advances to suppliers"
    )
    return df


df = reclassfy_fs_item(df)


# Write data
df.to_csv(output_file, index=False)
print("A file is created")
