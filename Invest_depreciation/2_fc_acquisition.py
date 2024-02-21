import pandas as pd
import numpy as np
from pathlib import Path
from janitor import clean_names


# Path
try:
    path = Path(__file__).parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent


# Variables
spending_total_col = "spend_fc_2023"
current_year = "2023"
current_year_end = pd.to_datetime(current_year + "-12-31")
actual_month_end = "2023-11-30"


# Filenames
input_file = path / "fc_output" / "fc_monthly_spending.csv"
meta_file = path / "meta" / "fc_GPA_master.xlsx"
output_file = path / "fc_output" / "fc_acquisition_future_assets.csv"


# Read data
df = pd.read_csv(
    input_file,
    dtype={
        "outlet_receiver": str,
        "fire_outlet": str,
        "fire_outlet_ny_receiver": str,
        "fire_plant_receiver": str,
        "investment_type": str,
        "financial_statement_item": str,
    },
)
df = df.drop(columns=spending_total_col)

df_meta = pd.read_excel(
    meta_file,
    sheet_name="Manual input",
    skiprows=3,
    parse_dates=["PPAP"],
)
df_meta = df_meta[
    ["sub", "new_gl_account", "new_cost_center", "PPAP", "useful_life_year"]
]


# Join two dataframes
df = df.merge(df_meta, how="left", on="sub")


# # Business Logic: Get the spending months
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


# New column: "Acquisition date" based on the spending months
def str_to_month_ends(series):
    # Convert year_month to datetime with day set to 1st
    series = pd.to_datetime(series, format="%Y_%m")
    # Add one month and subtract one day to get the month end
    series = series + pd.DateOffset(months=1, days=-1)
    return series


s = df["spend_month"].str.replace("spend_fc_", "")
s = str_to_month_ends(s)
df["acquisition_date"] = s


# New column: "Start of Depreciation"
df["start_of_depr"] = np.where(pd.isna(df["PPAP"]), df["acquisition_date"], df["PPAP"])


# New column: "asset category" based on the column "start_of_depr"
df["asset_category"] = np.where(
    df["start_of_depr"] < pd.to_datetime(actual_month_end), "past fc", "future fc"
)


# Overwrite existing values if Manual input value is available
df["gl_account"] = np.where(
    pd.isna(df["new_gl_account"]), df["gl_account"], df["new_gl_account"]
)
df["cost_center"] = np.where(
    pd.isna(df["new_cost_center"]), df["cost_center"], df["new_cost_center"]
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
