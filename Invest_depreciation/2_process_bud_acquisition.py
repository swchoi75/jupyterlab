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
spending_total_col = "plan_spend_2024"
current_year = "2024"
current_year_end = pd.to_datetime(current_year + "-12-31")
actual_month_end = "2023-07-31"


# Filenames
input_file = path / "bud_output" / "bud_monthly_spending.csv"
meta_file = path / "meta" / "bud_GPA_master.xlsx"
output_file = path / "bud_output" / "bud_acquisition_future_assets.csv"


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

df_meta = pd.read_excel(
    meta_file,
    sheet_name="Manual input",
    skiprows=3,
    usecols="E, M:P",
    parse_dates=["PPAP"],
)


# Join two dataframes
df = df.merge(df_meta, how="left", on="sub")


# # Business Logic: Get the last spending months
# Melt the dataframe
value_columns = df.columns[df.columns.str.contains("spend")].tolist()
value_columns = [item for item in value_columns if item != spending_total_col]
key_columns = [col for col in df.columns if col not in value_columns]

df = df.melt(
    id_vars=key_columns,
    value_vars=value_columns,
    var_name="spend_month",
    value_name="spend_amt",
)

df = df.where(df["spend_amt"] != 0, np.nan)  # turn 0 into n/a values
df = df.dropna(subset="spend_amt")  # remove rows with n/a values


# Aggregate data
last_months = df[["sub", "spend_month"]].groupby(["sub"]).last()  # .reset_index()
last_months.rename(columns={"spend_month": "last_month"}, inplace=True)


# New column: "Acquisition date" based on last spending months
def str_to_month_ends(series):
    # Convert year_month to datetime with day set to 1st
    series = pd.to_datetime(series, format="%m_%Y")
    # Add one month and subtract one day to get the month end
    series = series + pd.DateOffset(months=1, days=-1)
    return series


s = last_months["last_month"].str.replace("spend_plan", "")
s = str_to_month_ends(s)
last_months["acquisition_date"] = s


# New column: "Start of Depreciation"
df = df.merge(last_months, how="left", on="sub")
df["start_of_depr"] = np.where(pd.isna(df["PPAP"]), df["acquisition_date"], df["PPAP"])


# New column: "category" based on the temporary column "month_ends"
s = df["spend_month"].str.replace("spend_plan", "")
s = str_to_month_ends(s)
df["month_ends"] = s

df["category"] = np.where(
    df["month_ends"] <= pd.to_datetime(actual_month_end), "past", "future"
)

df = df.drop(columns=["month_ends"])


# Pivot wider
df = pd.pivot(
    df,
    index=[col for col in df.columns if col not in ["spend_month", "spend_amt"]],
    columns="spend_month",
    values="spend_amt",
).reset_index()


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
print("Files are created")
