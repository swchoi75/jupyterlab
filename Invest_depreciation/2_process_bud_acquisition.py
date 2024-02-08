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


# Filenames
input_file = path / "bud_output" / "bud_monthly_spending_1.csv"
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
)


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
    value_name="spend_fc",
)

df = df.where(df["spend_fc"] != 0, np.nan)  # turn 0 into n/a values
df = df.dropna(subset="spend_fc")  # remove rows with n/a values


# Aggregate data
last_months = df[["sub", "spend_month"]].groupby(["sub"]).last()  # .reset_index()
last_months.rename(columns={"spend_month": "last_spend_month"}, inplace=True)


# New column "Acquisition date" based on last spending months
def str_to_month_ends(series):
    # Convert year_month to datetime with day set to 1st
    series = pd.to_datetime(series, format="%m_%Y")
    # Add one month and subtract one day to get the month end
    series = series + pd.DateOffset(months=1, days=-1)
    return series


s = last_months["last_spend_month"].str.replace("spend_plan", "")
s = str_to_month_ends(s)
last_months["acquisition_date"] = s


# Join two dataframes
df = df.merge(last_months, how="left", on="sub")


# Pivot wider
df = pd.pivot(
    df,
    index=[col for col in df.columns if col not in ["spend_month", "spend_fc"]],
    columns="spend_month",
    values="spend_fc",
).reset_index()


# Write data
df.to_csv(output_file, index=False)
print("Files are created")
