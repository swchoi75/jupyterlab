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
GPA_version = "v379"
spending_total_col = "plan_spend_2024"
current_year = "2024"
current_year_end = pd.to_datetime(current_year + "-12-31")


# Filenames
input_file = path / "plan_data" / "2023_GPA_WMS - All data report_Budget.xlsx"
meta_file = path / "meta" / "category_of_investment.xlsx"
meta_file_2 = path / "meta" / "cost_centers.csv"
master_file = path / "plan_output" / "plan_GPA_master.csv"
output_file = path / "plan_output" / "plan_monthly_spending.csv"


# Read data
df = pd.read_excel(
    input_file,
    sheet_name="Sheet1",
    dtype={
        "Outlet(Receiver)": str,
        "Fire-Outlet": str,
        "FiRe Outlet NY(Receiver)": str,
        "FiRe plant(Receiver)": str,
        "Investment type": str,
    },
)

df_meta_coi = pd.read_excel(
    meta_file,
    sheet_name="Sheet1",
    usecols="A:G",
    dtype={"financial_statement_item": str},
).dropna()

df_meta_cc = pd.read_csv(meta_file_2)


# Functions to clean column names
def clean_new_lines(column_name):
    return column_name.replace("\n", "")


def clean_trailing_underscore(column_name):
    return column_name.rstrip("_")


# Apply the cleaning function to all column names
df.columns = df.columns.map(clean_new_lines)
df = df.clean_names()
df.columns = df.columns.map(clean_trailing_underscore)


# Rename columns
df = df.rename(
    columns={
        "categorie_of_investm": "category_of_investment",
        "unnamed_28": "master_description",
        "unnamed_30": "sub_description",
    }
)
# Remove the GPA version prefix from each column name
df.columns = df.columns.str.replace(GPA_version, "")


# Select columns
key_columns = [
    "outlet_sender",
    "outlet_receiver",
    "category_of_investment",
    "category_of_invest_historic",
    "fire_outlet",
    "fire_outlet_ny_receiver",
    "fire_plant_receiver",
    "location_receiver",
    "investment_type",
    "status",
    "master",
    "master_description",
    "sub",
    "sub_description",
]


# Select value columns and mutiply by 1000
value_columns = df.columns[df.columns.str.contains("spend")].tolist()
df[value_columns] = df[value_columns].mul(1000, axis="columns")


# Select key columns and value columns
df = df[key_columns + value_columns]


# Filter out missing or zero value
df = df.dropna(subset="master")
df = df.dropna(subset=spending_total_col)
df = df[df[spending_total_col] != 0]


# Add meta data
df = df.merge(df_meta_coi, how="left", on="category_of_investment").merge(
    df_meta_cc, how="left", on="sub"
)


# Create new column
df["basic_or_project"] = np.where(
    df["master_description"].str.contains("Basic", case=False),
    "basic",
    "project",
)


# # Business Logic: Aggregate CDF in GPA

# Split dataframe
df_cdf = df[df["outlet_sender"] == "PT - Quality"]
df_rest = df[df["outlet_sender"] != "PT - Quality"]

# Select columns
columns_to_drop = ["outlet_receiver", "fire_outlet", "fire_outlet_ny_receiver"]
key_columns_2 = [x for x in key_columns if x not in columns_to_drop]

# Aggregate CDF outlet
df_cdf = (
    df_cdf[key_columns_2 + value_columns]
    .groupby(key_columns_2)
    .agg("sum")
    .reset_index()
)

# Manual input of CDF outlet 7210
cdf_outlet = "7210"
df_cdf["outlet_receiver"] = cdf_outlet
df_cdf["fire_outlet"] = cdf_outlet
df_cdf["fire_outlet_ny_receiver"] = cdf_outlet

# Merge dataframes
df = pd.concat([df_rest, df_cdf])


# GPA master
selected_columns = [
    "outlet_sender",
    "status",
    "master",
    "master_description",
    "sub",
    "sub_description",
    "category_of_investment",
    "category_description",
    "gl_account",
    "gl_account_description",
    "basic_or_project",
    "cost_center",
]

df_master = (
    df[selected_columns + [spending_total_col]]
    .groupby(selected_columns, dropna=False)
    .agg({spending_total_col: "sum"})
    .reset_index()
)


# Write data
df_master.to_csv(master_file, index=False)
df.to_csv(output_file, index=False)
print("Files are created")
