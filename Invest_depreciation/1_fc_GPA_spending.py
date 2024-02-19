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
GPA_version = "v380"
spending_total_col = "spend_fc_2023"
current_year = "2023"
current_year_end = pd.to_datetime(current_year + "-12-31")


# Filenames
input_file = path / "fc_data" / "2023-11_GPA_WMS - All data report_FC.xlsx"
meta_file = path / "meta" / "category_of_investment.xlsx"
master_file = path / "fc_output" / "fc_GPA_master.csv"
output_file = path / "fc_output" / "fc_monthly_spending.csv"


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

df_meta = pd.read_excel(
    meta_file,
    sheet_name="Sheet1",
    usecols="A:G",
    dtype={"financial_statement_item": str},
).dropna()


# Functions to clean column names
def clean_new_lines(column_name):
    return column_name.replace("\n", "")


def clean_trailing_underscore(column_name):
    return column_name.rstrip("_")


# Apply the cleaning function to all column names
df.columns = df.columns.map(clean_new_lines)
df = df.clean_names()
df.columns = df.columns.map(clean_trailing_underscore)


# Select columns
key_columns = [
    "outlet_sender",
    "outlet_receiver",
    "categorie_of_investm",
    "category_of_invest_historic",
    "fire_outlet",
    "fire_outlet_ny_receiver",
    "fire_plant_receiver",
    "location_receiver",
    "investment_type",
    "status",
    "master",
    "unnamed_28",
    "sub",
    "unnamed_30",
]


# Select value columns and mutiply by 1000
value_columns = df.columns[df.columns.str.contains("spend")].tolist()
df[value_columns] = df[value_columns].mul(1000, axis="columns")


# Select key columns and value columns
df = df[key_columns + value_columns]


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


# Filter out missing or zero value
df = df.dropna(subset="master")
df = df.dropna(subset=spending_total_col)
df = df[df[spending_total_col] != 0]


# Join two dataframes
df = df.merge(df_meta, how="left", on="category_of_investment")


# Create new column
df["basic_or_project"] = np.where(
    df["master_description"].str.contains("Basic", case=False),
    "basic",
    "project",
)


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
]

df_master = (
    df[selected_columns + [spending_total_col]]
    .groupby(selected_columns)
    .agg({spending_total_col: "sum"})
    .reset_index()
)


# Write data
df_master.to_csv(master_file, index=False)
df.to_csv(output_file, index=False)
print("Files are created")
