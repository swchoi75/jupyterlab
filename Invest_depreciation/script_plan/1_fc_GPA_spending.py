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
GPA_version = "v380"
spending_total_col = "spend_fc_2023"
current_year = "2023"
current_year_end = pd.to_datetime(current_year + "-12-31")


# Filenames
input_file = path / "fc_data" / "2023-11_GPA_WMS - All data report_FC.xlsx"
meta_file = path / "meta" / "category_of_investment.xlsx"
meta_cc = path / "meta" / "cost_centers.csv"
meta_poc = path / "meta" / "POC_for_GPA.xlsx"
output_file = path / "fc_output" / "fc_monthly_spending.csv"


# Read data
df = pd.read_excel(
    input_file,
    sheet_name="Sheet1",
    dtype={"Investment type": str},
)


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
        "unnamed_20": "master_description",
        "unnamed_22": "sub_description",
    }
)


# Remove the GPA version prefix from each column name
df.columns = df.columns.str.replace(GPA_version, "")


# Select key columns
key_columns = [
    "location_sender",
    "outlet_sender",
    "category_of_investment",
    "category_of_invest_historic",
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


# Create new column
df["basic_or_project"] = np.where(
    df["master_description"].str.contains("Basic", case=False),
    "basic",
    "project",
)


# Read meta data
def handle_poc_master(filename):
    # read data
    df = pd.read_excel(filename, dtype=str)
    # string manipulation
    df["plant_name"] = df["plant_name"].str.replace("ICH ", "")
    # rename columns
    df = df.rename(
        columns={
            "plant_name": "location_sender",
            "outlet_name": "outlet_sender",
        }
    )
    # select columns
    df = df[["location_sender", "outlet_sender", "profit_center"]]

    return df


df_meta_coi = pd.read_excel(
    meta_file, sheet_name="Sheet1", usecols="A:H", dtype=str
).dropna()

cc_master = pd.read_csv(meta_cc, dtype=str)
poc_master = handle_poc_master(meta_poc)


# Add meta data
df = (
    df.merge(df_meta_coi, how="left", on="category_of_investment")
    .merge(cc_master, how="left", on="sub")
    .merge(poc_master, how="left", on=["location_sender", "outlet_sender"])
)


# Write data
df.to_csv(output_file, index=False)
print("A file is created")
