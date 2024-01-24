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
GPA_version = "v379"
spending_total_col = "plan_spend_2024"
current_year = "2024"
current_year_end = pd.to_datetime(current_year + "-12-31")


# Filenames
input_file = path / "bud_data" / "2023_GPA_WMS - All data report_Budget.xlsx"
meta_file = path / "meta" / "category_of_investment.xlsx"
ppap_file = path / "meta" / "PPAP.xlsx"
output_file = path / "bud_output" / "bud_monthly_spending.csv"


# Read data
df = pd.read_excel(
    input_file,
    sheet_name="Sheet1",
    dtype={
        "Fire-Outlet": str,
        "FiRe Outlet NY(Receiver)": str,
        "FiRe plant(Receiver)": str,
        "Investment type": str,
    },
)

df_meta = pd.read_excel(
    meta_file,
    sheet_name="Sheet1",
    usecols="A:D",
    dtype={"financial_statement_item": str},
).dropna()


df_ppap = pd.read_excel(
    ppap_file,
    header=2,
    sheet_name="PPAP",
    usecols="G,I",
    parse_dates=["PPAP"],
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
df.columns


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
    "unnamed_29",
    "sub",
    "unnamed_31",
]

value_columns = df.columns[df.columns.str.contains("spend")].tolist()

df = df[key_columns + value_columns]


# Rename columns
df = df.rename(
    columns={
        "categorie_of_investm": "category_of_investment",
        "unnamed_29": "master_description",
        "unnamed_31": "sub_description",
    }
)
# Remove the GPA version prefix from each column name
df.columns = df.columns.str.replace(GPA_version, '')


# Filter out missing or zero value
df = df.dropna(subset="master")
df = df.dropna(subset=spending_total_col)
df = df[df[spending_total_col] != 0]


# Join two dataframes
df = df.merge(df_meta, how="left", on="category_of_investment")
df = df.merge(df_ppap, how="left", on="sub")


# Asset Under Construction if PPAP is in the future year
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
