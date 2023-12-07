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
sales_pn_file = path / "data" / "Sales high runner PN.csv"
bom_file = path / "data" / "BOM_price.csv"
bom_fx_file = path / "data" / "Sales high runner PN_survey with PSM.xlsx"
fx_bud_file = path / "data" / "fx_rates_VT_budget.csv"
fx_act_file = path / "data" / "fx_rates_VT_actual.csv"
fx_hmg_file = path / "data" / "fx_rates_HMG.csv"

output_file = path / "output" / "bom_price_fx rate_delta.csv"


# Read data
sales_pn = pd.read_csv(sales_pn_file)
bom = pd.read_csv(bom_file)
fx_project = pd.read_excel(
    bom_fx_file, sheet_name="PSM entry", skiprows=3
).clean_names()
fx_bud = pd.read_csv(fx_bud_file)
fx_act = pd.read_csv(fx_act_file)
fx_hmg = pd.read_csv(fx_hmg_file)


# Functions
def process_sales_pn(df):
    # select columns
    df = df[["product", "product_hierarchy"]]
    # remove duplicates items per year
    df = df.drop_duplicates()
    return df


def process_fx_project(df):
    # convert into date type
    df["sop_year_month"] = pd.to_datetime(df["sop_year_month"], format="%Y/%m")
    # select columns
    df = df[["product", "quotation_year", "sop_year_month"]]
    return df


def process_fx_act(df):
    # Filter rows
    df = df[df["fx_type"] == "ytd"]
    # Select columns
    df = df[["cur", "year", "month", "fx_act"]]
    return df


def process_fx_hmg(df):
    # Combine 'year' and 'month' columns into a date column
    df["year_month"] = pd.to_datetime(df[["year", "month"]].assign(day=1))
    # Select columns
    df = df[["cur", "year_month", "fx_HMG"]]
    return df


def krw_month_table():
    currencies = ["KRW"] * 12
    months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
    df = pd.DataFrame({"cur": currencies, "period": months})
    return df


def calculate_delta(df):
    # Clculate delta price cause by fx rate difference
    df["fx_diff"] = df["fx_act"] - df["fx_HMG"]
    df["delta_price"] = df["amount_doc_"] * df["fx_diff"]
    return df


# Process data
# Pre-processing
sales_pn = process_sales_pn(sales_pn)
fx_project = process_fx_project(fx_project)
fx_act = process_fx_act(fx_act)
fx_hmg = process_fx_hmg(fx_hmg)


# Join dataframes: bom, sales_pn, fx_project, fx_bud
df = pd.merge(bom, sales_pn, how="left", on="product")
df = df.merge(
    fx_project,
    how="inner",  # dropna=True to exclude GM and KG Mobility
    on="product"
)

df = df.merge(fx_bud, how="left", on=["quotation_year", "cur"])


# Join dataframes: fx_hmg and fx_act
df = df.merge(
    fx_hmg,
    how="left",
    left_on=["cur", "sop_year_month"],
    right_on=["cur", "year_month"],
).drop(columns="year_month")

df = df.merge(fx_act, how="left", on=["cur", "year"])


# Add KRW for 12 months
krw_month = krw_month_table()
df = df.merge(krw_month, how="left", on="cur")
df["month"] = np.where(df["cur"] == "KRW", df["period"], df["month"])
df = df.drop(columns=["period"])


# Set 1 KRW = 1 KRW
df["fx_bud"] = df["fx_bud"].where(df["cur"] != "KRW", 1)
df["fx_act"] = df["fx_act"].where(df["cur"] != "KRW", 1)
df["fx_HMG"] = df["fx_HMG"].where(df["cur"] != "KRW", 1)

df = df.pipe(calculate_delta)


# Write data
df.to_csv(output_file, index=False)
print("A file is created")
