import pandas as pd
import numpy as np
from pathlib import Path


# Path
try:
    path = Path(__file__).parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent


# Filenames
bom_file = path / "data" / "BOM_price.csv"
fx_vt_file = path / "data" / "fx_rates_VT.csv"
fx_hmg_file = path / "data" / "fx_rates_HMG.csv"

output_file = path / "output" / "bom_price_fx rate_delta.csv"


# Read data
bom = pd.read_csv(bom_file)
fx_vt = pd.read_csv(fx_vt_file)
fx_hmg = pd.read_csv(fx_hmg_file)


# Functions
def select_fx_vt(df):
    # Filter rows
    df = df[df["fx_type"] == "ytd"]
    # Select columns
    df = df[["cur", "year", "month", "fx_rates_VT"]]
    return df


def select_fx_hmg(df):
    # Select columns
    df = df[["cur", "year", "month", "fx_rates_HMG"]]
    return df


def join_dataframes(base, df1, df2):
    df = base.merge(df1, how="left", on=["cur", "year"])
    df = df.merge(df2, how="left", on=["cur", "year", "month"])
    return df


def krw_month_table():
    currencies = ["KRW"] * 12
    months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
    df = pd.DataFrame({"cur": currencies, "period": months})
    return df


def calculate_delta(df):
    # Clculate delta price cause by fx rate difference
    df["fx_rate_diff"] = df["fx_rates_VT"] - df["fx_rates_HMG"]
    df["delta_price"] = df["amount_doc_"] * df["fx_rate_diff"]
    return df


# Process data
# Join 3 datatables
fx_vt = select_fx_vt(fx_vt)
fx_hmg = select_fx_hmg(fx_hmg)
df = join_dataframes(bom, fx_vt, fx_hmg)


# Add KRW for 12 months
krw_month = krw_month_table()
df = df.merge(krw_month, how="left", on="cur")
df["month"] = np.where(df["cur"] == "KRW", df["period"], df["month"])
df = df.drop(columns=["period"])


# Set 1 KRW = 1 KRW
df["fx_rates_VT"] = df["fx_rates_VT"].where(df["cur"] != "KRW", 1)
df["fx_rates_HMG"] = df["fx_rates_HMG"].where(df["cur"] != "KRW", 1)

df = df.pipe(calculate_delta)


# Write data
df.to_csv(output_file, index=False)
print("A file is created")
