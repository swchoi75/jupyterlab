import pandas as pd
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
def filter_fx_vt(df):
    # Filter rows
    df = df[df["fx_type"] == "ytd"]
    df = df[df["month"] == 12]
    # Select columns
    df = df[["cur", "year", "fx_rates_VT"]]
    return df


def filter_fx_hmg(df):
    # Filter rows
    df = df[df["month"] == 12]
    # Select columns
    df = df[["cur", "year", "fx_rates_HMG"]]
    return df


def join_dataframes(base, df1, df2):
    df = base.merge(df1, how="left", on=["cur", "year"])
    df = df.merge(df2, how="left", on=["cur", "year"])
    return df


def remove_low_halb(df):
    # Delete duplicate lines: exclude "HALB" with low amount (less than 200 KRW)
    df = df[~((df["type"] == "HALB") & (df["amount_doc_"] < 200))]
    return df


# def set_KRW(df):
#     # Set 1 KRW = 1 KRW
#     df["fx_rates_VT"] = df["fx_rates_VT"].where(df["cur"] != "KRW", 1)
#     df["fx_rates_HMG"] = df["fx_rates_HMG"].where(df["cur"] != "KRW", 1)
#     return df


def calculate_delta(df):
    # Clculate delta price cause by fx rate difference
    df["fx_rate_diff"] = df["fx_rates_VT"] - df["fx_rates_HMG"]
    df["delta_price"] = df["amount_doc_"] * df["fx_rate_diff"]
    return df


# Process data
fx_vt = filter_fx_vt(fx_vt)
fx_hmg = filter_fx_hmg(fx_hmg)

df = join_dataframes(bom, fx_vt, fx_hmg)
df = df.pipe(remove_low_halb)  # .pipe(set_KRW).pipe(calculate_delta)

# Set 1 KRW = 1 KRW
df["fx_rates_VT"] = df["fx_rates_VT"].where(df["cur"] != "KRW", 1)
df["fx_rates_HMG"] = df["fx_rates_HMG"].where(df["cur"] != "KRW", 1)

df = df.pipe(calculate_delta)


# Write data
df.to_csv(output_file, index=False)
print("A file is created")
