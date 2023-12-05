import pandas as pd
from pathlib import Path
from janitor import clean_names


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
sales_file = path / "data" / "Sales with representative PN.csv"

output_file = path / "output" / "bom_price_fx rate_delta.csv"


# Read data
bom = pd.read_csv(bom_file)
fx_vt = pd.read_csv(fx_vt_file)
fx_hmg = pd.read_csv(fx_hmg_file)
sales = pd.read_csv(sales_file)


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


# Process data
# Filter FX rates
fx_vt = filter_fx_vt(fx_vt)
fx_hmg = filter_fx_hmg(fx_hmg)

# Join two dataframes
df = pd.merge(bom, fx_vt, how="left", on=["cur", "year"])
df = pd.merge(df, fx_hmg, how="left", on=["cur", "year"])
df.info()

# Calculate the delta
df = df[df["cur"] != "KRW"]
df["fx_rate_diff"] = df["fx_rates_VT"] - df["fx_rates_HMG"]
df["delta_amt"] = df["amount_doc_"] * df["fx_rate_diff"]


# Write data
df.to_csv(output_file, index=False)
print("A file is created")
