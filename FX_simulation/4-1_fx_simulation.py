import pandas as pd
from pathlib import Path


# Path
try:
    path = Path(__file__).parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent


# Filenames
bom_file = path / "output" / "bom_price_fx rate_delta.csv"
sales_file = path / "data" / "Sales with representative PN.csv"
output_file = path / "output" / "sales with bom costs.csv"


# Read data
bom = pd.read_csv(bom_file)
sales = pd.read_csv(sales_file)


# Functions
def select_columns(df):
    df = df[
        [
            "year",
            "product",
            "cur",
            "amount_doc_",
            "fx_rates_VT",
            "fx_rates_HMG",
            "fx_rate_diff",
            "delta_price",
        ]
    ]
    return df


def join_dataframes(df1, df2):
    # join
    df = pd.merge(
        df1,
        df2,
        how="inner",  # dropna
        left_on=["fy", "representative_pn"],
        right_on=["year", "product"],
    )
    # post-processing
    df = df.drop(columns=["year", "product_y"])
    df = df.rename(columns={"product_x": "product"})
    return df


def calc_delta_costs(df):
    df["delta_costs"] = df["quantity"] * df["delta_price"] * -1  # Sign logic for costs
    return df


# Process data
sales = sales.dropna(subset=["representative_pn"])
bom = select_columns(bom)
# bom = bom[bom["cur"] != "KRW"]
df = join_dataframes(sales, bom)
# Calculate the delta costs
df = calc_delta_costs(df)


# Remove duplicate sales qty and amount
df["quantity"] = df["quantity"].where(df["cur"] == "KRW", 0)
df["totsaleslc"] = df["totsaleslc"].where(df["cur"] == "KRW", 0)


# Write data
df.to_csv(output_file, index=False)
print("A file is created")
