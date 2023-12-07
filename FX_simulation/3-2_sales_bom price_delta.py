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
            "month",
            "product",
            "cur",
            "amount_doc_",
            "fx_act",
            "fx_HMG",
            "fx_diff",
            "delta_price",
        ]
    ]
    return df


def join_dataframes(df1, df2):
    # join
    df = pd.merge(
        df1,
        df2,
        how="left",
        left_on=["fy", "period", "representative_pn"],
        right_on=["year", "month", "product"],
    )
    # post-processing
    df = df.drop(columns=["year", "month", "product"])
    return df


def calc_delta_costs(df):
    df["delta_costs"] = df["quantity"] * df["delta_price"] * -1  # Sign logic for costs
    return df


# Process data
# Aggregate data
sales = (
    sales.groupby(
        [
            "fy",
            "period",
            "recordtype",
            "customer_group",
            "material_type",
            "product_hierarchy",
            "PH_description",
            "customer_engines",
            "customer_products",
            "product_group",
            "productline",
            "representative_pn",
        ],
        dropna=False,
    )
    .agg({"quantity": "sum", "totsaleslc": "sum"})
    .reset_index()
)

bom = select_columns(bom)

df = join_dataframes(sales, bom)

df = calc_delta_costs(df)


# Remove duplicate sales qty and amount
cur_krw_or_missing = (df["cur"] == "KRW") | (df["cur"].isna())
df["quantity"] = df["quantity"].where(cur_krw_or_missing, 0)
df["totsaleslc"] = df["totsaleslc"].where(cur_krw_or_missing, 0)


# Write data
df.to_csv(output_file, index=False)
print("A file is created")
