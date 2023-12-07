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
def calc_delta_costs(df):
    # # Sign logic for costs: * -1
    df["delta_costs_to_bud_fx"] = df["quantity"] * df["delta_price_to_bud_fx"] * -1
    df["delta_costs_to_HMG_fx"] = df["quantity"] * df["delta_price_to_HMG_fx"] * -1
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

# Join dataframes
df = pd.merge(
    sales,
    bom,
    how="left",
    left_on=["fy", "period", "representative_pn"],
    right_on=["year", "month", "product"],
).drop(  # post-processing
    columns=["year", "month", "product"]
)

df = calc_delta_costs(df)


# Remove duplicate sales qty and amount
cur_krw_or_missing = (df["cur"] == "KRW") | (df["cur"].isna())
df["quantity"] = df["quantity"].where(cur_krw_or_missing, 0)
df["totsaleslc"] = df["totsaleslc"].where(cur_krw_or_missing, 0)


# Write data
df.to_csv(output_file, index=False)
print("A file is created")
