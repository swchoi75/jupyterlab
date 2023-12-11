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
sales_file = path / "data" / "Sales with representative PN.csv"
bom_file_1 = path / "output" / "1. bom_price_fx rate_delta.csv"
bom_file_2 = path / "output" / "2. bom_price_fx rate_delta.csv"
bom_file_3 = path / "output" / "3. bom_price_fx rate_delta.csv"
bom_file_4 = path / "output" / "4. bom_price_fx rate_delta.csv"
output_file = path / "output" / "sales with bom costs.csv"
output_parquet = path / "output" / "sales with bom costs.parquet"


# Read data
sales = pd.read_csv(sales_file)
bom_1 = pd.read_csv(bom_file_1)
bom_2 = pd.read_csv(bom_file_2)
bom_3 = pd.read_csv(bom_file_3)
bom_4 = pd.read_csv(bom_file_4)
bom = pd.concat([bom_1, bom_2, bom_3, bom_4])


# Functions
def calc_delta_costs(df):
    # # Sign logic for costs: * -1
    df["delta_costs_to_bud_fx"] = df["quantity"] * df["delta_price_to_bud_fx"] * -1
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


# Replace non-finite values with a default value (e.g., 0) and then change data type
df["quotation_year"] = (
    df["quotation_year"].replace([np.inf, -np.inf, np.nan], 0).astype(int)
)
df["sop_year"] = df["sop_year"].replace([np.inf, -np.inf, np.nan], 0).astype(int)


# Re-order and sort columns
df = df[["scenario"] + [col for col in df.columns if col not in ["scenario"]]]
df = df.sort_values(by=["scenario"])


# Write data
df.to_csv(output_file, index=False)
df.to_parquet(output_parquet)
print("A file is created")
