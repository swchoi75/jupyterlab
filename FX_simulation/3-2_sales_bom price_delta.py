import pandas as pd
import numpy as np
from pathlib import Path
import os


# Path
try:
    path = Path(__file__).parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent


# Filenames
sales_file = path / "data" / "Sales with representative PN.csv"
input_dir = path / "output"
bom_files = [
    f for f in os.listdir(input_dir) if f.endswith("bom_price_fx rate_delta.csv")
]
bom_files = [input_dir / f for f in bom_files]

output_file = path / "output" / "sales with bom costs.csv"
output_parquet = path / "output" / "sales with bom costs.parquet"


# Read data
sales = pd.read_csv(sales_file)
bom = pd.concat([pd.read_csv(f) for f in bom_files])


# Functions
def calc_delta_costs(df):
    # # Sign logic for costs: * -1
    df["delta_costs_to_plan_fx"] = df["quantity"] * df["delta_price_to_plan_fx"] * -1
    return df


def remove_duplacate_sales(df):
    # Remove duplicate sales qty and amount
    cur_krw_or_missing = (df["cur"] == "KRW") | (df["cur"].isna())
    df["quantity"] = df["quantity"].where(cur_krw_or_missing, 0)
    df["totsaleslc"] = df["totsaleslc"].where(cur_krw_or_missing, 0)
    return df


def change_dtypes_year(df):
    # Replace non-finite values with a default value (e.g., 0) and then change data type
    df["quotation_year"] = (
        df["quotation_year"].replace([np.inf, -np.inf, np.nan], 0).astype(int)
    )
    df["sop_year"] = df["sop_year"].replace([np.inf, -np.inf, np.nan], 0).astype(int)
    return df


def sort_by_fx_scenario(df):
    df = df.sort_values(by=["fx_scenario"])
    return df


def process_sales_year(df):
    # Sales Year
    df["fy"] = "Act " + df["fy"].astype(str)
    df["fy"] = df["fy"].str.replace("Act 2024", "YTD Act 2024")
    return df


def combine_id_cols(df, two_id_columns):
    # concatenate columns
    df["key_id"] = df[two_id_columns[0]] + "_" + df[two_id_columns[1]]
    return df


def reorder_columns(df, first_columns):
    # Reorder columns
    df = df[first_columns + [col for col in df.columns if col not in first_columns]]
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


two_id_columns = ["fy", "product_hierarchy"]
first_columns = [
    "key_id",
    "fx_scenario",
    "plan_fx_on",
    "plan_fx_from",
    "actual_fx_from",
]
df = (
    df.pipe(calc_delta_costs)
    .pipe(remove_duplacate_sales)
    .pipe(change_dtypes_year)
    .pipe(sort_by_fx_scenario)
    .pipe(process_sales_year)
    .pipe(combine_id_cols, two_id_columns)
    .pipe(reorder_columns, first_columns)
)


# Write data
df.to_csv(output_file, index=False)
df.to_parquet(output_parquet)
print("Files are created")
