import re
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
input_file = path / "fc_output" / "fc_depreciation_combined.csv"
meta_pc = path / "meta" / "2023_profit_center_master_KE5X.xlsx"
output_file = path / "fc_output" / "fc_depreciation.csv"


# Read data
df = pd.read_csv(
    input_file,
    dtype={
        # GPA data
        "investment_type": str,
        "financial_statement_item": str,
        # SAP data
        "asset_class": str,
        "cost_center": str,
        "asset_no": str,
        "sub_no": str,
    },
)


# Functions
def cf_allocation_ratio(filename):
    # Read data
    df = pd.read_excel(filename).clean_names()
    # Select columns
    df = df[["profit_ctr", "rec_prctr", "percentage"]]
    # Rename columns
    df = df.rename(columns={"profit_ctr": "profit_center"})
    # Filter central function
    df = df[df["profit_center"] == "50899-999"]
    # Change value to percentage
    df["percentage"] = df["percentage"] * 0.01

    return df


pc_master = cf_allocation_ratio(meta_pc)


# Add meta data
df = df.merge(pc_master, how="left", on="profit_center")


# Select value columns
pattern = re.compile(r"\d{4}-\d{2}-\d{2}")
monthly_depr_columns = df.columns[df.columns.str.contains(pattern)].tolist()
value_columns = monthly_depr_columns + ["acquisition", "previous", "current"]


# Multiply percentage to value columns
df["percentage"].fillna(1, inplace=True)
df[value_columns] = df[value_columns].mul(df["percentage"], axis=0)


# Write data
df.to_csv(output_file, index=False)
print("A file is created")
