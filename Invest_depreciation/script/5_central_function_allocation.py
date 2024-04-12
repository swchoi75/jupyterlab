import re
import pandas as pd
import numpy as np
from pathlib import Path
from janitor import clean_names


# Path
try:
    path = Path(__file__).parent.parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent.parent


# Functions
def read_data(filename):
    df = pd.read_csv(
        filename,
        dtype={
            # GPA data
            "investment_type": str,
            "financial_statement_item": str,
            "input_cost_center": str,
            # SAP data
            "asset_class": str,
            "cost_center": str,
            "asset_no": str,
            "sub_no": str,
        },
    )
    return df


def read_pc_master(filename):
    """Read Profit center master data"""
    # Read data
    df = pd.read_excel(filename).clean_names()
    # Select columns
    df = df[["profit_ctr", "rec_prctr", "percentage"]]
    # Rename columns
    df = df.rename(columns={"profit_ctr": "profit_center"})

    return df


def filter_central_function(df):
    # Filter central function
    df = df[df["profit_center"] == "50899-999"].copy()
    # Change value to percentage
    df["percentage"] = df["percentage"] * 0.01
    return df


def read_poc_master(filename):
    """Read POC (Plant Outlet Combination) master data"""
    # read data
    df = pd.read_excel(filename, dtype=str)
    # string manipulation
    df["plant_name"] = df["plant_name"].str.replace("ICH ", "")
    # rename columns
    df = df.rename(
        columns={
            "cu": "cu_no",
            "plant": "plant_no",
            "outlet": "outlet_no",
            "bu": "bu_receiver",
            "division": "division_receiver",
            "plant_name": "location_receiver",
            "outlet_name": "outlet_receiver",
            "profit_center": "rec_prctr",
        }
    )
    # drop duplicates
    df = df.drop_duplicates(subset="rec_prctr")

    return df


def add_pc_master(df, df_meta):
    """Add meta data"""
    df = df.merge(df_meta, how="left", on="profit_center")
    return df


def add_poc_master(df, df_meta):
    """Add meta data"""
    df["rec_prctr"] = np.where(
        pd.isna(df["rec_prctr"]), df["profit_center"], df["rec_prctr"]
    )
    df = df.merge(df_meta, how="left", on="rec_prctr")
    return df


def apply_cf_allocation_ratio(df):
    # Select value columns
    pattern = re.compile(r"\d{4}-\d{2}-\d{2}")
    monthly_depr_columns = df.columns[df.columns.str.contains(pattern)].tolist()
    value_columns = monthly_depr_columns + [
        "spend_amt",
        "acquisition",
        "previous",
        "current",
        "depr_current",
    ]
    # Multiply percentage to value columns
    df["percentage"] = df["percentage"].fillna(1)
    df[value_columns] = df[value_columns].mul(df["percentage"], axis=0)

    return df


def main():

    # Filenames
    input_file = path / "output" / "4_fc_depreciation_merged.csv"
    meta_pc = path / "meta" / "2024_profit_center_master_KE5X.xlsx"
    meta_poc = path / "meta" / "POC_for_GPA.xlsx"
    output_file = path / "output" / "5_fc_depreciation_cf_allocation.csv"

    # Read data
    df = read_data(input_file)
    cf_master = read_pc_master(meta_pc).pipe(filter_central_function)
    poc_master = read_poc_master(meta_poc)

    # Process data
    df = (
        df.pipe(add_pc_master, cf_master)
        .pipe(add_poc_master, poc_master)
        .pipe(apply_cf_allocation_ratio)
    )

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
