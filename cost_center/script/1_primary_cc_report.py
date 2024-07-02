import datetime
import pandas as pd
import numpy as np
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent


# Functions
def read_data(filename):
    df = pd.read_csv(filename, dtype={"Cctr": str}).clean_names()
    df["period"] = pd.to_datetime(df["period"])

    # clean trailing underscore
    df.columns = df.columns.map(lambda x: x.rstrip("_"))

    return df


def read_acc_master(filename):
    df = pd.read_csv(filename).clean_names()

    # clean trailing underscore
    df.columns = df.columns.map(lambda x: x.rstrip("_"))

    df = df[["account_no", "account_description", "acc_lv1", "acc_lv2", "acc_lv3"]]
    return df


def read_cc_master(filename):
    df = pd.read_csv(filename, dtype={"Cctr": str}).clean_names()
    df = df[["cctr", "responsible"]]
    return df


def read_poc_master(filename):
    df = pd.read_csv(filename).clean_names()
    df = df.rename(columns={"profit_center": "pctr"})
    df = df[["pctr", "division", "bu", "outlet_name", "plant_name"]]
    return df


def filter_primary_costs(df):
    df = df[df["acc_lv3"] == "500 Dir.cost centre costs"]
    return df


def filter_by_year(df, year):
    df = df[df["period"].dt.year == year]
    return df


def filter_by_responsible(df, responsible):
    df = df[df["responsible"] == responsible]
    return df


def pivot_wider(df):
    df = df.pivot_table(
        index=[col for col in df.columns if col not in ["filter", "amt"]],
        columns=["filter"],
        values="amt",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()
    return df


def sort_data(df):
    df = df.sort_values(by=["period", "cctr", "account_no"])
    return df


def rename_columns(df, cols_to_rename):
    df = df.rename(columns=cols_to_rename)
    return df


def reorder_columns(df, cols_to_reorder):
    df = df[[col for col in df.columns if col not in cols_to_reorder] + cols_to_reorder]
    return df


def add_ytd_plan(df, year, month):
    # Create a pandas Timestamp for the first day of the next month
    first_of_next_month = pd.Timestamp(year, month, 1) + pd.DateOffset(months=1)
    # Subtract one day to get the last day of the report month
    end_of_ytd_month = first_of_next_month - pd.DateOffset(days=1)
    # Copy plan data to create a new column
    df["ytd_plan"] = df["plan"]
    # Replace plan values after ytd_month with zero
    df["ytd_plan"] = np.where(df["period"] <= end_of_ytd_month, df["ytd_plan"], 0)

    return df


def divide_by_1000(df):
    numeric_columns = df.select_dtypes(include="number").columns.to_list()

    for col in numeric_columns:
        df[col] = df[col] / 1000
    return df


def main():

    # Variables
    from common_variable import year, month, responsible_name

    # Filenames
    input_file = path / "data" / "0004_TABLE_OUTPUT_Cctr report common.csv"
    meta_acc = path / "meta" / "0000_TABLE_MASTER_Acc level.csv"
    meta_cc = path / "meta" / "0000_TABLE_MASTER_Cost center_general.csv"
    meta_poc = path / "meta" / "POC.csv"
    output_file = path / "output" / "1_primary_cc_report.csv"

    # Read data
    df = read_data(input_file)
    df_cc = read_cc_master(meta_cc)
    df_acc = read_acc_master(meta_acc)
    df_poc = read_poc_master(meta_poc)

    # Process data
    df = (
        # Add master data
        df.merge(df_acc, on="account_no", how="left")
        .merge(df_cc, on="cctr", how="left")
        .merge(df_poc, on="pctr", how="left")
        # Filter data
        .pipe(filter_primary_costs)
        .pipe(filter_by_year, year)
        .pipe(filter_by_responsible, responsible_name)
        # Reshape data
        .pipe(pivot_wider)
        .pipe(sort_data)
    )

    columns_to_rename = {
        "FC": "fc",
        "Period": "actual",
        "Period Plan": "plan",
        "Period Target": "target",
    }

    columns_to_reorder = ["target", "actual", "plan", "fc"]

    df = (
        # Rename and Reorder value columns
        df.pipe(rename_columns, columns_to_rename).pipe(
            reorder_columns, columns_to_reorder
        )
        # Add new value column "ytd_plan"
        .pipe(add_ytd_plan, year, month)
        # Divide by 1000
        .pipe(divide_by_1000)
    )

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
