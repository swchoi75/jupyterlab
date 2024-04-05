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
def read_data(input_file):
    df = pd.read_excel(
        input_file,
        sheet_name="Sheet1",
        dtype={"Investment type": str},
    )
    return df


def clean_new_lines(column_name):
    """Functions to clean column names"""
    return column_name.replace("\n", "")


def clean_trailing_underscore(column_name):
    """Functions to clean column names"""
    return column_name.rstrip("_")


def clean_column_names(df):
    """Apply the cleaning function to all column names"""
    df.columns = df.columns.map(clean_new_lines)
    df = df.clean_names()
    df.columns = df.columns.map(clean_trailing_underscore)
    return df


def rename_columns(df):
    df = df.rename(
        columns={
            "categorie_of_investm": "category_of_investment",
            "unnamed_20": "master_description",
            "unnamed_22": "sub_description",
        }
    )
    return df


def remove_version(df, GPA_version):
    """Remove the GPA version prefix from each column name"""
    df.columns = df.columns.str.replace(GPA_version, "")
    return df


def select_columns(df):
    # Select key columns
    key_columns = [
        "location_sender",
        "outlet_sender",
        "category_of_investment",
        "category_of_invest_historic",
        "investment_type",
        "status",
        "master",
        "master_description",
        "sub",
        "sub_description",
    ]

    # Select value columns and mutiply by 1000
    value_columns = df.columns[df.columns.str.contains("spend")].tolist()
    df[value_columns] = df[value_columns].mul(1000, axis="columns")

    # Select key columns and value columns
    df = df[key_columns + value_columns]
    return df


def handle_missing_vals(df, column_name):
    """Filter out missing or zero value"""
    df = df.dropna(subset="master")
    df = df.dropna(subset=column_name)
    df = df[df[column_name] != 0]
    return df


def basic_or_project(df):
    """create new column"""
    df["basic_or_project"] = np.where(
        df["master_description"].str.contains("Basic", case=False),
        "basic",
        "project",
    )
    return df


def read_poc_master(filename):
    """Read meta data"""
    # read data
    df = pd.read_excel(filename, dtype=str)
    # string manipulation
    df["plant_name"] = df["plant_name"].str.replace("ICH ", "")
    # rename columns
    df = df.rename(
        columns={
            "plant_name": "location_sender",
            "outlet_name": "outlet_sender",
        }
    )
    df = df[["location_sender", "outlet_sender", "profit_center"]]
    return df


def read_cc_master(filename):
    df = pd.read_csv(filename, dtype=str)
    df = df[["sub", "cost_center"]]
    return df


def read_meta_coi(filename):
    df = pd.read_excel(filename, sheet_name="Sheet1", dtype=str)
    # Select columns explicitly for maintenance purpose
    df = df[
        [
            "category_of_investment",
            "category_description",
            "financial_statement_item",
            "fs_item_description",
            "fs_item_sub",
            "gl_account",
            "gl_account_description",
            "fix_var",
            "mv_type",
        ]
    ].dropna()
    return df


def enrich_dataset(df, df_meta_coi, cc_master, poc_master):
    """Add meta data"""
    df = (
        df.merge(df_meta_coi, how="left", on="category_of_investment")
        .merge(cc_master, how="left", on="sub")
        .merge(poc_master, how="left", on=["location_sender", "outlet_sender"])
    )
    return df


def main():
    # Variables
    from common_variable import GPA_filename, GPA_version, spending_total_col

    # Filenames
    input_file = path / "data" / GPA_filename
    meta_file = path / "meta" / "category_of_investment.xlsx"
    meta_cc = path / "meta" / "cost_centers.csv"
    meta_poc = path / "meta" / "POC_for_GPA.xlsx"
    output_file = path / "output" / "1_fc_monthly_spending.csv"

    # GPA data
    gpa = read_data(input_file)
    gpa = (
        gpa.pipe(clean_column_names)
        .pipe(rename_columns)
        .pipe(remove_version, GPA_version)
        .pipe(select_columns)
        .pipe(handle_missing_vals, spending_total_col)
        .pipe(basic_or_project)
    )

    # metadata
    meta_coi = read_meta_coi(meta_file)
    poc_master = read_poc_master(meta_poc)
    cc_master = read_cc_master(meta_cc)

    df = enrich_dataset(gpa, meta_coi, cc_master, poc_master)

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
