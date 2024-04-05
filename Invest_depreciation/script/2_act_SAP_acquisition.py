import pandas as pd
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
    df = pd.read_excel(
        filename,
        header=3,
        dtype={
            "Asset Clas": str,
            "Cost Cente": str,
            "Asset no": str,
            "Sub No": str,
        },
        parse_dates=["Acquisitio", "ODep.Start"],
    )
    df = df.drop(
        df.columns[:2], axis="columns"
    )  # Drop first two columns, which are empty
    return df


def clean_preceding_underscore(column_name):
    """Functions to clean column names"""
    return column_name.lstrip("_")


def clean_column_names(df):
    """Apply the cleaning function to all column names"""
    df = df.clean_names()
    df.columns = df.columns.map(clean_preceding_underscore)
    return df


def rename_columns(df):
    df = df.rename(
        columns={
            "asset_clas": "asset_class",
            "cost_cente": "cost_center",
            "description": "asset_description",
            "acquisitio": "acquisition_date",
            "con": "useful_life_year",
            "con_p": "useful_life_month",
            "acqusition": "acquisition",
            "odep_start": "start_of_depr",
        }
    )
    return df


def drop_columns(df):
    df = df.drop(
        columns=["kor", "sie", "total", "book_value", "vendor_name", "p_o", "vendor"]
    )
    return df


def handle_missing_vals(df):
    """Filter out missing or zero value"""
    df = df.dropna(subset="asset_class")
    return df


def read_metadata(filename):
    df = pd.read_excel(filename, sheet_name="Sheet1", dtype=str).clean_names()
    return df


def process_metadata(df):
    # Rename columns
    df = df.rename(
        columns={
            "sap_description": "asset_class_name",
            "fire_account": "financial_statement_item",
            "race_description": "fs_item_description",
        }
    )
    # select columns
    selected_columns = [
        "asset_class",
        "asset_class_name",
        "financial_statement_item",
        "fs_item_description",
        "fs_item_sub",
        "zv2_account",
        "gl_account",
        "gl_account_description",
        "fix_var",
        "mv_type",
    ]
    df = df.select(columns=selected_columns)
    return df


def read_cc_master(filename):
    """Read cost center master data"""
    df = pd.read_excel(filename, sheet_name="General master", dtype=str).clean_names()
    return df


def process_cc_master(df):
    # Rename columns
    df = df.rename(
        columns={
            "cctr": "cost_center",
            "validity": "cc_validity",
            "pctr": "profit_center",
        }
    )
    # select columns
    selected_columns = [
        "cost_center",
        "cc_validity",
        "profit_center",
    ]
    df = df.select(columns=selected_columns)

    return df


def read_poc_master(filename):
    """Read POC (Plant Outlet Combination) master data"""
    df = pd.read_excel(filename, dtype=str)
    return df


def process_poc_master(df):
    # string manipulation
    df["plant_name"] = df["plant_name"].str.replace("ICH ", "")
    # rename columns
    df = df.rename(
        columns={
            "plant_name": "location_sender",
            "outlet_name": "outlet_sender",
        }
    )
    # select columns
    df = df[["location_sender", "outlet_sender", "profit_center"]]
    # drop duplicates
    df = df.drop_duplicates(subset="profit_center")

    return df


def read_prj_master(filename):
    """Read project info for assets"""
    df = pd.read_csv(filename, dtype=str)
    return df


def process_prj_master(df):
    # select columns
    df = df[["sub", "wbs_element", "asset_no", "sub_no"]]
    return df


def read_gpa_master(filename):
    """Read GPA master to link to asset list"""
    # read data
    df = pd.read_excel(filename, sheet_name="Sheet1", dtype=str).clean_names()
    return df


def process_gpa_master(df):
    # rename columns
    df = df.rename(
        columns={
            "unnamed_4": "master_description",
            "unnamed_9": "sub_description",
        }
    )
    # select columns
    df = df[["master", "master_description", "sub", "sub_description"]]

    return df


def enrich_dataset(df, df_meta, cc_master, poc_master, prj_master, gpa_master):
    """Add meta data"""
    df = (
        df.merge(df_meta, how="left", on="asset_class")
        .merge(cc_master, how="left", on="cost_center")
        .merge(poc_master, how="left", on="profit_center")
        .merge(prj_master, how="left", on=["asset_no", "sub_no"])
        .merge(gpa_master, how="left", on="sub")
    )
    return df


def main():

    # Variables
    from common_variable import asset_filename

    # Filenames
    input_file = path / "data" / asset_filename
    meta_file = path / "meta" / "0012_TABLE_MASTER_SAP-Fire mapping table.xlsx"
    meta_cc = path / "meta" / "0000_TABLE_MASTER_Cost center.xlsx"
    meta_poc = path / "meta" / "POC_for_GPA.xlsx"
    meta_prj = path / "meta" / "project_for_assets.csv"
    meta_gpa = path / "data" / "920 GPA Register of Subs.xlsx"
    output_file = path / "output" / "2_fc_acquisition_existing_assets.csv"

    # Read data
    sap = (
        read_data(input_file)
        .pipe(clean_column_names)
        .pipe(rename_columns)
        .pipe(drop_columns)
        .pipe(handle_missing_vals)
    )
    df_meta = read_metadata(meta_file).pipe(process_metadata)
    cc_master = read_cc_master(meta_cc).pipe(process_cc_master)
    poc_master = read_poc_master(meta_poc).pipe(process_poc_master)
    prj_master = read_prj_master(meta_prj).pipe(process_prj_master)
    gpa_master = read_gpa_master(meta_gpa).pipe(process_gpa_master)

    df = enrich_dataset(sap, df_meta, cc_master, poc_master, prj_master, gpa_master)

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
