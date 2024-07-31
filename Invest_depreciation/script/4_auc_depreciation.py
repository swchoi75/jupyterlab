import re
import pandas as pd
import numpy as np
from pathlib import Path
from janitor import clean_names


# Variables
from common_variable import period_start, period_end


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
            "asset_class": str,
            "cost_center": str,
            "asset_no": str,
            "sub_no": str,
        },
        parse_dates=["acquisition_date", "start_of_depr"],
    )
    return df


def filter_auc(df):
    """Filter Asset Under Construction"""
    df = df[df["asset_class"].isin(["991", "997", "998"])]
    return df


def read_metadata(filename):
    df = pd.read_excel(
        filename,
        sheet_name="Manual input",
        skiprows=3,
        dtype={
            "asset_class": str,
            "cost_center": str,
            "asset_no": str,
            "sub_no": str,
            # "input_useful_life_year": int,
        },
        parse_dates=["PPAP"],
    )
    df = df[["asset_no", "PPAP", "input_useful_life_year"]]
    return df


def enrich_dataset(df, df_meta):
    """Join two dataframes"""
    df = df.merge(df_meta, how="left", on="asset_no")
    return df


def overwrite_values(df):
    """Asset Under Construction: overwrite existing values if Manual input value is available"""
    df["useful_life_year"] = np.where(
        pd.isna(df["input_useful_life_year"]),
        df["useful_life_year"],
        df["input_useful_life_year"],
    )
    df["start_of_depr"] = np.where(pd.isna(df["PPAP"]), df["start_of_depr"], df["PPAP"])
    return df


def fill_missing_with_zero(df):
    """Fill missing values with 0"""
    df["useful_life_year"] = df["useful_life_year"].fillna(0)
    df["useful_life_month"] = df["useful_life_month"].fillna(0)
    return df


def calc_depr_end(row):
    years = row["useful_life_year"]
    months = row["useful_life_month"]
    row = (
        row["start_of_depr"] + pd.DateOffset(years=years) + pd.DateOffset(months=months)
    )
    return row


def calc_monthly_depr(row, period_start, period_end):
    if (
        # To avoid Division by zero error (e.g. Asset under construction)
        row["useful_life_year"]
        == row["useful_life_month"]
        == 0
    ):
        return 0

    # Main calculation logic
    monthly_depr = row["acquisition"] / (
        row["useful_life_year"] * 12 + row["useful_life_month"]
    )
    return monthly_depr


def add_col_monthly_depr(df):
    """Create new columns"""
    df["depr_start"] = df["start_of_depr"]
    df["depr_end"] = df.apply(calc_depr_end, axis="columns")
    df["monthly_depr"] = df.apply(
        calc_monthly_depr,
        axis="columns",
        period_start=period_start,
        period_end=period_end,
    )
    return df


def month_table_df():
    """Dataframe for month end dates"""
    df = pd.DataFrame(
        {"month_ends": pd.date_range(period_start, period_end, freq="ME")}
    )
    return df


def filter_depr_periods(df, period_start):
    depr_periods = (df["depr_start"] < df["month_ends"]) & (
        df["month_ends"] < df["depr_end"]
    )
    # If it is still depreciated even after depr_end date as usage is restarted
    exceptions = (df["depr_end"] < pd.to_datetime(period_start)) & (
        abs(df["current"]) > 0
    )

    df["monthly_depr"] = df["monthly_depr"].where(depr_periods | exceptions, 0)
    return df


def pivot_wider(df):
    df["month_ends"] = df["month_ends"].astype(str)
    df = pd.pivot(
        df,
        index=[col for col in df.columns if col not in ["month_ends", "monthly_depr"]],
        columns="month_ends",
        values="monthly_depr",
    ).reset_index()
    return df


def add_col_depr_current(df):
    """New column: Depreciation in current year as sum of monthly values"""
    pattern = re.compile(r"\d{4}-\d{2}-\d{2}")
    value_columns = df.columns[df.columns.str.contains(pattern)].tolist()
    df["depr_current"] = df[value_columns].sum(axis="columns")
    return df


def reclassify_auc(df):
    """Reclassify "Asset Under Construction" to other FS Items
    based on 3 scenarios of useful life year"""
    conditions = [
        df["useful_life_year"] > 20,
        (df["useful_life_year"] > 4) & (df["useful_life_year"] <= 20),
        (df["useful_life_year"] > 0) & (df["useful_life_year"] <= 4),
    ]

    fs_item_values = ["122617100", "122622000", "122637000"]
    fs_item_description_values = [
        "Buildings and land rights",
        "Technical equipment & machinery",
        "Molds / containers / tooling",
    ]
    gl_account_values = ["K413", "K413", "K432"]
    fix_var_values = ["fix", "fix", "var"]

    df["financial_statement_item"] = np.select(
        conditions, fs_item_values, default="122632000"
    )
    df["fs_item_description"] = np.select(
        conditions,
        fs_item_description_values,
        default="Assets under construction and advances to suppliers",
    )
    df["gl_account"] = np.select(
        conditions,
        gl_account_values,
        default="K413",
    )
    df["fix_var"] = np.select(
        conditions,
        fix_var_values,
        default="fix",
    )
    return df


def main():
    # Variables
    from common_variable import manual_AUC_filename

    # Filenames
    input_file = path / "output" / "2_fc_acquisition_existing_assets.csv"
    master_file = path / "data" / manual_AUC_filename
    output_file = path / "output" / "4_fc_depreciation_asset_under_construction.csv"

    # Read data
    df = read_data(input_file)
    df_meta = read_metadata(master_file)

    # Process data
    df = (
        df.pipe(filter_auc)
        .pipe(enrich_dataset, df_meta)
        .pipe(overwrite_values)
        .pipe(fill_missing_with_zero)  # on col: useful_life_year
        # add new cols: monthly_depr, depr_start, depr_end
        .pipe(add_col_monthly_depr)
    )
    df_month_table = month_table_df()
    df = df_month_table.join(df, how="cross")
    df = (
        # Make monthly_depr values into zero if the periods are not in depr_periods
        df.pipe(filter_depr_periods, period_start)
        # new col: depr_current as sum of monthly values
        .pipe(pivot_wider)
        .pipe(add_col_depr_current)
        .pipe(reclassify_auc)
    )

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
