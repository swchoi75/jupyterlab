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
def read_data(filename, column_name):
    df = pd.read_csv(
        filename,
        dtype={
            "investment_type": str,
            "financial_statement_item": str,
        },
    )
    df = df.drop(columns=column_name)
    return df


def read_metadata(filename):
    df = pd.read_excel(
        filename,
        sheet_name="Manual input",
        skiprows=3,
        dtype={
            "input_cost_center": str,
            # "input_useful_life_year": int,
        },
        parse_dates=["PPAP"],
    )
    df = df[
        [
            "sub",
            "input_gl_account",
            "input_cost_center",
            "PPAP",
            "input_useful_life_year",
        ]
    ]
    return df


def pivot_longer(df):
    """Melt the dataframe"""
    value_columns = df.columns[df.columns.str.contains("spend")].tolist()
    key_columns = [col for col in df.columns if col not in value_columns]

    df = df.melt(
        id_vars=key_columns,
        value_vars=value_columns,
        var_name="spend_month",
        value_name="spend_amt",
    )

    df = df.where(df["spend_amt"] != 0, np.nan)  # turn 0 into n/a values
    df = df.dropna(subset="spend_amt")  # remove rows with n/a values

    return df


def col_acquisition(df):
    """New column: Forecast assumptions"""
    df["acquisition"] = df["spend_amt"]
    return df


def str_to_month_ends(series):
    """Helper function"""
    # Convert year_month to datetime with day set to 1st
    series = pd.to_datetime(series, format="%Y_%m")
    # Add one month and subtract one day to get the month end
    series = series + pd.DateOffset(months=1, days=-1)
    return series


def col_acquisition_date(df):
    """New column: "Acquisition date" based on the spending months"""
    s = df["spend_month"].str.replace("spend_fc_", "")
    s = str_to_month_ends(s)
    df["acquisition_date"] = s
    return df


def col_start_of_depr(df):
    """New column: "Start of Depreciation"""
    df["start_of_depr"] = np.where(
        pd.isna(df["PPAP"]), df["acquisition_date"], df["PPAP"]
    )
    return df


def col_asset_category(df, actual_month_end):
    """New column: "asset category" based on the column "start_of_depr"""
    df["asset_category"] = np.where(
        df["start_of_depr"] < pd.to_datetime(actual_month_end), "past fc", "future fc"
    )
    return df


def add_useful_life_year(row):
    """Helper function"""
    if row["fix_var"] == "fix":
        return 8
    elif row["fix_var"] == "var":
        return 4


def col_useful_life_year(df):
    """New column "useful_life_year" with default values"""
    df["useful_life_year"] = df.apply(add_useful_life_year, axis="columns")
    return df


def overwrite_values(df):
    """Overwrite existing values if Manual input value is available"""
    df["gl_account"] = np.where(
        pd.isna(df["input_gl_account"]), df["gl_account"], df["input_gl_account"]
    )
    df["cost_center"] = np.where(
        pd.isna(df["input_cost_center"]), df["cost_center"], df["input_cost_center"]
    )
    df["useful_life_year"] = np.where(
        pd.isna(df["input_useful_life_year"]),
        df["useful_life_year"],
        df["input_useful_life_year"],
    )
    return df


def reclassfy_fs_item(df, current_year_end):
    """Business Logic: Asset Under Construction if PPAP is in the future year"""
    ppap_future_year = df["PPAP"] > current_year_end

    df["financial_statement_item"] = df["financial_statement_item"].where(
        ~ppap_future_year, "122632000"
    )

    df["fs_item_description"] = df["fs_item_description"].where(
        ~ppap_future_year, "Assets under construction and advances to suppliers"
    )
    return df


def main():

    # Variables
    from common_variable import spending_total_col, current_year_end, actual_month_end

    # Filenames
    input_file = path / "output" / "1_fc_monthly_spending.csv"
    master_file = path / "data" / "fc_GPA_master.xlsx"
    output_file = path / "output" / "2_fc_acquisition_future_assets.csv"

    # Read data
    gpa_spending = read_data(input_file, spending_total_col)
    gpa_master = read_metadata(master_file)

    # Join two dataframes
    df = gpa_spending.merge(gpa_master, how="left", on="sub")

    # Business Logic: Get "Acquisition date" based on the spending months
    df = (
        df.pipe(pivot_longer)
        .pipe(col_acquisition)
        .pipe(col_acquisition_date)  # based on the spending months
        .pipe(col_start_of_depr)
        .pipe(col_asset_category, actual_month_end)
        .pipe(col_useful_life_year)
    )

    # Business Logic: Overwrite existing values if Manual input value is available
    df = overwrite_values(df)

    # Business Logic: Asset Under Construction if PPAP is in the future year
    df = reclassfy_fs_item(df, current_year_end)

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
