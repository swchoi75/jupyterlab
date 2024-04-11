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


def exclude_auc(df):
    """Exclude Asset Under Construction"""
    df = df[~df["asset_class"].isin(["991", "997", "998"])]
    return df


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
    alterative_monthly_depr = row["current"] * -1 / 12

    # If the depreciation starts after period_start
    if pd.to_datetime(period_start) < row["depr_start"]:
        date1 = pd.to_datetime(period_start)
        date2 = row["depr_start"]
        months_difference = (date2.year - date1.year) * 12 + date2.month - date1.month
        return row["current"] * -1 / (12 - months_difference)

    # If the depreciation ends between period_start and period_end
    elif pd.to_datetime(period_start) < row["depr_end"] and row[
        "depr_end"
    ] < pd.to_datetime(period_end):
        date1 = row["depr_end"]
        date2 = pd.to_datetime(period_end)
        months_difference = (date2.year - date1.year) * 12 + date2.month - date1.month
        return row["current"] * -1 / (12 - months_difference)

    # If it is still depreciated even after depr_end date because usage is restarted
    elif row["depr_end"] < pd.to_datetime(period_start) and abs(row["current"]) > 0:
        return alterative_monthly_depr

    # Yearly Depreciation is not equal to the expected value
    # due to additional investment, ICO asset transfer, etc
    elif abs(row["current"] + (monthly_depr * 12)) > 1:
        return alterative_monthly_depr

    else:
        return monthly_depr


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
    """Make monthly_depr values into zero if the periods are not in depr_periods"""
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


def main():

    # Filenames
    input_file = path / "output" / "2_fc_acquisition_existing_assets.csv"
    output_file = path / "output" / "4_fc_depreciation_existing_assets.csv"

    # Process data
    df = (
        read_data(input_file)
        .pipe(exclude_auc)
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
        .pipe(pivot_wider).pipe(add_col_depr_current)
    )

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
