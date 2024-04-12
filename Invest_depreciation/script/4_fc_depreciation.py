import re
import pandas as pd
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
            "investment_type": str,
            "financial_statement_item": str,
            "input_cost_center": str,
        },
        parse_dates=["acquisition_date", "PPAP", "start_of_depr"],
    )
    return df


def calc_depr_end(row):
    years = row["useful_life_year"]
    row = row["start_of_depr"] + pd.DateOffset(years=years)
    return row


def calc_monthly_depr(row):
    if row["useful_life_year"] == 0:
        return 0
    monthly_depr = row["acquisition"] / (row["useful_life_year"] * 12)
    return monthly_depr


def add_col_monthly_depr(df):
    """Create new columns"""
    df["depr_start"] = df["start_of_depr"]
    df["depr_end"] = df.apply(calc_depr_end, axis="columns")
    df["monthly_depr"] = df.apply(calc_monthly_depr, axis="columns")
    return df


def month_table_df():
    """Dataframe for month end dates"""
    df = pd.DataFrame(
        {"month_ends": pd.date_range(period_start, period_end, freq="ME")}
    )
    return df


def filter_depr_periods(df):
    depr_periods = (df["depr_start"] < df["month_ends"]) & (
        df["month_ends"] < df["depr_end"]
    )
    df["monthly_depr"] = df["monthly_depr"].where(depr_periods, 0)
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
    input_file = path / "output" / "3_fc_acquisition_future_assets_adj.csv"
    output_file = path / "output" / "4_fc_depreciation_future_assets.csv"

    # Process data
    df = (
        read_data(input_file)
        # add new cols: monthly_depr, depr_start, depr_end
        .pipe(add_col_monthly_depr)
    )
    df_month_table = month_table_df()
    df = df_month_table.join(df, how="cross")
    df = (
        # Make monthly_depr values into zero if the periods are not in depr_periods
        df.pipe(filter_depr_periods)
        # new col: depr_current as sum of monthly values
        .pipe(pivot_wider).pipe(add_col_depr_current)
    )

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
