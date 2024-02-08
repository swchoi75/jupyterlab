import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
try:
    path = Path(__file__).parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent


# Variables
period_start = "2024-01-31"
period_end = "2025-01-01"


# Filenames
input_file = (
    path / "bud_data" / "Budget 2024 Investment and depreciation all_20230913 v2.xlsx"
)
output_1 = path / "bud_output" / "investment_depreciation.csv"
output_2 = path / "bud_output" / "investment_depreciation_monthly.csv"


# Read excel sheet data and return pandas dataframe and ibis table
df = pd.read_excel(
    input_file,
    sheet_name="Asset ledger by Jul. acquis",
    skiprows=10,
    usecols="A:AD",
    dtype={
        "Asset": str,
        "Ending date": str,
        "Asset Class": str,
        "New CC": str,
        "New CC.1": str,
        "Asset no": str,
        "P/O": str,
        "Vendor": str,
    },
    parse_dates=["ODep-Start"],  # "Acquisitio", "Spending date"
)

# "Acquisitio", "Spending date" fields have mixed format
df["Acquisitio"] = pd.to_datetime(df["Acquisitio"].str.replace(".", "-"))
df["Spending date"] = pd.to_datetime(df["Spending date"].str.replace(".", "-"))
# df['ODep-Start'] = pd.to_datetime(df['ODep-Start'].str.replace('.', '-'))


# Functions to clean column names
def clean_preceding_underscore(column_name):
    return column_name.lstrip("_").rstrip("_")


# Apply the cleaning function to all column names
df = df.clean_names()
df.columns = df.columns.map(clean_preceding_underscore)


# Rename columns
df = df.rename(
    columns={
        "acquisitio": "acquisition_date",
        "con": "useful_life_year",
        "con_p": "useful_life_month",
        "acqusition": "acquisition",
        "odep_start": "start_of_depr",
    }
)


# Remove unnecessary columns
columns_to_exclude = [
    "ending_date",
    "cost_elem",
    "new_cc_1",
    "check",
    "wbs",
    "project",
    "sub_no",
    #
    "kor",
    "sie",
    "total",
    "book_value",
    "vendor",
    "vendor_name",
    "p_o",
]
selected_columns = [col for col in df.columns if col not in columns_to_exclude]
df = df.select(columns=selected_columns)


# Summary
df_depr = (
    df.groupby(["category", "new_profit_center", "new_cc"])
    .agg({"current": "sum"})
    .reset_index()
)


# # Business Logic: Monthly deprecation # #

# Dataframe for month end dates
df_month_ends = pd.DataFrame(
    {"month_ends": pd.date_range(period_start, period_end, freq="M")}
)


# Functions
def calc_monthly_depr(row, period_start, period_end):
    if (
        # To avoid Division by zero error (e.g. Asset under construction)
        row["useful_life_year"] == row["useful_life_month"] == 0
    ):
        return 0

    # Main calculation logic
    monthly_depr = (
        row["acquisition"] / (row["useful_life_year"] * 12 + row["useful_life_month"])
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


def calc_depr_end(row):
    years = row["useful_life_year"]
    months = row["useful_life_month"]
    row = (
        row["start_of_depr"] + pd.DateOffset(years=years) + pd.DateOffset(months=months)
    )
    return row


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


# Fill missing values with 0
df["useful_life_year"].fillna(0, inplace=True)
df["useful_life_month"].fillna(0, inplace=True)


# Create new columns
df["depr_start"] = df["start_of_depr"]
df["depr_end"] = df.apply(calc_depr_end, axis="columns")
df["monthly_depr"] = df.apply(
    calc_monthly_depr, axis="columns", period_start=period_start, period_end=period_end
)


# Cross Join & Filter deprciation periods
df = df_month_ends.join(df, how="cross")
df = filter_depr_periods(df, period_start)


# # Pivot_wider
# df["month_ends"] = df["month_ends"].astype(str)
# df = pd.pivot(
#     df,
#     index=[col for col in df.columns if col not in ["month_ends", "monthly_depr"]],
#     columns="month_ends",
#     values="monthly_depr",
# ).reset_index()


# Write data
df_depr.to_csv(output_1, index=False)
df.to_csv(output_2, index=False)
print("Files are created")
