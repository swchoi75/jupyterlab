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
        "con": "useful_life",
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


# Business Logic: Monthly deprecation #
df_month_ends = pd.DataFrame(
    {"month_ends": pd.date_range(period_start, period_end, freq="M")}
)


def calc_depr_end(row):
    years = row["useful_life"]
    row = row["acquisition_date"] + pd.DateOffset(years=years)
    return row


def filter_depr_periods(df):
    depr_periods = (df["depr_start"] < df["month_ends"]) & (
        df["month_ends"] < df["depr_end"]
    )
    df["monthly_depr"] = df["monthly_depr"].where(depr_periods, 0)
    return df


df = df.assign(
    monthly_depr=lambda row: row["acquisition"] / row["useful_life"] / 12,
    depr_start=lambda row: row["acquisition_date"],
)
df["useful_life"].fillna(0, inplace=True)  # fill missing values with 0
df["depr_end"] = df.apply(calc_depr_end, axis="columns")


df = df_month_ends.join(df, how="cross")
df = filter_depr_periods(df)


# Output data
df_depr.to_csv(output_1, index=False)
df.to_csv(output_2, index=False)
print("Files are created")
