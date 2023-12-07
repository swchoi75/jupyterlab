import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
try:
    path = Path(__file__).parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent


# Filenames
input_file = path / "data" / "FX Rates" / "HMG 고시환율_20231113.xlsx"
output_file = path / "data" / "fx_rates_HMG.csv"


# Read data
df = pd.read_excel(input_file)


# Functions
def filter_columns(df):
    df = df[["년도", "차수", "화폐", "환율"]]
    return df


def rename_columns(df):
    df = df.rename(
        columns={
            "년도": "year",
            "차수": "quarter",
            "화폐": "cur",
            "환율": "fx_HMG",
        }
    )
    return df


def reorder_columns(df):
    df = df[["cur", "year", "quarter", "fx_HMG"]]
    return df


def filter_rows(df):
    df = df[df["cur"].isin(["USD", "EUR", "JPY"])]
    # df = df[df["year"].isin([2019, 2020, 2021, 2022, 2023])]
    return df


def sort_table(df):
    df = df.sort_values(by=["cur", "year", "quarter"], ascending=[False, True, True])
    return df


def add_q_letter(df):
    # Add Q letter in Quarter column
    df["quarter"] = df["quarter"].astype(str) + "Q"
    return df


def add_fx_type(df):
    # 3 Months Average FX rates
    df["fx_type"] = "3Mo Avg"
    return df


def quarter_month_table():
    quarters = ["1Q", "1Q", "1Q", "2Q", "2Q", "2Q", "3Q", "3Q", "3Q", "4Q", "4Q", "4Q"]
    months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
    df = pd.DataFrame({"quarter": quarters, "month": months})
    return df


# Process data
df = (
    df.pipe(filter_columns)
    .pipe(rename_columns)
    .pipe(reorder_columns)
    .pipe(filter_rows)
    .pipe(sort_table)
    .pipe(add_q_letter)
    .pipe(add_fx_type)
)


# Merge two tables
month = quarter_month_table()
df = df.merge(month, how="left", on="quarter")
df = df[["fx_type", "cur", "year", "quarter", "month", "fx_HMG"]]


# Write data
df.to_csv(output_file, index=False)
print("A file is created")
