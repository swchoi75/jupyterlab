import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent


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


def quarter_to_month_table():
    quarters = ["1Q", "1Q", "1Q", "2Q", "2Q", "2Q", "3Q", "3Q", "3Q", "4Q", "4Q", "4Q"]
    months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
    df = pd.DataFrame({"quarter": quarters, "month": months})
    return df


def plan_fx(df):
    """Yearly average of Quartely FX rates from HMG"""
    df = df.groupby(["cur", "year"]).agg({"fx_HMG": "mean"}).reset_index()
    df = df.rename(columns={"fx_HMG": "fx_plan", "year": "plan_year"})
    return df


def main():

    # Filenames
    # input_file = path / "data" / "FX Rates" / "HMG 고시환율_20231113.xlsx"
    input_file = path / "data" / "FX Rates" / "HMG 고시환율 Y24.2Q.xlsx"
    output_1 = path / "data" / "fx_rates_HMG_actual.csv"
    output_2 = path / "data" / "fx_rates_HMG_plan.csv"

    # Read data
    df = pd.read_excel(input_file)

    # Process data
    df = (
        df.pipe(filter_columns)
        .pipe(rename_columns)
        .pipe(reorder_columns)
        .pipe(filter_rows)
        .pipe(sort_table)
        .pipe(add_q_letter)  # Add Q letter in Quarter column
        .pipe(add_fx_type)  # 3 Months Average FX rates
    )

    # Merge two tables
    month = quarter_to_month_table()
    df = df.merge(month, how="left", on="quarter")
    df = df[["fx_type", "cur", "year", "quarter", "month", "fx_HMG"]]

    # Plan FX rates
    df_plan = plan_fx(df)  # Yearly average of Quartely FX rates from HMG

    # Write data
    df.to_csv(output_1, index=False)
    df_plan.to_csv(output_2, index=False)
    print("Files are created")


if __name__ == "__main__":
    main()
