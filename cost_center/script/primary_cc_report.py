import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent


# Functions
def read_data(filename):
    df = pd.read_csv(filename, dtype={"Cctr": str}).clean_names()
    df["period"] = pd.to_datetime(df["period"])
    df = df.rename(columns={"account_no_": "account_no"})
    return df


def read_acc_master(filename):
    df = pd.read_csv(filename).clean_names()
    df = df.rename(columns={"account_no_": "account_no"})
    df = df[["account_no", "account_name", "acc_lv1", "acc_lv2", "acc_lv3"]]
    return df


def read_cc_master(filename):
    df = pd.read_csv(filename, dtype={"Cctr": str}).clean_names()
    df = df[["cctr", "responsible"]]
    return df


def filter_primary_costs(df):
    df = df[df["acc_lv3"] == "500 Dir.cost centre costs"]
    return df


def filter_by_year(df, year):
    df = df[df["period"].dt.year == year]
    return df


def filter_by_responsible(df, responsible):
    df = df[df["responsible"] == responsible]
    return df


def pivot_wider(df):
    df = df.pivot_table(
        index=[col for col in df.columns if col not in ["filter", "amt"]],
        columns=["filter"],
        values="amt",
        aggfunc="sum",
    ).reset_index()
    return df


def sort_data(df):
    df = df.sort_values(by=["period", "cctr", "account_no"])
    return df


def rename_columns(df, columns_to_rename):
    df = df.rename(columns=columns_to_rename)
    return df


def reorder_columns(df, colums_to_reorder):
    df = df[
        [col for col in df.columns if col not in colums_to_reorder] + colums_to_reorder
    ]
    return df


def main():

    # Variables
    report_year = 2024
    responsible_name = "Kim, Hagjae"

    # Filenames
    input_file = path / "data" / "0004_TABLE_OUTPUT_Cctr report common.csv"
    meta_acc = path / "meta" / "0000_TABLE_MASTER_Acc level.csv"
    meta_cc = path / "meta" / "0000_TABLE_MASTER_Cost center_general.csv"
    meta_poc = path / "meta" / "POC.csv"
    output_file = path / "output" / "primary_cc_report.csv"

    # Read data
    df = read_data(input_file)
    df_cc = read_cc_master(meta_cc)
    df_acc = read_acc_master(meta_acc)
    df_poc = pd.read_csv(meta_poc).clean_names()

    # Process data
    df = (
        # Add master data
        df.merge(df_acc, on="account_no", how="left")
        .merge(df_cc, on="cctr", how="left")
        # Filter data
        .pipe(filter_primary_costs)
        .pipe(filter_by_year, report_year)
        .pipe(filter_by_responsible, responsible_name)
        # Reshape data
        .pipe(pivot_wider)
        .pipe(sort_data)
    )

    columns_to_rename = {
        "FC": "fc",
        "Period": "actual",
        "Period Plan": "plan",
        "Period Target": "target",
    }

    colums_to_reorder = ["target", "actual", "plan", "fc"]

    df = (
        # Rename and Reorder value columns
        df.pipe(rename_columns, columns_to_rename).pipe(
            reorder_columns, colums_to_reorder
        )
    )

    # Write data
    df.to_csv(output_file, index=False, na_rep=0)
    print("A file is created")


if __name__ == "__main__":
    main()
