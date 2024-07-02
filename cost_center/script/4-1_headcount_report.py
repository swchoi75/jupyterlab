import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent


# Functions
def read_data(filename):
    df = pd.read_csv(filename, dtype={"Cctr": str}).clean_names()
    df = df.rename(
        columns={"month": "period", "data_type": "version", "profit_ctr": "pctr"}
    )
    df["period"] = pd.to_datetime(df["period"])
    return df


def read_cc_master(filename):
    df = pd.read_csv(filename, dtype={"Cctr": str}).clean_names()
    df = df[["cctr", "fix_var", "cctr_description", "responsible"]]
    return df


def read_poc_master(filename):
    df = pd.read_csv(filename).clean_names()
    df = df.rename(columns={"profit_center": "pctr"})
    df = df[["pctr", "division", "bu", "outlet_name", "plant_name"]]
    return df


def filter_source(df, filter_values):
    df = df[df["source"] == filter_values]
    return df


def filter_by_year(df, year):
    df = df[df["period"].dt.year == year]
    return df


def filter_version(df, filter_values):
    df = df[df["version"].isin(filter_values)]
    return df


def rename_columns(df, cols_to_rename):
    df = df.rename(columns=cols_to_rename)
    return df


def remove_columns(df, cols_to_remove):
    df = df[[col for col in df.columns if col not in cols_to_remove]]
    return df


def pivot_wider_by_version(df):
    df = df.pivot(
        index=[col for col in df.columns if col not in ["version", "hc"]],
        columns=["version"],
        values="hc",
    ).reset_index()
    return df


def convert_data_type(df, col):
    df[col] = df[col].astype(str)
    return df


def pivot_wider_by_period(df, val_cols):
    df = df.pivot_table(
        index=[col for col in df.columns if col not in ["period"] + val_cols],
        columns=["period"],
        values=val_cols,
        aggfunc="mean",
        fill_value=0,
        margins=True,  # add sub-total columns right & below
    ).reset_index()
    return df


def flatten_multi_index(df):
    """Flatten MultiIndex Columns"""
    df.columns = df.columns.to_flat_index()
    df.columns = ["_".join(col).strip() for col in df.columns]

    # clean trailing underscore
    df.columns = df.columns.map(lambda x: x.rstrip("_"))

    # remove "_All"
    df.columns = df.columns.map(lambda x: x.replace("_All", ""))

    return df


def remove_margin_row(df):
    df = df[df["cctr"] != "All"]
    return df


def clean_column_names(df):
    df.columns = df.columns.str.replace("Act", "act")
    df.columns = df.columns.str.replace("Bud.Eop", "plan")
    return df


def main():

    # Variables
    from common_variable import year

    # Filenames
    input_file = path / "data" / "0003_TABLE_OUTPUT_Headcount common.csv"
    meta_cc = path / "meta" / "0000_TABLE_MASTER_Cost center_general.csv"
    meta_poc = path / "meta" / "POC.csv"
    output_file = path / "output" / "0_headcount_report.csv"

    # Read data
    df = read_data(input_file)
    df_cc = read_cc_master(meta_cc)
    df_poc = read_poc_master(meta_poc)

    # Process data
    versions = ["Act", "Bud.Eop"]

    df = (
        df.merge(df_cc, on="cctr", how="left")
        .merge(df_poc, on="pctr", how="left")
        .pipe(filter_source, "HRIS")
        .pipe(filter_by_year, year)
        .pipe(filter_version, versions)
        .pipe(remove_columns, ["job_family", "fc_x+x"])
        .pipe(pivot_wider_by_version)
        .pipe(convert_data_type, "period")
        .pipe(pivot_wider_by_period, versions)
        .pipe(flatten_multi_index)
        .pipe(remove_margin_row)
        .pipe(clean_column_names)
    )

    columns_to_rename = {
        # monthly columns for
        # Actual
        f"act_{year:04d}-{month:02d}-01": f"act_{month:02d}"
        for month in range(1, 13)
    }

    columns_to_remove = (
        # monthly columns for
        # Plan
        [f"plan_{year:04d}-{month:02d}-01" for month in range(1, 13)]
    )
    df = df.pipe(rename_columns, columns_to_rename).pipe(
        remove_columns, columns_to_remove
    )

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
