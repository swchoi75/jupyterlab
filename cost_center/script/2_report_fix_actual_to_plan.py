import pandas as pd
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Functions
def filter_fix_costs(df):
    df = df[df["f_v_cost"] == "Fix cost"]
    return df


def remove_columns(df, cols_to_remove):
    df = df[[col for col in df.columns if col not in cols_to_remove]]
    return df


def pivot_wider(df, val_cols):
    df = df.pivot_table(
        index=[col for col in df.columns if col not in ["period"] + val_cols],
        columns=["period"],
        values=val_cols,
        aggfunc="sum",
        fill_value=0,
        margins=True,  # add sub-total columns right & below
    ).reset_index()
    return df


def flatten_columns(df):
    """Flatten MultiIndex Columns"""
    df.columns = [
        "_".join(col).strip() if isinstance(col, tuple) else col
        for col in df.columns.values
    ]

    # clean trailing underscore
    df.columns = df.columns.map(lambda x: x.rstrip("_"))

    # remove "_All"
    df.columns = df.columns.map(lambda x: x.replace("_All", ""))

    return df


def remove_margin_row(df):
    df = df[df["cctr"] != "All"]
    return df


def add_delta_to_ytd_plan(df):
    # actual delta to ytd_plan
    df["delta"] = df["actual"] - df["ytd_plan"]
    # sign logic: (+) for positive, (-) for negative variations
    df["delta"] = df["delta"] * -1
    return df


def rename_columns(df, cols_to_rename):
    df = df.rename(columns=cols_to_rename)
    return df


def reorder_first_columns(df, cols_to_reorder):
    df = df[cols_to_reorder + [col for col in df.columns if col not in cols_to_reorder]]
    return df


def reorder_last_columns(df, cols_to_reorder):
    df = df[[col for col in df.columns if col not in cols_to_reorder] + cols_to_reorder]
    return df


def main():

    # Variables
    from common_variable import year

    # Filenames
    input_file = path / "output" / "1_primary_cc_report.csv"
    output_file = path / "output" / "2_fix_act_to_plan_by_month.csv"

    # Read data
    df = pd.read_csv(input_file, dtype={"cctr": str})

    # Process data
    value_columns = ["actual", "ytd_plan", "plan", "fc"]  # remove "target"

    df = (
        df.pipe(filter_fix_costs)
        .pipe(remove_columns, ["target"])
        .pipe(pivot_wider, value_columns)
        .pipe(flatten_columns)
        .pipe(remove_margin_row)
        .pipe(add_delta_to_ytd_plan)
    )

    columns_to_rename = {
        # monthly columns for
        # Actual
        f"actual_{year:04d}-{month:02d}-01": f"act_{month:02d}"
        for month in range(1, 13)
    }

    columns_to_remove = (
        # monthly columns for
        # YTD Plan
        [f"ytd_plan_{year:04d}-{month:02d}-01" for month in range(1, 13)]
        # Plan
        + [f"plan_{year:04d}-{month:02d}-01" for month in range(1, 13)]
        # FC
        + [f"fc_{year:04d}-{month:02d}-01" for month in range(1, 13)]
    )

    first_columns_to_reorder = ["cctr", "account_no", "account_name", "plan", "fc"]
    last_columns_to_reorder = ["actual", "ytd_plan", "delta", "plan", "fc"]

    df = (
        df.pipe(rename_columns, columns_to_rename)
        .pipe(remove_columns, columns_to_remove)
        .pipe(reorder_first_columns, first_columns_to_reorder)
        .pipe(reorder_last_columns, last_columns_to_reorder)
    )

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
