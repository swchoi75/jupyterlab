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

    # # clean_trailing_underscore
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


def reorder_columns(df, cols_to_reorder):
    df = df[[col for col in df.columns if col not in cols_to_reorder] + cols_to_reorder]
    return df


def rename_columns(df, cols_to_rename):
    df = df.rename(columns=cols_to_rename)
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

    columns_to_reorder = ["actual", "ytd_plan", "delta", "plan", "fc"]

    columns_to_remove = [
        # YTD Plan
        f"ytd_plan_{year}-01-01",
        f"ytd_plan_{year}-02-01",
        f"ytd_plan_{year}-03-01",
        f"ytd_plan_{year}-04-01",
        f"ytd_plan_{year}-05-01",
        f"ytd_plan_{year}-06-01",
        f"ytd_plan_{year}-07-01",
        f"ytd_plan_{year}-08-01",
        f"ytd_plan_{year}-09-01",
        f"ytd_plan_{year}-10-01",
        f"ytd_plan_{year}-11-01",
        f"ytd_plan_{year}-12-01",
        # Plan
        f"plan_{year}-01-01",
        f"plan_{year}-02-01",
        f"plan_{year}-03-01",
        f"plan_{year}-04-01",
        f"plan_{year}-05-01",
        f"plan_{year}-06-01",
        f"plan_{year}-07-01",
        f"plan_{year}-08-01",
        f"plan_{year}-09-01",
        f"plan_{year}-10-01",
        f"plan_{year}-11-01",
        f"plan_{year}-12-01",
        # FC
        f"fc_{year}-01-01",
        f"fc_{year}-02-01",
        f"fc_{year}-03-01",
        f"fc_{year}-04-01",
        f"fc_{year}-05-01",
        f"fc_{year}-06-01",
        f"fc_{year}-07-01",
        f"fc_{year}-08-01",
        f"fc_{year}-09-01",
        f"fc_{year}-10-01",
        f"fc_{year}-11-01",
        f"fc_{year}-12-01",
    ]

    columns_to_rename = {
        "actual_2024-01-01": "act_01",
        "actual_2024-02-01": "act_02",
        "actual_2024-03-01": "act_03",
        "actual_2024-04-01": "act_04",
        "actual_2024-05-01": "act_05",
        "actual_2024-06-01": "act_06",
        "actual_2024-07-01": "act_07",
        "actual_2024-08-01": "act_08",
        "actual_2024-09-01": "act_09",
        "actual_2024-10-01": "act_10",
        "actual_2024-11-01": "act_11",
        "actual_2024-12-01": "act_12",
    }

    df = (
        df.pipe(filter_fix_costs)
        .pipe(remove_columns, ["target"])
        .pipe(pivot_wider, value_columns)
        .pipe(flatten_columns)
        .pipe(remove_margin_row)
        .pipe(add_delta_to_ytd_plan)
        .pipe(reorder_columns, columns_to_reorder)
        .pipe(remove_columns, columns_to_remove)
        .pipe(rename_columns, columns_to_rename)
    )

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
