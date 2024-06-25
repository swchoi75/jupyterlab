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


def add_delta_to_ytd_plan(df):
    # actual delta to ytd_plan
    df[("delta", "All")] = df[("actual", "All")] - df[("ytd_plan", "All")]
    # sign logic: (+) for positive, (-) for negative variations
    df[("delta", "All")] = df[("delta", "All")] * -1
    return df


def reorder_columns(df, cols_to_reorder):
    df = df[[col for col in df.columns if col not in cols_to_reorder] + cols_to_reorder]
    return df


def remove_margin_row(df):
    df = df[df[("cctr",)] != "All"]
    return df


def main():

    # Variables
    from common_variable import year

    # Filenames
    input_file = path / "output" / "1_primary_cc_report.csv"
    output_file = path / "output" / f"2_fix_act_to_plan_by_month.csv"

    # Read data
    df = pd.read_csv(input_file, dtype={"cctr": str})

    # Process data
    val_cols = ["actual", "ytd_plan", "plan", "fc"]  # remove "target"

    multi_idx_cols_to_remove = [
        # YTD Plan
        ("ytd_plan", f"{year}-01-01"),
        ("ytd_plan", f"{year}-02-01"),
        ("ytd_plan", f"{year}-03-01"),
        ("ytd_plan", f"{year}-04-01"),
        ("ytd_plan", f"{year}-05-01"),
        ("ytd_plan", f"{year}-06-01"),
        ("ytd_plan", f"{year}-07-01"),
        ("ytd_plan", f"{year}-08-01"),
        ("ytd_plan", f"{year}-09-01"),
        ("ytd_plan", f"{year}-10-01"),
        ("ytd_plan", f"{year}-11-01"),
        ("ytd_plan", f"{year}-12-01"),
        # Plan
        ("plan", f"{year}-01-01"),
        ("plan", f"{year}-02-01"),
        ("plan", f"{year}-03-01"),
        ("plan", f"{year}-04-01"),
        ("plan", f"{year}-05-01"),
        ("plan", f"{year}-06-01"),
        ("plan", f"{year}-07-01"),
        ("plan", f"{year}-08-01"),
        ("plan", f"{year}-09-01"),
        ("plan", f"{year}-10-01"),
        ("plan", f"{year}-11-01"),
        ("plan", f"{year}-12-01"),
        # FC
        ("fc", f"{year}-01-01"),
        ("fc", f"{year}-02-01"),
        ("fc", f"{year}-03-01"),
        ("fc", f"{year}-04-01"),
        ("fc", f"{year}-05-01"),
        ("fc", f"{year}-06-01"),
        ("fc", f"{year}-07-01"),
        ("fc", f"{year}-08-01"),
        ("fc", f"{year}-09-01"),
        ("fc", f"{year}-10-01"),
        ("fc", f"{year}-11-01"),
        ("fc", f"{year}-12-01"),
    ]

    columns_to_reorder = [
        ("ytd_plan", "All"),
        ("delta", "All"),
        ("plan", "All"),
        ("fc", "All"),
    ]

    df = (
        df.pipe(filter_fix_costs)
        .pipe(remove_columns, ["target"])
        .pipe(pivot_wider, val_cols)
        .pipe(remove_columns, multi_idx_cols_to_remove)
        .pipe(add_delta_to_ytd_plan)
        .pipe(reorder_columns, columns_to_reorder)
        # .pipe(remove_margin_row)
    )

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
