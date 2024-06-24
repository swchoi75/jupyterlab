import pandas as pd
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Functions
def remove_columns(df, cols_to_remove):
    df = df[[col for col in df.columns if col not in cols_to_remove]]
    return df


def pivot_wider(df):
    df = df.pivot_table(
        index=[col for col in df.columns if col not in ["period", "plan", "actual"]],
        columns=["period"],
        values=["plan", "actual"],
        aggfunc="sum",
        fill_value=0,
        margins=True,  # add sub-total columns
    ).reset_index()
    return df


def add_delta_to_plan(df):
    # actual delta to plan
    df[("delta", "All")] = df[("actual", "All")] - df[("plan", "All")]
    # sign logic: (+) for positive, (-) for negative variations
    df[("delta", "All")] = df[("delta", "All")] * -1
    return df


def main():

    # Variables
    year = 2024

    # Filenames
    input_file = path / "output" / "1_primary_cc_report.csv"
    output_file = path / "output" / "2_actual_to_plan_by_month.csv"

    # Read data
    df = pd.read_csv(input_file, dtype={"cctr": str})

    # Process data
    val_cols_to_remove = [
        "target",
        "fc",
        # "plan",
        # "actual",
    ]

    multi_idx_cols_to_remove = [
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
    ]

    df = (
        df.pipe(remove_columns, val_cols_to_remove)
        .pipe(pivot_wider)
        .pipe(remove_columns, multi_idx_cols_to_remove)
        .pipe(add_delta_to_plan)
    )

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
