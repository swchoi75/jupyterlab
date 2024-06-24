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
        index=[col for col in df.columns if col not in ["period", "actual", "fc"]],
        columns=["period"],
        values=["actual", "fc"],
        aggfunc="sum",
        fill_value=0,
        margins=True,  # add sub-total columns
    ).reset_index()
    return df


def add_delta_to_fc(df):
    # actual delta to plan
    df[("delta", "All")] = df[("actual", "All")] - df[("fc", "All")]
    # sign logic: (+) for positive, (-) for negative variations
    df[("delta", "All")] = df[("delta", "All")] * -1
    return df


def filter_fix_costs(df):
    df = df[df["f_v_cost"] == "Fix cost"]
    return df


def main():

    # Variables
    year = 2024

    # Filenames
    input_file = path / "output" / "1_primary_cc_report.csv"
    output_file = path / "output" / "3_fix_act_to_forecast_by_month.csv"

    # Read data
    df = pd.read_csv(input_file, dtype={"cctr": str})

    # Process data
    val_cols_to_remove = [
        "target",
        "plan",
        # "fc",
        # "actual",
    ]

    multi_cols_to_remove = [
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

    df = (
        df.pipe(remove_columns, val_cols_to_remove)
        .pipe(pivot_wider)
        .pipe(remove_columns, multi_cols_to_remove)
        .pipe(add_delta_to_fc)
        .pipe(filter_fix_costs)
    )

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
