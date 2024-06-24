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


def main():

    # Filenames
    input_file = path / "output" / "primary_cc_report.csv"
    output_file = path / "output" / "primary_cc_report_by_month.csv"

    # Read data
    df = pd.read_csv(input_file, dtype={"cctr": str})

    # Process data
    columns_to_remove = [
        "target",
        "fc",
        # "plan",
        # "actual",
    ]
    df = df.pipe(remove_columns, columns_to_remove).pipe(pivot_wider)

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
