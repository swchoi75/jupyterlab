import pandas as pd
from pathlib import Path
import sidetable  # for subtotal function


# Path
path = Path(__file__).parent.parent


# Functions
def remove_columns(df, cols_to_remove):
    df = df[[col for col in df.columns if col not in cols_to_remove]]
    return df


def add_subtotal(df, category_columns, numeric_columns):
    """Add subtotal rows for the hirerachical category columns"""

    # (pandas) groupby & aggregate
    agg_funcs = {col: "sum" for col in numeric_columns}
    df = df.groupby(category_columns).agg(agg_funcs)

    # (sidetable) add subtotal rows
    df = df.stb.subtotal(sub_level=[1, 2])  # 1 based

    # change back to original category_column names
    df = df.reset_index()
    df.columns = category_columns + numeric_columns

    return df


def remove_subtotal_rows(df, col_name):
    df = df[~df[col_name].str.contains(" - subtotal")]
    return df


def main():

    # Filenames
    input_file = path / "output" / "2_fix_primary_cost_report.csv"
    output_file = path / "output" / "3-1_summary_cost_report.csv"

    # Read data
    df = pd.read_csv(input_file, dtype={"cctr": str})

    # Process data
    ## Remove unnecessary columns
    columns_to_remove = [
        "f_v_cost",
        "acc_lv3",
        "account_no",
        "acc_lv1",
        "account_description",
        # remaining category columns
        # "responsible",
        # "cctr",
        # "acc_lv2",
        # "pctr",
    ]

    df = df.pipe(remove_columns, columns_to_remove)

    ## Sub-total
    category_columns = df.select_dtypes(include="object").columns.to_list()
    numeric_columns = df.select_dtypes(include="number").columns.to_list()

    df = df.pipe(add_subtotal, category_columns, numeric_columns).pipe(
        remove_subtotal_rows, "acc_lv2"
    )

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
