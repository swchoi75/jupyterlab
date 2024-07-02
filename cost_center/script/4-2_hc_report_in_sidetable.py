import pandas as pd
from pathlib import Path
import sidetable  # for subtotal function


# Path
path = Path(__file__).parent.parent


# Functions
def reorder_first_columns(df, cols_to_reorder):
    df = df[cols_to_reorder + [col for col in df.columns if col not in cols_to_reorder]]
    return df


def add_subtotal(df, category_columns, numeric_columns):
    """Add subtotal rows for the hirerachical category columns"""

    # (pandas) groupby & aggregate
    agg_funcs = {col: "sum" for col in numeric_columns}
    df = df.groupby(category_columns).agg(agg_funcs)

    # (sidetable) add subtotal rows
    df = df.stb.subtotal(sub_level=[1])  # 1 based

    # change back to original category_column names
    df = df.reset_index()
    df.columns = category_columns + numeric_columns

    return df


def main():

    # Filenames
    input_file = path / "output" / "4-1_headcount_report.csv"
    output_file = path / "output" / "4-2_hc_report_in_sidetable.csv"

    # Read data
    df = pd.read_csv(input_file, dtype={"cctr": str})

    # Process data
    df = df.pipe(
        reorder_first_columns,
        [
            "responsible",
            "cctr",
            "cctr_description",
            "fix_var",
            "type",
            "source",
            "pctr",
        ],
    )

    ## Sub-total
    category_columns = df.select_dtypes(include="object").columns.to_list()
    numeric_columns = df.select_dtypes(include="number").columns.to_list()

    df = df.pipe(add_subtotal, category_columns, numeric_columns)

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
