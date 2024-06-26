import pandas as pd
from pathlib import Path
import sidetable


# Path
path = Path(__file__).parent.parent


# Functions
def add_subtotal(df, category_columns, numeric_columns):
    """Add subtotal rows for the hirerachical category columns"""

    # (pandas) groupby & aggregate
    agg_funcs = {col: "sum" for col in numeric_columns}
    df = df.groupby(category_columns).agg(agg_funcs)

    # (sidetable) add subtotal rows
    df = df.stb.subtotal(sub_level=[4, 5])  # 1 based

    # change back to original category_column names
    df = df.reset_index()
    df.columns = category_columns + numeric_columns

    return df


def main():

    # Filenames
    input_file = path / "output" / "2_fix_act_to_plan_by_month.csv"
    output_file = path / "output" / "3_fix_act_to_plan_subtotal.csv"

    # Read data
    df = pd.read_csv(input_file, dtype={"cctr": str})

    # Process data
    category_columns = df.select_dtypes(include="object").columns.to_list()
    numeric_columns = df.select_dtypes(include="number").columns.to_list()

    df = df.pipe(add_subtotal, category_columns, numeric_columns)

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
