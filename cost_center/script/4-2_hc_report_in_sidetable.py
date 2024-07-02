import pandas as pd
from pathlib import Path
import sidetable  # for subtotal function


# Path
path = Path(__file__).parent.parent


# Functions
def reorder_first_columns(df, cols_to_reorder):
    df = df[cols_to_reorder + [col for col in df.columns if col not in cols_to_reorder]]
    return df


def main():

    # Filenames
    input_file = path / "output" / "2_fix_act_to_plan_by_month.csv"
    output_file = path / "output" / "3-1_fix_act_to_plan_subtotal.csv"

    # Read data
    df = pd.read_csv(input_file, dtype={"cctr": str})

    # Process data

    ## Sub-total
    category_columns = df.select_dtypes(include="object").columns.to_list()
    numeric_columns = df.select_dtypes(include="number").columns.to_list()

    df = df.pipe(add_subtotal, category_columns, numeric_columns)

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
