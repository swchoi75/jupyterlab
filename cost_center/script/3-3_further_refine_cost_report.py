import pandas as pd
import numpy as np
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Functions
def strip_blank_text_cell(df):
    """clean leading and trailing whitespaces from all string columns"""
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    return df


def fill_blank_with(df, col_1, col_2):
    """fill blank cell on col_1 with col_2"""
    df[col_1] = np.where(df[col_1] == "", df[col_2], df[col_1])
    return df


def remove_columns(df, cols_to_remove):
    df = df[[col for col in df.columns if col not in cols_to_remove]]
    return df


def split_and_get_last(df, col):
    df[col] = df[col].str.split(pat="|").str.get(-1).str.strip()
    return df


def main():

    # Filenames
    input_file = path / "output" / "3-1_fix_act_to_plan_subtotal.csv"
    output_file = path / "output" / "3-2_further_refine_report.csv"

    # Read data
    df = pd.read_csv(input_file, dtype={"cctr": str})

    # Process data
    df = (
        df.pipe(strip_blank_text_cell)
        .pipe(fill_blank_with, "acc_lv1", "acc_lv2")
        .pipe(fill_blank_with, "account_description", "acc_lv1")
        .pipe(remove_columns, ["acc_lv2", "acc_lv1"])
        .pipe(split_and_get_last, "account_description")
    )

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
