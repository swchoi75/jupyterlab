import pandas as pd
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Functions
def concat_sheet(input_file, list_of_sheets):
    """Define a function to read multiple excel sheets and concatenate them into a single DataFrame"""

    # Use list comprehension to read .xlsm files into a list of DataFrames
    dataframes = [
        pd.read_excel(
            input_file,
            sheet_name=sheet_name,
            header=None,
            skiprows=7,
            usecols="D:IL",
        )
        for sheet_name in list_of_sheets
    ]

    # Add a new column with filename to each DataFrame
    for i, df in enumerate(dataframes):
        df["source"] = list_of_sheets[i]

    # Merge the list of DataFrames into a single DataFrame
    df = pd.concat(dataframes)

    return df


def add_col_currency(df, list_of_currency):
    mapper_df = pd.DataFrame(list_of_currency, columns=["source", "currency"])
    df = pd.merge(df, mapper_df, on="source", how="left")
    return df


def reorder_columns(df):
    df = df[
        ["source", "currency"]
        + [col for col in df.columns if col not in ["source", "currency"]]
    ]
    return df


def apply_col_names(df, df_col_names):
    new_column_names = df_col_names.iloc[:, 0].tolist()
    df.columns = ["source", "currency"] + new_column_names
    return df


def remove_empty_col(df, text="empty"):
    columns_to_drop = [col for col in df.columns if text in col]
    df = df.drop(columns=columns_to_drop)
    return df


def main():
    # Filenames
    input_file = path / "data" / "MYP consolidation template.xlsx"
    meta_file = path / "meta" / "MYP_column_names.csv"
    output_file = path / "output" / "MYP_2025-2030.csv"

    # Read data
    list_of_sheets = [
        "Korea",
        "India",
        "Japan",
        "Thailand",
        "KR",
        "IN",
        "JP",
        "TH",
        "AP",
    ]
    df = concat_sheet(input_file, list_of_sheets)  # Read data
    df_meta = pd.read_csv(meta_file, header=None)  # Read column names

    # Process data
    list_of_currency = [
        ["Korea", "LC"],
        ["India", "LC"],
        ["Japan", "LC"],
        ["Thailand", "LC"],
        ["KR", "GC"],
        ["IN", "GC"],
        ["JP", "GC"],
        ["TH", "GC"],
        ["AP", "GC"],
    ]
    df = (
        df.pipe(add_col_currency, list_of_currency)
        .pipe(reorder_columns)
        .pipe(apply_col_names, df_meta)
        .pipe(remove_empty_col, "empty")
    )

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
