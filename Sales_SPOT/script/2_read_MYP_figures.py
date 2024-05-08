import pandas as pd
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Functions
def concat_sheet(input_file, list_of_sheets, skiprows):
    """Define a function to read multiple excel sheets and concatenate them into a single DataFrame"""

    # Use list comprehension to read .xlsm files into a list of DataFrames
    dataframes = [
        pd.read_excel(
            input_file,
            sheet_name=sheet_name,
            header=None,
            skiprows=skiprows,
            usecols="D:IL",
        )
        for sheet_name in list_of_sheets
    ]

    # Add a new column with filename to each DataFrame
    for i, df in enumerate(dataframes):
        df["source"] = list_of_sheets[i]
        df["row_number"] = df.index + skiprows + 1

    # Merge the list of DataFrames into a single DataFrame
    df = pd.concat(dataframes)

    return df


def add_col_currency(df, list_of_currency):
    mapper_df = pd.DataFrame(list_of_currency, columns=["source", "currency"])
    df = pd.merge(df, mapper_df, on="source", how="left")
    return df


def reorder_columns(df):
    first_columns = ["source", "currency", "row_number"]
    df = df[first_columns + [col for col in df.columns if col not in first_columns]]
    return df


def apply_col_names(df, df_col_names):
    first_columns = ["source", "currency", "row_number"]
    new_column_names = df_col_names.iloc[:, 0].tolist()
    df.columns = first_columns + new_column_names
    return df


def remove_empty_col(df, text="empty"):
    columns_to_drop = [col for col in df.columns if text in col]
    df = df.drop(columns=columns_to_drop)
    return df


def pivot_longer(df, key_cols, value_cols):
    # Melt the dataframe
    df_melted = df.melt(
        id_vars=key_cols, value_vars=value_cols, var_name="original_column"
    )
    # Splitting the 'original_column' into two parts
    df_melted[["outlets", "year"]] = df_melted["original_column"].str.split(
        "_", expand=True
    )
    # Drop the original column name column if not needed
    df = df_melted.drop("original_column", axis=1)
    return df


def pivot_wider(df, key_cols):
    df = df.pivot(index=key_cols + ["outlets"], columns="year", values="value")
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
    df = concat_sheet(input_file, list_of_sheets, skiprows=7)  # Read data
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

    key_cols = ["source", "currency", "row_number", "items"]
    value_cols = [col for col in df.columns if col not in key_cols]

    df = (
        df.pipe(pivot_longer, key_cols, value_cols)
        .pipe(pivot_wider, key_cols)
        .reset_index()
    )

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
