import pandas as pd
import numpy as np
from pathlib import Path
from common_function import clean_column_names


# Path
path = Path(__file__).parent.parent


# Functions
def add_attributes_cols(df, source_col, target_col):
    "Add two attribute columns for columns mapping"
    df["is_drop"] = np.where(df[target_col] == "drop", "yes", "no")
    df["is_same"] = np.where(df[source_col] == df[target_col], "yes", "no")
    return df


def drop_col_2024(df):
    columns_to_drop = df.filter(like="2024", axis=1)
    df = df.drop(columns_to_drop, axis=1)
    return df


def drop_col_2025(df):
    columns_to_drop = df.filter(like="2025", axis=1)
    df = df.drop(columns_to_drop, axis=1)
    return df


def rename_columns_from_csv(df, mapping_df, source_col, target_col):
    """
    This function reads a CSV file containing source and target column names,
    and renames columns in a separate data CSV using the mapping.
    """
    # Create a dictionary from the source and target columns
    column_mapping = dict(zip(mapping_df[source_col], mapping_df[target_col]))

    # Rename columns using the dictionary
    df_renamed = df.rename(columns=column_mapping)

    return df_renamed


def main():
    # Variables
    source_col = "old_name"  # Column name in mapping CSV containing source names
    target_col = "new_name"  # Column name in mapping CSV containing target names

    # Filenames
    input_file = (
        path
        / "data"
        / "2024-04-09 SPOT Overview 2025+9 vs. 2024+9_Legal view (AP).xlsb"
    )
    meta_file = path / "meta" / "column_mapping.csv"
    output_1 = path / "output" / "BP_2024+9 Division P.csv"
    output_2 = path / "output" / "BP_2025+9 Division P.csv"

    # Read data
    df = pd.read_excel(input_file, sheet_name="2025+9 vs 2024+9", skiprows=9)
    df_meta = pd.read_csv(meta_file)

    # Process data
    df = df.pipe(clean_column_names)

    df_meta = df_meta.pipe(add_attributes_cols, source_col, target_col)
    df_col_drop = df_meta[df_meta["is_drop"] == "yes"]
    df_col_mapping = df_meta[
        (df_meta["is_drop"] == "no") & (df_meta["is_same"] == "no")
    ]

    df = df.drop(columns=list(df_col_drop["old_name"]))
    # filter & rename columns 2024
    df_2024 = df[df["2025+9sales_2025"].isna()]
    df_2024 = df_2024.pipe(drop_col_2025).pipe(
        rename_columns_from_csv, df_col_mapping, source_col, target_col
    )
    # filter & rename columns 2025
    df_2025 = df[~df["2025+9sales_2025"].isna()]
    df_2025 = df_2025.pipe(drop_col_2024).pipe(
        rename_columns_from_csv, df_col_mapping, source_col, target_col
    )
    # add column "source"
    df_2024 = df_2024.assign(source="BP 2024+9")
    df_2025 = df_2025.assign(source="Early View 2025+9")

    # Write data
    df_2024.to_csv(output_1, index=False)
    df_2025.to_csv(output_2, index=False)
    print("Files are created")


if __name__ == "__main__":
    main()
