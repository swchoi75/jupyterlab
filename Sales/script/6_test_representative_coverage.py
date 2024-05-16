import pandas as pd
import numpy as np
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Functions
def filter_product_to_HMG(df):
    df = df[
        (df["recordtype"] == "F")
        & (df["material_type"].isin(["FERT", "HALB"]))
        # filter HMG and affiliates
        & (df["customer_group"].isin(["HMG", "Hyundai Transys", "Kefico", "MOBIS"]))
    ]
    return df


def add_check_col(df):
    df["has_representative_pn"] = np.where(df["representative_pn"].isna(), "no", "yes")
    return df


def select_columns(df, list_of_cols):
    df = df[list_of_cols]
    return df


def summarize_data(df):
    df = df.groupby(["fy", "has_representative_pn"]).sum().reset_index()
    df = df.pivot(
        index="fy", columns="has_representative_pn", values="totsaleslc"
    ).reset_index()
    # Add columns
    df["total"] = df["no"] + df["yes"]
    df["% of coverage"] = df["yes"] / df["total"] * 100
    return df


def main():

    # Filenames
    input_file = path / "output" / "Sales with representative PN.csv"
    output_file = path / "output" / "test_representative_PN_coverage.csv"

    # Read data
    df = pd.read_csv(input_file)

    # Process data
    cols_to_select = ["fy", "totsaleslc", "has_representative_pn"]
    df = (
        df.pipe(filter_product_to_HMG)
        .pipe(add_check_col)
        .pipe(select_columns, cols_to_select)
        .pipe(summarize_data)
    )

    # Write data
    df.to_csv(output_file, index=False)

    print("A file is created.")


if __name__ == "__main__":
    main()
