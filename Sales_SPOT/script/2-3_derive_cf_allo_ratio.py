import pandas as pd
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Functions
def pivot_longer(df, key_cols, value_cols):
    df = df.melt(
        id_vars=key_cols,
        value_vars=value_cols,
        var_name="year",
        value_name="sales",
    )
    return df


def pivot_wider(df, key_cols):
    df = df.pivot_table(
        values="sales",
        index=key_cols,
        columns="outlets",
        aggfunc="sum",
    )
    return df


def add_col_total(df, numeric_cols):
    df["total"] = df[numeric_cols].sum(axis="columns")
    return df


def division_by_total(df, numeric_cols):
    df[numeric_cols] = df[numeric_cols].apply(lambda x: x / df["total"])
    return df


def pivot_longer_2(df, numeric_cols):
    df = df.drop(columns=["total"])
    df = df.melt(
        id_vars=["source", "currency", "items", "year"],
        value_vars=numeric_cols,
        var_name="outlets",
        value_name="cf_allo_ratio",
    )
    return df


def drop_unnecessary_rows(df, col_name="cf_allo_ratio"):
    # Drop missing values
    df = df.dropna(subset=[col_name])
    # Drop zero values
    df = df[df[col_name] != 0]
    return df


def main():
    # Filenames
    input_file = path / "output" / "MYP_2025-2030_sales.csv"

    output_1 = path / "output" / "MYP_2025-2030_cf_allo_ratio.csv"
    output_2 = path / "meta" / "MYP_cf_allo_ratio.csv"

    # Read data
    df = pd.read_csv(input_file)
    # Process data
    cols_to_drop = ["Div", "BU", "PL", "row_number", "section"]
    df = df.drop(columns=cols_to_drop)

    ## Reshape data
    key_cols = ["source", "currency", "items", "outlets"]
    value_cols = [col for col in df.columns if col not in key_cols]

    df = (
        df.pipe(pivot_longer, key_cols, value_cols)
        .pipe(pivot_wider, ["source", "currency", "items", "year"])
        .reset_index()
    )

    ## Add "Total" column that sums up numeric columns
    pl_columns = [col for col in df.columns if col.startswith("PL")]
    df = (
        df.pipe(add_col_total, pl_columns)
        .pipe(division_by_total, pl_columns)
        .pipe(add_col_total, pl_columns)
    )

    ## Create meta file
    df_meta = (
        df.pipe(pivot_longer_2, pl_columns)
        .pipe(drop_unnecessary_rows)
        .sort_values(by=["source", "year"])
    )

    # Write data
    df.to_csv(output_1, index=False)
    df_meta.to_csv(output_2, index=False)
    print("Files are created")


if __name__ == "__main__":
    main()
