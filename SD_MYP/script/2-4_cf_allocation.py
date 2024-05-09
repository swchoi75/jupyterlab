import pandas as pd
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Functions
def filter_central_function(df):
    df = df[df["outlets"] == "Plant 9"]
    df = df.dropna(subset="section")
    return df


def pivot_longer(df, key_cols, value_cols):
    df = df.melt(
        id_vars=key_cols,
        value_vars=value_cols,
        var_name="year",
    )
    return df


def add_cf_allo_ratio(df, df_meta):
    df = df.merge(df_meta, on=["source", "year"], how="left")
    return df


def cf_allocation(df):
    df["value"] = df["value"] * df["cf_allo_ratio"]
    df = df.drop(columns=["cf_allo_ratio"])
    return df


def pivot_wider(df, key_cols):
    df = df.pivot(index=key_cols, columns="year", values="value")
    return df


def add_poc(df, df_meta):
    df = df.merge(df_meta, on="PL", how="left")
    return df


def main():
    # Filenames
    input_file = path / "output" / "MYP" / "MYP_2025-2030_v2.csv"
    meta_ratio = path / "meta" / "MYP_cf_allo_ratio.csv"
    meta_poc = path / "meta" / "MYP_POC.csv"
    output_file = path / "output" / "MYP" / "MYP_2025-2030_plant 9.csv"

    # Read data
    df = pd.read_csv(input_file).drop(columns=["Div", "BU", "PL"])
    df_meta = (
        pd.read_csv(meta_ratio, dtype={"year": str})
        .drop(columns=["currency", "items"])
        .rename(columns={"outlets": "PL"})
    )
    df_poc = pd.read_csv(meta_poc)[["Div", "BU", "PL"]]

    # Process data
    key_cols = [
        "source",
        "currency",
        "row_number",
        "section",
        "items",
        "outlets",
    ]
    value_cols = [col for col in df.columns if col not in key_cols]

    df = (
        df.pipe(filter_central_function)
        .pipe(pivot_longer, key_cols, value_cols)
        .pipe(add_cf_allo_ratio, df_meta)
        .pipe(cf_allocation)
        .pipe(pivot_wider, key_cols + ["PL"])
        .reset_index()
        .pipe(add_poc, df_poc)
    )

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
