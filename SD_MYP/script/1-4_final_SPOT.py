import pandas as pd
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Functions
def drop_columns(df, cols_to_drop):
    df = df.drop(columns=cols_to_drop)
    return df


def pivot_data_longer(df, list_of_columns):
    return (
        df.melt(
            id_vars=list_of_columns,
            # value_vars=
            var_name="key",
            value_name="values",
        ).assign(
            year=lambda row: row["key"].str.extract(r"(\d{4})"),
            measure=lambda row: row["key"]
            .str.extract(r"(sales|volume|price)")
            .astype(str),
        )
        # .drop(columns=["key"])
    )


def pivot_data_wider(df, list_of_columns):
    df = df.pivot_table(
        index=list_of_columns,
        values="values",
        columns="measure",
        aggfunc="sum",
        # dropna=False,
    ).reset_index()
    return df


def pivot_data_wider_2(df, list_of_columns):
    df = df.pivot_table(
        index=list_of_columns,
        values=["sales", "volume"],
        columns="source",
        aggfunc="sum",
        # dropna=False,
    ).reset_index()
    return df


def remove_zero_na(df):
    # Remove zero values
    df = df.drop(df[(df["sales"] == 0.0) & (df["volume"] == 0.0)].index)
    return df


def clean_trailing_underscore(column_name):
    """Functions to clean column names"""
    return column_name.rstrip("_")


def clean_column_names(df):
    """Apply the cleaning function to all column names"""
    df.columns = df.columns.map(clean_trailing_underscore)
    return df


def main():
    # Filenames
    input_file = path / "output" / "SPOT" / "2024-04-24_BP_2025+9 Division E and P.csv"
    meta_file = path / "meta" / "column_group.csv"
    output_1 = path / "output" / "SPOT" / "2024-04-24_BP_2025+9 Div E and P_v1.csv"
    output_2 = path / "output" / "SPOT" / "2024-04-24_BP_2025+9 Div E and P_v2.csv"

    # Read data
    df = pd.read_csv(input_file)
    df_meta = pd.read_csv(meta_file)

    # Process data
    condition_to_keep = df_meta["column_group"].isin(
        [
            ## Keep
            # "id_cols",
            "id_cols_full_data",
            "sales_vt",
            # "sales_sg",
            "volume",
            "price",
            ## Drop
            # "empty",
            # "prime",
            # "price_adder",
            # "price_target",
        ]
    )
    cols_to_drop = df_meta[~condition_to_keep]["column_names"].to_list()

    id_cols = df_meta[df_meta["column_group"].isin(["id_cols_full_data"])][
        "column_names"
    ].to_list()

    ## Reshape data
    df = (
        df.pipe(drop_columns, cols_to_drop)
        .pipe(pivot_data_longer, id_cols)
        .pipe(pivot_data_wider, id_cols + ["year"])
        .pipe(remove_zero_na)
    )

    ## Further reshape data for Excel pivot report
    id_cols_2 = id_cols
    id_cols_2.append("year")
    id_cols_2.remove("source")
    df_2 = df.pipe(drop_columns, ["price"]).pipe(pivot_data_wider_2, id_cols_2)

    ## Flatten the multi-index
    df_2.columns = df_2.columns.to_flat_index()
    df_2.columns = ["_".join(col).strip() for col in df_2.columns]
    df_2 = clean_column_names(df_2)

    ## Filter years
    years_to_keep = ["2025", "2026", "2027", "2028", "2029", "2030"]
    df_2 = df_2[df_2["year"].isin(years_to_keep)]

    # Write data
    df.to_csv(output_1, index=False)
    df_2.to_csv(output_2, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
