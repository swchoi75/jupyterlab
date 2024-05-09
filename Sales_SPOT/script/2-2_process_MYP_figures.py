import pandas as pd
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Functions
def add_poc(df, df_meta):
    df = df.merge(df_meta, left_on="outlets", right_on="PL", how="left")
    return df


def add_section(df, df_meta):
    df = df.merge(df_meta, on="row_number", how="left")
    return df


def remove_blank_rows(df, numeric_cols):
    # add "check" column that adds the specified numeric columns
    df["check"] = df[numeric_cols].sum(axis="columns")
    # remove blank rows
    df = df[df["check"] != 0]
    # remove year rows that add up to 16212 = 2023 + 2024 + ... + 2030
    df = df[df["check"] != 16212]
    # remove the temporary "check" column
    df = df.drop(columns=["check"])
    return df


def remove_total_rows(df):
    """Filter out total rows"""
    df = df[df["outlets"] != "TOTAL"]
    return df


def reorder_columns(df, first_columns):
    df = df[first_columns + [col for col in df.columns if col not in first_columns]]
    return df


def main():
    # Filenames
    input_file = path / "output" / "MYP_2025-2030.csv"
    meta_poc = path / "meta" / "MYP_POC.csv"
    meta_section = path / "meta" / "MYP_item_sections.csv"
    output_1 = path / "output" / "MYP_2025-2030_v2.csv"
    output_2 = path / "output" / "MYP_2025-2030_sales.csv"

    # Read data
    df = pd.read_csv(input_file)
    df_poc = pd.read_csv(meta_poc)[["DIV", "BU", "PL"]]
    df_section = pd.read_csv(meta_section)[["row_number", "section"]]

    # Process data
    id_cols = [
        "source",
        "currency",
        "row_number",
        "section",
        "items",
        "DIV",
        "BU",
        "PL",
        "outlets",
    ]
    numeric_cols = [
        "2023",
        "2024",
        "2025",
        "2026",
        "2027",
        "2028",
        "2029",
        "2030",
    ]

    df = (
        df.pipe(add_poc, df_poc)
        .pipe(add_section, df_section)
        .pipe(remove_total_rows)
        .pipe(remove_blank_rows, numeric_cols)
        .pipe(reorder_columns, id_cols)
    )

    df_sales = df[(df["items"] == "Sales (w/o ICO)") & (df["currency"] != "mn EUR")]

    # Write data
    # df.to_csv(output_1, index=False)
    df_sales.to_csv(output_2, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
