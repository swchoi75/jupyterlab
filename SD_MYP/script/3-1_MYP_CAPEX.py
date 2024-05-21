import pandas as pd
from pathlib import Path
from janitor import clean_names


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
            usecols="A:T",
            dtype={
                "Act 2023": float,
                "FC 2024": float,
                "2025": float,
                "2026": float,
                "2027": float,
                "2028": float,
                "2029": float,
                "2030": float,
            },
        )
        for sheet_name in list_of_sheets
    ]

    # Add a new column with filename to each DataFrame
    for i, df in enumerate(dataframes):
        df["country"] = list_of_sheets[i]

    # Merge the list of DataFrames into a single DataFrame
    df = pd.concat(dataframes)

    return df


def remove_blank_rows(df, numeric_cols):
    # add "check" column that adds the specified numeric columns
    df["check"] = df[numeric_cols].sum(axis="columns")
    # remove blank rows
    df = df[df["check"] != 0]
    # remove the temporary "check" column
    df = df.drop(columns=["check"])
    return df


def pivot_longer(df, key_cols, value_cols):
    # Melt the dataframe
    df = df.melt(
        id_vars=key_cols, value_vars=value_cols, var_name="year", value_name="k_LC"
    )
    return df


def drop_missing_or_zero_values(df):
    # drop missing values
    df = df.dropna(subset="k_LC")
    # drop zero values
    df = df[df["k_LC"] != 0]
    return df


def add_col_in_EUR(df):
    df["k_GC"] = df["k_LC"] / df["SG_fx_rate"]
    return df


def reorder_columns(df):
    first_columns = ["country", "currency"]
    last_columns = ["k_LC", "k_GC"]
    df = df[
        first_columns
        + [col for col in df.columns if col not in first_columns + last_columns]
        + last_columns
    ]
    return df


def main():
    # Filenames
    input_file = path / "data" / "MYP_investment_spending.xlsm"
    meta_file = path / "meta" / "fx_rates.csv"
    output_file = path / "output" / "CAPEX" / "MYP_CAPEX.csv"

    # Read data
    list_of_sheets = [
        "Korea",
        # "India",
        "Japan",
        "Thailand",
    ]
    df = (
        concat_sheet(input_file, list_of_sheets)
        .clean_names()
        .rename(
            columns={"loc_": "loc", "rou": "RoU", "act_2023": "2023", "fc_2024": "2024"}
        )
    )
    df_meta = pd.read_csv(meta_file, dtype={"year": str})

    # Process data
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
    id_cols = [col for col in df.columns if col not in numeric_cols]

    df = (
        df.dropna(subset="currency")
        .pipe(remove_blank_rows, numeric_cols)
        .pipe(pivot_longer, id_cols, numeric_cols)
        .pipe(drop_missing_or_zero_values)
        .merge(df_meta, on=["currency", "year"])
        .pipe(add_col_in_EUR)
        .pipe(reorder_columns)
    )

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created.")


if __name__ == "__main__":
    main()
