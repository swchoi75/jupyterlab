import pandas as pd
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Functions
def aggregate_data(df, selected_columns):
    df = df[selected_columns].groupby(selected_columns).first().reset_index()
    return df


def filter_data(df, selected_productlines):
    df = df[df["productline"].isin(selected_productlines)]
    return df


def process_financial_year(df):
    # rename year values
    df["fy"] = "Act " + df["fy"].astype(str)
    df["fy"] = df["fy"].str.replace("Act 2024", "YTD Act 2024")
    return df


def combine_id_cols(df, two_id_columns):
    # concatenate columns
    df["key_id"] = df[two_id_columns[0]] + "_" + df[two_id_columns[1]]
    # reorder columns
    df = df[["key_id"] + [col for col in df.columns if col not in ["key_id"]]]
    return df


def main():

    # Filenames
    input_file_1 = path / "data" / "Sales with representative PN.csv"
    input_file_2 = path / "data" / "fx_rates_HMG_actual.csv"

    output_file_1 = path / "output" / "power_bi_dimensions.csv"
    output_file_2 = path / "output" / "fx_rates_HMG_by_Quarter.csv"

    # Read data
    df_1 = pd.read_csv(input_file_1)
    df_2 = pd.read_csv(input_file_2)

    # Process data

    # Power BI Dimensions
    selected_columns = ["fy", "productline", "product_hierarchy", "PH_description"]
    selected_productlines = ["PL DTC", "PL ENC", "PL MTC"]
    two_id_columns = ["fy", "product_hierarchy"]

    df_1 = (
        df_1.pipe(aggregate_data, selected_columns)
        .pipe(filter_data, selected_productlines)
        .pipe(process_financial_year)
        .pipe(combine_id_cols, two_id_columns)
    )

    # HMG FX rate tables per Year / Quarter
    selected_columns = ["cur", "year", "quarter", "fx_HMG"]
    df_2 = df_2.pipe(aggregate_data, selected_columns)

    # Write data
    df_1.to_csv(output_file_1, index=False)
    df_2.to_csv(output_file_2, index=False)
    print("files are created")


if __name__ == "__main__":
    main()
