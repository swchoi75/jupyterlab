import pandas as pd
from pathlib import Path
from janitor import clean_names

pd.set_option("future.no_silent_downcasting", True)


# Path
path = Path(__file__).parent.parent.parent


# Functions
def select_columns(df, selected_columns):
    df = df[selected_columns]
    return df


def remove_columns(df, cols_to_remove):
    df = df[[col for col in df.columns if col not in cols_to_remove]]
    return df


def remove_missing_values(df):
    df = df.dropna(subset=["customer_pn"])
    return df


def filter_values(df):
    # filter non-zero values
    df = df[df["customer_pn"] != "0"]
    # filter alphanumeric characters (e.g. 한글 제외)
    pattern = r"^[a-zA-Z0-9-_\s]*$"
    df = df[df["customer_pn"].str.match(pattern)]
    return df


def change_data_type(df):
    """change from text or float to integer"""
    # remove NA values
    df["current_price"] = df["current_price"].fillna(0)
    # change data type
    df["current_price"] = df["current_price"].astype(int)
    return df


def get_latest_price(df):

    return df


def main():

    # Variables
    from common_variable import year, month, day

    # Filenames
    input_1 = path / "data" / "SAP" / f"Billing_0180_{year}_{month}.xlsx"
    input_2 = path / "data" / "SAP" / f"Billing_2182_{year}_{month}.xlsx"
    input_3 = path / "data" / "SAP" / f"Price_{year}-{month}-{day}.xls"

    meta_1 = path / "data" / "고객명.csv"
    meta_2 = path / "data" / "공장명.csv"

    output_1 = path / "output" / "1-2. SAP billing summary.csv"
    output_2 = path / "output" / "1-2. price_latest.csv"
    output_3 = path / "output" / "1-2. SAP billing details.csv"

    # Read data
    df_0180 = (
        pd.read_excel(input_1)
        .clean_names(strip_accents=False)
        .dropna(subset=["sales_organization"])
    )

    df_2182 = (
        pd.read_excel(input_2)
        .clean_names(strip_accents=False)
        .dropna(subset=["sales_organization"])
    )

    df_price = (
        pd.read_csv(
            input_3,
            sep="\t",
            encoding="UTF-16LE",
            skipinitialspace=True,
            thousands=",",
            engine="python",
        )
        .clean_names(strip_accents=False)
        .pipe(remove_columns, ["re_st"])
        .rename(
            columns={
                "unit_9": "curr",
                "unit_10": "per_unit",
                "amount": "current_price",
            }
        )
    )

    df_customer = (
        pd.read_csv(meta_1)
        .clean_names(strip_accents=False)
        .pipe(select_columns, ["sold_to_party", "고객명"])
    )

    df_customer_plant = (
        pd.read_csv(meta_2, dtype=str)
        .clean_names(strip_accents=False)
        .pipe(select_columns, ["ship_to_party", "공장명"])
    )

    # Process data
    df_0180["plant"] = "0180"
    df_2182["plant"] = "2182"
    df = pd.concat([df_0180, df_2182])

    df_price = df_price.pipe(change_data_type)

    # Write data
    df.to_csv(output_file, index=False, encoding="utf-8-sig")  # for 한글 표시
    print("Files are created")


if __name__ == "__main__":
    main()
