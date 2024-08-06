import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent.parent


# Functions
def read_txt_file(file_path):
    df = pd.read_csv(
        file_path,
        skiprows=3,
        sep="\t",
        encoding="UTF-16LE",
        skipinitialspace=True,
        thousands=",",
        engine="python",
    ).clean_names(strip_accents=False, case_type="snake")

    return df


def remove_columns(df):
    cols_to_remove = ["unnamed_0", "unnamed_1"]
    df = df[[col for col in df.columns if col not in cols_to_remove]]
    return df


def remove_missing_values(df):
    df = df.dropna(subset=["sales_org"])
    return df


def filter_price_missing(df):
    df = df[df["delivery_amount"] == 0]
    return df


def select_columns(df, selected_columns):
    df = df[selected_columns]
    return df


def main():
    # Variables
    year = "2024"
    month = "07"

    # Filenames
    input_1 = (
        path
        / "data"
        / "Sales Delivery Report"
        / f"Delivery report_0180_{year}_{month}.xls"
    )
    input_2 = (
        path
        / "data"
        / "Sales Delivery Report"
        / f"Delivery report_2182_{year}_{month}.xls"
    )
    output_1 = path / "output" / "Sales delivery report.csv"
    output_2 = path / "output" / "Sales price missing.csv"

    # Read data
    df_0180 = read_txt_file(input_1)
    df_2182 = read_txt_file(input_2)

    # Process data
    df = pd.concat([df_0180, df_2182])
    df = df.pipe(remove_columns).pipe(remove_missing_values)
    df_sub = df.pipe(filter_price_missing).pipe(
        select_columns,
        [
            "sold_to_party",
            "customer_material",
            "material_number",
            "material_description",
            "profit_center",
            "delivery_date",
            "mvt_type",
            "quantity",
            "delivery_amount",
            "delivery_number",
            "billing_number",
            "billing_amount",
            "item",
        ],
    )

    # Write data
    df.to_csv(output_1, index=False, encoding="utf-8-sig")  # for 한글 표시
    df_sub.to_csv(output_2, index=False)
    print("Files are created")


if __name__ == "__main__":
    main()
