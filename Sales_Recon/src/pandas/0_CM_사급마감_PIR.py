import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent.parent


# Functions
def rename_columns(df):
    df = df.rename(columns={
        "":"pir_net_price",
        "":"sa_net_price",
        "":"unit",
        "":"sa_valid_from",
        "":"pir_valid_from",
    })

    return df


def main():

    # Variables
    from common_variable import year, month

    # Filenames
    input_file = path / "data" / "VAN CM" / f"CAE VAN ALL {year}{month}.xlsx"
    meta_file = path / "meta" / "사급업체 품번 Master.xlsx"
    output_file = path / "data" / "VAN CM" / "result.csv"

    # Read data
    df = read_excel_multiple_sheets(input_file).clean_names(strip_accents=False)

    # Process data

    # Write data
    df.to_csv(output_file, index=False, encoding="utf-8-sig")  # for 한글 표시
    print("A file is created")


if __name__ == "__main__":
    main()
