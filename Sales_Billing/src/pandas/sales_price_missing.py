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
        encoding="UTF-16LE",
        dtype=str,
    ).clean_names()

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
    output_1 = path / "output" / "Sales delivery report_test.csv"
    output_2 = path / "output" / "Sales price missing_test.csv"

    # Read data
    df_0180 = read_txt_file(input_1)
    df_2182 = read_txt_file(input_2)

    # Process data
    df = pd.concat([df_0180, df_2182])

    # Write data
    df.to_csv(output_1, index=False, encoding="utf-8-sig")
    # df_sub.to_csv(output_2, index=False)
    print("Files are created")


if __name__ == "__main__":
    main()
