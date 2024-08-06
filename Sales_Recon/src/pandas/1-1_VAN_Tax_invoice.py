import pandas as pd
from pathlib import Path
from janitor import clean_names

pd.set_option("future.no_silent_downcasting", True)


# Path
path = Path(__file__).parent.parent.parent


# Functions
def read_excel_multiple_sheets(file_path):
    # Get the Excel sheet names
    xls = pd.ExcelFile(file_path)
    wb_sheets = xls.sheet_names[0:23]  # select sheets 1 to 23 (0-indexed in Python)

    # Read multiple Excel sheets
    dataframes = []
    for sheet in wb_sheets:
        df = pd.read_excel(
            file_path,
            sheet_name=sheet,
            skiprows=2,
            usecols="A:P",
            dtype={
                "Sold-to Party": str,
                "Ship-to Party": str,
                "Customer PN rev": str,
                "Customer PN": str,
            },
        )
        dataframes.append(df)

    # Concatenate all dataframes into one
    df = pd.concat(dataframes, ignore_index=True)
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
    columns_to_change = [
        "입고수량",
        "입고금액",
        "포장비",
        "단가소급",
        "관세정산",
        "sample",
        "glovis_price",
        "서열비",
    ]
    # remove NA values
    df[columns_to_change] = df[columns_to_change].fillna(0)
    # change data type
    df[columns_to_change] = df[columns_to_change].astype(int)
    return df


def main():

    # Variables
    from common_variable import year, month

    # Filenames
    input_file = (
        path / "data" / "VAN VT" / f"{year}{month}" / f"_{year}{month} Tax invoice.xlsx"
    )
    output_file = path / "output" / "1-1. Tax invoice all.csv"

    # Read data
    df = read_excel_multiple_sheets(input_file).clean_names(strip_accents=False)

    # Process data
    df = df.pipe(remove_missing_values).pipe(filter_values).pipe(change_data_type)

    # Write data
    df.to_csv(output_file, index=False, encoding="utf-8-sig")  # for 한글 표시
    print("A file is created")


if __name__ == "__main__":
    main()
