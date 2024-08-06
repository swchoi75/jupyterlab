import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent.parent


# Functions
def read_excel_multiple_sheets(file_path):
    # Get the Excel sheet names
    xls = pd.ExcelFile(file_path)
    wb_sheets = xls.sheet_names[1:7]  # select sheets 2 to 7 (0-indexed in Python)

    # Read multiple Excel sheets
    dataframes = []
    for sheet in wb_sheets:
        df = pd.read_excel(file_path, sheet_name=sheet, skiprows=2, dtype=str)
        dataframes.append(df)

    # Concatenate all dataframes into one
    df = pd.concat(dataframes, ignore_index=True)
    return df


def select_columns(df, selected_columns):
    df = df[selected_columns]
    return df


def change_data_type(df):
    # change from text to float
    columns_to_change = ["입고누계수량", "포장비누계"]
    df[columns_to_change] = df[columns_to_change].astype(float)
    return df


def filter_out_blank(df):
    df = df[df["part_no"] != 0]
    return df


def add_category_column(df, category):
    df["구분"] = category
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
    df_meta = (
        pd.read_excel(meta_file, sheet_name="Master", skiprows=1, usecols="A:J")
        .clean_names(
            strip_underscores=True,
            strip_accents=False,
            case_type="snake",
        )
        .dropna(subset=["업체"])
    )

    # Process data
    df = df.pipe(change_data_type).pipe(filter_out_blank)
    sub_1 = df_meta.pipe(
        select_columns, ["customer_p_n", "mat_type", "div", "업체"]
    ).drop_duplicates()
    sub_2 = df_meta.pipe(
        select_columns, ["casco_part_no", "mat_type", "div", "업체"]
    ).drop_duplicates()

    ## Add master data
    df_1 = (
        df.merge(sub_1, left_on="part_no", right_on="customer_p_n", how="left")
        .drop(columns="customer_p_n")
        .dropna(subset=["업체"])
        .sort_values(by=["업체"])
    ).pipe(add_category_column, "by Customer PN")

    df_2 = (
        df.merge(sub_2, left_on="cae", right_on="casco_part_no", how="left")
        .drop(columns="casco_part_no")
        .dropna(subset=["업체"])
        .sort_values(by=["업체"])
    ).pipe(add_category_column, "by Material")

    ## Join two dataframes
    df = pd.concat([df_1, df_2])

    # Write data
    df.to_csv(output_file, index=False, encoding="utf-8-sig")  # for 한글 표시
    print("A file is created")


if __name__ == "__main__":
    main()
