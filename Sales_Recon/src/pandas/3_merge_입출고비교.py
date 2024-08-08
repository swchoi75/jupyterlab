import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent.parent


# Functions
def read_excel_multiple_sheets(file_path):
    # Get the Excel sheet names
    xls = pd.ExcelFile(file_path)
    wb_sheets = xls.sheet_names[0:11]  # select sheets 1 to 11 (0-indexed in Python)

    # Read multiple Excel sheets
    dataframes = []
    for sheet in wb_sheets:
        df = pd.read_excel(
            file_path, sheet_name=sheet, skiprows=3, usecols="A:AI", dtype=str
        )
        dataframes.append(df)

    # Concatenate all dataframes into a single dataframe
    df = pd.concat(dataframes, keys=wb_sheets, names=["sheet", "row"])
    return df


def clean_column_names(df):
    df.columns = df.columns.str.replace("\n", "")
    df = df.clean_names(strip_accents=False, strip_underscores=True, case_type="snake")
    return df


def remove_unnecessary_row(df):
    # remove missing values & remove zero values
    df = df.dropna(subset="고객명")
    df = df[df["고객명"] != "0"]
    # remove "Kappa HEV ADJ"
    df = df[~df["customer_pn"].str.startswith("A2C")]
    return df


def format_price_list(df, list_of_cols):
    # select columns
    df = df.loc[:, list_of_cols]
    # filter values
    df = df[df["고객명"] != "MOBIS AS"]
    # remove missing values & remove zero values
    df = df.dropna(subset=["plant", "profit_center"])
    df = df[(df["plant"] != "0") & (df["profit_center"] != "0")]
    # sort data
    df = df.sort_values(by=["profit_center", "customer_pn_rev"])
    return df


def format_price_diff(df, list_of_cols):
    df = df.loc[:, list_of_cols]  # select columns
    return df


def format_uninvoiced_qty(df, list_of_cols):
    # select columns
    df = df.loc[:, list_of_cols]
    # change data type from text to float
    df["조정q"] = df["조정q"].astype(float)
    # remove missing values & remove zero values
    df = df.dropna(subset="조정q")
    df = df[df["조정q"] != 0]
    # add columns
    df["order_type"] = df.apply(
        lambda row: ("ZOR" if row["조정q"] > 0 else "ZRE"),
        axis="columns",
    )
    df["order_reason"] = "C02"
    df["adj_qty"] = abs(df["조정q"])
    df["이월체크"] = "X"
    return df


def format_uninvoiced_amt(df, list_of_cols):
    # select columns
    df = df.loc[:, list_of_cols]
    # pivot data longer
    df = (
        df.melt(
            id_vars=[
                "plant",
                "고객명",
                "sold_to",
                "ship_to",
                "mlfb",
                "customer_pn_rev",
                "customer_pn",
                "div",
                "profit_center",
                "comment",
            ],
            value_vars=[
                "조정금액",
                "포장비",
                "단가소급",
                "관세정산",
                "환율차이",
                "서열비",
                "sample",
            ],
            var_name="key",
            value_name="value",
        )
        .reset_index()
        .drop(columns="index")
    )
    # change data type from text to float
    df["value"] = df["value"].astype(float)
    # remove missing values & remove zero values
    df = df.dropna(subset="value")
    df = df[df["value"] != 0]
    # add columns
    df["order_type"] = df.apply(
        lambda row: ("ZDR" if row["value"] > 0 else "ZCR"),
        axis="columns",
    )
    df["order_type"] = df.apply(map_key_to_order_type, axis="columns")
    df["order_reason"] = "C02"
    df["adj_amt"] = abs(df["value"])
    df["이월체크"] = ""
    return df


def map_key_to_order_type(row):
    key_to_order_type = {
        "조정금액": "A02",
        "단가소급": "A03",
        "관세정산": "A11",
        "환율차이": "A04",
        "서열비": "A12",
    }
    return key_to_order_type.get(row["key"], "Unknown")


def main():

    # Variables
    from common_variable import year, month

    # Filenames
    input_file = path / "report in Excel" / f"{year}-{month} 입출고 비교.xlsx"

    output_0 = path / "output" / "입출고비교 all_test.csv"
    output_1 = path / "output" / "입출고비교 to Price list_test.csv"
    output_2 = path / "output" / "입출고비교 to Price diff_test.csv"
    output_3 = path / "output" / "입출고비교 to Adj_Qty_test.csv"
    output_4 = path / "output" / "입출고비교 to Adj_Amt_test.csv"

    # Read data
    df = read_excel_multiple_sheets(input_file).pipe(clean_column_names)

    # Process data
    df = df.pipe(remove_unnecessary_row).reset_index().drop(columns="row")

    columns_for_price_list = [
        "plant",
        "고객명",
        "sold_to",
        "공장명",
        "customer_pn_rev",
        "customer_pn",
        "mlfb",
        "div",
        "profit_center",
        "tax_invoice",
        "price_type",
        "sap_price",
        "입고q",
    ]
    price_list = format_price_list(df, columns_for_price_list)

    columns_for_price_diff = [
        "sold_to",
        "고객명",
        "customer_pn_rev",
        "customer_pn",
        "mlfb",
        "profit_center",
        "입고q",
        "출고q",
        "조정q",
        "tax_invoice",
        "price_type_before_adj",
        "billing_price_before_adj",
        "price_type",
        "sap_price",
        "입고금액_jj_수량*단가",
        "출고금액",
        "조정금액",
        "단가소급",
        "관세정산",
        "환율차이",
        "서열비",
    ]
    price_diff = format_price_diff(df, columns_for_price_diff)

    colummns_for_uninvoiced_qty = [
        "plant",
        "고객명",
        "sold_to",
        "ship_to",
        "mlfb",
        "customer_pn_rev",
        "customer_pn",
        "div",
        "profit_center",
        "조정q",
        "sap_price",
        "comment",
    ]
    adj_qty = format_uninvoiced_qty(df, colummns_for_uninvoiced_qty)

    colummns_for_uninvoiced_amt = [
        # id_vars
        "plant",
        "고객명",
        "sold_to",
        "ship_to",
        "mlfb",
        "customer_pn_rev",
        "customer_pn",
        "div",
        "profit_center",
        "comment",
        # value_vars
        "조정금액",
        "포장비",
        "단가소급",
        "관세정산",
        "환율차이",
        "서열비",
        "sample",
    ]
    adj_amt = format_uninvoiced_amt(df, colummns_for_uninvoiced_amt)

    # Write data
    ## encoding="utf-8-sig" for 한글 표시
    df.to_csv(output_0, index=False, encoding="utf-8-sig")
    price_list.to_csv(output_1, index=False, encoding="utf-8-sig")
    price_diff.to_csv(output_2, index=False, encoding="utf-8-sig")
    adj_qty.to_csv(output_3, index=False, encoding="utf-8-sig")
    adj_amt.to_csv(output_4, index=False, encoding="utf-8-sig")

    print("Files are created")


if __name__ == "__main__":
    main()
