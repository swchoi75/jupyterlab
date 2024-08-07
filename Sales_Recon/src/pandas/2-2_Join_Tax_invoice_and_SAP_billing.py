import pandas as pd
from pathlib import Path


# Path
path = Path(__file__).parent.parent.parent


# Functions
def main():
    # Filenames
    input_0 = path / "output" / "2-1. Join Customer PN.csv"
    input_1 = path / "output" / "1-1. Tax invoice all.csv"
    input_2 = path / "output" / "1-2. SAP billing summary.csv"
    output_file = path / "output" / "2-2. Join Tax invoice and SAP billing_test.csv"

    # Read data
    df = pd.read_csv(input_0, dtype={"sold_to_party": str})
    df_1 = pd.read_csv(input_1, dtype={"sold_to_party": str})
    df_2 = pd.read_csv(input_2, dtype={"sold_to_party": str})

    # Process data
    ## select columns
    key_columns = ["고객명", "sold_to_party", "customer_pn_rev"]
    value_columns = [
        "입고수량",
        "입고금액",
        "포장비",
        "단가소급",
        "관세정산",
        "sample",
        "glovis_price",
        "서열비",
    ]
    df_1 = df_1.loc[:, key_columns + value_columns]
    df_2 = df_2.loc[:, key_columns + ["qty", "amt"]].dropna()  # Remove missing values

    df = df.merge(df_1, on=key_columns, how="left").merge(
        df_2, on=key_columns, how="left"
    )

    # Write data
    df.to_csv(output_file, index=False, encoding="utf-8-sig")  # for 한글 표시
    print("A file is created")


if __name__ == "__main__":
    main()
