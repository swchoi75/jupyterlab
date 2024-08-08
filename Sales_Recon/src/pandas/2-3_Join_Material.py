import pandas as pd
from pathlib import Path


# Path
path = Path(__file__).parent.parent.parent


# Functions
def unite_columns(df):
    df["temp"] = df["고객명"] + "_" + df["sold_to_party"] + "_" + df["customer_pn_rev"]
    return df


def main():
    # Filenames
    input_0 = path / "output" / "2-2. Join Tax invoice and SAP billing.csv"
    input_1 = path / "output" / "1-1. Tax invoice all.csv"
    input_2 = path / "output" / "1-2. SAP billing summary.csv"
    output_file = path / "output" / "2-3. 입출고 비교.csv"

    # Read data
    df = pd.read_csv(input_0, dtype=str)
    df_1 = pd.read_csv(input_1, dtype=str)
    df_2 = pd.read_csv(input_2, dtype=str)

    # Process data
    key_columns = ["고객명", "sold_to_party", "customer_pn_rev"]

    df_1 = df_1.loc[:, key_columns + ["공장명", "ship_to_party"]].pipe(unite_columns)
    df_2 = df_2.drop(columns=["qty", "amt", "avg_billing_price"]).pipe(unite_columns)

    df_1a = df_1.drop_duplicates(subset="temp").drop(columns="temp")
    df_2a = df_2.drop_duplicates(subset="temp").drop(columns="temp")
    df_2b = df_2[df_2.duplicated(subset="temp")].drop(columns="temp")

    df = df.merge(df_2a, on=key_columns, how="left").merge(
        df_1a, on=key_columns, how="left"
    )

    df = pd.concat([df, df_2b]).sort_values(
        by=[
            "고객명",
            "sold_to_party",
            "customer_pn_rev",
            "plant",
            "profit_center",
            "material_number",
        ]
    )

    # Write data
    df.to_csv(output_file, index=False, encoding="utf-8-sig")  # for 한글 표시
    print("A file is created")


if __name__ == "__main__":
    main()
