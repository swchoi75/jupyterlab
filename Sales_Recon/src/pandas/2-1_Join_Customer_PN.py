import pandas as pd
from pathlib import Path


# Path
path = Path(__file__).parent.parent.parent


# Functions
def main():
    # Filenames
    input_1 = path / "output" / "1-1. Tax invoice all.csv"
    input_2 = path / "output" / "1-2. SAP billing summary.csv"
    output_file = path / "output" / "2-1. Join Customer PN.csv"

    # Read data
    df_1 = pd.read_csv(input_1, dtype=str)
    df_2 = pd.read_csv(input_2, dtype=str)

    # Process data
    ## select columns
    selected_columns = ["고객명", "sold_to_party", "customer_pn_rev"]
    df_1 = df_1.loc[:, selected_columns]
    df_2 = df_2.loc[:, selected_columns]

    df = (
        pd.merge(df_1, df_2, on=selected_columns, how="outer")
        .drop_duplicates()  # Unique values
        .dropna()  # Remove missing values
    )

    # Write data
    df.to_csv(output_file, index=False, encoding="utf-8-sig")  # for 한글 표시
    print("A file is created")


if __name__ == "__main__":
    main()
