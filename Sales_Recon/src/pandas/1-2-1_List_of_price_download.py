import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent.parent


# Functions
def main():

    # Variables
    from common_variable import year, month

    # Filenames
    input_1 = path / "data" / "SAP" / f"Billing_0180_{year}_{month}.xlsx"
    input_2 = path / "data" / "SAP" / f"Billing_2182_{year}_{month}.xlsx"
    output_file = path / "output" / "1-2-1. list of price download_test.csv"

    # Read data
    df_0180 = pd.read_excel(input_1).clean_names(strip_accents=False)
    df_2182 = pd.read_excel(input_2).clean_names(strip_accents=False)

    # Process data
    df = (
        pd.concat([df_0180, df_2182])
        .loc[:,"material_number"]  # select column
        .drop_duplicates()
    )

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
