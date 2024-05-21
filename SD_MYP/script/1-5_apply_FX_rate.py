import pandas as pd
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Functions
def change_country_name(df):
    """Change country name from South Korea to Korea"""
    columns_to_update = [
        "country_sales_plant",
        "country_prod_plant",
        "country_final_cust",
    ]

    for column in columns_to_update:
        df[column] = df[column].str.replace("South Korea", "Korea")

    return df


def change_fx_rate(df):
    """
    Change FX rate from VT to SG and
    change from k EUR to mn EUR
    """
    columns_to_update = ["sales_BP 2024+9", "sales_Early View 2025+9"]

    for column in columns_to_update:
        df[column] = df[column] * df["VT_fx_rate"] / df["SG_fx_rate"] / 1000

    return df


def main():
    # Filenames
    input_file = path / "output" / "SPOT" / "2024-04-24_BP_2025+9 Div E and P_v2.csv"
    meta_file = path / "meta" / "fx_rates.csv"
    output_file = path / "output" / "SPOT" / "2024-04-24_BP_2025+9 Div E and P_v3.csv"

    # Read data
    df = pd.read_csv(input_file)
    df_meta = pd.read_csv(meta_file)

    # Process data
    df = (
        df.pipe(change_country_name)
        .merge(
            df_meta,
            left_on=["country_sales_plant", "year"],
            right_on=["country", "year"],
            how="left",
        )
        .pipe(change_fx_rate)
    )

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created.")


if __name__ == "__main__":
    main()
