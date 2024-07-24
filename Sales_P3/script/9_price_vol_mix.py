import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent


# Functions
def add_cm_ratio(df, df_meta):
    df = df.merge(df_meta, on="profit_ctr", how="left")
    return df


def add_delta_sales(df):
    df["delta_sales"] = df.apply(
        lambda row: row["sales_lc"] if row["version"] == "Actual" else -row["sales_lc"],
        axis=1,
    )
    return df


def add_delta_sales_price(df):
    df["delta_sales_price"] = df.apply(
        lambda row: (
            row["price_impact"] if row["version"] == "Actual" else -row["price_impact"]
        ),
        axis=1,
    )
    return df


def add_delta_sales_vol(df):
    df["delta_sales_volume"] = df["delta_sales"] - df["delta_sales_price"]
    return df


def add_delta_margin(df):
    df["delta_margin"] = df.apply(
        lambda row: (
            (row["sales_lc"] - row["std_costs"])
            if row["version"] == "Actual"
            else -(row["sales_lc"] - row["std_costs"])
        ),
        axis=1,
    )
    return df


def add_delta_margin_price(df):
    df["delta_margin_price"] = df["price_impact"]
    return df


def add_delta_margin_vol(df):
    df["delta_margin_volume"] = df["delta_sales_volume"] * df["cm_ratio"] / 100
    return df


def add_delta_margin_mix(df):
    df["delta_margin_mix"] = (
        df["delta_margin"] - df["delta_margin_price"] - df["delta_margin_volume"]
    )
    return df


def main():

    # Filenames
    input_file = path / "output" / "8_ytd_sales_price_impact.csv"
    meta_file = path / "meta" / "Budget Contribution Margin ratio.csv"
    output_file = path / "output" / "9_ytd_sales_p3_impact.csv"

    # Read data
    df = pd.read_csv(input_file)
    df_meta = pd.read_csv(meta_file, usecols=["Profit Ctr", "CM ratio"]).clean_names()

    # Process data
    df = (
        df.pipe(add_cm_ratio, df_meta)
        .pipe(add_delta_sales)
        .pipe(add_delta_sales_price)
        .pipe(add_delta_sales_vol)
        .pipe(add_delta_margin)
        .pipe(add_delta_margin_price)
        .pipe(add_delta_margin_vol)
        .pipe(add_delta_margin_mix)
    )

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
