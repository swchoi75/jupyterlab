import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent


# Functions
def act_price(row):
    if row["version"] == "Budget":
        return 0
    if row["qty"] == 0:
        return 0
    else:
        return round(row["sales_lc"] / row["qty"], 0)


def price_diff(row):
    if row["qty"] == 0:
        return 0
    else:
        return row["act_price"] - row["bud_price"]


def price_impact(row):
    if row["cm_cluster"] == "OES":
        return 0
    elif row["product"] == "A2C1797520201" or row["product"] == "A2C1636530101":
        # Kappa HEV adjustment is volume compensation, thus has no price impact
        return 0
    elif row["qty"] == 0:
        return row["sales_lc"]
    else:
        return row["qty"] * row["price_diff"]


def calculate_price_impact(df):
    df["act_price"] = df.apply(act_price, axis="columns")
    df["price_diff"] = df.apply(price_diff, axis="columns")
    df["price_impact"] = df.apply(price_impact, axis="columns")
    df["price_impact_ratio"] = df["price_impact"] / df["sales_lc"] * 100
    return df


def replace_missing_values(df):
    # Replace missing values with zero for numeric columns
    df = df.fillna(
        {
            "qty": 0,
            "sales_lc": 0,
            "std_costs": 0,
            "standard_price": 0,
            "bud_price": 0,
            "act_price": 0,
            "price_diff": 0,
            "price_impact": 0,
            "price_impact_ratio": 0,
        }
    )
    return df


def main():

    # Filenames
    input_file = path / "output" / "6_ytd_sales_spv_mapping.csv"
    output_file = path / "output" / "7_ytd_sales_price_impact.csv"

    # Read data
    df = pd.read_csv(input_file)

    # Process data
    df = df.pipe(calculate_price_impact).pipe(replace_missing_values)

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
