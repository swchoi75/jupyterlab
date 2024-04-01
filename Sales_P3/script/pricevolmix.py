import pandas as pd


def join_with_cm_ratio(df):
    # Read Budget Contribution Margin ratio from file
    bud_cm_ratio = pd.read_csv(
        "meta/Budget Contribution Margin ratio.csv", usecols=["Profit Ctr", "CM ratio"]
    )

    # Join on Profit Ctr
    df = df.merge(bud_cm_ratio, on="Profit Ctr", how="left")

    return df


def delta_impact(df):
    # Compute delta Sales
    df["delta Sales"] = df.apply(
        lambda row: row["Sales_LC"] if row["Version"] == "Actual" else -row["Sales_LC"],
        axis=1,
    )

    # Compute delta Sales Price
    df["delta Sales Price"] = df.apply(
        lambda row: row["price_impact"]
        if row["Version"] == "Actual"
        else -row["price_impact"],
        axis=1,
    )

    # Compute delta Sales Volume
    df["delta Sales Volume"] = df["delta Sales"] - df["delta Sales Price"]

    # Compute delta Margin
    df["delta Margin"] = df.apply(
        lambda row: (row["Sales_LC"] - row["STD_Costs"])
        if row["Version"] == "Actual"
        else -(row["Sales_LC"] - row["STD_Costs"]),
        axis=1,
    )

    # Compute delta Margin Price
    df["delta Margin Price"] = df["price_impact"]

    # Compute delta Margin Volume
    df["delta Margin Volume"] = df["delta Sales Volume"] * df["CM ratio"] / 100

    # Compute delta Margin Mix
    df["delta Margin Mix"] = (
        df["delta Margin"] - df["delta Margin Price"] - df["delta Margin Volume"]
    )

    return df
