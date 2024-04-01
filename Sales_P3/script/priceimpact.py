def calculate_price_impact(df):
    df["act_price"] = df.apply(act_price, axis="columns")
    df["price_diff"] = df.apply(price_diff, axis="columns")
    df["price_impact"] = df.apply(price_impact, axis="columns")
    df["price_impact_ratio"] = df["price_impact"] / df["Sales_LC"] * 100
    return df


def act_price(row):
    if row["Version"] == "Budget":
        return 0
    if row["Qty"] == 0:
        return 0
    else:
        return round(row["Sales_LC"] / row["Qty"], 0)


def price_diff(row):
    if row["Qty"] == 0:
        return 0
    else:
        return row["act_price"] - row["bud_price"]


def price_impact(row):
    if row["CM Cluster"] == "OES":
        return 0
    elif row["Product"] == "A2C1797520201" or row["Product"] == "A2C1636530101":
        # Kappa HEV adjustment is volume compensation, thus has no price impact
        return 0
    elif row["Qty"] == 0:
        return row["Sales_LC"]
    else:
        return row["Qty"] * row["price_diff"]


def replace_missing_values(df):
    # Replace missing values with zero for numeric columns
    df = df.fillna(
        {
            "Qty": 0,
            "Sales_LC": 0,
            "STD_Costs": 0,
            "Standard Price": 0,
            "bud_price": 0,
            "act_price": 0,
            "price_diff": 0,
            "price_impact": 0,
            "price_impact_ratio": 0,
        }
    )
    return df
