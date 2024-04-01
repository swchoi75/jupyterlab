import pandas as pd
from masterdata import material_master


def budget_std_costs(df):
    # Get material master data
    mat = material_master().loc[:, ["Product", "Profit Ctr", "Standard Price"]]

    # Merge material master data with budget data
    df = pd.merge(df, mat, on=["Profit Ctr", "Product"], how="left")

    # Calculate standard costs
    df["STD_Costs"] = df["Qty"] * df["Standard Price"]

    # Specify the data types
    df["STD_Costs"] = df["STD_Costs"].astype(float).fillna(0)

    # Drop the standard price column
    df = df.drop(columns=["Standard Price"])

    return df
