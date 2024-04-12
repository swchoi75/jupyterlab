import pandas as pd
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Filenames
input_file = path / "output" / "2_fc_acquisition_future_assets.csv"
output_file = path / "output" / "3_fc_acquisition_future_assets_adj.csv"


# Read data
df = pd.read_csv(
    input_file,
    dtype={
        "investment_type": str,
        "financial_statement_item": str,
        "input_cost_center": str,
    },
)


# Process data


# HQ created sub, and we cannot change category of investment
selected_sub = ["IF310241"]


# Split dataframe
df_prj = df[df["sub"].isin(selected_sub)].reset_index().drop(columns="index")
df_remaining = df[~df["sub"].isin(selected_sub)].reset_index().drop(columns="index")


# Manual adjustment
def supplier_tooling(df):
    df["category_of_investment"] = "8"
    df["category_description"] = "Tooling located at supplier"
    df["financial_statement_item"] = "122637000"
    df["fs_item_description"] = "Molds / containers / tooling"
    df["gl_account"] = "K432"
    df["gl_account_description"] = "Depreciation special tools - contr spplr"
    df["fix_var"] = "var"
    df["mv_type"] = "211 Depr. of tools"
    df["useful_life_year"] = 4  # important change
    return df


df_prj = supplier_tooling(df_prj)


# Concatenate dataframe
df = pd.concat([df_prj, df_remaining])


# Write data
df.to_csv(output_file, index=False)
print("A file is created")
