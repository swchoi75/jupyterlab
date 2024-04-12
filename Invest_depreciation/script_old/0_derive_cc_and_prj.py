import pandas as pd
import numpy as np
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent


# Filenames
input_1 = path / "meta_cc" / "IMPR.XLS"
input_2 = path / "meta_cc" / "IMZO.XLS"
input_3 = path / "meta_cc" / "PRPS.XLS"
input_4 = path / "meta_cc" / "OARP.XLS"

output_1 = path / "meta" / "project_for_assets_temp.csv"
output_2 = path / "meta" / "project_for_assets.csv"
output_3 = path / "meta" / "cost_centers.csv"


# Read data
def read_tsv_file(filename, skiprows):
    df = pd.read_csv(
        filename,
        sep="\t",
        skiprows=skiprows,
        usecols=lambda col: col != 0,  # skip the first column
        encoding="UTF-16LE",
        skipinitialspace=True,
        dtype="str",
    )
    return df


df_1 = read_tsv_file(input_1, skiprows=3)
df_2 = read_tsv_file(input_2, skiprows=3)
df_3 = read_tsv_file(input_3, skiprows=3)
df_4 = read_tsv_file(input_4, skiprows=7)


# Process further for OARP data
def preprocess_dataframe(df):
    df = df.rename(
        columns={
            "Year": "asset_no",
            "Unnamed: 3": "sub_no",
            "Unnamed: 7": "asset_description",
            "Object": "wbs_element",
            "Text": "amount",
        }
    )
    df["wbs_element"] = df["wbs_element"].bfill()  # .fillna(method="bfill")
    df = df.dropna(subset=["sub_no"])
    df = df[df["asset_no"] != "Asset"]
    return df


df_4 = preprocess_dataframe(df_4)


# Select columns
df_1 = df_1[["Position ID", "InvProgPos"]]  # "IMPR.XLS"
df_2 = df_2[["InvProgPos", "OBJNR"]]  # "IMZO.XLS"
df_3 = df_3[["Object number", "Short Identification", "Req. CC"]].dropna(
    subset="Object number"
)  # "PRPS.XLS"
df_4 = df_4[["asset_no", "sub_no", "asset_description", "wbs_element", "amount"]]

# Join dataframes
df = df_1.merge(df_2, how="left", on="InvProgPos").merge(
    df_3, how="left", left_on="OBJNR", right_on="Object number"
)


# Select and Rename columns
df = df[["Position ID", "Short Identification", "Req. CC"]]
df = df.rename(
    columns={
        "Position ID": "sub",
        "Short Identification": "wbs_element",
        "Req. CC": "cost_center",
    }
)


# Drop missing value & drop duplicates
df = df.dropna(subset=["wbs_element"]).drop_duplicates()


# Process OARP to link asset no to GPA sub master

# Create fallback scenarios
df_4["fallback_wbs_element_1"] = df_4["wbs_element"].str[:-1] + "0"
df_4["fallback_wbs_element_2"] = df_4["wbs_element"].str[:-2] + "00"
df_4["fallback_wbs_element_3"] = df_4["wbs_element"].str[:-3] + "000"

# Merge with fallback scenarios
result0 = pd.merge(df, df_4, left_on="wbs_element", right_on="wbs_element", how="left")
result1 = pd.merge(
    df, df_4, left_on="wbs_element", right_on="fallback_wbs_element_1", how="left"
)
result2 = pd.merge(
    df, df_4, left_on="wbs_element", right_on="fallback_wbs_element_2", how="left"
)
result3 = pd.merge(
    df, df_4, left_on="wbs_element", right_on="fallback_wbs_element_3", how="left"
)

# Combine the results
result = pd.concat([result0, result1, result2, result3], ignore_index=True)
result = result.dropna(subset=["asset_no"])

# Fill in missing values on column "wbs_element"
result["wbs_element"] = np.where(
    pd.isna(result["wbs_element"]), result["wbs_element_y"], result["wbs_element"]
)

# Select columns
selected_columns = [
    "sub",
    "wbs_element",
    # "cost_center",
    "asset_no",
    "sub_no",
    "asset_description",
    "amount",
]
df_prj = result[selected_columns]

# Drop duplicates
df_prj = df_prj.drop_duplicates(
    subset=[
        # "sub",  # To remove several fallback scenarios are working
        "wbs_element",
        "asset_no",
        "sub_no",
        "asset_description",
        "amount",
    ]
)


# Business Logic : handle the multiple cost centers


def preprocess_dataframe(df):
    # Remove leading zeroes from "cost center" column
    df["cost_center"] = df["cost_center"].str.lstrip("0")
    # Select columns
    df = df[["sub", "cost_center"]]
    # Drop missing value & drop duplicates
    df = df.dropna().drop_duplicates()
    return df


df = preprocess_dataframe(df)

# Group by sub and join the values with a comma
df_cc = (
    df.groupby("sub")["cost_center"]
    .apply(lambda x: ", ".join(x.astype(str)))
    .reset_index()
)


# Write data
# result.to_csv(output_1, index=False)
df_prj.to_csv(output_2, index=False)
df_cc.to_csv(output_3, index=False)
print("Files are created")
