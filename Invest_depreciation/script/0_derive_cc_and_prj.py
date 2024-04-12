import pandas as pd
import numpy as np
from pathlib import Path
from janitor import clean_names


# Path
try:
    path = Path(__file__).parent.parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent.parent


# Functions
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


def preprocess_OARP(df):
    """Process further for OARP data"""
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


def select_columns_dfs(df_1, df_2, df_3, df_4):
    df_1 = df_1[["Position ID", "InvProgPos"]]  # "IMPR.XLS"
    df_2 = df_2[["InvProgPos", "OBJNR"]]  # "IMZO.XLS"
    df_3 = df_3[["Object number", "Short Identification", "Req. CC"]].dropna(
        subset="Object number"
    )  # "PRPS.XLS"
    df_4 = df_4[["asset_no", "sub_no", "asset_description", "wbs_element", "amount"]]

    return df_1, df_2, df_3, df_4


def join_dataframes(df_1, df_2, df_3):
    df = df_1.merge(df_2, how="left", on="InvProgPos").merge(
        df_3, how="left", left_on="OBJNR", right_on="Object number"
    )
    return df


def select_and_rename_cols(df):
    df = df[["Position ID", "Short Identification", "Req. CC"]]
    df = df.rename(
        columns={
            "Position ID": "sub",
            "Short Identification": "wbs_element",
            "Req. CC": "cost_center",
        }
    )
    return df


def validate_wbs_element(df):
    """Drop missing value & drop duplicates"""
    df = df.dropna(subset=["wbs_element"]).drop_duplicates()
    return df


# Process OARP to link asset no to GPA sub master


def fallback_wbs_elements(df):
    """Create fallback scenarios for OARP"""
    df["fallback_wbs_element_1"] = df["wbs_element"].str[:-1] + "0"
    df["fallback_wbs_element_2"] = df["wbs_element"].str[:-2] + "00"
    df["fallback_wbs_element_3"] = df["wbs_element"].str[:-3] + "000"
    return df


def apply_fallback_scenarios(df, df_4):
    # Merge with fallback scenarios
    result0 = pd.merge(
        df, df_4, left_on="wbs_element", right_on="wbs_element", how="left"
    )
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
    return result


def select_columns(df):
    selected_columns = [
        "sub",
        "wbs_element",
        # "cost_center",
        "asset_no",
        "sub_no",
        "asset_description",
        "amount",
    ]
    df = df[selected_columns]
    return df


def drop_duplicates(df):
    df = df.drop_duplicates(
        subset=[
            # "sub",  # To remove several fallback scenarios are working
            "wbs_element",
            "asset_no",
            "sub_no",
            "asset_description",
            "amount",
        ]
    )
    return df


# Business Logic : handle the multiple cost centers


def preprocess_dataframe(df):
    # Remove leading zeroes from "cost center" column
    df["cost_center"] = df["cost_center"].str.lstrip("0")
    # Select columns
    df = df[["sub", "cost_center"]]
    # Drop missing value & drop duplicates
    df = df.dropna().drop_duplicates()
    return df


def handle_multiple_cc(df):
    # Group by sub and join the values with a comma
    df = (
        df.groupby("sub")["cost_center"]
        .apply(lambda x: ", ".join(x.astype(str)))
        .reset_index()
    )
    return df


def main():

    # Filenames
    input_1 = path / "meta_cc" / "IMPR.XLS"
    input_2 = path / "meta_cc" / "IMZO.XLS"
    input_3 = path / "meta_cc" / "PRPS.XLS"
    input_4 = path / "meta_cc" / "OARP.XLS"

    output_prj = path / "meta" / "project_for_assets.csv"
    output_cc = path / "meta" / "cost_centers.csv"

    # Read data
    df_1 = read_tsv_file(input_1, skiprows=3)
    df_2 = read_tsv_file(input_2, skiprows=3)
    df_3 = read_tsv_file(input_3, skiprows=3)
    df_4 = read_tsv_file(input_4, skiprows=7)

    # Process data
    df_4 = preprocess_OARP(df_4)
    df_1, df_2, df_3, df_4 = select_columns_dfs(df_1, df_2, df_3, df_4)
    df = (
        join_dataframes(df_1, df_2, df_3)
        .pipe(select_and_rename_cols)
        .pipe(validate_wbs_element)
    )
    df_oarp = fallback_wbs_elements(df_4)
    df_prj = (
        apply_fallback_scenarios(df, df_oarp).pipe(select_columns).pipe(drop_duplicates)
    )
    df_cc = df.pipe(preprocess_dataframe).pipe(handle_multiple_cc)

    # Write data
    df_prj.to_csv(output_prj, index=False)
    df_cc.to_csv(output_cc, index=False)
    print("Files are created")


if __name__ == "__main__":
    main()
