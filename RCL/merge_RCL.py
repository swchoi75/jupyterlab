import pandas as pd
import os
from pathlib import Path
from janitor import clean_names


# Path
path = Path.cwd()
data_path = path / "data/"


# Budget FX rate
bud_fx = 1329  # Budget FX rate in 2023


# Input data: List of multiple text files
xls_files = [
    file for file in data_path.iterdir() if file.is_file() and file.suffix == ".xlsm"
]


def merge_rcl(list_of_files):
    df = read_multiple_files(list_of_files)
    df = wrangle_dataframe(df)
    return df


def read_multiple_files(list_of_files):
    dataframes = [
        pd.read_excel(
            file,
            sheet_name="RCL Input GC",
            skiprows=4,
            dtype={"Item Structure": str, "Outlet": str, "Plant": str},
        )
        for file in list_of_files
    ]

    # Add a new column with filename to each DataFrame
    for i, df in enumerate(dataframes):
        df["source"] = list_of_files[i].stem

    # Merge the list of DataFrames into a single DataFrame
    df = pd.concat(dataframes)

    return df


def wrangle_dataframe(df):
    df = (df
          .pipe(rename_columns)
          .pipe(clean_names)
          .pipe(convert_currency)
          .pipe(extract_poc)
          .pipe(join_with_poc)
          .pipe(add_key_column)
          )
    return df


def rename_columns(df):
    df = df.drop(columns=df.loc[:, "ACT Period GC":"FC GC fx impact"])
    df = df.rename(
        columns={
            "Single Period BUD GC": "Period Plan",
            "Single Period ACT GC @ BUD fx": "Period Act",
            "Single Period ACT-BUD GC @ BUD fx": "Delta Period",
            "Comment Single Period ACT-BUD Period": "Comments Period",
            "YTD BUD GC": "YTD Plan",
            "YTD ACT GC @ BUD fx": "YTD Act",
            "YTD ACT-BUD GC @ BUD fx": "Delta YTD",
            "Comment YTD ACT-BUD": "Comments YTD",
            "FY BUD GC": "Plan",
            "FC GC @ BUD fx": "FC",
            "FC - BUD GC @ BUD fx": "Delta to Plan",
            "Comment FC - BUD GC": "Comments FC",
            "LFC GC @ BUD fx": "LFC",
            "FC GC @ BUD fx.1": "FC_",
            "FC - LFC GC @ BUD fx": "Delta to LFC",
            "Comment FC - LFC": "Comments FC changes",
        }
    )
    return df


def convert_currency(df):
    # Change GC to LC
    # DataFrame.select_dtypes method
    df.loc[:, df.select_dtypes(include=[float]).columns] *= bud_fx
    return df


def extract_poc(df):
    # Get Outlet, Plant information from file names using Regex
    df["outlet_plant"] = df["source"].str.extract(r"([0-9\_]{7,9})")
    df["outlet"] = df["outlet_plant"].str.extract(r"([0-9]{3,4})")
    df["plant"] = df["outlet_plant"].str.extract(r"([0-9]{3}$)")
    # Remove columns
    df = df.drop(columns=["source", "outlet_plant"])
    return df


def join_with_poc(df):
    # Plant Outlet Combination
    poc_df = poc()
    df = df.merge(poc_df, on=["outlet", "plant"], how="left")
    # Reorder columns
    head_col = ["division", "bu", "outlet_name",
                "plant_name", "outlet", "plant"]
    df = df[head_col + [col for col in df.columns if col not in head_col]]
    return df


def poc():
    df = pd.read_csv(path / "meta" / "POC.csv", dtype="str").clean_names()
    df = df.drop(columns=["cu", "profit_center"])
    return df


def add_key_column(df):
    # Add key column for look up RCL comments
    df["key"] = df["outlet"] + "_" + \
        df["plant"] + "_" + df["rcl_item_structure"]
    return df


# Output data
df = merge_rcl(xls_files)
df.to_csv(path / "output" / "RCL.csv", index=False)
