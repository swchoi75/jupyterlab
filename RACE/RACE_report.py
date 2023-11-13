import pandas as pd
import re
from pathlib import Path
from janitor import clean_names


# Path
# path = Path('datasets/home-dataset/data/')
path = Path(__file__).parent


# Functions
def read_excel_file(filename):
    df = pd.read_excel(
        filename,
        sheet_name="Query",
        skiprows=11,
        # dtype={"ConsUnit": str, "Plant": str, "Outlet": str},
    )
    # change data type
    df = df.astype({"ConsUnit": str, "Plant": str, "Outlet": str})

    # change column names
    df = df.rename(
        columns={
            "Unnamed: 1": "FS item description",
            "Unnamed: 3": "CU name",
            "Unnamed: 5": "Plant name",
            "Unnamed: 7": "Outlet name",
            "YTD - 1": "YTD PM",  # PM means Previous Month
        }
    )
    df = df.rename(columns=lambda x: re.sub("\nACT", "", x))
    # clean column names
    df = clean_names(df)
    return df


def process_currency(df, lc_gc):
    df = df.assign(currency=lc_gc)
    df = df[["currency"] + [col for col in df.columns if col != "currency"]]
    return df


def outlet():
    # POC
    col_poc = ["division", "bu", "new_outlet", "new_outlet_name"]

    df = pd.read_excel(path / "meta" / "New outlet.xlsx", usecols="A:F", dtype="str")
    df = clean_names(df)
    df = df.drop(columns=["outlet_name"])
    df = df[["outlet"] + col_poc]
    return df


def join_with_outlet(df):
    outlet_df = outlet()
    df = df.merge(outlet_df, on="outlet", how="left")
    return df


# Split P&L and Balance sheet
def profit_and_loss(df):
    cols_to_drop = ["period_0"] + [f"ytd_{i}" for i in range(13)]
    df = df.drop(cols_to_drop, axis=1)
    df = df.loc[df["financial_statement_item"].str.contains("^3|^CO")]
    return df


def balance_sheet(df):
    cols_to_drop = [f"period_{i}" for i in range(13)]
    df = df.drop(cols_to_drop, axis=1)
    df = df.loc[df["financial_statement_item"].str.contains("^1|^2")]
    return df


# Input data
path_lc = path / "data" / "Analysis FS Item Hierarchy for CU 698_LC.xlsx"
path_gc = path / "data" / "Analysis FS Item Hierarchy for CU 698_GC.xlsx"


# Combine data
lc = read_excel_file(path_lc)
lc = process_currency(lc, "LC")

gc = read_excel_file(path_gc)
gc = process_currency(gc, "GC")

race = pd.concat([lc, gc])
race = join_with_outlet(race)


# Split data
race_pnl = profit_and_loss(race)
race_bs = balance_sheet(race)


# Output data
race_pnl.to_csv(path / "output" / "RACE Profit and Loss.csv", index=False, na_rep="0")
race_bs.to_csv(path / "output" / "RACE Balance sheet.csv", index=False, na_rep="0")
