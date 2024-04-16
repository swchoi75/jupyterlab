import pandas as pd
import re
from pathlib import Path
from janitor import clean_names


# Path
try:
    path = Path(__file__).parent.parent.parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent.parent.parent


# Functions
def read_excel_file(filename, version):
    """Read RACE data"""
    df = pd.read_excel(filename, sheet_name="Query", skiprows=11)
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
    df = df.rename(columns=lambda x: re.sub(version, "", x))
    # clean column names
    df = clean_names(df)
    return df


def process_currency(df, lc_gc):
    """Add currency column"""
    df = df.assign(currency=lc_gc)
    df = df[["currency"] + [col for col in df.columns if col != "currency"]]
    return df


def outlet_df(path_meta):
    """Read Plant Outlet Combination (=POC)"""
    df = pd.read_excel(path_meta, usecols="A:F", dtype="str")
    df = clean_names(df)
    df = df.drop(columns=["outlet_name"])
    # select columns
    col_poc = ["division", "bu", "new_outlet", "new_outlet_name"]
    df = df[["outlet"] + col_poc]
    return df


def join_with_outlet(df, meta_df):
    """Join RACE with POC data"""
    df = df.merge(meta_df, on="outlet", how="left")
    return df


def profit_and_loss(df):
    """Split P&L and Balance sheet"""
    cols_to_drop = ["period_0"] + [f"ytd_{i}" for i in range(13)]
    df = df.drop(cols_to_drop, axis=1)
    df = df.loc[df["financial_statement_item"].str.contains("^3|^CO")]
    return df


def balance_sheet(df):
    """Split P&L and Balance sheet"""
    cols_to_drop = [f"period_{i}" for i in range(13)]
    df = df.drop(cols_to_drop, axis=1)
    df = df.loc[df["financial_statement_item"].str.contains("^1|^2")]
    return df


def race_df(path_lc, path_gc, version):
    """Read and Combine RACE data"""
    lc = read_excel_file(path_lc, version)
    lc = process_currency(lc, "LC")

    gc = read_excel_file(path_gc, version)
    gc = process_currency(gc, "GC")

    race = pd.concat([lc, gc])
    return race


def report_df(df):
    """Split RACE data into P&L and B/S"""
    race_pnl = profit_and_loss(df)
    race_bs = balance_sheet(df)
    return race_pnl, race_bs


def main():
    # Variable
    version = "\nPLAN"

    # Filenames
    input_lc = path / "data" / "(Plan) Analysis FS Item Hierarchy for CU 698_LC.xlsx"
    input_gc = path / "data" / "(Plan) Analysis FS Item Hierarchy for CU 698_GC.xlsx"
    input_meta = path / "meta" / "New outlet.xlsx"

    output_pnl = path / "output" / "(Plan) RACE Profit and Loss.csv"
    output_bs = path / "output" / "(Plan) RACE Balance sheet.csv"

    # Process data
    race = race_df(input_lc, input_gc, version)
    outlet = outlet_df(input_meta)
    race_with_outlet = join_with_outlet(race, outlet)
    race_pnl, race_bs = report_df(race_with_outlet)

    # Output data
    race_pnl.to_csv(output_pnl, index=False, na_rep="0")
    race_bs.to_csv(output_bs, index=False, na_rep="0")
    print("Files are created")


if __name__ == "__main__":
    main()
