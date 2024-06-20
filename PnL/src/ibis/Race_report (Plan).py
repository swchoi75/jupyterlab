import pandas as pd
import ibis
from ibis import selectors as s
from ibis import _

# ibis.options.interactive = True
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
def read_excel_file(path, version):
    df = pd.read_excel(path, sheet_name="Query", skiprows=11)

    df = df.astype({"ConsUnit": str, "Plant": str, "Outlet": str})
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
    df = clean_names(df)
    return df


def race_tbl(path_lc, path_gc, version):
    """Read and Combine RACE data"""
    # Read excel file
    df_lc = read_excel_file(path_lc, version)
    df_gc = read_excel_file(path_gc, version)
    # Convert from dataframe to ibis table
    tbl_lc = ibis.memtable(df_lc, name="tbl_lc")
    tbl_gc = ibis.memtable(df_gc, name="tbl_gc")
    # add currency column
    tbl_lc = tbl_lc.mutate(currency="LC")
    tbl_gc = tbl_gc.mutate(currency="GC")
    # combine two tables
    tbl = tbl_lc.union(tbl_gc)
    # reorder columns
    tbl = tbl.projection(
        ["currency"] + [col for col in tbl.columns if col != "currency"]
    )
    return tbl


def outlet_tbl(filename):
    """Read Plant Outlet Combination (=POC)"""
    df = pd.read_excel(filename, usecols="A:F", dtype="str").clean_names()
    tbl = ibis.memtable(df, name="tbl_outlet")
    tbl = tbl.select("outlet", "division", "bu", "new_outlet", "new_outlet_name")
    return tbl


def join_with_outlet(tbl, meta_tbl):
    """Join RACE with POC data"""
    tbl = tbl.join(meta_tbl, "outlet", how="left").drop(s.endswith("_right"))
    return tbl


def profit_and_loss(tbl):
    """Split P&L and Balance sheet"""
    tbl = tbl.filter(
        _.financial_statement_item.startswith("3")
        | _.financial_statement_item.startswith("CO")
    )
    tbl = tbl.drop("period_0", s.matches(r"ytd_\d+"))
    return tbl


def balance_sheet(tbl):
    """Split P&L and Balance sheet"""
    tbl = tbl.filter(
        _["financial_statement_item"].startswith("1")
        | _["financial_statement_item"].startswith("2")
    )
    tbl = tbl.drop(s.matches(r"period_\d+"))
    return tbl


def report_tbl(tbl):
    """Split RACE data into P&L and B/S"""
    race_pnl = profit_and_loss(tbl)
    race_bs = balance_sheet(tbl)
    return race_pnl, race_bs


def main():

    # Variables
    version = "\nPLAN"

    # Filenames
    input_lc = (
        path / "data" / "RACE" / "(Plan) Analysis FS Item Hierarchy for CU 698_LC.xlsx"
    )
    input_gc = (
        path / "data" / "RACE" / "(Plan) Analysis FS Item Hierarchy for CU 698_GC.xlsx"
    )
    input_meta = path / "meta" / "New outlet.xlsx"

    output_bs = path / "output" / "(Plan) RACE Balance sheet.csv"
    output_pnl = path / "output" / "(Plan) RACE Profit and Loss.csv"

    # Read data
    tbl_race = race_tbl(input_lc, input_gc, version)
    tbl_outlet = outlet_tbl(input_meta)

    # Process data
    race_with_outlet = join_with_outlet(tbl_race, tbl_outlet)
    race_pnl, race_bs = report_tbl(race_with_outlet)

    # Write data
    race_pnl.to_pandas().to_csv(output_pnl, index=False, na_rep="0")
    race_bs.to_pandas().to_csv(output_bs, index=False, na_rep="0")
    print("Files are created")


if __name__ == "__main__":
    main()
