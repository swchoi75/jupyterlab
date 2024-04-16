import pandas as pd
import ibis
from ibis import selectors as s
from ibis import _

ibis.options.interactive = True
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


def main():

    # Variables
    version = "\nACT"

    # Path
    data_path = path / "data" / "RACE"

    # Filenames
    input_lc = data_path / "Analysis FS Item Hierarchy for CU 698_LC.xlsx"
    input_gc = data_path / "Analysis FS Item Hierarchy for CU 698_GC.xlsx"
    meta_file = path / "meta" / "New outlet.xlsx"
    output_bs = path / "output" / "RACE Balance sheet.csv"
    output_pnl = path / "output" / "RACE Profit and Loss.csv"

    # Read data
    lc = read_excel_file(input_lc, version)
    gc = read_excel_file(input_gc, version)
    lookup_df = pd.read_excel(meta_file, usecols="A:F", dtype="str").clean_names()

    LC = ibis.memtable(lc, name="LC")
    GC = ibis.memtable(gc, name="GC")
    l = ibis.memtable(lookup_df, name="l")

    # Process data
    LC = LC.mutate(currency="LC")
    LC = LC.projection(["currency"] + [col for col in LC.columns if col != "currency"])

    GC = GC.mutate(currency="GC")
    GC = GC.projection(["currency"] + [col for col in GC.columns if col != "currency"])

    race = LC.union(GC)

    l = l.select("outlet", "division", "bu", "new_outlet", "new_outlet_name")

    joined = race.join(l, "outlet", how="left")
    joined = joined.drop(s.endswith("_right"))

    bs = joined.filter(
        _["financial_statement_item"].startswith("1")
        | _["financial_statement_item"].startswith("2")
    )
    bs = bs.drop(s.matches(r"period_\d+"))

    pnl = joined.filter(
        _.financial_statement_item.startswith("3")
        | _.financial_statement_item.startswith("CO")
    )
    pnl = pnl.drop("period_0", s.matches(r"ytd_\d+"))

    # Write data
    race_bs = bs.to_pandas()
    race_bs.to_csv(output_bs, index=False, na_rep="0")

    race_pnl = pnl.to_pandas()
    race_pnl.to_csv(output_pnl, index=False, na_rep="0")

    print("Files are created")


if __name__ == "__main__":
    main()
