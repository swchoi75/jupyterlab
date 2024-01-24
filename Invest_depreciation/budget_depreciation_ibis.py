# In Terminal, "pip install ibis-framework[duckdb] pyjanitor"
import pandas as pd
import ibis
from ibis import selectors as s
from ibis import _
ibis.options.interactive = True

from pathlib import Path
from janitor import clean_names


# Path
try:
    path = Path(__file__).parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent


# Filenames
input_file = path / "bud_data" / "Budget 2024 Investment and depreciation all_20230913 v2.xlsx"
output_1 = path / "bud_output" / "investment_depreciation.csv"
output_2 = path / "bud_output" / "investment_depreciation_monthly.csv"


# Read excel sheet data and return pandas dataframe and ibis table
df = pd.read_excel(
    input_file,
    sheet_name="Asset ledger by Jul. acquis",
    skiprows=10,
    usecols="A:AD",
).clean_names()
# df = df.astype({"asset":str, "ending_date":str, "asset_class":str, "asset_no":str, "new_cc":str, "new_cc_1":str,
#                 "p_o":str, "vendor":str})
df.acquisitio = df.acquisitio.str.replace(".", "-").astype("datetime64[ns]")
df.spending_date = df.spending_date.str.replace(".", "-").astype("datetime64[ns]")
df.odep_start = df.odep_start.str.replace(".", "-").astype("datetime64[ns]")
df.info()


# Convert pandas dataframe to ibis table
tbl = ibis.memtable(df)
tbl.head(3)


# Change data type from Unknown to String
tbl = tbl.cast(
    {
        "asset": str,
        "asset_class": str,
        "asset_no": str,
        "new_cc": str,
        "new_cc_1": str,
        "p_o": str,
        "vendor": str,
        "con": int,
        "con_p": int,
        "kor": int,
        "sie": int,
        "_acqusition": int,
        "_previous": int,
        "_current": int,
        "_total": int,
        "_book_value": int,
    }
)
tbl.head(3)


# Remove ".0" in string data
tbl = tbl.mutate(
    asset=_.asset.replace(".0", ""),
    asset_class=_.asset_class.replace(".0", ""),
    new_cc=_.new_cc.replace(".0", ""),
    asset_no=_.asset_no.replace(".0", ""),
)
tbl.head(3)


# Remove unnecessary columns
columns_to_exclude = [
    "ending_date",
    "cost_elem_",
    "new_cc_1",
    "check",
    "wbs",
    "project",
    "sub_no",
    "vendor",
    "vendor_name",
    "p_o",
]
selected_columns = [col for col in tbl.columns if col not in columns_to_exclude]
tbl = tbl.select(selected_columns)
tbl.head(3)


depr = tbl.group_by(["category", "new_profit_center", "new_cc"]).aggregate(
    _._current.sum()
)
depr.head(3)


# Output data
depr.to_pandas().to_csv(output_1, index=False)


# Monthly calculation
df_month_ends = pd.DataFrame(
    {"months": pd.date_range("2024-01-31", "2025-01-01", freq="M")}
)
df_month_ends.head(3)


month_ends = ibis.memtable(df_month_ends)
month_ends.head(3)


# Cross Join two tables
t = month_ends.join(tbl, how="cross")
t.head(3)


t = t.mutate(
    current_depr=_._acqusition / _.con / 12,
    depr_start=_.acquisitio,
    # depr_end=_.acquisitio + _.con * 12 * 30,
)
t.head(3)


# Output data
t.to_pandas().to_csv(output_2, index=False)
print("Files are created")
