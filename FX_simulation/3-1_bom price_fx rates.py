import pandas as pd
import numpy as np
from pathlib import Path
from janitor import clean_names


# Path
try:
    path = Path(__file__).parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent


# Scenario
vt_fx_type = "ytd"  # fx_type: ytd or spot
anchor_year = "quotation_year"  # anchor_year: quotation_year or sop_year
VT_or_HMG = "VT"  # fx_actual: VT or HMG
scenario_txt = f"fx_bud on {anchor_year} and fx_act from {VT_or_HMG}"
output_file = path / "output" / "1. bom_price_fx rate_delta.csv"


# Filenames
bom_file = path / "data" / "BOM_price.csv"
prj_file = path / "data" / "Sales high runner PN_survey with PSM.xlsx"
fx_bud_file = path / "data" / "fx_rates_VT_budget.csv"
fx_act_file = path / "data" / "fx_rates_VT_actual.csv"
fx_hmg_file = path / "data" / "fx_rates_HMG.csv"


# Read data
bom = pd.read_csv(bom_file)
prj = pd.read_excel(prj_file, sheet_name="PSM entry", skiprows=3).clean_names()
fx_bud = pd.read_csv(fx_bud_file)
fx_vt = pd.read_csv(fx_act_file)
fx_hmg = pd.read_csv(fx_hmg_file)


# Functions
def process_project_year(df):
    # take first 4 letters for year only
    df["sop_year_month"] = df["sop_year_month"].str[:4].astype(int)
    df = df.rename(columns={"sop_year_month": "sop_year"})
    # select columns
    df = df[["product", "quotation_year", "sop_year"]]
    return df


def process_fx_vt(df, fx_type):
    # Filter rows
    df = df[df["fx_type"] == fx_type]  # fx_type: ytd or spot
    # Select columns
    df = df[["cur", "year", "month", "fx_act"]]
    return df


def process_fx_hmg(df):
    # copy column
    df["fx_act"] = df["fx_HMG"]
    # Select columns
    df = df[["cur", "year", "month", "fx_act"]]
    return df


def select_fx_act(fx_vt, fx_hmg, VT_HMG):
    if VT_HMG == "VT":
        df = fx_vt
    elif VT_HMG == "HMG":
        df = fx_hmg
    return df


def process_bom():
    df = pd.merge(
        bom,
        prj_year,
        how="inner",
        on="product",  # dropna=True to exclude GM and KG Mobility
    )
    return df


def process_fx_bud(df, anchor_year):  # anchor_year: quotation_year or sop_year
    df = pd.merge(
        df,
        fx_bud,
        how="left",
        left_on=[anchor_year, "cur"],
        right_on=["plan_year", "cur"],
    ).drop(columns=["plan_year"])
    return df


def process_fx_act(df, fx_act):  # fx_act: fx_act_vt or fx_act_hmg
    df = df.merge(fx_act, how="left", on=["cur", "year"])  # month is exploded
    return df


def krw_month_table():
    currencies = ["KRW"] * 12
    months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
    df = pd.DataFrame({"cur": currencies, "period": months})
    return df


def calc_delta_price(df):
    df["fx_diff_to_bud"] = df["fx_act"] - df["fx_bud"]
    df["delta_price_to_bud_fx"] = df["fx_diff_to_bud"] * df["total_amount_org_cur_"]
    return df


def add_scenario_text(df, text):
    df["scenario"] = text

    return df


# Process data
# Pre-processing
prj_year = process_project_year(prj)
fx_act_vt = process_fx_vt(fx_vt, vt_fx_type)
fx_act_hmg = process_fx_hmg(fx_hmg)
fx_act = select_fx_act(fx_act_vt, fx_act_hmg, VT_or_HMG)


# Join dataframes: bom, project_year, fx_bud, fx_act
df = process_bom()
df = process_fx_bud(df, anchor_year)
df = process_fx_act(df, fx_act)


# Add KRW for 12 months
krw_month = krw_month_table()
df = df.merge(krw_month, how="left", on="cur")
df["month"] = np.where(df["cur"] == "KRW", df["period"], df["month"])
df = df.drop(columns=["period"])


# Set 1 KRW = 1 KRW
df["fx_bud"] = df["fx_bud"].where(df["cur"] != "KRW", 1)
df["fx_act"] = df["fx_act"].where(df["cur"] != "KRW", 1)


df = calc_delta_price(df)
df = add_scenario_text(df, scenario_txt)


# Write data
df.to_csv(output_file, index=False)
print("A file is created")
print(scenario_txt)
