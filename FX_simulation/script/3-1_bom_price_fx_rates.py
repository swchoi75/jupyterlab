import sys
import pandas as pd
import numpy as np
from pathlib import Path
from janitor import clean_names
from enum import Enum


# Path
try:
    path = Path(__file__).parent.parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent.parent


# FX Scenario
try:
    fx_scenario_option = int(sys.argv[1])
except:
    fx_scenario_option = 1  # 1 to 8


# Filenames
bom_file = path / "data" / "BOM_price.csv"
mm_file = path / "meta" / "Representative PN_Material_Master.xlsx"
prj_file = path / "meta" / "Sales high runner PN_survey with PSM.xlsx"

fx_vt_plan_file = path / "data" / "fx_rates_VT_plan.csv"
fx_vt_act_file = path / "data" / "fx_rates_VT_actual.csv"
fx_hmg_act_file = path / "data" / "fx_rates_HMG_actual.csv"
fx_hmg_plan_file = path / "data" / "fx_rates_HMG_plan.csv"


# Read data
bom = pd.read_csv(bom_file)
mm = (
    pd.read_excel(mm_file)
    .clean_names()[["material", "product_hierachy"]]
    .rename(columns={"material": "product", "product_hierachy": "product_hierarchy"})
)
prj = pd.read_excel(prj_file, sheet_name="PSM entry", skiprows=3).clean_names()

fx_vt_plan = pd.read_csv(fx_vt_plan_file)
fx_vt_act = pd.read_csv(fx_vt_act_file)
fx_hmg_act = pd.read_csv(fx_hmg_act_file)
fx_hmg_plan = pd.read_csv(fx_hmg_plan_file)


class Option(Enum):
    ONE = 1
    TWO = 2
    THREE = 3
    FOUR = 4
    FIVE = 5
    SIX = 6
    SEVEN = 7
    EIGHT = 8


# Functions
def fx_scenario(option: Option) -> tuple:
    vt_fx_type = "ytd"  # fx_type: ytd or spot

    options = {
        Option.ONE: ("quotation_year", "VT", "VT", "1. bom_price_fx rate_delta.csv"),
        Option.TWO: ("quotation_year", "VT", "HMG", "2. bom_price_fx rate_delta.csv"),
        Option.THREE: ("sop_year", "VT", "VT", "3. bom_price_fx rate_delta.csv"),
        Option.FOUR: ("sop_year", "VT", "HMG", "4. bom_price_fx rate_delta.csv"),
        Option.FIVE: ("quotation_year", "HMG", "VT", "5. bom_price_fx rate_delta.csv"),
        Option.SIX: ("quotation_year", "HMG", "HMG", "6. bom_price_fx rate_delta.csv"),
        Option.SEVEN: ("sop_year", "HMG", "VT", "7. bom_price_fx rate_delta.csv"),
        Option.EIGHT: ("sop_year", "HMG", "HMG", "8. bom_price_fx rate_delta.csv"),
    }

    anchor_year, plan_fx_source, act_fx_source, output_file = options[option]
    output_file = Path("output") / output_file

    scenario_txt = f"Plan FX rate on {anchor_year} from {plan_fx_source} and Actual FX rate from {act_fx_source}"

    return (
        vt_fx_type,
        anchor_year,
        plan_fx_source,
        act_fx_source,
        scenario_txt,
        output_file,
    )


def process_project_year(df):
    # take first 4 letters for year only
    df["sop_year_month"] = df["sop_year_month"].str[:4].astype(int)
    df = df.rename(columns={"sop_year_month": "sop_year"})
    # select columns
    df = df[["product_hierarchy", "quotation_year", "sop_year"]]
    # drop duplicates
    df = df.drop_duplicates(subset="product_hierarchy")
    return df


def process_fx_vt_act(df, fx_type):
    # Filter rows
    df = df[df["fx_type"] == fx_type]  # fx_type: ytd or spot
    # Select columns
    df = df[["cur", "year", "month", "fx_act"]]
    return df


def process_fx_hmg_act(df):
    # copy column
    df["fx_act"] = df["fx_HMG"]
    # Select columns
    df = df[["cur", "year", "month", "fx_act"]]
    return df


def select_fx_plan(fx_vt_plan, fx_hmg_plan, plan_fx_source):
    if plan_fx_source == "VT":
        df = fx_vt_plan
    elif plan_fx_source == "HMG":
        df = fx_hmg_plan
    return df


def select_fx_act(fx_vt_act, fx_hmg_act, act_fx_source):
    if act_fx_source == "VT":
        df = fx_vt_act
    elif act_fx_source == "HMG":
        df = fx_hmg_act
    return df


def process_bom():
    df = pd.merge(
        bom,
        mm,
        how="inner",
        on="product",  # dropna=True
    )
    df = df.merge(
        prj_year,
        how="inner",
        on="product_hierarchy",  # dropna=True to exclude GM and KG Mobility
    )
    return df


def process_fx_plan(df, fx_plan, anchor_year):
    df = pd.merge(
        df,
        fx_plan,
        how="left",
        # anchor_year: quotation_year or sop_year
        left_on=[anchor_year, "cur"],
        right_on=["plan_year", "cur"],
    ).drop(columns=["plan_year"])
    return df


def process_fx_act(df, fx_act):  # fx_act: fx_act_vt or fx_act_hmg
    df = df.merge(fx_act, how="left", on=["cur", "year"])  # month is exploded
    return df


def krw_month_table():
    currencies = ["KRW"] * 12
    months = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"]
    df = pd.DataFrame({"cur": currencies, "period": months})
    return df


def calc_delta_price(df):
    df["fx_diff_to_plan"] = df["fx_act"] - df["fx_plan"]
    df["delta_price_to_plan_fx"] = df["fx_diff_to_plan"] * df["total_amount_org_cur_"]
    return df


def add_col_fx_scenario(df, text, anchor_year, plan_fx_source, act_fx_source):
    df["fx_scenario"] = text
    df["plan_fx_on"] = anchor_year
    df["plan_fx_from"] = plan_fx_source
    df["actual_fx_from"] = act_fx_source
    return df


# Process data
# Pre-processing
(
    vt_fx_type,
    anchor_year,
    plan_fx_source,
    act_fx_source,
    scenario_txt,
    output_file,
) = fx_scenario(Option(fx_scenario_option))

prj_year = process_project_year(prj)
fx_vt_act = process_fx_vt_act(fx_vt_act, vt_fx_type)
fx_hmg_act = process_fx_hmg_act(fx_hmg_act)
fx_plan = select_fx_plan(fx_vt_plan, fx_hmg_plan, plan_fx_source)
fx_act = select_fx_act(fx_vt_act, fx_hmg_act, act_fx_source)


# Join dataframes: bom, project_year, fx_plan, fx_act
df = process_bom()
df = process_fx_plan(df, fx_plan, anchor_year)
df = process_fx_act(df, fx_act)


# Add KRW for 12 months
krw_month = krw_month_table()
df = df.merge(krw_month, how="left", on="cur")
df["month"] = np.where(df["cur"] == "KRW", df["period"], df["month"])
df["month"] = df["month"].fillna(0).astype(int)
df = df.drop(columns=["period"])


# Set 1 KRW = 1 KRW
df["fx_plan"] = df["fx_plan"].where(df["cur"] != "KRW", 1)
df["fx_act"] = df["fx_act"].where(df["cur"] != "KRW", 1)


df = calc_delta_price(df)
df = add_col_fx_scenario(df, scenario_txt, anchor_year, plan_fx_source, act_fx_source)


# Write data
df.to_csv(output_file, index=False)
print("A file is created")
print(scenario_txt)
