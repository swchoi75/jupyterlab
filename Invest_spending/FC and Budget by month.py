import pandas as pd
import numpy as np
from pathlib import Path
from janitor import clean_names


# Path
path = Path.cwd()


# Functions
def read_excel_file(path):
    df = pd.read_excel(path, sheet_name="Sheet1")
    df = clean_names(df)
    df = df.dropna(subset=["values"])  # Remove missing rows
    df["items"] = df["items"].str.replace("\n", " ")  # Remove "\n"
    return df


def rename_columns(df):
    df = df.rename(
        columns={
            "location_receiver_": "location",
            "location_receiver_1": "location_key",
            "master": "master",
            "master_1": "master_id",
            # To match with janitor in R
            "division_receiver_": "division_receiver",
            "bu_receiver_": "bu_receiver",
            "product_line_rec_": "product_line_rec",
            "outlet_receiver_": "outlet_receiver",
        }
    )
    return df


def add_version(df):
    df["version"] = df["items"].str.extract("(plan|FC|Actual)")
    df["version"] = df["version"].str.replace("plan", "Budget")
    df = df.sort_values("version", ascending=False)
    df = df[["version"] + [col for col in df.columns if col != "version"]]
    return df


def add_month(df):
    df["month"] = df["items"].str.extract(
        "(Jan|Feb|Mar|Apr|May|June|July|Aug|Sep|Oct|Nov|Dec)"
    )
    df = df.dropna(subset=["month"])
    return df


def add_quarter(df):
    months_quarters = {
        "Jan": "Q1",
        "Feb": "Q1",
        "Mar": "Q1",
        "Apr": "Q2",
        "May": "Q2",
        "June": "Q2",
        "July": "Q3",
        "Aug": "Q3",
        "Sep": "Q3",
        "Oct": "Q4",
        "Nov": "Q4",
        "Dec": "Q4",
    }
    df["quarter"] = df["month"].map(months_quarters)
    return df


def process_numeric_columns(df):
    df = df.rename(columns={"values": "k_lc"})
    df["k_eur"] = np.where(
        df["version"] == "Budget",
        np.round(df["k_lc"] / budget_fx, 3),
        np.round(df["k_lc"] / fc_fx, 3),
    )
    df["k_eur_at_budget_fx"] = np.round(df["k_lc"] / budget_fx, 3)
    return df


def join_top_15_projects(df):
    df = df.merge(top, on="master_id", how="left")
    df["category"] = np.where(df["category"].isna(),
                              "Other Projects", df["category"])
    return df


def main(df):
    df = (
        df
        .pipe(rename_columns)
        .pipe(add_version)
        .pipe(add_month)
        .pipe(add_quarter)
        .pipe(process_numeric_columns)
        .pipe(join_top_15_projects)
    )
    return df


# Budget FX rate in 2023
budget_fx = 1329
fc_fx = 1411.92300  # YTD October P&L rate (KRW / EUR)

# fc_version = "FC10+2"


# Input data
fc_path = path / "data" / "GFW_ICH_V378 FC10+2_KRW.xlsx"
bud_path = path / "data" / "GFW_ICH_V359 Budget 2023_KRW.xlsx"
top = pd.read_csv(path / "data" / "top 15 projects.csv",
                  usecols=[0, 1]).clean_names()


# Process data
fc = read_excel_file(fc_path)
bud = read_excel_file(bud_path)

fc_bud = pd.concat([fc, bud], axis="rows")

df = main(fc_bud)

# Output data
df.to_csv(path / "output" / "Monthly Spending FC10+2.csv", index=False)
