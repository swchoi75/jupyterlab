import pandas as pd
import numpy as np
from janitor import clean_names


# Functions
def read_db(path):
    # Read main data
    df = pd.read_csv(path, dtype="str")
    df = df.astype(
        {
            "actual": float,
            "plan": float,
            "target": float,
        }
    )
    df = df[~df["gl_accounts"].isna()]
    return df


def remove_unnecessary_columns(df):
    return df.drop(
        columns=[
            "validity",
            "responsible",
            "account_description",
            "acc_lv1_by_consolidated",
            "acc_lv3",
            "acc_lv4",
            "acc_lv5",
            "acc_lv6",
        ]
    )


def process_numeric_columns(df):
    # Change sign logic, unit in k KRW, Add a new column
    df[["actual", "plan", "target"]] = (
        df[["actual", "plan", "target"]].astype(float) / -1000
    )
    df["delta_to_plan"] = (df["actual"] - df["plan"]).round(3)
    return df


# Read master data
def read_master_data(filename):
    df = pd.read_csv(filename, dtype="str")
    df = clean_names(df)
    return df


def master_cc(df_cc_general, df_cc_hierarchy):
    df = df_cc_general.merge(df_cc_hierarchy, on="cctr", how="left")
    # df = clean_names(df)
    return df


def master_coom(filename):
    df = pd.read_csv(filename, dtype="str", usecols=[0, 1, 2])
    df = clean_names(df)
    return df


def process_master_data_1(df, df_cc, df_acc):
    df = df.merge(df_cc, left_on="costctr", right_on="cctr", how="left")
    df = df.merge(df_acc, left_on="gl_accounts", right_on="account_no_", how="left")
    df = df.drop(columns=["cctr", "account_no_"])
    return df


def process_master_data_2(df, df_cc, df_acc, df_coom, df_poc):
    df = df.merge(df_cc, left_on="costctr", right_on="cctr", how="left").drop(
        columns=["cctr"]
    )
    df = df.merge(
        df_acc, left_on="gl_accounts", right_on="account_no_", how="left"
    ).drop(columns=["account_no_"])
    df = df.merge(
        df_coom,
        left_on=["costctr", "gl_accounts"],
        right_on=["cctr", "account_no_"],
        how="left",
    ).drop(columns=["cctr", "account_no_"])
    df = df.merge(df_poc, left_on="pctr", right_on="profit_center", how="left").drop(
        columns=["pctr"]
    )

    # df = df.rename(columns={"pctr": "profit_center"})
    df = df[~df["profit_center"].isna()]

    return df


def add_vol_diff(df):
    df["volume_difference"] = round(df["plan"] - df["target"], 3)
    # df.insert(8, "Volume difference", vol_diff)
    return df


def split_fix_var(df):
    # Process COOM data for fix and variable costs
    df["coom"] = np.where(
        (df["fix_var"] == "Var") & (df["gl_accounts"] == "K399"), "Var", df["coom"]
    )
    df["coom"] = np.where(df["coom"].isna(), "Fix", df["coom"])
    return df


def get_cc_function(df):
    df["function_2"] = df["lv3"].str.split("-").str[1]
    return df


def remove_s90xxx_accounts(df):
    return df.query("`acc_lv6` != 'Assessments to COPA'")
