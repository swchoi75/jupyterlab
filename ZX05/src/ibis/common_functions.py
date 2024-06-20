import pandas as pd
import ibis
from ibis import selectors as s
from ibis import _


# Functions
def read_data(filename):
    df = pd.read_csv(filename, sep="\t").clean_names()
    df = df.rename(columns={"cost_center": "text_col"})
    return df


def extract_text(df):
    """Extract Cost center and GL accounts using regex"""
    df = df.assign(
        costctr=df["text_col"].str.extract(
            r"(^[0-9]{4,5}|^IC-.{4,5}|^CY-.{4,5}|^DUMMY_.{3})"
        ),  # ICH-.{4,5}|
        gl_accounts=df["text_col"].str.extract(r"(^K[0-9]+|^S[0-9]+)"),
    )
    df["costctr"] = df["costctr"].str.strip()
    # Fill in missing values for CostCtr
    df["costctr"] = df["costctr"].bfill()  # .fillna(method="backfill")
    return df


def process_num_cols(tbl):
    """Process numeric columns"""
    tbl = (
        tbl.mutate(
            fc_tot=_.fc_1
            + _.fc_2
            + _.fc_3
            + _.fc_4
            + _.fc_5
            + _.fc_6
            + _.fc_7
            + _.fc_8
            + _.fc_9
            + _.fc_10
            + _.fc_11
            + _.fc_12
        )
        .mutate(s.across(s.numeric(), _ / -1000))
        .mutate(delta_to_plan=(_.fc_tot - _.plan).round(3))
    )
    return tbl


def remove_unnecessary_cols(tbl):
    tbl = tbl.drop(
        "validity",
        "responsible",
        "account_description",
        "acc_lv1_by_consolidated",
        "acc_lv3",
        "acc_lv4",
        "acc_lv5",
        "acc_lv6",
    )
    return tbl


def add_col_coom(tbl):
    """Process COOM data for fix and variable costs"""
    tbl = tbl.mutate(
        coom=ibis.case()
        .when((tbl.fix_var == "Var") & (tbl.gl_accounts == "K399"), "Var")
        .when(tbl.coom == ibis.NA, "Fix")
        .when(True, tbl.coom)
        .end()
    )
    return tbl


def add_col_function_2(tbl):
    """Extract function from cost center hierarchy level 3"""
    tbl = tbl.mutate(function_2=tbl.lv3.split("-")[1])
    return tbl


def filter_pl_fix(tbl):
    tbl = (
        tbl.filter(tbl.coom == "Fix")
        # remove_s90xxx_accounts
        .filter(tbl.acc_lv6 != "Assessments to COPA")
    )
    return tbl


def filter_pl_var(tbl):
    tbl = (
        tbl.filter(tbl.coom == "Var")
        # remove_s90xxx_accounts
        .filter(tbl.acc_lv6 != "Assessments to COPA")
    )
    return tbl


def add_race_item(tbl):
    tbl = tbl.mutate(
        race_item=ibis.case()
        .when(tbl.function_2 == "FGK", "PE production")
        .when(tbl.function_2 == "MGK", "PE materials management")
        .when(tbl.function_2 == "WVK", "PE plant administration")
        .when(tbl.function_2 == "VK", "PE distribution")
        # ALLOC
        .when(
            (tbl.function_2 == "ALLOC") & (tbl.gl_accounts == "K66271"),
            "PE production",
        )
        .when(
            (tbl.function_2 == "ALLOC") & (tbl.gl_accounts == "K66270"),
            "PE materials management",
        )
        .when(
            (tbl.function_2 == "ALLOC") & (tbl.gl_accounts == "K66281"),
            "R, D & E allocation in",
        )
        .when(
            (tbl.function_2 == "ALLOC") & (tbl.gl_accounts == "K66283"),
            "R, D & E allocation in",
        )
        .when((tbl.function_2 == "ALLOC") & (tbl.gl_accounts == "K66273"), "PE selling")
        .when(
            (tbl.function_2 == "ALLOC") & (tbl.gl_accounts == "K66275"),
            "PE communication",
        )
        .when(
            (tbl.function_2 == "ALLOC") & (tbl.gl_accounts == "K66278"),
            "F, G & A expenses",
        )
        .when(
            (tbl.function_2 == "ALLOC") & (tbl.gl_accounts == "S87564"),
            "F, G & A expenses",
        )
        .when(True, "NA")
        .end()
    )
    return tbl


def add_pl_acc_info(tbl):
    """Add account information"""
    tbl = tbl.mutate(
        ce_text=ibis.case()
        # PV Costs : special logic for Division P in 2023 (temporary)
        .when((tbl.function == "PV") & (tbl.gl_accounts == "S99116"), "12_PMME Others")
        # PV Costs
        .when(tbl.function == "PV", "10_Product Validation / Requalification after G60")
        .when(
            tbl.costctr[:2] == "58", "10_Product Validation / Requalification after G60"
        )
        # E01-585
        .when(
            (tbl.gl_accounts == "K66270") | (tbl.gl_accounts == "K66271"),
            "01_NSHS Allocations in PE MGK & PE FGK",
        )
        .when(tbl.gl_accounts == "K66280", "02_NSHS Services in PE MGK & PE FGK")
        # E01-299
        .when(tbl.acc_lv2 == "299 Total Labor Costs", "06_Compensation")
        # E01-465
        .when(
            tbl.gl_accounts == "K403",
            "08_PMME Depreciation intangible development assets",
        )
        .when(
            tbl.acc_lv1 == "345 Depreciation long life",
            "09_PMME Depreciation w/o intangible",
        )
        .when(tbl.acc_lv1 == "320 Purchased maintenance", "07_Maintenance")
        .when(tbl.acc_lv1 == "325 Project costs", "11_Related project expenses (RPE)")
        # E01-520 / 525
        # FSC costs changed from PMME to FG&A from FY2023
        .when(tbl.gl_accounts == "S87564", "Assessment from FSC (CDP) to FG&A")
        # FF Assessment from FY2023 for QMPP reorganization
        .when(tbl.gl_accounts == "S87310", "04_Assessment from FF (520)")
        # Normal case
        .when(
            tbl.acc_lv2 == "520 Assessments In",
            "03_Assessment from Central Functions (520)",
        )
        .when(
            tbl.acc_lv2 == "525 Residual Costs",
            "03_Assessment from Central Functions (520)",
        )  # CM specific topic from 2024
        # E01-535
        .when(tbl.gl_accounts == "K6626", '05_Shared equipment "K662x" accounts')
        .when(tbl.gl_accounts == "K6620", "12_PMME Others_US regident Q engineer")
        # E01-630
        # S99xxx accounts for te-minute, tgb-minute, ast-hours
        # .when(pl.gl_accounts[:3] == "S99", "12_PMME Others")
        .when(True, "12_PMME Others")
        .end()
    )
    return tbl


def add_cf_acc_info(tbl):
    """Add account information"""
    tbl = tbl.mutate(
        ce_text=ibis.case()
        # E01-299
        .when(tbl.acc_lv2 == "299 Total Labor Costs", "10_Compensation")
        # E01-465
        .when(
            (tbl.acc_lv1 == "345 Depreciation long life")
            | (tbl.acc_lv1 == "370 Rental/Leasing"),
            "11_Depreciation & Leasing",
        )
        .when(tbl.acc_lv1 == "375 Utilities", "12_Energy")
        .when(
            tbl.acc_lv1 == "435 Fees and purchased services",
            "13_Fees and Purchased Services",
        )
        .when(tbl.acc_lv1 == "320 Purchased maintenance", "15_Maintenance")
        .when(
            (tbl.acc_lv1 == "430 Entertainment/Travel expense")
            | (tbl.acc_lv1 == "440 Recruitment/Training/Development"),
            "16_Travel Training",
        )
        .when(tbl.acc_lv2 == "465 Cost of materials", "17_Other cost")
        # E01-535
        .when(tbl.gl_accounts == "K6620", "18_Services In / Out")
        .when(tbl.gl_accounts == "K6626", "19_Transfer out")
        .when(
            (tbl.gl_accounts == "K6623")
            | (tbl.gl_accounts == "K6624")
            | (tbl.gl_accounts == "K6625"),
            "20_IT Allocation",
        )
        # E01-520
        .when(tbl.acc_lv2 == "520 Assessments In", "CF cost assessment out")
        .when(True, "NA")
        .end()
    )
    return tbl
