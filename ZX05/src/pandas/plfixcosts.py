# Functions
def filter_fix_costs(df):
    return df[df["coom"] == "Fix"]


def add_ce_text(df):
    # Fix costs : Add CE account information
    df["ce_text"] = df.apply(helper_add_ce_text, axis="columns")
    return df


def add_race_item(df):
    # Fix costs: Add RACE account information
    df["race_item"] = df.apply(helper_add_race_item, axis="columns")
    return df


def helper_add_ce_text(row):
    # PV Costs : special logic for Division P in 2023 (temporary)
    if row["function"] == "PV" and row["gl_accounts"] == "S99116":
        return "12_PMME Others"

    # PV Costs
    elif row["function"] == "PV":
        return "10_Product Validation / Requalification after G60"
    elif row["costctr"][:2] == "58":
        return "10_Product Validation / Requalification after G60"

    # E01-585
    elif row["gl_accounts"] == "K66270" or row["gl_accounts"] == "K66271":
        return "01_NSHS Allocations in PE MGK & PE FGK"
    elif row["gl_accounts"] == "K66280":
        return "02_NSHS Services in PE MGK & PE FGK"

    # E01-299
    elif row["acc_lv2"] == "299 Total Labor Costs":
        return "06_Compensation"

    # E01-465
    elif row["gl_accounts"] == "K403":
        return "08_PMME Depreciation intangible development assets"
    elif row["acc_lv1"] == "345 Depreciation long life":
        return "09_PMME Depreciation w/o intangible"
    elif row["acc_lv1"] == "320 Purchased maintenance":
        return "07_Maintenance"
    elif row["acc_lv1"] == "325 Project costs":
        return "11_Related project expenses (RPE)"

    # E01-520 / 525
    # FSC costs changed from PMME to FG&A from FY2023
    elif row["gl_accounts"] == "S87564":
        return "Assessment from FSC (CDP) to FG&A"
    # FF Assessment from FY2023 for QMPP reorganization
    elif row["gl_accounts"] == "S87310":
        return "04_Assessment from FF (520)"
    # Normal case
    elif row["acc_lv2"] == "520 Assessments In":
        return "03_Assessment from Central Functions (520)"
    # CM specific topic from 2024
    elif row["acc_lv2"] == "525 Residual Costs":
        return "03_Assessment from Central Functions (520)"

    # E01-535
    elif row["gl_accounts"] == "K6626":
        return '05_Shared equipment "K662x" accounts'
    elif row["gl_accounts"] == "K6620":
        return "12_PMME Others_US regident Q engineer"

    # E01-630
    # S99xxx accounts for te-minute, tgb-minute, ast-hours
    # elif row["GL_accounts"][:3] == "S99":
    #     return "12_PMME Others"

    else:
        return "12_PMME Others"


def helper_add_race_item(row):
    if row["function_2"] == "FGK":
        return "PE production"
    elif row["function_2"] == "MGK":
        return "PE materials management"
    elif row["function_2"] == "WVK":
        return "PE plant administration"
    elif row["function_2"] == "VK":
        return "PE distribution"

    # ALLOC
    elif row["function_2"] == "ALLOC":
        if row["gl_accounts"] == "S87564":  # FSC changed from PMME to FG&A ini 2023
            return "F, G & A expenses"
        elif row["gl_accounts"] == "K66271":
            return "PE production"
        elif row["gl_accounts"] == "K66270":
            return "PE materials management"
        elif row["gl_accounts"] == "K66273":
            return "PE selling"
        elif row["gl_accounts"] == "K66275":
            return "PE communication"
        elif row["gl_accounts"] == "K66278":
            return "F, G & A expenses"
        elif row["gl_accounts"] == "K66281" or row["gl_accounts"] == "K66283":
            return "R, D & E allocation in"

    # "else:" is NOT needed after nested if statement
    return "NA"
