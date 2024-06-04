# Functions
def filter_var_cost(df):
    df = df[df["coom"] == "Var"]
    return df


def filter_fgk_p(df):
    """filter out cost center group FGK-S"""
    df = df.dropna(subset=["lv4"])
    df = df[df["lv4"].str.endswith("FGK-P")]
    return df


def filter_scm(df):
    """filter out cost center group FGK-S"""
    # df = df.dropna(subset=["department"])
    df = df[df["department"].str.endswith("SCM")]
    return df


def add_ldc_mdc(df):
    # Var costs : Add LDC / MDC information
    df["ldc_mdc"] = df.apply(helper_add_ldc_mdc, axis="columns")
    return df


def add_ce_text(df):
    # Var costs : Add CE account information
    df["ce_text"] = df.apply(helper_add_ce_text, axis="columns")
    return df


def helper_add_ldc_mdc(row):
    if row["costctr"][:1] == "8":  # CC that starts with "8"
        return "Start up costs"
    elif row["function_2"] == "FGK":
        if row["acc_lv2"] == "299 Total Labor Costs":
            return "LDC"
        elif row["gl_accounts"] == "K4503":
            return "LDC"
        elif row["acc_lv2"] == "465 Cost of materials":
            return "MDC"
        elif row["gl_accounts"] == "S87413":  # Maintenance technician from 2024
            return "MDC"
        elif row["gl_accounts"] == "S99116":
            return "MDC"

    # "else:" is NOT needed after nested if statement
    return "NA"


def helper_add_ce_text(row):
    if row["gl_accounts"] == "K250" or row["gl_accounts"] == "K256":
        return "120 Premium wages"
    elif row["acc_lv1"] == "158 Social benefit rate wages variable":
        return "158 SLB wages"
    elif row["acc_lv2"] == "299 Total Labor Costs":
        return "115 Direct labor"
    else:
        return row["acc_lv1"]
