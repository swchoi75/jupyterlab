# Functions
def add_ce_text(df):
    df["ce_text"] = df.apply(helper_add_ce_text, axis="columns")
    return df


def helper_add_ce_text(row):
    # Add account information
    # E01-299
    if row["acc_lv2"] == "299 Total Labor Costs":
        return "10_Compensation"

    # E01-465
    elif (
        row["acc_lv1"] == "345 Depreciation long life"
        or row["acc_lv1"] == "370 Rental/Leasing"
    ):
        return "11_Depreciation & Leasing"
    elif row["acc_lv1"] == "375 Utilities":
        return "12_Energy"
    elif row["acc_lv1"] == "435 Fees and purchased services":
        return "13_Fees and Purchased Services"
    elif row["acc_lv1"] == "320 Purchased maintenance":
        return "15_Maintenance"
    elif (
        row["acc_lv1"] == "430 Entertainment/Travel expense"
        or row["acc_lv1"] == "440 Recruitment/Training/Development"
    ):
        return "16_Travel Training"
    elif row["acc_lv2"] == "465 Cost of materials":
        return "17_Other cost"

    # E01-535
    elif row["gl_accounts"] == "K6620":
        return "18_Services In / Out"
    elif row["gl_accounts"] == "K6626":
        return "19_Transfer out"
    elif (
        row["gl_accounts"] == "K6623"
        or row["gl_accounts"] == "K6624"
        or row["gl_accounts"] == "K6625"
    ):
        return "20_IT Allocation"

    # E01-520
    elif row["acc_lv2"] == "520 Assessments In":
        return "CF cost assessment out"
    else:
        return "NA"
