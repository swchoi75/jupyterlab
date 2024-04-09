# Functions
def add_useful_life_year(row):
    if row["fix_var"] == "fix":
        return 8
    elif row["fix_var"] == "var":
        return 4


def add_responsibilities(row):
    # define variables
    outlet_cf = ["Central Functions"]
    outlet_pl1 = ["PL ENC", "PL DTC", "PL MTC", "PL VBC"]
    outlet_pl2 = [
        "PL HVD",
        "PL EAC",
        "PL DAC E",
        "PL MES",
        "PL HYD",
        "PL CM CCN",
        "PL CM CVS",
        "PL CM PSS",
    ]
    outlet_rnd = ["DIV E Eng.", "PL DAC", "PL MES"]
    outlet_shs = [
        "DIV E C",
        "DIV P C",
        "PT - Quality",
        "PT - DFP(frm.Div.Fun",
        "Central Group Functions",
    ]
    # If conditions
    if row["outlet_sender"] in outlet_cf:
        return "Central Functions"
    elif (row["location_sender"] == "Icheon") & (row["outlet_sender"] in outlet_pl1):
        return "Productlines_1"
    elif (row["location_sender"] == "Icheon") & (row["outlet_sender"] in outlet_pl2):
        return "Productlines_2"
    elif row["location_sender"] == "Sejong":  # CM Inbound
        return "Productlines_2"
    elif (row["location_sender"] == "Icheon NPF") & (
        row["outlet_sender"] in outlet_rnd
    ):
        return "R&D"
    elif (row["location_sender"] == "Icheon NPF") & (
        row["outlet_sender"] in outlet_shs
    ):
        return "Share Service"
