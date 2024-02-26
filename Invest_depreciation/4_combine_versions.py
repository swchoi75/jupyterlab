import pandas as pd
from pathlib import Path


# Path
try:
    path = Path(__file__).parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent


# Filenames
input_1 = path / "fc_output" / "fc_depreciation_future_assets.csv"
input_2 = path / "fc_output" / "fc_depreciation_existing_assets.csv"
output_file = path / "fc_output" / "fc_depreciation_combined.csv"


# Read data
df_1 = pd.read_csv(
    input_1,
    dtype={
        "investment_type": str,
        "financial_statement_item": str,
    },
)
df_2 = pd.read_csv(
    input_2,
    dtype={
        "asset_class": str,
        "cost_center": str,
        "asset_no": str,
        "sub_no": str,
    },
)


# Add source column
df_1["source"] = "GPA"
df_2["source"] = "SAP"


# Add new columns
def add_mv_type(row):
    if row["financial_statement_item"] == "122637000":  # Molds / containers / tooling
        return "211 Depr. of tools"
    else:
        return "210 Depr./amortiz."


df_1["mv_type"] = df_1.apply(add_mv_type, axis="columns")
df_2["asset_category"] = "existing"


# Combine two dataframes
df = pd.concat([df_1, df_2])


# Businss logic: Add a new column
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


df["responsibilities"] = df.apply(add_responsibilities, axis="columns")


# Reorder columns
df = df[
    ["source", "responsibilities"]
    + [col for col in df.columns if col not in ["source", "responsibilities"]]
]


# Write data
df.to_csv(output_file, index=False)
print("A file is created")
