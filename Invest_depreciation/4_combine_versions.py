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
meta_file = path / "meta" / "POC_for_GPA.xlsx"
output_file = path / "fc_output" / "fc_depreciation_combined.csv"


# Read data
df_1 = pd.read_csv(
    input_1,
    dtype={
        "outlet_receiver": str,
        "fire_outlet": str,
        "fire_outlet_ny_receiver": str,
        "fire_plant_receiver": str,
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
df_meta = pd.read_excel(
    meta_file,
    sheet_name="Sheet1",
    dtype={
        "cu": str,
        "plant": str,
        "outlet": str,
    },
)
df_meta["plant_name"] = df_meta["plant_name"].str.replace("ICH ", "")


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


# Add meta data to GPA data
df_meta_1 = df_meta[["outlet", "bu", "division", "plant_name", "profit_center"]]
df_meta_1 = df_meta_1.rename(
    columns={
        "outlet": "outlet_receiver",
        "plant_name": "location_receiver",
    }
)
df_1 = df_1.merge(df_meta_1, how="left", on=["outlet_receiver", "location_receiver"])


# Add meta data to SAP data on profit_center
df_meta_2 = df_meta[["outlet_name", "profit_center"]]
df_meta_2 = df_meta_2.rename(columns={"outlet_name": "outlet_sender"})
df_2 = df_2.merge(df_meta_2, how="left", on="profit_center")


# Add meta data to SAP data on rec_prctr
df_2["rec_prctr"].fillna(df_2["profit_center"], inplace=True)

df_meta_3 = df_meta[
    ["plant", "outlet", "bu", "division", "plant_name", "profit_center"]
]
df_meta_3 = df_meta_3.rename(
    columns={
        "outlet": "outlet_receiver",
        "plant": "fire_plant_receiver",
        "plant_name": "location_receiver",
        "profit_center": "rec_prctr",
    }
)
df_2 = df_2.merge(df_meta_3, how="left", on="rec_prctr")


# Add profit center information to GPA data
def add_missing_value(row):
    if row["outlet_sender"] == "Central Functions":
        return "50899-999"  # Central Function Icheon
    else:
        return row["profit_center"]


df_1["rec_prctr"] = df_1["profit_center"]
df_1["profit_center"] = df_1.apply(add_missing_value, axis="columns")


# Add outlet information to SAP data
df_2["fire_outlet"] = df_2["outlet_receiver"]
df_2["fire_outlet_ny_receiver"] = df_2["outlet_receiver"]


# Businss logic: Combine data
df = pd.concat([df_1, df_2])


# Businss logic: Add a new column
def add_responsibilities(row):
    # define variable
    outlet_cf = ["Central Functions"]
    outlet_pl1 = ["PL ENC", "PL DTC", "PL MTC", "PL VBC"]
    outlet_pl2 = [
        "PL HVD",
        "PL EAC",
        "PL DAC",
        "PL MES",
        "PL HYD",
        "PL CM CCN",
        "PL CM CVS",
        "PL CM PSS",
    ]
    outlet_rnd = ["DIV E Eng.", "PL DAC", "PL MES"]
    outlet_shs = ["DIV E C", "DIV P C", "PT - Quality"]
    # If conditions
    if row["outlet_sender"] in outlet_cf:
        return "Central Functions"
    elif (row["location_receiver"] == "Icheon") & (row["outlet_sender"] in outlet_pl1):
        return "Productlines_1"
    elif (row["location_receiver"] == "Icheon") & (row["outlet_sender"] in outlet_pl2):
        return "Productlines_2"
    elif (row["location_receiver"] == "Icheon NPF") & (
        row["outlet_sender"] in outlet_rnd
    ):
        return "R&D"
    elif (row["location_receiver"] == "Icheon NPF") & (
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
print("A files is created")
