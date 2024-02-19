import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
try:
    path = Path(__file__).parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent


# Filenames
input_1 = path / "fc_output" / "fc_depreciation_future_assets.csv"
input_2 = path / "fc_output" / "fc_depreciation_existing_assets.csv"
meta_file = path / "meta" / "POC_for_GPA.csv"
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
df_meta = pd.read_csv(
    meta_file,
    dtype={
        "CU": str,
        "Plant": str,
        "Outlet": str,
    },
).clean_names()


# Add source column
df_1["source"] = "GPA"
df_2["source"] = "SAP"


# Add values
def add_mv_type(row):
    if row["financial_statement_item"] == "122637000":  # Molds / containers / tooling
        return "211 Depr. of tools"
    else:
        return "210 Depr./amortiz."


df_1["mv_type"] = df_1.apply(add_mv_type, axis="columns")
df_2["category"] = "existing"


# Rename columns
df_meta = df_meta.rename(
    columns={
        "outlet": "fire_outlet",
        "outlet_name": "outlet_sender",
        "plant": "fire_plant_receiver",
        "plant_name": "location_receiver",
    }
)
df_meta["location_receiver"] = df_meta["location_receiver"].str.replace("ICH ", "")


# Add meta data
df_meta_1 = df_meta.drop(columns=["fire_outlet", "fire_plant_receiver"])
df_1 = df_1.merge(df_meta_1, how="left", on=["outlet_sender", "location_receiver"])
df_2 = df_2.merge(df_meta, how="left", on="profit_center")


# Add missing value
def add_missing_value(row):
    if row["outlet_sender"] == "Central Functions":
        return "50899-999"  # Central Function Icheon
    elif row["outlet_sender"] == "PT - Quality":
        return "50803-320"  # Central Group Functions Icheon NPF
    else:
        return row["profit_center"]


df_1["profit_center"] = df_1.apply(add_missing_value, axis="columns")


# Combine data
df = pd.concat([df_1, df_2])


# Reorder columns
df = df[["source"] + [col for col in df.columns if col not in ["source"]]]


# Write data
df.to_csv(output_file, index=False)
print("A files is created")
