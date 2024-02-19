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
meta_file = path / "meta" / "POC.csv"
output_file = path / "fc_output" / "fc_depreciation_combined.csv"


# Read data
df_1 = pd.read_csv(input_1)
df_2 = pd.read_csv(input_2)
df_meta = pd.read_csv(meta_file).clean_names()


# Add source column
df_1["source"] = "GPA"
df_2["source"] = "SAP"


# Add value
df_2["category"] = "existing"


# Rename columns
df_meta = df_meta.rename(
    columns={
        "outlet": "fire_outlet",
        "plant": "fire_plant_receiver",
        "plant_name": "location_receiver",
    }
)
df_meta["location_receiver"] = df_meta["location_receiver"].str.replace("ICH ", "")


# Add meta data
df_1 = df_1.merge(
    df_meta, how="left", on=["fire_outlet", "fire_plant_receiver", "location_receiver"]
)
df_2 = df_2.merge(df_meta, how="left", on="profit_center")


# Combine data
df = pd.concat([df_1, df_2])


# Reorder columns
df = df[["source"] + [col for col in df.columns if col not in ["source"]]]


# Write data
df.to_csv(output_file, index=False)
print("A files is created")
