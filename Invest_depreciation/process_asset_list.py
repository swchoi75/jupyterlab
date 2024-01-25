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
input_file = path / "fc_data" / "2023-11_Asset History Leger_20231130.xlsx"
output_file = path / "fc_output" / "fc_depreciation_existing_assets.csv"


# Read data
df = pd.read_excel(
    input_file,
    sheet_name="Asset ledger 1130",
    header=3,
    usecols="C:U",
    dtype={
        "Asset Clas": str,
        "Cost Cente": str,
        "Asset no": str,
        "Sub No": str,
    },
    parse_dates=["Acquisitio", "ODep.Start"],
)


# Functions to clean column names
def clean_preceding_underscore(column_name):
    return column_name.lstrip("_")


# Apply the cleaning function to all column names
df = df.clean_names()
df.columns = df.columns.map(clean_preceding_underscore)


# Select columns
selected_columns = [
    "asset_clas",
    "cost_cente",
    "acquisitio",
    "odep_start",
    # "":"",
    # "":"",
]

# df = df[selected_columns]


# Rename columns
df = df.rename(
    columns={
        "asset_clas": "asset_class",
        "cost_cente": "cost_center",
        "acquisitio": "acquisition_date",
        "odep_start":"start_of_depr",
        # "":"",
        # "":"",
    }
)


# Filter out missing or zero value
df = df.dropna(subset="asset_class")


# Write data
df.to_csv(output_file, index=False)
print("A file is created")
