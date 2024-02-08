import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
try:
    path = Path(__file__).parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent


# Variables
current_year = "2024"
current_year_end = pd.to_datetime(current_year + "-12-31")


# Filenames
input_file = path / "bud_output" / "bud_acquisition_future_assets.csv"
output_file = path / "bud_output" / "bud_adjustment_future_assets.csv"


# Read data
df = pd.read_csv(
    input_file,
    dtype={
        "outlet_receiver": str,
        "fire_outlet": str,
        "fire_outlet_ny_receiver": str,
        "fire_plant_receiver": str,
        "investment_type": str,
    },
    parse_dates=["acquisition_date", "PPAP", "start_of_depr"],
)


# # Adjustment by PPAP date # #


# Business Logic: Asset Under Construction if PPAP is in the future year #
def reclassfy_fs_item(df):
    ppap_future_year = df["PPAP"] > current_year_end

    df["financial_statement_item"] = df["financial_statement_item"].where(
        ~ppap_future_year, "122632000"
    )

    df["fs_item_description"] = df["fs_item_description"].where(
        ~ppap_future_year, "Assets under construction and advances to suppliers"
    )
    return df


df = reclassfy_fs_item(df)


# Write data
df.to_csv(output_file, index=False)
print("A file is created")
