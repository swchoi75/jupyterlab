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


# Write data
df.to_csv(output_file, index=False)
print("A file is created")
