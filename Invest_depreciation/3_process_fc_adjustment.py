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
input_file = path / "fc_output" / "fc_acquisition_future_assets.csv"
output_file = path / "fc_output" / "fc_adjustment_future_assets.csv"


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


# Melt the dataframe
value_columns = df.columns[df.columns.str.contains("spend")].tolist()
key_columns = [col for col in df.columns if col not in value_columns]

df = df.melt(
    id_vars=key_columns,
    value_vars=value_columns,
    var_name="spend_month",
    value_name="spend_amt",
)

# df = df.where(df["spend_amt"] != 0, np.nan)  # turn 0 into n/a values
df = df.dropna(subset="spend_amt")  # remove rows with n/a values


# Write data
df.to_csv(output_file, index=False)
print("A file is created")
