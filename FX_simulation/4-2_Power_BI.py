import pandas as pd
from pathlib import Path


# Path
try:
    path = Path(__file__).parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent


# Filenames
input_file_1 = path / "data" / "Sales with representative PN.csv"
input_file_2 = path / "data" / "fx_rates_HMG.csv"

output_file_1 = path / "output" / "power_bi_dimensions.csv"
output_file_2 = path / "output" / "fx_rates_HMG_by_Quarter.csv"


# Read data
df_1 = pd.read_csv(input_file_1)
df_2 = pd.read_csv(input_file_2)


# Process data
# Aggregate data
selected_columns = ["productline", "product_hierarchy", "PH_description"]
df_1 = df_1[selected_columns].groupby(selected_columns).first().reset_index()
# Filter data
df_1 = df_1[df_1["productline"].isin(["PL DTC", "PL ENC", "PL MTC"])]


# Aggregate data
selected_columns = ["cur", "year", "quarter", "fx_HMG"]
df_2 = df_2[selected_columns].groupby(selected_columns).first().reset_index()


# Write data
df_1.to_csv(output_file_1, index=False)
df_2.to_csv(output_file_2, index=False)
print("files are created")
