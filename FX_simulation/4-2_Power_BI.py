import pandas as pd
from pathlib import Path


# Path
try:
    path = Path(__file__).parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent


# Filenames
input_file = path / "output" / "sales with bom costs.csv"
output_file = path / "output" / "power_bi_dimensions.csv"


# Read data
df = pd.read_csv(input_file)


# Process data
# Aggregate data
selected_columns = ["productline", "product_hierarchy", "PH_description"]
df = df[selected_columns].groupby(selected_columns).first().reset_index()
# Filter data
df = df[df["productline"].isin(["PL DTC", "PL ENC", "PL MTC"])]


# Write data
df.to_csv(output_file, index=False)
print("A file is created")
