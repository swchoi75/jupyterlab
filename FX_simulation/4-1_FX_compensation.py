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
input_file = path / "data" / "React - FX compensation.csv"
output_file = path / "output" / "react_fx_compensation.csv"


# Read data
df = pd.read_csv(input_file).clean_names()


# Process data
df = df.groupby("product_hierarchy").agg({"react_fx": "sum"}).reset_index()


# Write data
df.to_csv(output_file, index=False)
print("A file is created")
