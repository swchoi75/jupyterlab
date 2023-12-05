import pandas as pd
from pathlib import Path


# Path
try:
    path = Path(__file__).parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent


# Filenames
bom_file = path / "output" / "bom_price_fx rate_delta.csv"
sales_file = path / "data" / "Sales with representative PN.csv"
output_file = path / "output" / "output.csv"



# Read data
bom = pd.read_csv(bom_file)
sales = pd.read_csv(sales_file)


# Functions


# Process data



# Write data
df.to_csv(output_file, index=False)
print("A file is created")
