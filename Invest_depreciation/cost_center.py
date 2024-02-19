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
input_1 = path / "meta_cc" / "IMPR.xlsx"
input_2 = path / "meta_cc" / "IMZO.xlsx"
input_3 = path / "meta_cc" / "PRPS.xlsx"
output_file = path / "meta" / "cost_centers.csv"


# Read data
df_1 = pd.read_excel(
    input_1,
    sheet_name="Sheet1",
    skiprows=3,
)
df_2 = pd.read_excel(
    input_2,
    sheet_name="Sheet1",
    skiprows=3,
)
df_3 = pd.read_excel(
    input_3,
    sheet_name="Sheet1",
    skiprows=3,
)

# Write data
df.to_csv(output_file, index=False)
print("A files is created")
