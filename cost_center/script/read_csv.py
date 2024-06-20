import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent


# Filenames
input_file = path / "data" / "0004_TABLE_OUTPUT_Cctr report common.csv"
output_file = path / "output" / "cost_center_report.csv"


# Read data
# df = pd.read_excel(excel_file)
df = pd.read_csv(input_file, dtype={"Cctr": str}).clean_names()


# Process data


# Write data
df.to_csv(output_file, index=False)
