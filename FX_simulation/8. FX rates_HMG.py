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
input_file = path / "data" / "FX Rates" / "HMG 고시환율_20231113.xlsx"
output_file = path / "data" / "fx_rates_HMG.csv"


# Read data
df = pd.read_excel(input_file)
df.info()
df.head()


# Process data
# Filter columns
df = df[["년도", "차수", "화폐", "환율"]]

# Rename columns
df = df.rename(
    columns={
        "년도": "year",
        "차수": "quarter",
        "화폐": "cur",
        "환율": "fx_rates_HMG",
    }
)

# Reorder columns
df = df[["cur", "year", "quarter", "fx_rates_HMG"]]

# Filter rows
df = df[df["cur"].isin(["USD", "EUR", "JPY"])]

# Sort table
df = df.sort_values(by=["cur", "year", "quarter"],
                    ascending=[False, False, True])

# Add Q letter in Quarter column
df["quarter"] = df["quarter"].str.join("Q")


# Write data
df.to_csv(output_file, index=False)
print("A file is created")
