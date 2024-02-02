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


# Functions
def process_financial_year(df):
    # correct SAP year to Financial Year
    df["year"] = df["year"] - 1
    df = df.rename(columns={"year": "fy"})
    # rename year values
    df["fy"] = "Act " + df["fy"].astype(str)
    df["fy"] = df["fy"].str.replace("Act 2024", "YTD Act 2024")
    return df


def combine_id_cols(df, two_id_columns):
    # concatenate columns
    df["key_id"] = df[two_id_columns[0]] + "_" + df[two_id_columns[1]]
    # reorder columns
    df = df[["key_id"] + [col for col in df.columns if col not in ["key_id"]]]
    return df


# # Process data #
two_id_columns = ["fy", "product_hierarchy"]
df = df.groupby(["product_hierarchy", "year"]).agg({"react_fx": "sum"}).reset_index()
df = df.pipe(process_financial_year).pipe(combine_id_cols, two_id_columns)


# Write data
df.to_csv(output_file, index=False)
print("A file is created")
