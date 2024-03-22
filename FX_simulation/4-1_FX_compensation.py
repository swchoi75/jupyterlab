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
input_file = path / "data" / "P3 flat-data form(YTD).csv"
output_file = path / "output" / "react_fx_compensation.csv"


# Read FX Compensation by year / product_hierarchy
def read_data(filename):
    # Read data
    df = pd.read_csv(filename).clean_names()
    # Filter data
    df = df[(df["attribute"] == "FX Compensation")]
    df = df[(df["version"] == "Actual")]
    # Select columns
    df = df[["attribute", "version", "year", "product_hierarchy", "value"]]
    return df


df = read_data(input_file)


# Summarize the data by Pivot table
df = (
    df.pivot_table(
        index=["version", "year", "product_hierarchy"],
        columns=["attribute"],
        values="value",
    )
    .reset_index()
    .rename(columns={"FX Compensation": "fx_compensation"})
)


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
df = (
    df.groupby(["year", "product_hierarchy"])
    .agg({"fx_compensation": "sum"})
    .reset_index()
)
df = df.pipe(process_financial_year).pipe(combine_id_cols, two_id_columns)


# Write data
df.to_csv(output_file, index=False)
print("A file is created")
