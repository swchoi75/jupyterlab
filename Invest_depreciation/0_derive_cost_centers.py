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
input_1 = path / "meta_cc" / "IMPR.XLS"
input_2 = path / "meta_cc" / "IMZO.XLS"
input_3 = path / "meta_cc" / "PRPS.XLS"
output_file = path / "meta" / "cost_centers.csv"


# Read data
def read_tsv_file(filename):
    df = pd.read_csv(
        filename,
        sep="\t",
        skiprows=3,
        encoding="UTF-16LE",
        skipinitialspace=True,
        dtype="str",
    )
    return df


df_1 = read_tsv_file(input_1)
df_2 = read_tsv_file(input_2)
df_3 = read_tsv_file(input_3)


# Select columns
df_1 = df_1[["POSID", "POSNR"]]
df_2 = df_2[["POSNR", "OBJNR"]]
df_3 = df_3[["OBJNR", "POSKI", "AKSTL"]]


# Join dataframes
df = df_1.merge(df_2, how="left", on="POSNR").merge(df_3, how="left", on="OBJNR")


# Select and Rename columns
df = df[["POSID", "AKSTL"]]
df = df.rename(columns={"POSID": "sub", "AKSTL": "cost_center"})


# Drop missing values & drop duplicates
df = df.dropna().drop_duplicates(["sub", "cost_center"])


# Business Logic : handle the multiple cost centers
# Group by sub and join the values with a comma
result = (
    df.groupby("sub")["cost_center"]
    .apply(lambda x: ", ".join(x.astype(str)))
    .reset_index()
)


# Write data
result.to_csv(output_file, index=False)
print("A files is created")
