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
df_1 = pd.read_csv(
    input_1,
    sep="\t",
    skiprows=3,
    encoding="UTF-16LE",
    skipinitialspace=True,
    dtype="str",
)
df_2 = pd.read_csv(
    input_2,
    sep="\t",
    skiprows=3,
    encoding="UTF-16LE",
    skipinitialspace=True,
    dtype="str",
)
df_3 = pd.read_csv(
    input_3,
    sep="\t",
    skiprows=3,
    encoding="UTF-16LE",
    skipinitialspace=True,
    dtype="str",
)


# Select columns
df_1 = df_1[["POSID", "POSNR"]]
df_2 = df_2[["POSNR", "OBJNR"]]
df_3 = df_3[["OBJNR", "POSKI", "AKSTL"]]


# Join dataframes
df = df_1.merge(df_2, how="left", on="POSNR").merge(df_3, how="left", on="OBJNR")


# Select and Rename columns
df = df[["POSID", "AKSTL"]]
df = df.rename(columns={"POSID": "sub", "AKSTL": "cost_center"})


# Aggregate data
df = df.groupby(["sub", "cost_center"]).first().reset_index()


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
