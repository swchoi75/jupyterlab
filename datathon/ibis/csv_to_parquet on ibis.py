import os
import pandas as pd
import ibis
from ibis import selectors as s
from ibis import _
from janitor import clean_names

ibis.options.interactive = True


# Path
path = "../"
data_path = path + "data/datathon/"
expanded_path = os.path.expanduser(data_path)


# pricing.csv on pandas
df = pd.read_csv(data_path + "pricing.csv")
df.head(3)
df.to_parquet(data_path + "pricing.parquet")


# metrics.csv on pandas
df = pd.read_csv(
    data_path + "metrics.csv",
    parse_dates=[
        "data_timestamp",
        "created_at",
        "updated_at",
        "last_patch",
    ],
    infer_datetime_format=True,
)
df.head(3)
df.to_parquet(data_path + "metrics.parquet")


# department.csv on pandas
df = pd.read_csv(data_path + "department.csv")
df.department = df.department.str.strip()
df.department.unique()
df.head(3)
df.to_parquet(data_path + "department.parquet")


# department.csv on ibis
tbl = ibis.read_csv(expanded_path + "department.csv")
tbl = tbl.mutate(department=_.department.strip())
tbl.group_by("department").aggregate()
tbl.head(3)
tbl.to_pandas().to_parquet(data_path + "department.parquet")


# vgsales.csv on pandas
df = pd.read_csv(data_path + "vgsales.csv").clean_names()

id_cols = [col for col in df.columns if not col.endswith("_sales")]
region_columns = [col for col in df.columns if col.endswith("_sales")]

df = pd.melt(
    df,
    id_vars=id_cols,
    value_vars=region_columns,
    var_name="region",
    value_name="sales",
)
df.region = df.region.str.replace("_sales", "")
df = df[df.region != "global"]
df.region = df.region.str.upper()
df

df.to_parquet(data_path + "vgsales.parquet")


# vgsales.csv on ibis
df = pd.read_csv(data_path + "vgsales.csv").clean_names()

tbl = ibis.memtable(df)
tbl = tbl.pivot_longer(
    s.endswith("_sales"),
    names_to="region",
    values_to="sales",
)
tbl = tbl.mutate(region=_.region.replace("_sales", ""))
tbl = tbl.filter(_.region != "global")
tbl = tbl.mutate(region=_.region.upper())
tbl.head(3)

tbl.to_pandas().to_parquet(data_path + "vgsales.parquet")
