import duckdb
import pandas as pd


# Path
path = "../data/"
data_path = path + "datathon/"

# Datathon

tbl = duckdb.read_csv(data_path + "pricing.csv")
tbl.write_parquet(data_path + "pricing.parquet")


tbl = duckdb.read_csv(data_path + "metrics.csv",
                      timestamp_format="%m/%d/%Y %-H:%M")
tbl.write_parquet(data_path + "metrics.parquet")


tb1 = duckdb.read_csv(data_path + "department.csv", header=True)
tb2 = duckdb.sql("SELECT id, trim(department) AS department FROM tb1")
df = tb2.fetchdf()
df["department"].unique()
tb2.write_parquet(data_path + "department.parquet")


# Video Game Sales

con = duckdb.connect(database=":memory:")
con.sql(
    """CREATE TABLE vgsales as SELECT * FROM
         read_csv_auto('../data/datathon/vgsales.csv')"""
)
con.sql(
    """
        CREATE TABLE vgsales_long AS
        UNPIVOT vgsales
        ON NA_Sales, EU_Sales, JP_Sales, Other_Sales, Global_Sales
        INTO
            NAME Region
            VALUE Sales;
        """
)
tbl = con.sql("SELECT * FROM vgsales_long")
tbl.write_parquet(data_path + "vgsales.parquet")
