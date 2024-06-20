import pandas as pd
import ibis
from ibis import selectors as s
from ibis import _
from pathlib import Path


# Path
path = Path(__file__).parent.parent.parent


# Functions
def read_txt_file(path):
    df = pd.read_csv(path, sep="\t", usecols=[0, 1, 2, 3])
    # rename columns
    df = df.rename(
        columns={
            "Cost center": "text_col",
            "Act": "actual",
            "Plan": "plan",
            "Tgt": "target",
        }
    )
    return df


def extract_text(df):
    """Extract Cost center and GL accounts using regex"""
    df = df.assign(
        costctr=df["text_col"].str.extract(
            r"(^[0-9]{4,5}|^IC-.{4,5}|^CY-.{4,5}|^DUMMY_.{3})"
        ),  # ICH-.{4,5}|
        gl_accounts=df["text_col"].str.extract(r"(^K[0-9]+|^S[0-9]+)"),
    )
    df["costctr"] = df["costctr"].str.strip()
    # Fill in missing values for CostCtr
    df["costctr"] = df["costctr"].bfill()  # .fillna(method="backfill")
    return df


def ibis_wrangling(df, year, month):
    df = (
        df
        # add columns
        .mutate(fy=year, period=month)
        # change data type from string to integer
        .mutate(fy=_.fy.cast("int"), period=_.period.cast("int"))
        # reorder columns
        .select("fy", "period", "costctr", "gl_accounts", "actual", "plan", "target")
        # filter out missing values
        .filter(_.gl_accounts != ibis.NA)
        # filter out rows with Actual, Plan and Target all equal to 0
        .filter(~((_.actual == 0) & (_.plan == 0) & (_.target == 0)))
    )
    return df


def main():

    # Variables
    year = "2024"
    month = "02"  # Monthly to be updated

    # Filenames
    input_file_cf = path / "data" / "CF_2024_02.dat"  # Monthly to be updated
    input_file_pl = path / "data" / "PL_2024_02.dat"  # Monthly to be updated
    db_file_cf = path / "db" / "CF_2024.csv"
    db_file_pl = path / "db" / "PL_2024.csv"

    # ## Add data to Database

    # Read data: Central functions
    df = read_txt_file(input_file_cf)
    df = extract_text(df)
    cf = ibis.memtable(df)

    # Read data:  Productlines
    df = read_txt_file(input_file_pl)
    df = extract_text(df)
    pl = ibis.memtable(df)

    # Process data
    cf = ibis_wrangling(cf, year, month)
    pl = ibis_wrangling(pl, year, month)

    # Output data
    cf.to_pandas().to_csv(db_file_cf, mode="a", header=False, index=False)
    pl.to_pandas().to_csv(db_file_pl, mode="a", header=False, index=False)
    print("Files are updated")


if __name__ == "__main__":
    main()
