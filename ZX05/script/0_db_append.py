import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent


# Functions
def read_txt_file(path):
    df = pd.read_csv(path, sep="\t", usecols=[0, 1, 2, 3])
    # select columns
    df = df.loc[:, ["Cost center", "Act", "Plan", "Tgt"]]
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


def create_columns(df, year, month):
    # Extract Cost center and GL accounts using regex
    df = df.assign(
        costctr=df["text_col"].str.extract(
            r"(^[0-9]{4,5}|^IC-.{4,5}|^CY-.{4,5}|^DUMMY_.{3})"
        ),  # ICH-.{4,5}|
        gl_accounts=df["text_col"].str.extract(r"(^K[0-9]+|^S[0-9]+)"),
    )
    df["costctr"] = df["costctr"].str.strip()
    # Fill in missing values for CostCtr
    df["costctr"] = df["costctr"].bfill()  # .fillna(method="backfill")

    # Add FY and Period columns
    df["fy"] = year
    df["period"] = month
    # Specify data types
    df = df.astype(
        {
            "fy": int,
            "period": int,
        }
    )
    return df


def filter_columns(df):
    # filter out missing values
    df = df[~df["gl_accounts"].isna()]
    # Filter out rows with Actual, Plan and Target all equal to 0
    df = df[~((df["actual"] == 0) & (df["plan"] == 0) & (df["target"] == 0))]
    return df


def reorder_columns(df):
    df = df[["fy", "period", "costctr", "gl_accounts", "actual", "plan", "target"]]
    return df


def main():

    # Variables
    year = "2023"
    month = "10"  # Monthly to be updated

    # Filnames
    input_file_cf = path / "data" / "CF_2023_10.dat"  # Monthly to be updated
    input_file_pl = path / "data" / "PL_2023_10.dat"  # Monthly to be updated

    output_file_cf = path / "db" / "CF_2023.csv"
    output_file_pl = path / "db" / "PL_2023.csv"

    # Read data
    df_cf = read_txt_file(input_file_cf)
    df_pl = read_txt_file(input_file_pl)

    # Process data
    df_cf = (
        df_cf.pipe(create_columns, year, month)
        .pipe(filter_columns)
        .pipe(reorder_columns)
    )
    df_pl = (
        df_pl.pipe(create_columns, year, month)
        .pipe(filter_columns)
        .pipe(reorder_columns)
    )

    # Write data
    df_cf.to_csv(output_file_cf, mode="a", header=False, index=False)
    df_pl.to_csv(output_file_pl, mode="a", header=False, index=False)
    print("Files are updated")


if __name__ == "__main__":
    main()
