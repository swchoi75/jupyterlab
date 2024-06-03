import pandas as pd
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Functions
def concat_data_files(list_of_files):
    """Define a function to read multiple data files and concatenate them into a single DataFrame"""

    # Use list comprehension to read .dat files into a list of DataFrames
    dataframes = [
        pd.read_csv(file, sep="\t", usecols=[0, 1, 2, 3]) for file in list_of_files
    ]
    # Add a new column with filename to each DataFrame
    for i, df in enumerate(dataframes):
        df["source"] = list_of_files[i].stem

    # Merge the list of DataFrames into a single DataFrame
    df = pd.concat(dataframes)

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


def extract_cost_center(df):
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


def extract_year_month(df):
    """Extract Cost center group and Year / Month using regex"""

    df["cc_group"] = df["source"].str.extract(r"(CF|PL)")

    # create FY and Period columns
    df["date"] = df["source"].str.extract(r"([0-9]{4}_[0-9]{2})")
    df["date"] = pd.to_datetime(df["date"], format="%Y_%m")

    df["fy"] = df["date"].dt.year
    df["period"] = df["date"].dt.month

    return df


def filter_columns(df):
    # filter out missing values
    df = df[~df["gl_accounts"].isna()]
    # Filter out rows with Actual, Plan and Target all equal to 0
    df = df[~((df["actual"] == 0) & (df["plan"] == 0) & (df["target"] == 0))]
    return df


def reorder_columns(df):
    df = df[
        [
            # "source",
            "cc_group",
            # "date",
            "fy",
            "period",
            "costctr",
            "gl_accounts",
            "actual",
            "plan",
            "target",
        ]
    ]
    return df


def main():

    # path
    data_path = path / "data"

    # Filnames
    output_file_cf = path / "db" / "CF_2024.csv"
    output_file_pl = path / "db" / "PL_2024.csv"

    # List of multiple files
    dat_files = [
        file for file in data_path.iterdir() if file.is_file() and file.suffix == ".dat"
    ]

    # Read data
    df = concat_data_files(dat_files)

    # Process data
    df = (
        df.pipe(extract_cost_center)
        .pipe(extract_year_month)
        .pipe(filter_columns)
        .pipe(reorder_columns)
    )
    df_cf = df[df["cc_group"] == "CF"].drop(columns="cc_group")
    df_pl = df[df["cc_group"] == "PL"].drop(columns="cc_group")

    # Write data
    df_cf.to_csv(output_file_cf, index=False)
    df_pl.to_csv(output_file_pl, index=False)
    print("Files are created")


if __name__ == "__main__":
    main()
