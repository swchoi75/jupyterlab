import pandas as pd


input_file = "data/Actual/COPA_2023_03.TXT"  # Update monthly


def db_append(filename):
    df = read_txt_file(filename)
    df = remove_first_two_columns(df)
    df = remove_sub_total_rows(df)
    return df


def read_txt_file(filename):
    # Read a tab-delimited file
    df = pd.read_csv(
        filename,
        delimiter="\t",
        skiprows=10,
        encoding="UTF-16LE",
        skipinitialspace=True,
        thousands=",",
        engine="python",
        dtype=str,
    )
    return df


def remove_first_two_columns(df):
    return df.iloc[:, 2:]


def remove_sub_total_rows(df):
    return df.loc[df["RecordType"].notna()]


# Write to CSV file
db_append(input_file).to_csv("db/COPA_2023.csv", mode="a", header=False, index=False)
