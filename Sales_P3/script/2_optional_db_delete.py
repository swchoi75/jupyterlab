import pandas as pd

db_path = "db/COPA_2023.csv"


def db_delete(path):
    # Read a data file in CSV format
    df = pd.read_csv(path)

    # last_month = df["Period"].iloc[-1] # Find the last row
    last_month = df["Period"].max()  # Find the biggest value

    # Filter out last month
    df = df[df["Period"] != last_month]

    return df


# Write a data file in CSV format


df_filtered = db_delete(db_path)
df_filtered.to_csv(db_path, index=False)
