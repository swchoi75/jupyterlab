import pandas as pd
from pathlib import Path


# Path
try:
    path = Path(__file__).parent.parent.parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent.parent.parent


# Functions
def read_data(filename):
    df = pd.read_csv(
        filename,
        dtype={
            "m_y_from_": str,
            "sap_plant": str,
            "outlet": str,
            "vendor": str,
            "trading_pr": str,
            "accounts_f": str,
            "document_d": str,
        },
    )
    return df


def delete_last_month(df):

    # last_month = df["M/Y (from-"].iloc[-1] # Find the last row value
    last_month = df["m_y_from_"].max()  # Find the biggest value

    # Filter out last month
    df = df[df["m_y_from_"] != last_month]

    return df


def main():

    # Filenames
    db_file = path / "db" / "ZMPV_2024.csv"

    # Read data
    df = read_data(db_file)

    # Process data
    df = df.pipe(delete_last_month)

    # Write data
    df.to_csv(db_file, index=False)
    print("A files is updated")


if __name__ == "__main__":
    main()
