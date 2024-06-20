import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
try:
    path = Path(__file__).parent.parent.parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent.parent.parent


# Functions
def read_txt_file(path):
    # Read a tab-delimited file

    return pd.read_csv(
        path,
        delimiter="\t",
        skiprows=6,
        encoding="UTF-16LE",
        skipinitialspace=True,
        thousands=",",
        # engine='python'  # Error occurs
        dtype={
            "M/Y (from-": str,
            "SAP Plant": str,
            "Outlet": str,
            "Vendor": str,
            "Trading Pr": str,
            "Accounts f": str,
            "Document d": str,
        },
    )


def remove_col_row(df):
    # Remove first two columns and sub-total rows
    df = df.drop(columns=["Unnamed: 0", "Unnamed: 1"])
    df = df.dropna(subset="Profit Cen")
    return df


def rename_cols(df):
    new_columns = {
        "Net PPV...23": "Net PPV",
        "Net PM_PPV...24": "Net PPV ratio",
        "STD Other...39": "STD Other",
        "STD Other...40": "STD Other 2",
    }
    return df.rename(columns=new_columns)


def main():

    # Filenames
    input_file = path / "data" / "ZMPV_2024_01.txt"
    db_file = path / "db" / "ZMPV_2024.csv"

    # Read data
    df = read_txt_file(input_file)

    # Process data
    df = df.pipe(remove_col_row).pipe(rename_cols).pipe(clean_names)

    # Write data
    df.to_csv(db_file, mode="a", header=False, index=False)
    print("A files is updated")


if __name__ == "__main__":
    main()
