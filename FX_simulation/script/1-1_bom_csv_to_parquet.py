import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
try:
    path = Path(__file__).parent.parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent.parent


# Functions
def read_txt_file(filename):
    df = pd.read_csv(
        filename,
        delimiter="\t",
        skiprows=3,
        encoding="UTF-16LE",
        skipinitialspace=True,
        thousands=",",
        engine="python",
        dtype={
            "DV": str,
            # "Quota": int,
            "Amount(Doc)": float,
            "Total Amount(Org. CUR)": float,
            "Total Amount(Locl)": float,
            "Exchange Rate": float,
            "Vendor ID": str,
            "Planned price 1": float,
            "Planned price 2": float,
        },
    )
    return df


def remove_fist_two_cols(df):
    return df.iloc[:, 2:]


def remove_subtotal_rows(df):
    return df.dropna(subset="product")


def add_year_column(df, year):
    # Add year column
    df.loc[:, "year"] = year
    return df


def reorder_columns(df):
    return df[["year"] + [col for col in df.columns if col not in ["year"]]]


def main():

    # Filenames
    year = "2024"
    input_file = path / "data" / "BOM" / f"BOM_{year}.txt"
    output_file = path / "data" / "BOM" / f"BOM_{year}.parquet"

    # read_data
    df = read_txt_file(input_file)

    # Process data
    df = (
        df.pipe(remove_fist_two_cols)
        .pipe(clean_names)
        .pipe(remove_subtotal_rows)
        .pipe(add_year_column, year)
        .pipe(reorder_columns)
    )

    # Write data
    df.to_parquet(output_file)
    print("A parquet file is created")


if __name__ == "__main__":
    main()
