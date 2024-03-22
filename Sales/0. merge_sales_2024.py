import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
try:
    path = Path(__file__).parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent

data_path = path / "data"


# Filename
output_file = path / "db" / "COPA_Sales_2024.parquet"


# Input data: List of multiple text files
txt_files = [
    file for file in data_path.iterdir() if file.is_file() and file.suffix == ".TXT"
]


# Functions
def read_multiple_files(list_of_files):
    dataframes = [
        pd.read_csv(
            file,
            delimiter="\t",
            skiprows=10,
            encoding="UTF-16LE",
            skipinitialspace=True,
            thousands=",",
            engine="python",
            dtype={
                "Period": str,
                "CoCd": str,
                "Doc. no.": str,
                "Ref.doc.no": str,
                "AC DocumentNo": str,
                "Delivery": str,
                # "Item": int,
                "Plnt": str,
                "Tr.Prt": str,
                "ConsUnit": str,
                "FIRE Plant": str,
                "FIREOutlet": str,
            },
        )
        for file in list_of_files
    ]

    # Merge the list of DataFrames into a single DataFrame
    df = pd.concat(dataframes)

    return df


def remove_first_two_columns(df):
    return df.iloc[:, 2:]


def remove_sub_total_rows(df):
    return df.loc[df["RecordType"].notna()]


def merge_sales(list_of_files):
    df = read_multiple_files(list_of_files)
    df = df.pipe(remove_first_two_columns).pipe(remove_sub_total_rows).pipe(clean_names)
    return df


# Write to Parquet file
df = merge_sales(txt_files)
df.to_parquet(output_file, index=False)
print("A parquet file is created.")
