import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent.parent


# Functions
def read_multiple_files(list_of_files):
    dataframes = [
        pd.read_excel(
            file,
            sheet_name="Seasonalized Variations",
            skiprows=15,
            nrows=240,
            usecols="A:V",
            # thousands=",",
            # dtype="str",
            dtype={
                "Unnamed: 0": str,  # Plant
                "Unnamed: 1": str,  # Outlet
            },
        ).assign(source=file)
        for file in list_of_files
    ]

    # Add a new column with filename to each DataFrame
    for i, df in enumerate(dataframes):
        df["source"] = list_of_files[i].stem

    # Merge the list of DataFrames into a single DataFrame
    df = pd.concat(dataframes)

    # reorder columns
    df = df[["source"] + [col for col in df.columns if col not in ["source"]]]
    return df


def read_poc(filename):
    df = pd.read_csv(filename, dtype="str")  # .clean_names()
    return df


def read_category_item(filename):
    df = pd.read_csv(filename, dtype="str")  # .clean_names()
    return df


def process_columns(df):
    # clean column names
    # Remove "\n" in the column names
    df.columns = df.columns.str.replace("\n", " ")
    # remove columns
    df = df.drop(columns=["Unnamed: 2", "Unnamed: 3", "Unnamed: 4"])
    # rename columns
    df = df.rename(
        columns={
            "Unnamed: 0": "Plant",
            "Unnamed: 1": "Outlet",
            "Unnamed: 5": "Items",
        }
    )
    # df = df.clean_names()

    return df


def filter_rows(df):
    df = df[df["Plant"] == "242"]
    return df


def process_text(df):
    df["source"] = df["source"].str.extract(r"(CW\d{2})")
    return df


def join_with_poc(df, df_poc):
    df_merged = pd.merge(df, df_poc, on=["Plant", "Outlet"], how="left")
    return df_merged


def join_with_category(df, df_category):
    df_merged = pd.merge(df, df_category, on="Items", how="left")
    return df_merged


def main():

    # Path
    data_path = path / "data" / "Variations tracking"

    # Filenames
    meta_poc = path / "meta" / "POC.csv"
    meta_category = path / "meta" / "Variations items.csv"
    output_file = path / "output" / "Variations trackings.csv"

    # Input data: List of multiple text files
    xls_files = [
        file
        for file in data_path.iterdir()
        if file.is_file() and file.suffix == ".xlsx"
    ]

    # Read data
    df = read_multiple_files(xls_files)
    df_poc = read_poc(meta_poc)
    df_category = read_category_item(meta_category)

    # Process data
    df = (
        df.pipe(process_columns)
        .pipe(filter_rows)
        .pipe(process_text)
        .pipe(join_with_poc, df_poc)
        .pipe(join_with_category, df_category)
    )

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
