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
    return df


def filter_rows(df):
    df = df[df["plant"] == "242"]
    return df


def process_text(df):
    df["source"] = df["source"].str.replace("_2023", "")
    df["source"] = df["source"].str.replace(".xlsx", "")
    df["source"] = df["source"].str.replace("variations_tracking_ICH_", "")
    return df


def poc(filename):
    df = pd.read_csv(filename, dtype="str").clean_names()
    return df


def join_with_poc(df, filename):
    poc_df = poc(filename)
    df_merged = pd.merge(df, poc_df, on=["plant", "outlet"], how="left")
    return df_merged


def variations_item(filename):
    df = pd.read_csv(filename).clean_names()
    return df


def join_with_variations(df, filename):
    variations_df = variations_item(filename)
    df_merged = pd.merge(df, variations_df, on="items", how="left")
    return df_merged


def main():
    # Path
    data_path = path / "data" / "Variations tracking"

    # Filenames
    meta_poc = path / "meta" / "POC.csv"
    meta_file = path / "meta" / "Variations items.csv"
    output_file = path / "output" / "Variations trackings.csv"

    # Input data: List of multiple text files
    xls_files = [
        file
        for file in data_path.iterdir()
        if file.is_file() and file.suffix == ".xlsx"
    ]

    # Read data
    df = read_multiple_files(xls_files)

    # Process data
    df = (
        df.pipe(process_columns)
        .pipe(clean_names)
        .pipe(filter_rows)
        .pipe(process_text)
        .pipe(join_with_poc, meta_poc)
        .pipe(join_with_variations, meta_file)
    )

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
